from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from app.core.utils import get_now
import copy
import math
from pathlib import Path
import re
from typing import Any, Optional
import unicodedata
from urllib.parse import quote

from app.repositories.production_repository import ProductionRepository
from app.schemas.production import (
    ProductionAlertsResponse,
    ProductionAlertItem,
    ProductionItem,
    ProductionOverviewResponse,
    ProductionOverviewAlert,
    ProductionSummaryStat,
    ProductionRegistrationRequest,
    ProductionRegistrationHistoryItem,
    ProductionRegistrationHistoryResponse,
    ProductionRegistrationSummaryResponse,
    ProductionRegistrationResponse,
    ProductionRegistrationFormItem,
    ProductionRegistrationFormResponse,
    ProductionSimulationRequest,
    ProductionSimulationResponse,
    SimulationChartPoint,
    SimulationSummaryMetrics,
    GetProductionSkuListResponse,
    ProductionSkuItem,
    ProductionSkuDetailResponse,
    ProductionSkuDecision,
    Pagination,
    InventoryStatusItem,
    InventoryStatusResponse,
    WasteItem,
    WasteSummaryResponse,
    FifoLotItem,
    FifoLotSummaryResponse,
)
import logging

from app.services.audit_service import AuditService
from app.services.ai_client import AIServiceClient
from app.services.explainability_service import create_ready_payload
from app.core.ttl_cache import TTLMemoryCache

logger = logging.getLogger(__name__)


class ProductionService:
    INVENTORY_STATUS_FILTER_CODE_TO_LABEL = {
        "excess": "여유",
        "shortage": "부족",
        "normal": "적정",
    }
    _menu_image_index: dict[str, str] | None = None
    _response_cache = TTLMemoryCache(max_size=128)
    _waste_ttl_sec = 300
    _inventory_ttl_sec = 300
    _ai_summary_timeout_sec = 1.2

    def __init__(
        self,
        repository: ProductionRepository,
        audit_service: Optional[AuditService] = None,
        ai_client: Optional[AIServiceClient] = None
    ) -> None:
        self.repository = repository
        self.audit_service = audit_service
        self.ai_client = ai_client

    @staticmethod
    def _cache_key(prefix: str, **kwargs: object) -> str:
        ordered = "|".join(f"{key}={kwargs[key]}" for key in sorted(kwargs))
        return f"{prefix}|{ordered}"

    @classmethod
    def _normalize_inventory_status_filters(
        cls,
        status_filters: list[str] | None,
    ) -> tuple[list[str] | None, str]:
        if status_filters is None:
            return None, "all"

        normalized_codes: list[str] = []
        seen_codes: set[str] = set()

        for raw_code in status_filters:
            code = str(raw_code).strip().lower()
            if not code:
                raise ValueError("status must be a comma-separated list of non-empty values")
            if code not in cls.INVENTORY_STATUS_FILTER_CODE_TO_LABEL:
                allowed = ",".join(cls.INVENTORY_STATUS_FILTER_CODE_TO_LABEL.keys())
                raise ValueError(f"status must be one of: {allowed}")
            if code in seen_codes:
                continue
            seen_codes.add(code)
            normalized_codes.append(code)

        if not normalized_codes:
            raise ValueError("status must include at least one value")

        return normalized_codes, ",".join(normalized_codes)

    def _cached_payload(self, key: str) -> dict[str, Any] | None:
        cached = self._response_cache.get(key)
        if not isinstance(cached, dict):
            return None
        payload = copy.deepcopy(cached)
        meta = payload.get("meta")
        if isinstance(meta, dict):
            meta["data_freshness"] = "cached"
        return payload

    @staticmethod
    def _normalize_menu_key(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).strip().lower()
        normalized = re.sub(r"\.[a-z0-9]+$", "", normalized)
        normalized = re.sub(r"_[0-9]+$", "", normalized)
        normalized = re.sub(r"^\((.*?)\)", "", normalized).strip()
        normalized = normalized.removeprefix("던킨").strip()
        return "".join(ch for ch in normalized if ch.isalnum())

    @classmethod
    def _menu_image_directories(cls) -> list[Path]:
        repo_root = Path(__file__).resolve().parents[3]
        front_public_dir = repo_root / "br-korea-poc-front" / "public" / "images"
        docker_front_image_dir = Path("/menu-images")
        local_resource_dir = repo_root / "resource" / "05. 던킨도너츠 메뉴"
        docker_resource_dir = Path("/resource/05. 던킨도너츠 메뉴")

        candidates = [front_public_dir, docker_front_image_dir, local_resource_dir, docker_resource_dir]
        return [path for path in candidates if path.exists()]

    @classmethod
    def _load_menu_image_index(cls) -> dict[str, str]:
        if cls._menu_image_index is not None:
            return cls._menu_image_index

        image_index: dict[str, str] = {}
        image_dirs = cls._menu_image_directories()
        if not image_dirs:
            logger.warning("메뉴 이미지 디렉터리가 없어 image_url을 생성하지 못합니다.")
            cls._menu_image_index = image_index
            return image_index

        for image_dir in image_dirs:
            for pattern in ("*.png", "*.jpg", "*.jpeg", "*.webp", "*.PNG", "*.JPG", "*.JPEG", "*.WEBP"):
                for path in image_dir.glob(pattern):
                    key = cls._normalize_menu_key(path.name)
                    if key and key not in image_index:
                        image_index[key] = path.name

        cls._menu_image_index = image_index
        return image_index

    @classmethod
    def _resolve_image_url(cls, sku_name: str) -> str | None:
        key = cls._normalize_menu_key(sku_name)
        if not key:
            return None

        image_index = cls._load_menu_image_index()
        filename = image_index.get(key)
        if filename is None:
            for index_key, candidate in image_index.items():
                if key in index_key or index_key in key:
                    filename = candidate
                    break
        if filename is None:
            return None
        return f"/images/{quote(filename)}"

    @staticmethod
    def _calc_chance_loss_pct(current: int, forecast: int, status: str) -> int:
        """실데이터(현재고·예상판매량) 기반 찬스 로스 절감율 산출 (A-10)."""
        if status == "safe" or forecast <= 0:
            return 0
        if status == "danger":
            deficit_ratio = max(0.0, (forecast - current) / forecast)
            return min(30, max(10, int(round(deficit_ratio * 30))))
        # warning: buffer(1.5×forecast) 대비 부족분
        buffer = forecast * 1.5
        buffer_deficit_ratio = max(0.0, (buffer - current) / max(buffer, 1))
        return min(10, max(3, int(round(buffer_deficit_ratio * 10))))

    @staticmethod
    def _parse_prod(raw_value: str) -> tuple[str, int]:
        parts = str(raw_value or "").split(" / ")
        time_text = parts[0].strip() if parts else "00:00"
        qty_text = parts[1].replace("개", "").strip() if len(parts) > 1 else "0"
        return time_text or "00:00", ProductionService._safe_int(qty_text)

    @classmethod
    def _recommended_qty_from_row(cls, raw: dict) -> int:
        explicit_recommended = cls._safe_int(raw.get("recommended"))
        if explicit_recommended > 0:
            return explicit_recommended
        _, p1_qty = cls._parse_prod(str(raw.get("prod1") or ""))
        return max(p1_qty, 0)

    @classmethod
    def _chance_loss_amount_from_row(cls, raw: dict) -> int:
        if raw.get("chance_loss_amt") is not None:
            return cls._safe_int(raw.get("chance_loss_amt"))
        if raw.get("chance_loss_reduction_pct") is not None:
            return cls._safe_int(raw.get("chance_loss_reduction_pct"))
        current_stock = max(cls._safe_int(raw.get("current")), 0)
        predicted_sales_1h = max(
            cls._safe_int(raw.get("predicted_sales_1h", raw.get("forecast"))),
            0,
        )
        recommended_qty = max(cls._recommended_qty_from_row(raw), 0)
        shortage_without = max(predicted_sales_1h - current_stock, 0)
        shortage_with = max(predicted_sales_1h - (current_stock + recommended_qty), 0)
        prevented_units = max(shortage_without - shortage_with, 0)
        return prevented_units * 1200

    @staticmethod
    def _assumed_shelf_life_days(item_name: str) -> int:
        name = item_name.strip()
        if any(keyword in name for keyword in ("샌드", "샐러드", "크림")):
            return 1
        if any(keyword in name for keyword in ("케이크", "파이")):
            return 2
        if any(keyword in name for keyword in ("도넛", "베이글", "빵")):
            return 1
        if any(keyword in name for keyword in ("커피", "음료")):
            return 0
        return 1

    def _resolve_shelf_life_days(
        self,
        *,
        shelf_life_map: dict[str, int],
        item_cd: str | None,
        item_nm: str,
    ) -> int:
        normalized_item_cd = str(item_cd or "").strip()
        normalized_item_nm = str(item_nm).strip()
        if normalized_item_cd and normalized_item_cd in shelf_life_map:
            return int(shelf_life_map[normalized_item_cd])
        if normalized_item_nm and normalized_item_nm in shelf_life_map:
            return int(shelf_life_map[normalized_item_nm])
        return self._assumed_shelf_life_days(normalized_item_nm)

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if not math.isfinite(parsed):
            return default
        return parsed

    @classmethod
    def _safe_int(cls, value: object, default: int = 0) -> int:
        return int(round(cls._safe_float(value, float(default))))

    @staticmethod
    def _normalize_inventory_status_result(result: object) -> tuple[list[dict], int, dict[str, Any]]:
        """레포지토리 반환값을 (rows, total_items, summary_metrics) 형식으로 정규화."""
        if isinstance(result, tuple):
            if len(result) == 3:
                rows, total_items, summary_metrics = result
                normalized_rows = rows if isinstance(rows, list) else []
                normalized_total = (
                    int(total_items)
                    if isinstance(total_items, (int, float)) and not isinstance(total_items, bool)
                    else 0
                )
                normalized_summary = summary_metrics if isinstance(summary_metrics, dict) else {}
                return normalized_rows, normalized_total, normalized_summary
            if len(result) == 2:
                rows, total_items = result
                normalized_rows = rows if isinstance(rows, list) else []
                normalized_total = (
                    int(total_items)
                    if isinstance(total_items, (int, float)) and not isinstance(total_items, bool)
                    else 0
                )
                return normalized_rows, normalized_total, {}
        return [], 0, {}

    async def _attach_ai_grounded_summary(
        self,
        *,
        store_id: str,
        topic: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.ai_client:
            return evidence

        base_items = evidence.get("items")
        if not isinstance(base_items, list):
            return evidence

        ai_evidence_items: list[dict[str, str]] = []
        for idx, item in enumerate(base_items, start=1):
            if not isinstance(item, dict):
                continue
            ai_evidence_items.append(
                {
                    "id": f"E{idx}",
                    "label": str(item.get("label") or ""),
                    "value": str(item.get("value") or ""),
                    "calculation": str(item.get("calculation") or ""),
                }
            )
        if not ai_evidence_items:
            return evidence

        try:
            ai_summary = await asyncio.wait_for(
                self.ai_client.generate_grounded_explanation(
                    store_id=store_id,
                    topic=topic,
                    evidence_items=ai_evidence_items,
                ),
                timeout=self._ai_summary_timeout_sec,
            )
        except TimeoutError:
            logger.warning(
                "AI 근거 요약 시간 초과: store_id=%s topic=%s timeout=%.1fs",
                store_id,
                topic,
                self._ai_summary_timeout_sec,
            )
            return evidence
        except Exception as exc:
            logger.warning(
                "AI 근거 요약 생성 실패: store_id=%s topic=%s error=%s",
                store_id,
                topic,
                exc,
            )
            return evidence

        if not ai_summary:
            return evidence

        merged = {**evidence}
        merged["ai_summary"] = ai_summary.get("text")
        merged["ai_citations"] = ai_summary.get("citations", [])
        merged["ai_grounded"] = True
        merged["items"] = [
            {**item, "id": ai_evidence_items[idx]["id"]} if idx < len(ai_evidence_items) and isinstance(item, dict) else item
            for idx, item in enumerate(base_items)
        ]
        return merged

    @classmethod
    def _decision_for_row(cls, raw: dict) -> ProductionSkuDecision:
        risk_label = "정상"
        if raw["status"] == "danger":
            risk_label = "즉시생산"
        elif raw["status"] == "warning":
            risk_label = "주의"

        current = int(raw.get("current", 0))
        forecast = int(raw.get("forecast", 0))
        predicted_sales_1h = cls._safe_int(raw.get("predicted_sales_1h", forecast))

        # 실데이터 기반 판매 속도: 예상판매량 / 현재고 비율
        explicit_velocity = cls._safe_float(raw.get("sales_velocity"))
        if explicit_velocity > 0:
            velocity = round(min(3.0, max(0.5, explicit_velocity)), 1)
        elif current > 0 and predicted_sales_1h > 0:
            velocity = round(min(3.0, max(0.5, predicted_sales_1h / current)), 1)
        else:
            velocity = 1.2 if raw["status"] != "safe" else 0.9

        # 실데이터 수치를 포함한 알림 메시지
        if raw.get("alert_message"):
            alert_msg = str(raw["alert_message"])
        elif raw["status"] == "danger":
            hours = max(1, int(round(current / max(predicted_sales_1h / 8.0, 0.1))))
            alert_msg = f"현재 재고 {current}개, 약 {hours}시간 이내 소진 예상. 즉시 생산이 필요합니다."
        elif raw["status"] == "warning":
            alert_msg = f"현재 재고 {current}개, 예상 판매량 {forecast}개. 생산 준비가 필요합니다."
        else:
            alert_msg = "현재 재고가 안정적입니다."

        if raw["status"] == "warning" and not raw.get("alert_message"):
            alert_msg = (
                f"?꾩옱 ?ш퀬 {current}媛? ?덉긽 ?먮ℓ??{predicted_sales_1h}媛? "
                "?앹궛 以鍮꾧? ?꾩슂?⑸땲??"
            )

        recommended_qty = cls._recommended_qty_from_row(raw)
        chance_loss_amt = cls._chance_loss_amount_from_row(raw)
        chance_loss_prevented_amount = (
            chance_loss_amt
            if chance_loss_amt > 0
            else recommended_qty * 1200 if recommended_qty > 0 else None
        )

        return ProductionSkuDecision(
            risk_level_label=risk_label,
            sales_velocity=velocity,
            tags=["속도↑"] if raw["status"] != "safe" else [],
            alert_message=alert_msg,
            can_produce=True,
            predicted_stockout_time=raw.get("stockout_expected_at")
            or (raw["depletion_time"] if raw["depletion_time"] != "-" else None),
            suggested_production_qty=recommended_qty,
            chance_loss_prevented_amount=chance_loss_prevented_amount,
        )

    @classmethod
    def _sku_item_from_row(cls, raw: dict) -> ProductionSkuItem:
        def parse_prod(s: str):
            parts = s.split(" / ")
            time = parts[0] if len(parts) > 0 else "00:00"
            qty = int(parts[1].replace("개", "")) if len(parts) > 1 else 0
            return time, qty

        p1_time, p1_qty = cls._parse_prod(raw["prod1"])
        p2_time, p2_qty = cls._parse_prod(raw["prod2"])
        decision = cls._decision_for_row(raw)
        recommended_qty = cls._recommended_qty_from_row(raw)
        chance_loss_amount = cls._chance_loss_amount_from_row(raw)

        return ProductionSkuItem(
            sku_id=raw["sku_id"],
            sku_name=raw["name"],
            image_url=cls._resolve_image_url(str(raw["name"])),
            current_stock=raw["current"],
            forecast_stock_1h=raw["forecast"],
            predicted_sales_1h=cls._safe_int(raw.get("predicted_sales_1h")),
            avg_first_production_qty_4w=p1_qty,
            avg_first_production_time_4w=p1_time,
            avg_second_production_qty_4w=p2_qty,
            avg_second_production_time_4w=p2_time,
            status=raw["status"],
            chance_loss_saving_pct=chance_loss_amount,
            recommended_production_qty=recommended_qty,
            chance_loss_basis_text=str(raw.get("chance_loss_basis_text") or "추천 생산 수량은 최근 4주 평균 생산량과 1시간 후 예측 재고를 반영해 계산했습니다."),
            decision=decision,
            depletion_eta_minutes=60 if raw["status"] != "safe" else None,
            tags=decision.tags,
            alert_message=decision.alert_message,
            can_produce=decision.can_produce,
            predicted_stockout_time=decision.predicted_stockout_time,
            sales_velocity=decision.sales_velocity,
            material_alert=False,
            material_alert_message=None,
        )

    async def _get_sku_items(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> list[ProductionSkuItem]:
        raw_items = await self.repository.list_items(
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )
        return [self._sku_item_from_row(raw) for raw in raw_items]

    async def _enrich_items_with_ai(self, items: list[ProductionSkuItem], store_id: str | None = None) -> list[ProductionSkuItem]:
        """AI 예측 결과로 각 SKU 항목을 보강합니다. ML 모델 연동 시 predicted_stock_1h → forecast_stock_1h 반영."""
        if not self.ai_client or not items:
            return items

        enriched: list[ProductionSkuItem] = []
        for item in items:
            history = [
                {"stock": item.current_stock, "sales": item.avg_first_production_qty_4w or 0, "production": 0}
            ]
            pattern_4w: list[float] = []

            try:
                result = await self.ai_client.predict_production(
                    sku=item.sku_id,
                    current_stock=item.current_stock,
                    history=history,
                    pattern_4w=pattern_4w,
                    store_id=store_id,
                )
            except Exception as exc:
                logger.warning("AI 예측 호출 실패 (sku=%s): %s", item.sku_id, exc)
                result = None

            if not result:
                enriched.append(item)
                continue

            updates: dict = {}

            # ML 모델 또는 AI 서비스의 predicted_stock_1h → forecast_stock_1h 반영
            predicted_stock_1h = result.get("predicted_stock_1h")
            if predicted_stock_1h is not None:
                updates["forecast_stock_1h"] = int(round(float(predicted_stock_1h)))

            if result.get("alert_message"):
                updates["alert_message"] = result["alert_message"]
            stockout = result.get("stockout_expected_at") or result.get("predicted_stockout_time")
            if stockout:
                updates["predicted_stockout_time"] = stockout
                if item.decision:
                    updates["decision"] = item.decision.model_copy(
                        update={"predicted_stockout_time": stockout}
                    )
            if result.get("risk_detected") is True:
                updates["sales_velocity"] = max(float(item.sales_velocity or 1.0), 1.2)

            enriched.append(item.model_copy(update=updates) if updates else item)

        return enriched

    async def get_overview(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> ProductionOverviewResponse:
        raw_items = await self.repository.list_items(
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )
        sku_items = [self._sku_item_from_row(raw) for raw in raw_items]
        danger_count = sum(1 for item in sku_items if item.status == "danger")
        warning_count = sum(1 for item in sku_items if item.status == "warning")
        safe_count = sum(1 for item in sku_items if item.status == "safe")
        alerts = [
            ProductionOverviewAlert(
                id=f"alert-{item.sku_id}",
                type=(
                    "inventory_risk"
                    if item.status == "danger"
                    else "speed_risk"
                    if item.status == "warning"
                    else "material_risk"
                ),
                severity="high" if item.status == "danger" else "medium",
                title=(
                    f"긴급: {item.sku_name} 재고 소진 위험"
                    if item.status == "danger"
                    else f"주의: {item.sku_name} 생산 속도 점검 필요"
                ),
                description=item.alert_message or "현재 생산 상태를 확인해 주세요.",
                sku_id=item.sku_id,
            )
            for item in sku_items
            if item.status in {"danger", "warning"}
        ]
        return ProductionOverviewResponse(
            updated_at=get_now().strftime("%Y-%m-%d %H:%M"),
            refresh_interval_minutes=3 if danger_count >= 1 else (5 if warning_count >= 1 else 10),
            summary_stats=[
                ProductionSummaryStat(key="danger_count", label="품절 위험", value=f"{danger_count}개", tone="danger"),
                ProductionSummaryStat(key="warning_count", label="주의 필요", value=f"{warning_count}개", tone="primary"),
                ProductionSummaryStat(key="safe_count", label="안전 재고", value=f"{safe_count}개", tone="success"),
                ProductionSummaryStat(
                    key="chance_loss_saving_total",
                    label="찬스 로스 절감액",
                    value=f"{sum(item.chance_loss_saving_pct for item in sku_items):,}원",
                    tone="default",
                ),
            ],
            alerts=alerts,
            production_lead_time_minutes=60,
            danger_count=danger_count,
            items=[
                ProductionItem(**{**item, "recommended": self._recommended_qty_from_row(item)})
                for item in raw_items
            ],
            explainability=create_ready_payload(
                trace_id=f"production-overview-{store_id or 'all'}",
                actions=[
                    "품절 위험 SKU를 우선 생산 항목으로 지정하고 담당자를 배정하세요.",
                    "주의 SKU는 1시간 단위로 재고·판매속도를 재점검하세요.",
                ],
                evidence=[
                    f"품절 위험: {danger_count}개",
                    f"주의 필요: {warning_count}개",
                    f"안전 재고: {safe_count}개",
                ],
            ),
        )

    async def get_sku_list(
        self,
        page: int = 1,
        page_size: int = 20,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> GetProductionSkuListResponse:
        items = await self._get_sku_items(
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )

        start = max(0, (page - 1) * page_size)
        end = start + page_size
        paged_items = items[start:end]

        pagination = Pagination(
            page=page,
            page_size=page_size,
            total_items=len(items),
            total_pages=max(1, ((len(items) - 1) // page_size) + 1),
        )

        return GetProductionSkuListResponse(items=paged_items, pagination=pagination)

    async def get_sku_detail(
        self,
        sku_id: str,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> ProductionSkuDetailResponse:
        items = await self._get_sku_items(
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )
        target = next((item for item in items if item.sku_id == sku_id), None)
        if target is None and store_id is not None:
            # store_id 필터로 매칭 결과가 없으면 전체 조회로 재시도
            logger.info("get_sku_detail: store_id=%s 로 sku_id=%s 없음, 전체 조회로 재시도", store_id, sku_id)
            items = await self._get_sku_items(
                store_id=None,
                business_date=business_date,
                reference_datetime=reference_datetime,
            )
            target = next((item for item in items if item.sku_id == sku_id), None)
        if target is None:
            raise ValueError(f"sku not found: {sku_id}")

        return ProductionSkuDetailResponse(
            sku_id=target.sku_id,
            sku_name=target.sku_name,
            image_url=target.image_url,
            current_stock=target.current_stock,
            forecast_stock_1h=target.forecast_stock_1h,
            predicted_sales_1h=target.predicted_sales_1h,
            recommended_qty=target.recommended_production_qty,
            chance_loss_saving_pct=target.chance_loss_saving_pct,
            chance_loss_basis_text=target.chance_loss_basis_text,
            predicted_stockout_time=target.predicted_stockout_time,
            can_produce=target.can_produce,
            sales_velocity=target.sales_velocity,
            tags=target.tags or [],
            alert_message=target.alert_message,
            material_alert=target.material_alert,
            material_alert_message=target.material_alert_message,
        )

    async def get_registration_form(self, store_id: str | None = None) -> ProductionRegistrationFormResponse:
        items = await self._get_sku_items()
        history = await self.repository.list_registration_history(limit=50, store_id=store_id)
        last_reg_map: dict[str, dict] = {}
        for entry in history:
            sku_id = entry.get("sku_id", "")
            if sku_id not in last_reg_map:
                last_reg_map[sku_id] = entry

        form_items = [
            ProductionRegistrationFormItem(
                sku_id=item.sku_id,
                sku_name=item.sku_name,
                recommended_qty=item.recommended_production_qty,
                current_stock=item.current_stock,
                forecast_stock_1h=item.forecast_stock_1h,
                basis_text=item.chance_loss_basis_text,
                last_registered_at=last_reg_map[item.sku_id]["registered_at"] if item.sku_id in last_reg_map else None,
                last_registered_qty=last_reg_map[item.sku_id]["qty"] if item.sku_id in last_reg_map else None,
            )
            for item in items
        ]
        return ProductionRegistrationFormResponse(
            items=form_items,
            generated_at=get_now().strftime("%Y-%m-%d %H:%M"),
        )

    async def get_alerts(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> ProductionAlertsResponse:
        items = await self.repository.list_items(
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )
        alerts = [
            ProductionAlertItem(
                sku_id=item["sku_id"],
                name=item["name"],
                current=item["current"],
                forecast=item["forecast"],
                depletion_time=item["depletion_time"],
                recommended=self._recommended_qty_from_row(item),
                prod1=item["prod1"],
                prod2=item["prod2"],
                severity=item["status"],
                push_title=f"{item['name']} 생산이 필요합니다",
                push_message=(
                    f"현재고 {item['current']}개, 1시간 후 예상 {item['forecast']}개입니다. "
                    f"{item['depletion_time']} 전 소진 가능성이 있어 {self._recommended_qty_from_row(item)}개 생산을 권장합니다."
                ),
                target_roles=["store_owner", "store_operator"],
            )
            for item in items
            if item["status"] in {"danger", "warning"}
        ]
        return ProductionAlertsResponse(
            generated_at=get_now().strftime("%Y-%m-%d %H:%M:%S"),
            lead_time_minutes=60,
            alerts=alerts,
            explainability=create_ready_payload(
                trace_id=f"production-alerts-{store_id or 'all'}",
                actions=["알림에 표시된 SKU 순서대로 생산/재고 조치를 즉시 실행하세요."],
                evidence=[f"생성 알림 수: {len(alerts)}"],
            ),
        )

    async def get_waste_summary(
        self,
        store_id: str | None = None,
        page: int = 1,
        page_size: int = 10,
        reference_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> WasteSummaryResponse:
        if not store_id:
            raise ValueError("store_id is required")

        page = max(1, page)
        page_size = max(1, min(page_size, 100))
        base_datetime = reference_datetime or (
            datetime.strptime(reference_date, "%Y-%m-%d") if reference_date else get_now()
        )
        target_day = base_datetime.date() - timedelta(days=1)
        period_start_day = target_day - timedelta(days=29)
        target_date = target_day.isoformat()
        period_start_date = period_start_day.isoformat()
        target_date_yyyymmdd = target_day.strftime("%Y%m%d")
        period_start_yyyymmdd = period_start_day.strftime("%Y%m%d")

        cache_key = self._cache_key(
            "waste-summary",
            store_id=store_id,
            page=page,
            page_size=page_size,
            period_start=period_start_date,
            target_date=target_date,
        )
        cached_payload = self._cached_payload(cache_key)
        if cached_payload:
            return WasteSummaryResponse.model_validate(cached_payload)

        get_production_waste_rows = getattr(self.repository, "get_production_waste_rows", None)
        waste_rows = (
            get_production_waste_rows(
                store_id=store_id,
                date_from=period_start_yyyymmdd,
                date_to=target_date_yyyymmdd,
            )
            if callable(get_production_waste_rows)
            else []
        )

        shelf_life_map: dict[str, int] = {}
        get_shelf_life_days_map = getattr(self.repository, "get_shelf_life_days_map", None)
        if callable(get_shelf_life_days_map) and waste_rows:
            shelf_life_map = get_shelf_life_days_map(
                item_codes=[str(r.get("item_cd") or "").strip() for r in waste_rows],
                item_names=[str(r.get("item_nm") or "").strip() for r in waste_rows],
            )

        aggregated_rows: dict[str, dict[str, Any]] = {}
        for row in waste_rows:
            item_nm = str(row.get("item_nm") or "").strip()
            item_cd = str(row.get("item_cd") or "").strip()
            if not item_nm:
                continue
            item_key = item_cd or item_nm
            bucket = aggregated_rows.setdefault(
                item_key,
                {
                    "item_cd": item_cd,
                    "item_nm": item_nm,
                    "total_waste_qty": 0.0,
                    "total_waste_amount": 0.0,
                    "adjusted_loss_qty": 0.0,
                    "adjusted_loss_amount": 0.0,
                    "estimated_expiry_loss_qty": 0.0,
                },
            )
            bucket["item_cd"] = item_cd or bucket["item_cd"]
            bucket["item_nm"] = item_nm or bucket["item_nm"]
            bucket["total_waste_qty"] += self._safe_float(row.get("total_waste_qty"))
            bucket["total_waste_amount"] += self._safe_float(row.get("total_waste_amount"))
            bucket["adjusted_loss_qty"] += self._safe_float(row.get("adjusted_loss_qty"))
            bucket["adjusted_loss_amount"] += self._safe_float(row.get("adjusted_loss_amount"))
            bucket["estimated_expiry_loss_qty"] += self._safe_float(
                row.get("estimated_expiry_loss_qty")
            )

        items: list[WasteItem] = []
        total_disuse_amount = 0.0
        total_estimated_expiry_loss_qty = 0.0
        for row in aggregated_rows.values():
            item_nm = str(row.get("item_nm") or "").strip()
            if not item_nm:
                continue
            confirmed_disuse_qty = round(self._safe_float(row.get("total_waste_qty")), 2)
            disuse_amount = round(self._safe_float(row.get("total_waste_amount")), 2)
            shelf_life_days = self._resolve_shelf_life_days(
                shelf_life_map=shelf_life_map,
                item_cd=str(row.get("item_cd") or "").strip(),
                item_nm=item_nm,
            )
            expiry_risk_level = (
                "high"
                if shelf_life_days <= 1 and confirmed_disuse_qty > 0
                else "medium"
                if confirmed_disuse_qty > 0
                else "low"
            )
            total_disuse_amount += disuse_amount
            total_estimated_expiry_loss_qty += confirmed_disuse_qty
            items.append(
                WasteItem(
                    item_nm=item_nm,
                    image_url=self._resolve_image_url(item_nm),
                    adjusted_loss_qty=confirmed_disuse_qty,
                    confirmed_disuse_qty=confirmed_disuse_qty,
                    estimated_expiry_loss_qty=confirmed_disuse_qty,
                    adjusted_loss_amount=disuse_amount,
                    disuse_amount=disuse_amount,
                    assumed_shelf_life_days=shelf_life_days,
                    expiry_risk_level=expiry_risk_level,
                )
            )

        items.sort(key=lambda item: item.confirmed_disuse_qty, reverse=True)
        total_items = len(items)
        total_pages = max(1, math.ceil(max(total_items, 1) / page_size))
        current_page = min(page, total_pages)
        start_index = (current_page - 1) * page_size
        paginated_items = items[start_index : start_index + page_size]
        top_items = items[:3]

        base_evidence: dict[str, Any] = {
            "summary_reason": "Waste is aggregated for the 30 days before the reference date, ending on the target date.",
            "processing_route": "repository",
            "fallback_used": False,
            "items": [
                {
                    "label": "waste target",
                    "value": "expired remaining qty after FIFO consumption within the last 30 days",
                    "calculation": "remaining_qty after FIFO consumption AND target_date BETWEEN period_start AND period_end",
                    "source_table": "raw_production_extract + raw_daily_store_item",
                },
                {
                    "label": "loss amount",
                    "value": "estimated by average selling amount per unit",
                    "calculation": "expired_remaining_qty * avg_unit_price",
                    "source_table": "raw_daily_store_item",
                },
                {
                    "label": "target period",
                    "value": f"{period_start_date}~{target_date}",
                    "calculation": "30-day window ending at reference_date - 1 day",
                    "source_table": "X-Reference-Datetime",
                },
            ],
        }
        evidence = await self._attach_ai_grounded_summary(
            store_id=store_id,
            topic="production waste",
            evidence=base_evidence,
        )

        response = WasteSummaryResponse(
            items=paginated_items,
            total_adjusted_loss_amount=round(total_disuse_amount, 2),
            total_disuse_amount=round(total_disuse_amount, 2),
            total_estimated_expiry_loss_qty=round(total_estimated_expiry_loss_qty, 2),
            monthly_top_items=[
                {
                    "item_nm": item.item_nm,
                    "confirmed_disuse_qty": round(item.confirmed_disuse_qty, 2),
                }
                for item in top_items
            ],
            summary={
                "store_id": store_id,
                "item_count": total_items,
                "period_start": period_start_date,
                "period_end": target_date,
                "period_total_disuse_amount": round(total_disuse_amount, 2),
                "period_total_disuse_qty": round(total_estimated_expiry_loss_qty, 2),
                "gap_amount": 0.0,
            },
            highlights=(
                [
                    {
                        "label": "largest waste item",
                        "item_nm": top_items[0].item_nm,
                        "value": round(top_items[0].confirmed_disuse_qty, 2),
                    },
                    {
                        "label": "30-day estimated loss amount",
                        "item_nm": f"{period_start_date}~{target_date}",
                        "value": round(total_disuse_amount, 2),
                    },
                ]
                if top_items
                else [
                    {
                        "label": "30-day estimated loss amount",
                        "item_nm": f"{period_start_date}~{target_date}",
                        "value": round(total_disuse_amount, 2),
                    }
                ]
            ),
            actions=[
                "Review over-produced items with expired remaining stock first.",
                "Short shelf-life items should get stronger end-of-day sell-through actions.",
                "Repeated waste items should have lower production and ordering baselines.",
            ],
            evidence=evidence,
            pagination=Pagination(
                page=current_page,
                page_size=page_size,
                total_items=total_items,
                total_pages=total_pages,
            ),
            explainability=create_ready_payload(
                trace_id=f"production-waste-{store_id}",
                actions=[
                    "Review items with the largest estimated waste first.",
                    "Tighten end-of-day depletion actions for short shelf-life products.",
                ],
                evidence=[
                    f"target period: {period_start_date}~{target_date}",
                    f"total loss amount: {round(total_disuse_amount, 2)} won",
                    f"total waste qty: {round(total_estimated_expiry_loss_qty, 2)}",
                    f"item count: {total_items}",
                ],
            ),
        )
        self._response_cache.set(
            cache_key,
            response.model_dump(mode="json"),
            ttl_sec=self._waste_ttl_sec,
        )
        return response

    async def get_inventory_status(
        self,
        store_id: str | None = None,
        page: int = 1,
        page_size: int = 10,
        status_filters: list[str] | None = None,
        business_date: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> InventoryStatusResponse:
        if not store_id:
            raise ValueError("store_id is required")
        page = max(1, page)
        page_size = max(1, min(page_size, 100))
        normalized_status_filters, normalized_status_filter_key = (
            self._normalize_inventory_status_filters(status_filters)
        )
        cache_business_date = business_date
        if cache_business_date is None and reference_datetime is not None:
            cache_business_date = reference_datetime.strftime("%Y-%m-%d")

        cache_key = self._cache_key(
            "inventory-status",
            store_id=store_id,
            page=page,
            page_size=page_size,
            status=normalized_status_filter_key,
            business_date=cache_business_date or "",
        )
        cached_payload = self._cached_payload(cache_key)
        if cached_payload:
            return InventoryStatusResponse.model_validate(cached_payload)

        try:
            raw_result = self.repository.get_inventory_status(
                store_id=store_id,
                page=page,
                page_size=page_size,
                status_filters=normalized_status_filters,
                business_date=business_date,
                reference_datetime=reference_datetime,
            )
        except TypeError:
            raw_result = self.repository.get_inventory_status(
                store_id=store_id,
                page=page,
                page_size=page_size,
                status_filters=normalized_status_filters,
            )
        rows, total_items, summary_metrics = self._normalize_inventory_status_result(raw_result)
        if not rows and total_items == 0:
            raise LookupError("해당 점포의 재고 진단 데이터가 없습니다.")

        items: list[InventoryStatusItem] = []
        use_precomputed_inventory_metrics = all(
            row.get("assumed_shelf_life_days") is not None
            and row.get("expiry_risk_level") is not None
            and row.get("status") is not None
            for row in rows
        )
        shelf_life_map: dict[str, int] = {}
        if not use_precomputed_inventory_metrics:
            get_shelf_life_days_map = getattr(self.repository, "get_shelf_life_days_map", None)
            if callable(get_shelf_life_days_map):
                shelf_life_map = get_shelf_life_days_map(
                    item_codes=[str(row.get("item_cd") or "").strip() for row in rows],
                    item_names=[str(row.get("item_nm") or "").strip() for row in rows],
                )
        for row in rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            item_nm = str(row.get("item_nm") or item_cd)
            total_stock = self._safe_float(row.get("stk_avg"))
            total_sold = self._safe_float(row.get("sal_avg"))
            total_orderable = self._safe_float(row.get("ord_avg"))
            stock_rate = self._safe_float(row.get("stk_rt"))
            is_stockout = bool(int(self._safe_float(row.get("is_stockout"))))
            stockout_hour = (
                int(self._safe_float(row.get("stockout_hour")))
                if row.get("stockout_hour") is not None
                else None
            )

            if is_stockout or stock_rate < 0:
                status = "부족"
            elif stock_rate >= 0.35:
                status = "여유"
            else:
                status = "적정"

            if use_precomputed_inventory_metrics:
                status = str(row.get("status") or status)

            shelf_life_days = self._resolve_shelf_life_days(
                shelf_life_map=shelf_life_map,
                item_cd=item_cd,
                item_nm=item_nm,
            )
            expiry_risk_level = (
                "높음"
                if shelf_life_days <= 1 and stock_rate > 0.25
                else "중간"
                if stock_rate > 0.15
                else "낮음"
            )
            if use_precomputed_inventory_metrics:
                shelf_life_days = self._safe_int(row.get("assumed_shelf_life_days"), shelf_life_days)
                expiry_risk_level = str(row.get("expiry_risk_level") or expiry_risk_level)

            item_group_raw = row.get("item_group")
            item_group = str(item_group_raw).strip() if item_group_raw else None
            items.append(
                InventoryStatusItem(
                    item_cd=item_cd,
                    item_nm=item_nm,
                    item_group=item_group or None,
                    image_url=self._resolve_image_url(item_nm),
                    total_stock=total_stock,
                    total_sold=total_sold,
                    total_orderable=total_orderable,
                    stock_rate=stock_rate,
                    stockout_hour=stockout_hour,
                    is_stockout=is_stockout,
                    assumed_shelf_life_days=shelf_life_days,
                    expiry_risk_level=expiry_risk_level,
                    status=status,
                )
            )

        items.sort(key=lambda row: row.stock_rate)
        top_shortage = [item for item in items if item.status == "부족"][:3]
        top_excess = sorted(
            [item for item in items if item.status == "여유"],
            key=lambda row: row.stock_rate,
            reverse=True,
        )[:3]

        shortage_count = self._safe_int(summary_metrics.get("shortage_count"), 0)
        excess_count = self._safe_int(summary_metrics.get("excess_count"), 0)
        normal_count = self._safe_int(summary_metrics.get("normal_count"), 0)
        avg_stock_rate = round(self._safe_float(summary_metrics.get("avg_stock_rate"), 0.0), 3)

        base_evidence: dict[str, Any] = {
            "summary_reason": "재고율, 품절시간, 판매가능수량을 함께 반영해 상태를 분류했습니다.",
            "processing_route": "repository",
            "fallback_used": False,
            "items": [
                {
                    "label": "재고율 기반 분류",
                    "value": f"부족 {shortage_count}개 / 여유 {excess_count}개",
                    "calculation": "stock_rate<0 또는 품절=true: 부족, stock_rate>=0.35: 여유, 그 외 적정",
                    "source_table": "core_stock_rate/core_stockout_time or raw_inventory_extract/core_hourly_item_sales",
                },
                {
                    "label": "가설 유통기한",
                    "value": "raw_product_shelf_life 우선",
                    "calculation": "DB 유통기한 우선 적용, 미존재 시 키워드 규칙 fallback",
                    "source_table": "raw_product_shelf_life or assumption_rule",
                },
            ],
        }
        evidence = await self._attach_ai_grounded_summary(
            store_id=store_id,
            topic="재고 수준 진단",
            evidence=base_evidence,
        )
        response = InventoryStatusResponse(
            summary={
                "store_id": store_id,
                "item_count": total_items,
                "shortage_count": shortage_count,
                "excess_count": excess_count,
                "normal_count": normal_count,
                "avg_stock_rate": avg_stock_rate,
            },
            highlights=[
                {
                    "label": "부족 위험 TOP",
                    "value": f"{item.item_nm} ({item.stock_rate:.2f})",
                }
                for item in top_shortage
            ]
            + [
                {
                    "label": "여유 품목 TOP",
                    "value": f"{item.item_nm} ({item.stock_rate:.2f})",
                }
                for item in top_excess
            ],
            actions=[
                "부족 품목은 다음 생산/발주 사이클을 앞당겨 품절 시각을 늦추세요.",
                "여유 품목은 다음날 발주량을 축소하고, 프로모션/세트로 소진을 유도하세요.",
                "유통기한 가설 1일 품목은 마감 전 재고 소진 우선순위를 높이세요.",
            ],
            evidence=evidence,
            items=items,
            pagination=Pagination(
                page=page,
                page_size=page_size,
                total_items=total_items,
                total_pages=max(1, math.ceil(total_items / max(page_size, 1))),
            ),
            explainability=create_ready_payload(
                trace_id=f"production-inventory-status-{store_id}",
                actions=[
                    "부족 품목은 즉시 생산/발주 타이밍을 앞당기세요.",
                    "여유 품목은 다음 발주량을 줄이고 판촉 소진 계획을 실행하세요.",
                ],
                evidence=[
                    f"부족: {shortage_count}개",
                    f"여유: {excess_count}개",
                    f"평균 재고율: {avg_stock_rate}",
                ],
            ),
        )
        self._response_cache.set(
            cache_key,
            response.model_dump(mode="json"),
            ttl_sec=self._inventory_ttl_sec,
        )
        return response

    async def register_production(self, payload: ProductionRegistrationRequest) -> ProductionRegistrationResponse:
        await self.repository.save_registration(payload.model_dump())
        response = ProductionRegistrationResponse(
            sku_id=payload.sku_id,
            qty=payload.qty,
            registered_by=payload.registered_by,
            feedback_type="chance_loss_reduced",
            feedback_message="재고 소진 전에 등록되어 찬스 로스 감소 효과를 기록했습니다.",
            store_id=payload.store_id,
        )
        if self.audit_service:
            await self.audit_service.record(
                domain="production",
                event_type="production_registered",
                actor_role=payload.registered_by,
                route="api",
                outcome="success",
                message=f"{payload.sku_id} 생산 등록을 저장했습니다.",
                metadata={"sku_id": payload.sku_id, "qty": payload.qty, "feedback_type": response.feedback_type},
            )
        return response

    async def list_registration_history(
        self,
        limit: int = 20,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> ProductionRegistrationHistoryResponse:
        items = await self.repository.list_registration_history(
            limit=limit,
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
        return ProductionRegistrationHistoryResponse(
            items=[ProductionRegistrationHistoryItem(**item) for item in items],
            total=len(items),
            filtered_store_id=store_id,
            filtered_date_from=date_from,
            filtered_date_to=date_to,
        )

    async def run_simulation(self, payload: ProductionSimulationRequest) -> ProductionSimulationResponse:
        inventory_data, production_data, sales_data = await self.repository.fetch_simulation_data(
            store_id=payload.store_id,
            item_id=payload.item_id,
            simulation_date=payload.simulation_date,
        )

        if self.ai_client:
            result = await self.ai_client.run_simulation(
                store_id=payload.store_id,
                item_id=payload.item_id,
                simulation_date=payload.simulation_date,
                lead_time_hour=payload.lead_time_hour,
                margin_rate=payload.margin_rate,
                inventory_data=inventory_data,
                production_data=production_data,
                sales_data=sales_data,
            )
            if result:
                return ProductionSimulationResponse(**result)

        return self._build_repository_simulation(
            payload=payload,
            inventory_data=inventory_data,
            production_data=production_data,
            sales_data=sales_data,
        )

    @staticmethod
    def _build_repository_simulation(
        payload: ProductionSimulationRequest,
        inventory_data: list[dict],
        production_data: list[dict],
        sales_data: list[dict],
    ) -> ProductionSimulationResponse:
        inventory_rows = [
            row for row in inventory_data
            if str(row.get("ITEM_CD") or "") == payload.item_id
        ]
        production_rows = [
            row for row in production_data
            if str(row.get("ITEM_CD") or "") == payload.item_id
        ]
        sales_rows = [
            row for row in sales_data
            if str(row.get("ITEM_CD") or "") == payload.item_id
        ]

        current_stock = sum(int(float(row.get("STOCK_QTY") or 0)) for row in inventory_rows)
        sales_qty = sum(int(float(row.get("SALE_QTY") or 0)) for row in sales_rows)
        production_qty = sum(int(float(row.get("PROD_QTY") or 0)) for row in production_rows)
        actual_end_stock = max(current_stock - sales_qty, 0)
        ai_guided_end_stock = max(current_stock + production_qty - sales_qty, 0)
        additional_sales_qty = max(min(production_qty, sales_qty) - max(current_stock - sales_qty, 0), 0)
        additional_profit_amt = int(round(additional_sales_qty * 1500 * payload.margin_rate))
        additional_waste_qty = float(max(ai_guided_end_stock - actual_end_stock, 0))
        additional_waste_cost = int(round(additional_waste_qty * 700))
        net_profit_change = additional_profit_amt - additional_waste_cost

        timeline = []
        if production_qty > 0:
            timeline.append(f"[생산 데이터] 누적 생산 {production_qty}개 반영")
        if sales_qty > 0:
            timeline.append(f"[판매 데이터] 누적 판매 {sales_qty}개 반영")

        chart_points = []
        for hour in ("08:00", "12:00", "16:00", "20:00"):
            chart_points.append(
                SimulationChartPoint(
                    time=hour,
                    actual_stock=float(actual_end_stock),
                    ai_guided_stock=float(ai_guided_end_stock),
                )
            )

        return ProductionSimulationResponse(
            metadata={
                "store_id": payload.store_id,
                "item_id": payload.item_id,
                "date": payload.simulation_date,
                "source": "repository",
                "inventory_rows": len(inventory_rows),
                "production_rows": len(production_rows),
                "sales_rows": len(sales_rows),
            },
            summary_metrics=SimulationSummaryMetrics(
                additional_sales_qty=float(additional_sales_qty),
                additional_profit_amt=additional_profit_amt,
                additional_waste_qty=additional_waste_qty,
                additional_waste_cost=additional_waste_cost,
                net_profit_change=net_profit_change,
                performance_status="POSITIVE" if net_profit_change >= 0 else "NEGATIVE",
                chance_loss_reduction=float(max(additional_profit_amt, 0)),
            ),
            time_series_data=chart_points,
            actions_timeline=timeline,
        )

    async def get_registration_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> ProductionRegistrationSummaryResponse:
        summary = await self.repository.get_registration_summary(
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
        latest = summary.get("latest")
        return ProductionRegistrationSummaryResponse(
            total=int(summary["total"]),
            latest=ProductionRegistrationHistoryItem(**latest) if latest else None,
            total_registered_qty=int(summary["total_registered_qty"]),
            recent_registered_by=list(summary["recent_registered_by"]),
            recent_registration_count_7d=int(summary["recent_registration_count_7d"]),
            recent_registered_qty_7d=int(summary["recent_registered_qty_7d"]),
            affected_sku_count=int(summary["affected_sku_count"]),
            summary_status=str(summary["summary_status"]),
            filtered_store_id=summary.get("filtered_store_id"),
            filtered_date_from=summary.get("filtered_date_from"),
            filtered_date_to=summary.get("filtered_date_to"),
        )

    async def get_fifo_lot_summary(
        self,
        store_id: str,
        lot_type: str | None = None,
        page: int = 1,
        page_size: int = 20,
        date: str | None = None,
    ) -> FifoLotSummaryResponse:
        """점포별 FIFO Lot 품목 요약 조회"""
        rows, total = self.repository.get_fifo_lot_summary(
            store_id=store_id,
            lot_type=lot_type,
            page=page,
            page_size=page_size,
            date=date,
        )
        summary_rows = rows
        if total > len(rows):
            summary_rows, _ = self.repository.get_fifo_lot_summary(
                store_id=store_id,
                lot_type=lot_type,
                page=1,
                page_size=max(total, 1),
                date=date,
            )

        items = [
            FifoLotItem(
                item_nm=r["item_nm"],
                lot_type=r["lot_type"],
                shelf_life_days=r.get("shelf_life_days"),
                last_lot_date=str(r["last_lot_date"]) if r.get("last_lot_date") else None,
                total_initial_qty=float(r.get("total_initial_qty") or 0),
                total_consumed_qty=float(r.get("total_consumed_qty") or 0),
                total_wasted_qty=float(r.get("total_wasted_qty") or 0),
                active_remaining_qty=float(r.get("active_remaining_qty") or 0),
                active_lot_count=int(r.get("active_lot_count") or 0),
                sold_out_lot_count=int(r.get("sold_out_lot_count") or 0),
                expired_lot_count=int(r.get("expired_lot_count") or 0),
            )
            for r in rows
        ]
        summary_items = [
            FifoLotItem(
                item_nm=r["item_nm"],
                lot_type=r["lot_type"],
                shelf_life_days=r.get("shelf_life_days"),
                last_lot_date=str(r["last_lot_date"]) if r.get("last_lot_date") else None,
                total_initial_qty=float(r.get("total_initial_qty") or 0),
                total_consumed_qty=float(r.get("total_consumed_qty") or 0),
                total_wasted_qty=float(r.get("total_wasted_qty") or 0),
                active_remaining_qty=float(r.get("active_remaining_qty") or 0),
                active_lot_count=int(r.get("active_lot_count") or 0),
                sold_out_lot_count=int(r.get("sold_out_lot_count") or 0),
                expired_lot_count=int(r.get("expired_lot_count") or 0),
            )
            for r in summary_rows
        ]

        total_wasted = sum(i.total_wasted_qty for i in summary_items)
        total_active = sum(i.active_remaining_qty for i in summary_items)
        items_with_waste = sum(1 for i in summary_items if i.total_wasted_qty > 0)

        total_pages = max(1, math.ceil(total / page_size))
        return FifoLotSummaryResponse(
            items=items,
            summary={
                "total_items": total,
                "items_with_waste": items_with_waste,
                "total_wasted_qty": total_wasted,
                "total_active_qty": total_active,
            },
            pagination=Pagination(
                page=page,
                page_size=page_size,
                total_items=total,
                total_pages=total_pages,
            ),
        )
