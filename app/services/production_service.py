from __future__ import annotations

from app.core.utils import get_now
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
)
import logging

from app.services.audit_service import AuditService
from app.services.ai_client import AIServiceClient

logger = logging.getLogger(__name__)


class ProductionService:
    _menu_image_index: dict[str, str] | None = None

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
    def _normalize_menu_key(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).strip().lower()
        normalized = re.sub(r"\.[a-z0-9]+$", "", normalized)
        normalized = re.sub(r"_[0-9]+$", "", normalized)
        normalized = re.sub(r"[^0-9a-z가-힣]+", "", normalized)
        return normalized

    @classmethod
    def _menu_image_directories(cls) -> list[Path]:
        repo_root = Path(__file__).resolve().parents[3]
        front_public_dir = repo_root / "br-korea-poc-front" / "public" / "images"
        local_resource_dir = repo_root / "resource" / "05. 던킨도너츠 메뉴"
        docker_resource_dir = Path("/resource/05. 던킨도너츠 메뉴")

        candidates = [front_public_dir, local_resource_dir, docker_resource_dir]
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

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if not math.isfinite(parsed):
            return default
        return parsed

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

        ai_summary = await self.ai_client.generate_grounded_explanation(
            store_id=store_id,
            topic=topic,
            evidence_items=ai_evidence_items,
        )
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

    @staticmethod
    def _decision_for_row(raw: dict) -> ProductionSkuDecision:
        risk_label = "정상"
        if raw["status"] == "danger":
            risk_label = "즉시생산"
        elif raw["status"] == "warning":
            risk_label = "주의"

        current = int(raw.get("current", 0))
        forecast = int(raw.get("forecast", 0))

        # 실데이터 기반 판매 속도: 예상판매량 / 현재고 비율
        if current > 0 and forecast > 0:
            velocity = round(min(3.0, max(0.5, forecast / current)), 1)
        else:
            velocity = 1.2 if raw["status"] != "safe" else 0.9

        # 실데이터 수치를 포함한 알림 메시지
        if raw["status"] == "danger":
            hours = max(1, int(round(current / max(forecast / 8.0, 0.1))))
            alert_msg = f"현재 재고 {current}개, 약 {hours}시간 이내 소진 예상. 즉시 생산이 필요합니다."
        elif raw["status"] == "warning":
            alert_msg = f"현재 재고 {current}개, 예상 판매량 {forecast}개. 생산 준비가 필요합니다."
        else:
            alert_msg = "현재 재고가 안정적입니다."

        return ProductionSkuDecision(
            risk_level_label=risk_label,
            sales_velocity=velocity,
            tags=["속도↑"] if raw["status"] != "safe" else [],
            alert_message=alert_msg,
            can_produce=True,
            predicted_stockout_time=raw["depletion_time"] if raw["depletion_time"] != "-" else None,
            suggested_production_qty=raw["recommended"],
            chance_loss_prevented_amount=raw["recommended"] * 1200 if raw["recommended"] > 0 else None,
        )

    @classmethod
    def _sku_item_from_row(cls, raw: dict) -> ProductionSkuItem:
        def parse_prod(s: str):
            parts = s.split(" / ")
            time = parts[0] if len(parts) > 0 else "00:00"
            qty = int(parts[1].replace("개", "")) if len(parts) > 1 else 0
            return time, qty

        p1_time, p1_qty = parse_prod(raw["prod1"])
        p2_time, p2_qty = parse_prod(raw["prod2"])
        decision = cls._decision_for_row(raw)

        return ProductionSkuItem(
            sku_id=raw["sku_id"],
            sku_name=raw["name"],
            image_url=cls._resolve_image_url(str(raw["name"])),
            current_stock=raw["current"],
            forecast_stock_1h=raw["forecast"],
            avg_first_production_qty_4w=p1_qty,
            avg_first_production_time_4w=p1_time,
            avg_second_production_qty_4w=p2_qty,
            avg_second_production_time_4w=p2_time,
            status=raw["status"],
            chance_loss_saving_pct=cls._calc_chance_loss_pct(raw["current"], raw["forecast"], raw["status"]),
            recommended_production_qty=raw["recommended"],
            chance_loss_basis_text="1시간 후 재고 예측 및 4주(28일) 생산 발생일 평균 기준",
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

    async def _get_sku_items(self, store_id: str | None = None, business_date: str | None = None) -> list[ProductionSkuItem]:
        raw_items = await self.repository.list_items(store_id=store_id, business_date=business_date)
        items = [self._sku_item_from_row(raw) for raw in raw_items]
        return await self._enrich_items_with_ai(items, store_id=store_id)

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
                confidence = float(result.get("confidence") or 0.7)
                updates["chance_loss_saving_pct"] = max(item.chance_loss_saving_pct, int(round(confidence * 20)))

            enriched.append(item.model_copy(update=updates) if updates else item)

        return enriched

    async def get_overview(self, store_id: str | None = None, business_date: str | None = None) -> ProductionOverviewResponse:
        raw_items = await self.repository.list_items(store_id=store_id, business_date=business_date)
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
                ProductionSummaryStat(key="chance_loss_saving_total", label="찬스 로스 절감", value=f"{int(round(sum(item.chance_loss_saving_pct for item in sku_items) / len(sku_items)) if sku_items else 0)}%", tone="default"),
            ],
            alerts=alerts,
            production_lead_time_minutes=60,
            danger_count=danger_count,
            items=[ProductionItem(**item) for item in raw_items],
        )

    async def get_sku_list(
        self,
        page: int = 1,
        page_size: int = 20,
        store_id: str | None = None,
        business_date: str | None = None,
    ) -> GetProductionSkuListResponse:
        items = await self._get_sku_items(store_id=store_id, business_date=business_date)

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

    async def get_sku_detail(self, sku_id: str, store_id: str | None = None, business_date: str | None = None) -> ProductionSkuDetailResponse:
        items = await self._get_sku_items(store_id=store_id, business_date=business_date)
        target = next((item for item in items if item.sku_id == sku_id), None)
        if target is None and store_id is not None:
            # store_id 필터로 매칭 결과가 없으면 전체 조회로 재시도
            logger.info("get_sku_detail: store_id=%s 로 sku_id=%s 없음, 전체 조회로 재시도", store_id, sku_id)
            items = await self._get_sku_items(store_id=None)
            target = next((item for item in items if item.sku_id == sku_id), None)
        if target is None:
            raise ValueError(f"sku not found: {sku_id}")

        return ProductionSkuDetailResponse(
            sku_id=target.sku_id,
            sku_name=target.sku_name,
            image_url=target.image_url,
            current_stock=target.current_stock,
            forecast_stock_1h=target.forecast_stock_1h,
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

    async def get_alerts(self, store_id: str | None = None) -> ProductionAlertsResponse:
        items = await self.repository.list_items(store_id=store_id)
        alerts = [
            ProductionAlertItem(
                sku_id=item["sku_id"],
                name=item["name"],
                current=item["current"],
                forecast=item["forecast"],
                depletion_time=item["depletion_time"],
                recommended=item["recommended"],
                prod1=item["prod1"],
                prod2=item["prod2"],
                severity=item["status"],
                push_title=f"{item['name']} 생산이 필요합니다",
                push_message=(
                    f"현재고 {item['current']}개, 1시간 후 예상 {item['forecast']}개입니다. "
                    f"{item['depletion_time']} 전 소진 가능성이 있어 {item['recommended']}개 생산을 권장합니다."
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
        )

    async def get_waste_summary(self, store_id: str | None = None) -> WasteSummaryResponse:
        if not store_id:
            raise ValueError("store_id is required")

        rows = self.repository.get_stock_rate_recent_rows(store_id=store_id)
        if not rows:
            raise LookupError("해당 점포의 재고율 데이터가 없습니다.")

        by_item: dict[str, dict[int, dict[str, Any]]] = {}
        for row in rows:
            item_key = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            if not item_key:
                continue
            by_item.setdefault(item_key, {})[int(row.get("dr") or 1)] = row

        disuse_rows = self.repository.get_disuse_and_cost_latest_rows(store_id=store_id)
        disuse_map = {
            str(row.get("item_cd") or row.get("item_nm") or "").strip(): row for row in disuse_rows
        }

        items: list[WasteItem] = []
        total_adjusted_loss_amount = 0.0
        total_disuse_amount = 0.0
        total_estimated_expiry_loss_qty = 0.0

        for _, window in by_item.items():
            latest = window.get(1)
            previous = window.get(2)
            pivot = previous or latest
            if not pivot:
                continue

            item_nm = str(pivot.get("item_nm") or "")
            surplus_qty = max(
                self._safe_float(pivot.get("ord_avg")) - self._safe_float(pivot.get("sal_avg")),
                0.0,
            )
            absorb_qty = (
                max(
                    self._safe_float(latest.get("sal_avg")) - self._safe_float(latest.get("ord_avg")),
                    0.0,
                )
                if latest and previous
                else 0.0
            )
            adjusted_loss_qty = max(surplus_qty - absorb_qty, 0.0)

            disuse_row = disuse_map.get(str(pivot.get("item_cd") or item_nm).strip()) or {}
            confirmed_disuse_qty = self._safe_float(disuse_row.get("total_disuse_qty"))
            avg_cost = self._safe_float(disuse_row.get("avg_cost"))
            shelf_life_days = self._assumed_shelf_life_days(item_nm)
            estimated_expiry_loss_qty = (
                adjusted_loss_qty
                if shelf_life_days <= 1
                else round(adjusted_loss_qty * 0.6, 2)
            )
            expiry_risk_level = (
                "높음"
                if shelf_life_days <= 1 and adjusted_loss_qty > 0
                else "중간"
                if adjusted_loss_qty > 0
                else "낮음"
            )
            adjusted_loss_amount = round(adjusted_loss_qty * avg_cost, 2)
            disuse_amount = round(confirmed_disuse_qty * avg_cost, 2)

            total_adjusted_loss_amount += adjusted_loss_amount
            total_disuse_amount += disuse_amount
            total_estimated_expiry_loss_qty += estimated_expiry_loss_qty
            items.append(
                WasteItem(
                    item_nm=item_nm,
                    image_url=self._resolve_image_url(item_nm),
                    adjusted_loss_qty=round(adjusted_loss_qty, 2),
                    confirmed_disuse_qty=round(confirmed_disuse_qty, 2),
                    estimated_expiry_loss_qty=round(estimated_expiry_loss_qty, 2),
                    adjusted_loss_amount=adjusted_loss_amount,
                    disuse_amount=disuse_amount,
                    assumed_shelf_life_days=shelf_life_days,
                    expiry_risk_level=expiry_risk_level,
                )
            )

        items.sort(key=lambda row: row.adjusted_loss_amount, reverse=True)
        if not items:
            raise LookupError("해당 점포의 폐기 손실 데이터가 없습니다.")

        top_item = items[0]
        base_evidence: dict[str, Any] = {
            "summary_reason": "D+1 흡수 보정 로스와 실폐기를 분리 집계했습니다.",
            "processing_route": "repository",
            "fallback_used": False,
            "items": [
                {
                    "label": "보정 로스 수식",
                    "value": f"{round(total_adjusted_loss_amount, 2)}원",
                    "calculation": "adjusted_loss=max((ord_avg_t-sal_avg_t)-max(sal_avg_t+1-ord_avg_t+1,0),0)",
                    "source_table": "core_stock_rate",
                },
                {
                    "label": "실폐기 수량",
                    "value": f"{round(sum(item.confirmed_disuse_qty for item in items), 2)}개",
                    "calculation": "confirmed_disuse=SUM(disuse_qty latest_date)",
                    "source_table": "raw_inventory_extract",
                },
                {
                    "label": "가설 유통기한",
                    "value": "품목군 키워드 규칙",
                    "calculation": "도넛/샌드/샐러드=1일, 케이크=2일, 음료=0일",
                    "source_table": "assumption_rule",
                },
            ],
        }
        evidence = await self._attach_ai_grounded_summary(
            store_id=store_id,
            topic="폐기 손실 분석",
            evidence=base_evidence,
        )

        return WasteSummaryResponse(
            items=items,
            total_adjusted_loss_amount=round(total_adjusted_loss_amount, 2),
            total_disuse_amount=round(total_disuse_amount, 2),
            total_estimated_expiry_loss_qty=round(total_estimated_expiry_loss_qty, 2),
            summary={
                "store_id": store_id,
                "item_count": len(items),
                "gap_amount": round(total_adjusted_loss_amount - total_disuse_amount, 2),
            },
            highlights=[
                {
                    "label": "보정 로스 최대 품목",
                    "item_nm": top_item.item_nm,
                    "value": top_item.adjusted_loss_amount,
                },
                {
                    "label": "유통기한 가설 손실 최대 품목",
                    "item_nm": max(items, key=lambda row: row.estimated_expiry_loss_qty).item_nm,
                    "value": max(items, key=lambda row: row.estimated_expiry_loss_qty).estimated_expiry_loss_qty,
                },
            ],
            actions=[
                "보정 로스 상위 3개 품목의 다음날 발주량을 하향 조정하세요.",
                "가정 유통기한 1일 품목은 당일 마감 2시간 전 판촉/세트 전환을 적용하세요.",
                "실폐기와 보정로스 차이가 큰 품목은 재고 등록 정확도를 점검하세요.",
            ],
            evidence=evidence,
        )

    async def get_inventory_status(self, store_id: str | None = None) -> InventoryStatusResponse:
        if not store_id:
            raise ValueError("store_id is required")

        rows = self.repository.get_stock_rate_recent_rows(store_id=store_id)
        if not rows:
            raise LookupError("해당 점포의 재고 진단 데이터가 없습니다.")

        latest_rows = [row for row in rows if int(row.get("dr") or 1) == 1]
        if not latest_rows:
            raise LookupError("해당 점포의 최신 재고율 데이터가 없습니다.")

        stockout_rows = self.repository.get_stockout_latest_rows(store_id=store_id)
        stockout_map = {
            str(row.get("item_cd") or row.get("item_nm") or "").strip(): row for row in stockout_rows
        }

        items: list[InventoryStatusItem] = []
        shortage_count = 0
        excess_count = 0
        normal_count = 0

        for row in latest_rows:
            item_cd = str(row.get("item_cd") or row.get("item_nm") or "").strip()
            item_nm = str(row.get("item_nm") or item_cd)
            total_stock = self._safe_float(row.get("stk_avg"))
            total_sold = self._safe_float(row.get("sal_avg"))
            total_orderable = self._safe_float(row.get("ord_avg"))
            stock_rate = self._safe_float(row.get("stk_rt"))
            stockout = stockout_map.get(item_cd) or stockout_map.get(item_nm) or {}
            is_stockout = bool(stockout.get("is_stockout"))
            stockout_hour = int(stockout["stockout_hour"]) if stockout.get("stockout_hour") is not None else None

            if is_stockout or stock_rate < 0:
                status = "부족"
            elif stock_rate >= 0.35:
                status = "과잉"
            else:
                status = "적정"

            if status == "부족":
                shortage_count += 1
            elif status == "과잉":
                excess_count += 1
            else:
                normal_count += 1

            shelf_life_days = self._assumed_shelf_life_days(item_nm)
            expiry_risk_level = (
                "높음"
                if shelf_life_days <= 1 and stock_rate > 0.25
                else "중간"
                if stock_rate > 0.15
                else "낮음"
            )

            items.append(
                InventoryStatusItem(
                    item_cd=item_cd,
                    item_nm=item_nm,
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
            [item for item in items if item.status == "과잉"],
            key=lambda row: row.stock_rate,
            reverse=True,
        )[:3]

        base_evidence: dict[str, Any] = {
            "summary_reason": "재고율, 품절시간, 판매가능수량을 함께 반영해 상태를 분류했습니다.",
            "processing_route": "repository",
            "fallback_used": False,
            "items": [
                {
                    "label": "재고율 기반 분류",
                    "value": f"부족 {shortage_count}개 / 과잉 {excess_count}개",
                    "calculation": "stock_rate<0 또는 품절=true: 부족, stock_rate>=0.35: 과잉, 그 외 적정",
                    "source_table": "core_stock_rate, core_stockout_time",
                },
                {
                    "label": "가설 유통기한",
                    "value": "품목군 키워드 규칙",
                    "calculation": "도넛/샌드/샐러드=1일, 케이크=2일, 음료=0일",
                    "source_table": "assumption_rule",
                },
            ],
        }
        evidence = await self._attach_ai_grounded_summary(
            store_id=store_id,
            topic="재고 수준 진단",
            evidence=base_evidence,
        )

        return InventoryStatusResponse(
            summary={
                "store_id": store_id,
                "item_count": len(items),
                "shortage_count": shortage_count,
                "excess_count": excess_count,
                "normal_count": normal_count,
                "avg_stock_rate": round(sum(item.stock_rate for item in items) / max(len(items), 1), 3),
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
                    "label": "과잉 위험 TOP",
                    "value": f"{item.item_nm} ({item.stock_rate:.2f})",
                }
                for item in top_excess
            ],
            actions=[
                "부족 품목은 다음 생산/발주 사이클을 앞당겨 품절 시각을 늦추세요.",
                "과잉 품목은 다음날 발주량을 축소하고, 프로모션/세트로 소진을 유도하세요.",
                "유통기한 가설 1일 품목은 마감 전 재고 소진 우선순위를 높이세요.",
            ],
            evidence=evidence,
            items=items,
        )

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
