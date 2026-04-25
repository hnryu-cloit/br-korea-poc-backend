from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.utils import get_now
from app.repositories.ordering_repository import OrderingRepository
from app.schemas.ordering import (
    OrderingAlertsResponse,
    OrderingContextResponse,
    OrderingDeadlineAlert,
    OrderingDeadlineItem,
    OrderingHistoryAnomalyItem,
    OrderingHistoryChangedItem,
    OrderingHistoryItem,
    OrderingHistoryInsightKpi,
    OrderingHistoryInsightsResponse,
    OrderingHistoryResponse,
    OrderingOptionsResponse,
    OrderingWeather,
    OrderOption,
    OrderSelectionHistoryItem,
    OrderSelectionHistoryResponse,
    OrderSelectionRequest,
    OrderSelectionResponse,
    OrderSelectionSummaryResponse,
)
from app.schemas.simulation import SimulationInput, SimulationResponse
from app.services.ai_client import AIServiceClient
from app.services.audit_service import AuditService
from app.services.explainability_service import create_ready_payload

_KST = timezone(timedelta(hours=9))
_DEFAULT_DEADLINE_HOUR = 12
_DEFAULT_DEADLINE_MINUTE = 0
_ALERT_THRESHOLD_MINUTES = 20
_DEFAULT_ORDERING_STORE_ID = "POC_010"
_DEFAULT_ORDERING_HISTORY_REFERENCE = datetime(2026, 3, 5, 9, 0, 0)


def _now_kst() -> datetime:
    """현재 KST 시각 반환 (MOCK_NOW 반영)."""
    return get_now()


def _minutes_to_deadline(
    now: datetime,
    deadline_hour: int = _DEFAULT_DEADLINE_HOUR,
    deadline_minute: int = _DEFAULT_DEADLINE_MINUTE,
) -> int:
    """마감까지 남은 분 수 반환 (마감 지났으면 음수)."""
    deadline = now.replace(hour=deadline_hour, minute=deadline_minute, second=0, microsecond=0)
    return int((deadline - now).total_seconds() / 60)


class OrderingService:
    def __init__(
        self,
        repository: OrderingRepository,
        audit_service: AuditService | None = None,
        ai_client: AIServiceClient | None = None,
    ) -> None:
        self.repository = repository
        self.audit_service = audit_service
        self.ai_client = ai_client
        self.history_insights_cache_path = (
            Path(__file__).resolve().parents[2] / "data" / "ordering_history_insights_cache.json"
        )

    @staticmethod
    def _sort_history_anomalies(anomalies: list[dict]) -> list[dict]:
        priority = {"high": 0, "medium": 1, "low": 2}
        return sorted(
            anomalies,
            key=lambda anomaly: priority.get(str(anomaly.get("severity", "")).lower(), 99),
        )

    @staticmethod
    def _parse_history_date(value: str | None) -> datetime.date | None:
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _today_kst(reference_datetime: datetime | None = None) -> str:
        base = reference_datetime or _now_kst()
        return base.strftime("%Y-%m-%d")

    @staticmethod
    def _resolve_history_reference_datetime(reference_datetime: datetime | None) -> datetime:
        return reference_datetime or _DEFAULT_ORDERING_HISTORY_REFERENCE

    @staticmethod
    def _safe_str(value: object | None) -> str | None:
        if value in (None, ""):
            return None
        return str(value)

    @staticmethod
    def _metric(key: str, value: object) -> dict[str, str]:
        return {"key": key, "value": str(value)}

    @staticmethod
    def _safe_int(value: object | None) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_deadline_time(value: str | None) -> tuple[int, int] | None:
        if not value:
            return None
        text = value.strip()
        if ":" not in text:
            return None
        hour_str, minute_str = text.split(":", 1)
        if not hour_str.isdigit() or not minute_str.isdigit():
            return None
        hour = int(hour_str)
        minute = int(minute_str)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
        return hour, minute

    @classmethod
    def _extract_deadline_label(cls, *, note: str | None, arrival: dict[str, str] | None) -> str | None:
        arrival_deadline = cls._safe_str((arrival or {}).get("order_deadline_at"))
        if cls._parse_deadline_time(arrival_deadline):
            return str(arrival_deadline).strip()

        if not note:
            return None
        matched = re.search(r"마감\s*([0-2]?\d:[0-5]\d)", note)
        if not matched:
            return None
        parsed = cls._parse_deadline_time(matched.group(1))
        if parsed is None:
            return None
        return f"{parsed[0]:02d}:{parsed[1]:02d}"

    @classmethod
    def _normalize_weather_payload(cls, payload: object | None) -> OrderingWeather | None:
        if not isinstance(payload, dict):
            return None

        region = cls._safe_str(payload.get("region") or payload.get("sido"))
        forecast_date = cls._safe_str(payload.get("forecast_date") or payload.get("date"))
        weather_type = cls._safe_str(payload.get("weather_type") or payload.get("type"))

        if not region or not forecast_date or not weather_type:
            return None

        return OrderingWeather(
            region=region,
            forecast_date=forecast_date,
            weather_type=weather_type,
            max_temperature_c=cls._safe_int(payload.get("max_temperature_c") or payload.get("max_temp")),
            min_temperature_c=cls._safe_int(payload.get("min_temperature_c") or payload.get("min_temp")),
            precipitation_probability=cls._safe_int(
                payload.get("precipitation_probability") or payload.get("rain_probability")
            ),
        )

    @staticmethod
    def _build_option_item_note(
        *,
        base_note: str | None,
        arrival: dict[str, str] | None,
        shelf_life_days: int | None,
    ) -> str | None:
        segments: list[str] = []
        if base_note:
            segments.append(base_note)
        if arrival:
            deadline = str(arrival.get("order_deadline_at") or "").strip()
            day_offset = str(arrival.get("arrival_day_offset") or "").strip()
            expected_at = str(arrival.get("arrival_expected_at") or "").strip()
            if deadline:
                segments.append(f"마감 {deadline}")
            if day_offset or expected_at:
                arrival_label = f"{day_offset} {expected_at}".strip()
                segments.append(f"도착 {arrival_label}")
        if shelf_life_days is not None:
            segments.append(f"유통기한 {shelf_life_days}일")
        if not segments:
            return None
        return " · ".join(segments)

    @staticmethod
    def _matches_business_date(weather: OrderingWeather | None, business_date: str | None) -> bool:
        if weather is None or not business_date:
            return False
        return str(weather.forecast_date).replace("-", "") == str(business_date).replace("-", "")

    def _require_history_store_id(self, store_id: str | None) -> str:
        normalized = (store_id or "").strip()
        if not normalized:
            raise ValueError("store_id is required")
        if not self.repository.is_known_store(normalized):
            raise ValueError(f"Unknown store_id: {normalized}")
        return normalized

    async def _get_ai_ordering_recommendation(
        self,
        store_id: str,
        current_date: str,
        current_context: dict[str, object] | None = None,
    ) -> dict | None:
        if not self.ai_client:
            return None
        return await self.ai_client.recommend_ordering(
            store_id=store_id,
            current_date=current_date,
            current_context=current_context,
        )

    async def _get_ai_deadline_alert(self, store_id: str) -> dict | None:
        if not self.ai_client:
            return None
        return await self.ai_client.get_ordering_deadline_alert(store_id)

    async def get_deadline_alerts_batch(self, store_ids: list[str]) -> list[dict]:
        """여러 매장의 주문 마감 알림을 일괄 조회합니다."""
        normalized_store_ids = [store_id.strip() for store_id in store_ids if store_id and store_id.strip()]
        if not normalized_store_ids:
            return []
        if self.ai_client:
            return await self.ai_client.get_ordering_deadline_alerts_batch(normalized_store_ids)
        results: list[dict] = []
        for store_id in normalized_store_ids:
            local_deadline = await self.get_deadline(store_id=store_id)
            results.append(
                {
                    "store_id": store_id,
                    "deadline": local_deadline["deadline"],
                    "minutes_remaining": local_deadline["minutes_remaining"],
                    "alert_level": "passed"
                    if local_deadline["is_passed"]
                    else ("urgent" if local_deadline["is_urgent"] else "normal"),
                    "message": "주문 마감 정보를 확인해 주세요.",
                    "should_alert": bool(local_deadline["is_urgent"] and not local_deadline["is_passed"]),
                }
            )
        return results

    def _merge_option_payloads(self, option: dict, ai_option: dict | None = None, index: int = 0) -> dict:
        fallback_id = option.get("option_id") or f"opt-{chr(97 + index)}"
        ai_reasoning = OrderingService._safe_str(ai_option.get("reasoning_text") if ai_option else None)
        ai_metrics = ai_option.get("reasoning_metrics") if ai_option else None
        ai_special_factors = ai_option.get("special_factors") if ai_option else None
        option_basis = OrderingService._safe_str(option.get("basis"))
        merged = {
            "option_id": OrderingService._safe_str(ai_option.get("option_id") if ai_option else None) or fallback_id,
            "title": OrderingService._safe_str(ai_option.get("title") if ai_option else None) or option.get("title") or f"추천안 {index + 1}",
            "basis": option_basis or OrderingService._safe_str(ai_option.get("basis") if ai_option else None) or "-",
            "description": OrderingService._safe_str(ai_option.get("description") if ai_option else None) or option.get("description") or "",
            "recommended": bool((ai_option or {}).get("recommended", option.get("recommended", index == 0))),
            "reasoning_text": ai_reasoning or option.get("reasoning_text") or option.get("description") or "",
            "reasoning_metrics": option.get("reasoning_metrics") or [],
            "special_factors": option.get("special_factors") or [],
            "items": option.get("items") or [],
        }
        if not merged["reasoning_metrics"]:
            total_qty = sum(item.get("quantity", 0) for item in merged["items"])
            if total_qty:
                merged["reasoning_metrics"] = [
                    self._metric("total_qty", f"{total_qty}개"),
                    self._metric("line_count", f"{len(merged['items'])}개 SKU"),
                ]
        return merged

    @staticmethod
    def _derive_focus_option_id(options: list[dict]) -> str | None:
        if not options:
            return None
        focus = next((option for option in options if option.get("recommended")), options[0])
        return OrderingService._safe_str(focus.get("option_id"))

    @staticmethod
    def _build_ai_option_summaries(options: list[dict]) -> list[dict[str, object]]:
        summaries: list[dict[str, object]] = []
        for option in options:
            if not isinstance(option, dict):
                continue
            items = [item for item in (option.get("items") or []) if isinstance(item, dict)]
            total_qty = sum(int(item.get("quantity") or 0) for item in items)
            summaries.append(
                {
                    "option_id": str(option.get("option_id") or ""),
                    "title": str(option.get("title") or ""),
                    "basis": str(option.get("basis") or ""),
                    "total_qty": total_qty,
                    "line_count": len(items),
                    "reasoning_metrics": option.get("reasoning_metrics") or [],
                    "special_factors": option.get("special_factors") or [],
                }
            )
        return summaries

    async def list_options(
        self,
        notification_entry: bool = False,
        store_id: str | None = None,
        skip_ai: bool = False,
        reference_datetime: datetime | None = None,
    ) -> OrderingOptionsResponse:
        normalized_store_id = (store_id or _DEFAULT_ORDERING_STORE_ID).strip() or _DEFAULT_ORDERING_STORE_ID
        business_date = self._today_kst(reference_datetime)
        options = await self.repository.list_options(store_id=normalized_store_id)
        arrival_schedule = None
        get_order_arrival_schedule = getattr(self.repository, "get_order_arrival_schedule", None)
        if callable(get_order_arrival_schedule):
            arrival_schedule = get_order_arrival_schedule(store_id=normalized_store_id)
        ai_current_context = {
            "trend_summary": (
                f"납품 기준: 주문 마감 {self._safe_str((arrival_schedule or {}).get('order_deadline_at')) or '-'}, "
                f"도착 {self._safe_str((arrival_schedule or {}).get('arrival_day_offset')) or '-'} "
                f"{self._safe_str((arrival_schedule or {}).get('arrival_expected_at')) or '-'}"
            ).strip(),
            "option_summaries": self._build_ai_option_summaries(options),
        }
        options = await self.repository.list_options(
            store_id=normalized_store_id,
            reference_date=business_date,
        )
        use_ai_ordering = not skip_ai and not self.repository.uses_ordering_join_table(normalized_store_id)
        ai_payload = (
            None
            if skip_ai
            else await self._get_ai_ordering_recommendation(
                store_id=normalized_store_id,
                current_date=business_date,
                current_context=ai_current_context,
            )
            if not use_ai_ordering
            else await self._get_ai_ordering_recommendation(store_id=normalized_store_id, current_date=business_date)
        )
        ai_options = (ai_payload or {}).get("options") or []
        merged_options = [
            self._merge_option_payloads(option, ai_options[index] if index < len(ai_options) else None, index=index)
            for index, option in enumerate(options)
        ]
        sku_ids = [
            str(item.get("sku_id") or "").strip()
            for option in merged_options
            for item in (option.get("items") or [])
            if isinstance(item, dict)
        ]
        sku_names = [
            str(item.get("sku_name") or "").strip()
            for option in merged_options
            for item in (option.get("items") or [])
            if isinstance(item, dict)
        ]
        schedule_map: dict[str, dict[str, str]] = {}
        shelf_life_map: dict[str, int] = {}
        get_order_arrival_schedule_map = getattr(self.repository, "get_order_arrival_schedule_map", None)
        if callable(get_order_arrival_schedule_map):
            schedule_map = get_order_arrival_schedule_map(
                store_id=normalized_store_id,
                item_codes=sku_ids,
                item_names=sku_names,
            )
        get_shelf_life_days_map = getattr(self.repository, "get_shelf_life_days_map", None)
        if callable(get_shelf_life_days_map):
            shelf_life_map = get_shelf_life_days_map(
                item_codes=sku_ids,
                item_names=sku_names,
            )

        for option in merged_options:
            items = option.get("items") or []
            for item in items:
                if not isinstance(item, dict):
                    continue
                sku_id = str(item.get("sku_id") or "").strip()
                sku_name = str(item.get("sku_name") or "").strip()
                arrival = schedule_map.get(sku_id) or schedule_map.get(sku_name)
                shelf_life_days = shelf_life_map.get(sku_id)
                if shelf_life_days is None:
                    shelf_life_days = shelf_life_map.get(sku_name)
                item["note"] = self._build_option_item_note(
                    base_note=self._safe_str(item.get("note")),
                    arrival=arrival,
                    shelf_life_days=shelf_life_days,
                )

        deadline_items: list[dict[str, object]] = []
        seen_deadline_keys: set[str] = set()
        for option in merged_options:
            for item in option.get("items") or []:
                if not isinstance(item, dict):
                    continue
                sku_id = str(item.get("sku_id") or "").strip()
                sku_name = str(item.get("sku_name") or "").strip()
                if not sku_name:
                    continue
                arrival = schedule_map.get(sku_id) or schedule_map.get(sku_name)
                deadline_label = self._extract_deadline_label(
                    note=self._safe_str(item.get("note")),
                    arrival=arrival,
                )
                if not deadline_label:
                    continue
                dedupe_key = sku_id or sku_name
                if dedupe_key in seen_deadline_keys:
                    continue
                seen_deadline_keys.add(dedupe_key)
                deadline_items.append(
                    {
                        "id": dedupe_key,
                        "sku_name": sku_name,
                        "deadline_at": deadline_label,
                        "is_ordered": False,
                    }
                )

        deadline_minutes = 20
        deadline_at: str | None = None
        purpose_text = "주문 누락을 방지하고 최적 수량을 선택하세요."
        caution_text = "최종 주문 결정은 점주 권한입니다. 추천 옵션은 보조 자료로만 활용해주세요."
        weather: OrderingWeather | None = None
        trend_summary = self.repository.get_ordering_trend_summary(
            store_id=normalized_store_id,
            reference_date=business_date,
        )

        if ai_payload:
            deadline_minutes = int(ai_payload.get("deadline_minutes") or deadline_minutes)
            deadline_at = self._safe_str(ai_payload.get("deadline_at"))
            purpose_text = self._safe_str(ai_payload.get("purpose_text")) or purpose_text
            caution_text = (
                self._safe_str(ai_payload.get("caution_text") or ai_payload.get("guardrail_note"))
                or caution_text
            )
            weather = self._normalize_weather_payload(ai_payload.get("weather"))
            trend_summary = self._safe_str(ai_payload.get("trend_summary") or ai_payload.get("reasoning"))
            if weather is not None and not self._matches_business_date(weather, business_date):
                weather = None

        if trend_summary is None:
            if arrival_schedule:
                deadline_label = self._safe_str(arrival_schedule.get("order_deadline_at")) or "-"
                day_offset_label = self._safe_str(arrival_schedule.get("arrival_day_offset")) or "-"
                arrival_time_label = self._safe_str(arrival_schedule.get("arrival_expected_at")) or "-"
                trend_summary = (
                    f"납품 기준: 주문 마감 {deadline_label}, 도착 {day_offset_label} {arrival_time_label}".strip()
                )

        if weather is None:
            weather_payload = await self.repository.get_weather_forecast(
                store_id=normalized_store_id,
                reference_date=business_date,
            )
            weather = self._normalize_weather_payload(weather_payload)

        if deadline_at is None:
            deadline = await self.get_deadline(store_id=normalized_store_id, reference_datetime=reference_datetime)
            deadline_at = deadline["deadline"]
            deadline_minutes = deadline["minutes_remaining"]
        deadline_items = self.repository.get_deadline_items(
            store_id=normalized_store_id,
            reference_datetime=reference_datetime,
        )

        return OrderingOptionsResponse(
            deadline_minutes=deadline_minutes,
            deadline_at=deadline_at,
            notification_entry=notification_entry,
            purpose_text=purpose_text,
            caution_text=caution_text,
            weather=weather,
            trend_summary=trend_summary,
            business_date=business_date,
            deadline_items=[OrderingDeadlineItem(**item) for item in deadline_items],
            options=[OrderOption(**o) for o in merged_options],
            explainability=create_ready_payload(
                trace_id=f"ordering-options-{normalized_store_id}",
                actions=[
                    "추천안 3개를 비교한 뒤 최종 주문안을 확정하세요.",
                    "마감 전 재고 부족 위험 품목을 우선 점검하세요.",
                ],
                evidence=[
                    f"주문 마감: {deadline_at}",
                    f"남은 시간: {deadline_minutes}분",
                    f"추천 옵션 수: {len(merged_options)}",
                ],
            ),
        )

    async def get_notification_context(self, notification_id: int, store_id: str | None = None) -> OrderingContextResponse:
        alerts = await self.list_deadline_alerts(store_id=store_id)
        matched = next((alert for alert in alerts.alerts if alert.notification_id == notification_id), None)
        if matched:
            return OrderingContextResponse(
                notification_id=notification_id,
                target_path=matched.target_path,
                focus_option_id=matched.focus_option_id,
                message=matched.message,
            )
        context = await self.repository.get_notification_context(notification_id)
        return OrderingContextResponse(**context)

    async def list_deadline_alerts(
        self,
        before_minutes: int = 20,
        store_id: str | None = None,
        reference_datetime: datetime | None = None,
    ) -> OrderingAlertsResponse:
        options = await self.repository.list_options(
            store_id=store_id,
            reference_date=self._today_kst(reference_datetime),
        )
        focus_option_id = self._derive_focus_option_id(options)
        alerts: list[OrderingDeadlineAlert] = []
        ai_deadline = await self._get_ai_deadline_alert(store_id)
        if ai_deadline is not None:
            deadline_minutes = int(ai_deadline.get("deadline_minutes") or ai_deadline.get("minutes_remaining") or before_minutes)
            alerts.append(
                OrderingDeadlineAlert(
                    notification_id=int(ai_deadline.get("notification_id") or 2),
                    title=self._safe_str(ai_deadline.get("title")) or f"주문 마감 {deadline_minutes}분 전입니다",
                    message=self._safe_str(ai_deadline.get("message")) or "주문 추천안을 확인해 주세요.",
                    deadline_minutes=deadline_minutes,
                    target_path=self._safe_str(ai_deadline.get("target_path")) or "/ordering",
                    focus_option_id=self._safe_str(ai_deadline.get("focus_option_id")) or focus_option_id,
                    target_roles=list(ai_deadline.get("target_roles") or ["store_owner"]),
                )
            )
        elif focus_option_id is not None:
            focus_option = next((option for option in options if option.get("option_id") == focus_option_id), options[0] if options else None)
            if focus_option is not None:
                alerts.append(
                    OrderingDeadlineAlert(
                        notification_id=2,
                        title=f"주문 마감 {before_minutes}분 전입니다",
                        message=f"{focus_option['title']} 옵션을 우선 확인해 주세요. 추천 주문 수량 {len(options)}개 옵션이 준비되었습니다.",
                        deadline_minutes=before_minutes,
                        target_path="/ordering",
                        focus_option_id=focus_option_id,
                        target_roles=["store_owner"],
                    )
                )
        return OrderingAlertsResponse(
            generated_at=(reference_datetime or _now_kst()).strftime("%Y-%m-%d %H:%M:%S"),
            alerts=alerts,
            explainability=create_ready_payload(
                trace_id=f"ordering-alerts-{store_id or 'default'}",
                actions=["주문 마감 알림을 확인하고 주문 화면으로 즉시 이동하세요."],
                evidence=[f"알림 건수: {len(alerts)}"],
            ),
        )

    async def get_deadline(
        self,
        store_id: str | None = None,
        deadline_hour: int = _DEFAULT_DEADLINE_HOUR,
        deadline_minute: int = _DEFAULT_DEADLINE_MINUTE,
        reference_datetime: datetime | None = None,
    ) -> dict:
        """주문 마감까지 남은 시간 정보를 반환합니다."""
        sid = store_id or "default"
        ai_deadline = await self._get_ai_deadline_alert(store_id)
        if ai_deadline is not None:
            minutes_remaining = int(ai_deadline.get("minutes_remaining") or ai_deadline.get("deadline_minutes") or 0)
            alert_level = self._safe_str(ai_deadline.get("alert_level")) or "normal"
            deadline = self._safe_str(ai_deadline.get("deadline")) or f"{deadline_hour:02d}:{deadline_minute:02d}"
            return {
                "store_id": sid,
                "deadline": deadline,
                "minutes_remaining": max(0, minutes_remaining),
                "is_urgent": alert_level == "urgent",
                "is_passed": alert_level == "passed",
            }

        arrival_schedule: dict[str, object] | None = None
        get_order_arrival_schedule = getattr(self.repository, "get_order_arrival_schedule", None)
        if callable(get_order_arrival_schedule):
            arrival_schedule = get_order_arrival_schedule(store_id=store_id)
        schedule_deadline = self._parse_deadline_time(
            self._safe_str((arrival_schedule or {}).get("order_deadline_at"))
        )
        if schedule_deadline is not None:
            deadline_hour, deadline_minute = schedule_deadline

        now = reference_datetime or _now_kst()
        delta = _minutes_to_deadline(now, deadline_hour, deadline_minute)
        deadline_str = f"{deadline_hour:02d}:{deadline_minute:02d}"
        return {
            "store_id": sid,
            "deadline": deadline_str,
            "minutes_remaining": max(0, delta),
            "is_urgent": 0 <= delta <= _ALERT_THRESHOLD_MINUTES,
            "is_passed": delta < 0,
        }

    async def save_selection(self, payload: OrderSelectionRequest) -> OrderSelectionResponse:
        saved = await self.repository.save_selection(payload.model_dump())
        if self.audit_service:
            await self.audit_service.record(
                domain="ordering",
                event_type="order_selection_saved",
                actor_role=payload.actor_role,
                route="api",
                outcome="success",
                message=f"{payload.option_id} 주문 선택을 저장했습니다.",
                metadata={"reason_provided": bool(payload.reason), "option_id": payload.option_id},
            )
        return OrderSelectionResponse(
            selection_id=f"sel-{payload.option_id}-{datetime.now().strftime('%H%M%S')}",
            option_id=payload.option_id,
            reason=payload.reason,
            saved=saved.get("saved", True),
        )

    async def list_selection_history(
        self,
        limit: int = 20,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> OrderSelectionHistoryResponse:
        items = await self.repository.list_selection_history(
            limit=limit,
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
        return OrderSelectionHistoryResponse(
            items=[OrderSelectionHistoryItem(**item) for item in items],
            total=len(items),
            filtered_store_id=store_id,
            filtered_date_from=date_from,
            filtered_date_to=date_to,
        )

    async def get_selection_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> OrderSelectionSummaryResponse:
        summary = await self.repository.get_selection_summary(store_id=store_id, date_from=date_from, date_to=date_to)
        latest = summary.get("latest")
        return OrderSelectionSummaryResponse(
            total=int(summary["total"]),
            latest=OrderSelectionHistoryItem(**latest) if latest else None,
            recommended_selected=bool(summary["recommended_selected"]),
            recent_actor_roles=list(summary["recent_actor_roles"]),
            recent_selection_count_7d=int(summary["recent_selection_count_7d"]),
            option_counts=dict(summary["option_counts"]),
            summary_status=str(summary["summary_status"]),
            filtered_store_id=summary.get("filtered_store_id"),
            filtered_date_from=summary.get("filtered_date_from"),
            filtered_date_to=summary.get("filtered_date_to"),
        )

    async def simulate(self, payload: SimulationInput) -> SimulationResponse:
        expected_patients = round(payload.expected_leads * payload.close_rate, 1)
        per_patient_revenue = (
            payload.promo_price
            + payload.upsell_rate * payload.average_upsell_revenue
            + payload.repeat_visit_rate * payload.repeat_visit_revenue
        )
        expected_revenue = round(expected_patients * per_patient_revenue, 0)
        expected_cost = round(expected_patients * payload.procedure_cost + payload.ad_budget, 0)
        projected_profit = round(expected_revenue - expected_cost, 0)
        contribution_margin = max(per_patient_revenue - payload.procedure_cost, 1)
        break_even_patients = round(payload.ad_budget / contribution_margin, 1)
        allowed_ad_budget = round(max(expected_revenue - expected_patients * payload.procedure_cost, 0), 0)
        return SimulationResponse(
            promotion_name=payload.promotion_name,
            expected_patients=expected_patients,
            expected_revenue=expected_revenue,
            expected_cost=expected_cost,
            projected_profit=projected_profit,
            break_even_patients=break_even_patients,
            allowed_ad_budget=allowed_ad_budget,
            breakeven_reached=expected_patients >= break_even_patients,
        )

    def get_history(
        self,
        *,
        store_id: str | None = None,
        limit: int | None = None,
        page: int = 1,
        page_size: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
        item_nm: str | None = None,
        is_auto: bool | None = None,
        reference_datetime: datetime | None = None,
    ):
        normalized_store_id = self._require_history_store_id(store_id)
        resolved_reference_datetime = self._resolve_history_reference_datetime(reference_datetime)
        resolved_page_size = limit if limit is not None else page_size
        data = self.repository.get_history_filtered(
            store_id=normalized_store_id,
            limit=resolved_page_size,
            page=page,
            date_from=date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            reference_datetime=resolved_reference_datetime,
        )
        return OrderingHistoryResponse(
            items=[OrderingHistoryItem(**item) for item in data["items"]],
            auto_rate=data["auto_rate"],
            manual_rate=data["manual_rate"],
            total_count=data["total_count"],
            page=int(data.get("page", page)),
            page_size=int(data.get("page_size", resolved_page_size)),
            total_pages=int(data.get("total_pages", 1)),
            explainability=create_ready_payload(
                trace_id=f"ordering-history-{normalized_store_id}",
                actions=["자동/수동 발주 비중을 점검하고 다음 주문 기준을 조정하세요."],
                evidence=[
                    f"자동 비율: {data['auto_rate']:.2f}",
                    f"수동 비율: {data['manual_rate']:.2f}",
                    f"조회 건수: {data['total_count']}",
                    "주문/납품 기준 데이터: raw_order_arrival_schedule",
                    "SKU 유통기한 기준 데이터: raw_product_shelf_life",
                ],
            ),
        )

    @staticmethod
    def _build_history_summary_stats(
        items: list[OrderingHistoryItem],
        total_count: int,
        auto_rate: float,
        manual_rate: float,
        comparison_items: list[OrderingHistoryItem] | None = None,
        recent_date_from: str | None = None,
        recent_date_to: str | None = None,
    ) -> dict[str, object]:
        ord_values = [int(item.ord_qty or 0) for item in items if item.ord_qty is not None]
        avg_ord_qty = round((sum(ord_values) / len(ord_values)) if ord_values else 0.0, 2)

        confirm_gap_count = len(
            [
                item
                for item in items
                if item.ord_qty not in (None, 0)
                and item.confrm_qty is not None
                and abs((item.confrm_qty or 0) - (item.ord_qty or 0))
                / max(int(item.ord_qty or 1), 1)
                >= 0.3
            ]
        )

        grouped: dict[str, list[tuple[datetime.date, int]]] = {}
        comparison_source = comparison_items or items
        for item in comparison_source:
            if not item.item_nm or item.ord_qty is None:
                continue
            item_date = OrderingService._parse_history_date(item.dlv_dt)
            if item_date is None:
                continue
            grouped.setdefault(item.item_nm, []).append((item_date, int(item.ord_qty)))

        recent_start = OrderingService._parse_history_date(recent_date_from)
        recent_end = OrderingService._parse_history_date(recent_date_to)
        top_changed_items: list[dict[str, object]] = []
        for item_nm, dated_qty_values in grouped.items():
            dated_qty_values.sort(key=lambda row: row[0], reverse=True)
            recent_candidates = dated_qty_values
            if recent_start is not None or recent_end is not None:
                recent_candidates = [
                    (item_date, qty)
                    for item_date, qty in dated_qty_values
                    if (recent_start is None or item_date >= recent_start)
                    and (recent_end is None or item_date <= recent_end)
                ]
            if not recent_candidates:
                continue
            latest_date, latest = max(recent_candidates, key=lambda row: row[0])
            baseline_start = latest_date - timedelta(days=28)
            baseline_end = latest_date - timedelta(days=1)
            baseline_values = [
                qty
                for item_date, qty in dated_qty_values
                if baseline_start <= item_date <= baseline_end
            ]
            if not baseline_values:
                continue
            baseline = sum(baseline_values) / max(len(baseline_values), 1)
            if baseline <= 0:
                continue
            change_ratio = round((latest - baseline) / baseline, 4)
            if abs(change_ratio) <= 0:
                continue
            top_changed_items.append(
                {
                    "item_nm": item_nm,
                    "avg_ord_qty": round(baseline, 2),
                    "latest_ord_qty": int(latest),
                    "change_ratio": change_ratio,
                }
            )
        top_changed_items.sort(key=lambda row: abs(float(row.get("change_ratio", 0))), reverse=True)

        return {
            "total_count": total_count,
            "auto_rate": auto_rate,
            "manual_rate": manual_rate,
            "avg_order_qty": avg_ord_qty,
            "confirm_gap_count": confirm_gap_count,
            "top_changed_items_preview": top_changed_items[:10],
        }

    @staticmethod
    def _build_history_insights_cache_key(
        *,
        store_id: str,
        filters: dict[str, object],
        history_items: list[dict[str, object]],
        comparison_history_items: list[dict[str, object]] | None = None,
    ) -> str:
        payload = {
            "version": 2,
            "store_id": store_id,
            "filters": filters,
            "history_items": history_items,
            "comparison_history_items": comparison_history_items or [],
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _load_cached_history_insights(self, cache_key: str) -> dict | None:
        cache_path = self.history_insights_cache_path
        if not cache_path.exists():
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return None
        if not isinstance(payload, dict):
            return None
        cached = payload.get(cache_key)
        return cached if isinstance(cached, dict) else None

    def _save_cached_history_insights(self, cache_key: str, ai_payload: dict) -> None:
        cache_path = self.history_insights_cache_path
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if cache_path.exists():
                current = json.loads(cache_path.read_text(encoding="utf-8"))
                if not isinstance(current, dict):
                    current = {}
            else:
                current = {}
        except (OSError, ValueError, TypeError):
            current = {}

        current[cache_key] = ai_payload
        cache_path.write_text(
            json.dumps(current, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    @staticmethod
    def _build_deterministic_history_insights(
        *,
        store_id: str,
        summary_stats: dict[str, object],
    ) -> OrderingHistoryInsightsResponse:
        auto_rate = float(summary_stats.get("auto_rate") or 0.0)
        manual_rate = float(summary_stats.get("manual_rate") or 0.0)
        avg_order_qty = float(summary_stats.get("avg_order_qty") or 0.0)
        confirm_gap_count = int(summary_stats.get("confirm_gap_count") or 0)
        changed_items_preview = summary_stats.get("top_changed_items_preview") or []

        kpis = [
            OrderingHistoryInsightKpi(
                key="auto_rate",
                label="자동 발주 비율",
                value=f"{auto_rate * 100:.1f}%",
                tone="primary",
            ),
            OrderingHistoryInsightKpi(
                key="manual_rate",
                label="수동 발주 비율",
                value=f"{manual_rate * 100:.1f}%",
                tone="warning",
            ),
            OrderingHistoryInsightKpi(
                key="avg_order_qty",
                label="평균 발주 수량",
                value=f"{avg_order_qty:.1f}개",
                tone="default",
            ),
        ]

        anomalies: list[OrderingHistoryAnomalyItem] = []
        if changed_items_preview:
            top_item = changed_items_preview[0]
            change_ratio = float(top_item.get("change_ratio") or 0.0)
            latest_qty = int(top_item.get("latest_ord_qty") or 0)
            baseline_qty = float(top_item.get("avg_ord_qty") or 0.0)
            direction = "증가" if change_ratio > 0 else "감소"
            severity = "high" if abs(change_ratio) >= 0.5 else "medium"
            anomalies.append(
                OrderingHistoryAnomalyItem(
                    id="top-changed-item",
                    severity=severity,
                    kind="ordering_change",
                    message=(
                        f"{top_item.get('item_nm')} 발주량이 평균 {baseline_qty:.1f}개에서 "
                        f"최근 {latest_qty}개로 {abs(change_ratio) * 100:.1f}% {direction}했습니다."
                    ),
                    recommended_action="해당 품목의 최근 판매 추이와 발주 기준을 함께 점검하세요.",
                    related_items=[str(top_item.get("item_nm"))],
                )
            )
        if confirm_gap_count > 0:
            anomalies.append(
                OrderingHistoryAnomalyItem(
                    id="confirm-gap",
                    severity="medium",
                    kind="confirm_gap",
                    message=f"주문수량과 확정수량 차이가 큰 발주건이 {confirm_gap_count}건 있습니다.",
                    recommended_action="확정수량 차이가 반복되는 품목은 발주 기준을 조정하세요.",
                    related_items=[],
                )
            )

        return OrderingHistoryInsightsResponse(
            kpis=kpis,
            anomalies=[
                OrderingHistoryAnomalyItem(**anomaly)
                for anomaly in OrderingService._sort_history_anomalies(
                    [anomaly.model_dump() for anomaly in anomalies]
                )
            ][:8],
            top_changed_items=[
                OrderingHistoryChangedItem(**item)
                for item in changed_items_preview
                if isinstance(item, dict)
            ][:5],
            sources=["ordering_history_summary_stats"],
            retrieved_contexts=["POC_010 전용 주문 조인 테이블 기반 결정식 인사이트"],
            confidence=0.95,
            explainability=create_ready_payload(
                trace_id=f"ordering-history-insights-{store_id}",
                actions=["변화율 상위 품목과 주문-확정 차이 건수를 기준으로 발주 이상징후를 요약했습니다."],
                evidence=[
                    f"평균 발주 수량: {avg_order_qty:.2f}",
                    f"주문-확정 차이 건수: {confirm_gap_count}",
                    f"변화율 상위 품목 수: {len(changed_items_preview)}",
                ],
            ),
        )

    async def get_history_insights(
        self,
        *,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        item_nm: str | None = None,
        is_auto: bool | None = None,
        limit: int = 200,
        reference_datetime: datetime | None = None,
    ) -> OrderingHistoryInsightsResponse:
        normalized_store_id = self._require_history_store_id(store_id)
        if not self.ai_client:
            raise RuntimeError("AI service is unavailable")
        resolved_reference_datetime = self._resolve_history_reference_datetime(reference_datetime)

        data = self.repository.get_history_filtered(
            store_id=normalized_store_id,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            reference_datetime=resolved_reference_datetime,
        )
        items = [OrderingHistoryItem(**item) for item in data["items"]]
        comparison_date_from = None
        if date_from:
            parsed_date_from = self._parse_history_date(date_from)
            if parsed_date_from is not None:
                comparison_date_from = (parsed_date_from - timedelta(days=28)).isoformat()
        comparison_data = self.repository.get_history_filtered(
            store_id=normalized_store_id,
            limit=max(limit, 500),
            date_from=comparison_date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            reference_datetime=resolved_reference_datetime,
        )
        comparison_items = [OrderingHistoryItem(**item) for item in comparison_data["items"]]
        summary_stats = self._build_history_summary_stats(
            items=items,
            total_count=data["total_count"],
            auto_rate=data["auto_rate"],
            manual_rate=data["manual_rate"],
            comparison_items=comparison_items,
            recent_date_from=date_from,
            recent_date_to=date_to,
        )
        if self.repository.uses_ordering_join_table(normalized_store_id):
            return self._build_deterministic_history_insights(
                store_id=normalized_store_id,
                summary_stats=summary_stats,
            )

        if not self.ai_client:
            raise RuntimeError("AI service is unavailable")

        filters = {
            "date_from": date_from,
            "date_to": date_to,
            "item_nm": item_nm,
            "is_auto": is_auto,
            "limit": limit,
        }
        history_items_payload = [item.model_dump() for item in items]
        comparison_history_items_payload = [item.model_dump() for item in comparison_items]
        cache_key = self._build_history_insights_cache_key(
            store_id=normalized_store_id,
            filters=filters,
            history_items=history_items_payload,
            comparison_history_items=comparison_history_items_payload,
        )
        ai_payload = self._load_cached_history_insights(cache_key)
        if ai_payload is None:
            ai_payload = await self.ai_client.generate_ordering_history_insights(
                store_id=normalized_store_id,
                filters=filters,
                history_items=comparison_history_items_payload,
                summary_stats=summary_stats,
            )
            if ai_payload is not None:
                self._save_cached_history_insights(cache_key, ai_payload)
        if ai_payload is None:
            raise RuntimeError("AI service returned no payload")

        try:
            anomaly_payloads = [
                anomaly
                for anomaly in (ai_payload.get("anomalies") or [])
                if isinstance(anomaly, dict)
            ]
            return OrderingHistoryInsightsResponse(
                kpis=[
                    OrderingHistoryInsightKpi(**kpi)
                    for kpi in (ai_payload.get("kpis") or [])
                    if isinstance(kpi, dict)
                ],
                anomalies=[
                    OrderingHistoryAnomalyItem(**anomaly)
                    for anomaly in self._sort_history_anomalies(anomaly_payloads)
                ][:8],
                top_changed_items=[
                    OrderingHistoryChangedItem(**item)
                    for item in (summary_stats.get("top_changed_items_preview") or [])
                    if isinstance(item, dict)
                ][:5],
                sources=[str(source) for source in (ai_payload.get("sources") or [])],
                retrieved_contexts=[
                    str(context) for context in (ai_payload.get("retrieved_contexts") or [])
                ],
                confidence=float(ai_payload["confidence"])
                if ai_payload.get("confidence") is not None
                else None,
                explainability=create_ready_payload(
                    trace_id=f"ordering-history-insights-{normalized_store_id}",
                    actions=["이상징후 항목을 우선순위대로 점검하고 재주문 여부를 확정하세요."],
                    evidence=[f"이상징후 건수: {len(ai_payload.get('anomalies') or [])}"],
                ),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Invalid AI ordering insights payload") from exc
