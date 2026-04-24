from __future__ import annotations

import hashlib
import json
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
    ) -> dict | None:
        if not self.ai_client:
            return None
        return await self.ai_client.recommend_ordering(
            store_id=store_id,
            current_date=current_date,
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
            "reasoning_metrics": ai_metrics or option.get("reasoning_metrics") or [],
            "special_factors": ai_special_factors or option.get("special_factors") or [],
            "items": option.get("items") or (ai_option or {}).get("items") or [],
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

    async def list_options(
        self,
        notification_entry: bool = False,
        store_id: str | None = None,
        skip_ai: bool = False,
        reference_datetime: datetime | None = None,
    ) -> OrderingOptionsResponse:
        normalized_store_id = (store_id or _DEFAULT_ORDERING_STORE_ID).strip() or _DEFAULT_ORDERING_STORE_ID
        business_date = self._today_kst(reference_datetime)
        options = await self.repository.list_options(
            store_id=normalized_store_id,
            reference_date=business_date,
        )
        use_ai_ordering = not skip_ai and not self.repository.uses_ordering_join_table(normalized_store_id)
        ai_payload = (
            None
            if not use_ai_ordering
            else await self._get_ai_ordering_recommendation(store_id=normalized_store_id, current_date=business_date)
        )
        ai_options = (ai_payload or {}).get("options") or []
        merged_options = [
            self._merge_option_payloads(option, ai_options[index] if index < len(ai_options) else None, index=index)
            for index, option in enumerate(options)
        ]

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
            if weather is not None and not self._matches_business_date(weather, business_date):
                weather = None

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
        limit: int = 30,
        date_from: str | None = None,
        date_to: str | None = None,
        item_nm: str | None = None,
        is_auto: bool | None = None,
        reference_datetime: datetime | None = None,
    ):
        normalized_store_id = self._require_history_store_id(store_id)
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
        return OrderingHistoryResponse(
            items=[OrderingHistoryItem(**item) for item in data["items"]],
            auto_rate=data["auto_rate"],
            manual_rate=data["manual_rate"],
            total_count=data["total_count"],
            explainability=create_ready_payload(
                trace_id=f"ordering-history-{normalized_store_id}",
                actions=["자동/수동 발주 비중을 점검하고 다음 주문 기준을 조정하세요."],
                evidence=[
                    f"자동 비율: {data['auto_rate']:.2f}",
                    f"수동 비율: {data['manual_rate']:.2f}",
                    f"조회 건수: {data['total_count']}",
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
