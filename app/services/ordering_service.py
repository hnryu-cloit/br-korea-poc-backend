from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

from app.repositories.ordering_repository import OrderingRepository
from app.schemas.ordering import (
    OrderingAlertsResponse,
    OrderingContextResponse,
    OrderingDeadlineAlert,
    OrderOption,
    OrderSelectionHistoryItem,
    OrderSelectionHistoryResponse,
    OrderSelectionSummaryResponse,
    OrderingOptionsResponse,
    OrderSelectionRequest,
    OrderSelectionResponse,
)
from app.schemas.simulation import SimulationInput, SimulationResponse
from app.services.ai_client import AIServiceClient
from app.services.audit_service import AuditService

_KST = timezone(timedelta(hours=9))
_DEFAULT_DEADLINE_HOUR = 14
_DEFAULT_DEADLINE_MINUTE = 0
_ALERT_THRESHOLD_MINUTES = 20
_DEFAULT_STORE_ID = "gangnam"


def _now_kst() -> datetime:
    """현재 KST 시각 반환 (pytz 미설치 환경 호환)."""
    try:
        import pytz
        return datetime.now(pytz.timezone("Asia/Seoul"))
    except ImportError:
        return datetime.now(timezone.utc).astimezone(_KST)


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
        audit_service: Optional[AuditService] = None,
        ai_client: Optional[AIServiceClient] = None,
    ) -> None:
        self.repository = repository
        self.audit_service = audit_service
        self.ai_client = ai_client

    @staticmethod
    def _today_kst() -> str:
        return _now_kst().strftime("%Y-%m-%d")

    @staticmethod
    def _safe_str(value: object | None) -> str | None:
        if value in (None, ""):
            return None
        return str(value)

    @staticmethod
    def _metric(key: str, value: object) -> dict[str, str]:
        return {"key": key, "value": str(value)}

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

    def _merge_option_payloads(self, option: dict, ai_option: dict | None = None, index: int = 0) -> dict:
        fallback_id = option.get("option_id") or f"opt-{chr(97 + index)}"
        ai_reasoning = OrderingService._safe_str(ai_option.get("reasoning_text") if ai_option else None)
        ai_metrics = ai_option.get("reasoning_metrics") if ai_option else None
        ai_special_factors = ai_option.get("special_factors") if ai_option else None
        merged = {
            "option_id": OrderingService._safe_str(ai_option.get("option_id") if ai_option else None) or fallback_id,
            "title": OrderingService._safe_str(ai_option.get("title") if ai_option else None) or option.get("title") or f"추천안 {index + 1}",
            "basis": OrderingService._safe_str(ai_option.get("basis") if ai_option else None) or option.get("basis") or "-",
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
    ) -> OrderingOptionsResponse:
        business_date = self._today_kst()
        options = await self.repository.list_options(store_id=store_id)
        ai_store_id = store_id or _DEFAULT_STORE_ID
        ai_payload = await self._get_ai_ordering_recommendation(store_id=ai_store_id, current_date=business_date)
        ai_options = (ai_payload or {}).get("options") or []
        merged_options = [
            self._merge_option_payloads(option, ai_options[index] if index < len(ai_options) else None, index=index)
            for index, option in enumerate(options)
        ]

        deadline_minutes = 20
        deadline_at: str | None = None
        purpose_text = "주문 누락을 방지하고 최적 수량을 선택하세요."
        caution_text = "최종 주문 결정은 점주 권한입니다. 추천 옵션은 보조 자료로만 활용해주세요."
        weather_summary: str | None = None
        trend_summary: str | None = None

        if ai_payload:
            deadline_minutes = int(ai_payload.get("deadline_minutes") or deadline_minutes)
            deadline_at = self._safe_str(ai_payload.get("deadline_at"))
            purpose_text = self._safe_str(ai_payload.get("purpose_text")) or purpose_text
            caution_text = self._safe_str(ai_payload.get("caution_text") or ai_payload.get("guardrail_note")) or caution_text
            weather_summary = self._safe_str(ai_payload.get("weather_summary"))
            trend_summary = self._safe_str(ai_payload.get("trend_summary") or ai_payload.get("reasoning"))

        if deadline_at is None:
            deadline = await self.get_deadline(store_id=store_id)
            deadline_at = deadline["deadline"]
            deadline_minutes = deadline["minutes_remaining"]

        return OrderingOptionsResponse(
            deadline_minutes=deadline_minutes,
            deadline_at=deadline_at,
            notification_entry=notification_entry,
            purpose_text=purpose_text,
            caution_text=caution_text,
            weather_summary=weather_summary,
            trend_summary=trend_summary,
            business_date=business_date,
            options=[OrderOption(**o) for o in merged_options],
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
    ) -> OrderingAlertsResponse:
        options = await self.repository.list_options(store_id=store_id)
        focus_option_id = self._derive_focus_option_id(options)
        alerts: list[OrderingDeadlineAlert] = []
        ai_store_id = store_id or _DEFAULT_STORE_ID
        ai_deadline = await self._get_ai_deadline_alert(ai_store_id)
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
            generated_at=_now_kst().strftime("%Y-%m-%d %H:%M:%S"),
            alerts=alerts,
        )

    async def get_deadline(
        self,
        store_id: str | None = None,
        deadline_hour: int = _DEFAULT_DEADLINE_HOUR,
        deadline_minute: int = _DEFAULT_DEADLINE_MINUTE,
    ) -> dict:
        """주문 마감까지 남은 시간 정보를 반환합니다."""
        sid = store_id or "default"
        ai_lookup_store_id = store_id or _DEFAULT_STORE_ID
        ai_deadline = await self._get_ai_deadline_alert(ai_lookup_store_id)
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
        now = _now_kst()
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
