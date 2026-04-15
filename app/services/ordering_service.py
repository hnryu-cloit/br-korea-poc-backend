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
from app.services.audit_service import AuditService

_KST = timezone(timedelta(hours=9))
_DEFAULT_DEADLINE_HOUR = 14
_DEFAULT_DEADLINE_MINUTE = 0
_ALERT_THRESHOLD_MINUTES = 20


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
    def __init__(self, repository: OrderingRepository, audit_service: Optional[AuditService] = None) -> None:
        self.repository = repository
        self.audit_service = audit_service

    async def list_options(self, notification_entry: bool = False) -> OrderingOptionsResponse:
        options = await self.repository.list_options()
        return OrderingOptionsResponse(
            deadline_minutes=20,
            notification_entry=notification_entry,
            options=[OrderOption(**o) for o in options],
        )

    async def get_notification_context(self, notification_id: int) -> OrderingContextResponse:
        context = await self.repository.get_notification_context(notification_id)
        return OrderingContextResponse(**context)

    async def list_deadline_alerts(self, before_minutes: int = 20) -> OrderingAlertsResponse:
        options = await self.repository.list_options()
        focus_option = next((option for option in options if option.get("recommended")), options[0] if options else None)
        alerts: list[OrderingDeadlineAlert] = []
        if focus_option is not None:
            alerts.append(
                OrderingDeadlineAlert(
                    notification_id=2,
                    title=f"주문 마감 {before_minutes}분 전입니다",
                    message=f"{focus_option['title']} 옵션을 우선 확인해 주세요. 추천 주문 수량 3개 옵션이 준비되었습니다.",
                    deadline_minutes=before_minutes,
                    target_path="/ordering",
                    focus_option_id=focus_option["option_id"],
                    target_roles=["store_owner"],
                )
            )
        return OrderingAlertsResponse(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            alerts=alerts,
        )

    def get_deadline(
        self,
        store_id: str | None = None,
        deadline_hour: int = _DEFAULT_DEADLINE_HOUR,
        deadline_minute: int = _DEFAULT_DEADLINE_MINUTE,
    ) -> dict:
        """주문 마감까지 남은 시간 정보를 반환합니다."""
        now = _now_kst()
        delta = _minutes_to_deadline(now, deadline_hour, deadline_minute)
        sid = store_id or "default"
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
