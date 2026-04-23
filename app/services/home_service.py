from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from app.core.utils import get_now
from app.repositories.home_repository import HomeRepository
from app.schemas.dashboard import DashboardHomeRequest, ScheduleEvent, ScheduleResponse, ScheduleTodoItem
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService
from app.services.prompt_settings_service import PromptSettingsService

logger = logging.getLogger(__name__)


class HomeService:
    def __init__(
        self,
        production_service: ProductionService,
        ordering_service: OrderingService,
        repository: HomeRepository,
        prompt_settings_service: PromptSettingsService | None = None,
    ) -> None:
        self.production_service = production_service
        self.ordering_service = ordering_service
        self.repository = repository
        self.prompt_settings_service = prompt_settings_service

    async def get_schedule(self, payload: DashboardHomeRequest) -> ScheduleResponse:
        target_date = self._resolve_date(payload.business_date)
        date_str = target_date.strftime("%Y-%m-%d")

        events_result, production_result, ordering_result = await asyncio.gather(
            self.repository.list_schedule_events(store_id=payload.store_id, today=target_date),
            self.production_service.get_overview(
                store_id=payload.store_id,
                business_date=payload.business_date,
            ),
            self.ordering_service.get_selection_summary(
                store_id=payload.store_id,
                date_from=date_str,
                date_to=date_str,
            ),
            return_exceptions=True,
        )

        if isinstance(events_result, Exception):
            logger.warning(
                "home schedule 이벤트 조회 실패(store_id=%s, business_date=%s): %s",
                payload.store_id,
                payload.business_date,
                events_result,
            )
            events: list[dict[str, str]] = []
        else:
            events = events_result

        calendar_events = [ScheduleEvent(**self._normalize_event(event)) for event in events]
        selected_date = target_date.strftime("%Y%m%d")
        daily_events = [
            event
            for event in calendar_events
            if event.date == selected_date or event.startDate <= selected_date <= event.endDate
        ]

        try:
            todos = self._build_schedule_todos(
                production=None if isinstance(production_result, Exception) else production_result,
                ordering_summary=None if isinstance(ordering_result, Exception) else ordering_result,
                events=events,
            )
        except Exception as exc:
            logger.warning("home schedule todo 조회 실패(store_id=%s): %s", payload.store_id, exc)
            todos = []

        return ScheduleResponse(
            selected_date=selected_date,
            calendar_events=calendar_events,
            daily_events=daily_events,
            todos=todos,
        )

    @staticmethod
    def _resolve_date(value: str | None):
        if not value:
            return get_now().date()
        return datetime.strptime(value, "%Y-%m-%d").date()

    @staticmethod
    def _normalize_event(event: dict[str, str]) -> dict[str, str]:
        return {
            "date": str(event.get("date") or "").replace("-", ""),
            "title": str(event.get("title") or "운영 일정"),
            "category": str(event.get("category") or "notice")
            if str(event.get("category") or "notice") in {"campaign", "telecom", "notice"}
            else "notice",
            "type": str(event.get("type") or ""),
            "startDate": str(event.get("startDate") or event.get("date") or "").replace("-", ""),
            "endDate": str(event.get("endDate") or event.get("date") or "").replace("-", ""),
        }

    def _build_schedule_todos(
        self,
        production: object | None,
        ordering_summary: object | None,
        events: list[dict[str, str]],
    ) -> list[ScheduleTodoItem]:
        todos: list[ScheduleTodoItem] = []

        if production is not None and production.danger_count > 0:
            todos.append(
                ScheduleTodoItem(
                    id="todo-production-risk",
                    label=f"품절 위험 SKU {production.danger_count}개 생산 계획 확인",
                    recurring=True,
                )
            )
        if ordering_summary is not None and not ordering_summary.recommended_selected:
            todos.append(
                ScheduleTodoItem(
                    id="todo-ordering-review",
                    label="주문 추천안 검토 후 마감 전 확정",
                    recurring=True,
                )
            )
        if any((event.get("category") or event.get("type")) == "campaign" for event in events):
            todos.append(
                ScheduleTodoItem(
                    id="todo-campaign-check",
                    label="진행 중 캠페인 일정과 진열/재고 점검",
                    recurring=False,
                )
            )
        if any((event.get("category") or event.get("type")) == "telecom" for event in events):
            todos.append(
                ScheduleTodoItem(
                    id="todo-telecom-check",
                    label="통신사 제휴 할인 적용 여부 점검",
                    recurring=False,
                )
            )

        return todos[:6]
