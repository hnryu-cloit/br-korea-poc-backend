from __future__ import annotations

import asyncio
import logging

from sqlalchemy.exc import SQLAlchemyError

from app.core.utils import get_now
from app.repositories.home_repository import HomeRepository
from app.schemas.home import (
    HomeCardMetric,
    HomeCta,
    HomeOrderingDeadline,
    HomeOverviewRequest,
    HomeOverviewResponse,
    HomePriorityAction,
    HomePriorityActionBasisData,
    HomeStatItem,
    HomeSummaryCard,
    ScheduleNotice,
    ScheduleResponse,
    ScheduleTodoItem,
)
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

    async def get_overview(self, payload: HomeOverviewRequest) -> HomeOverviewResponse:
        production, ordering_summary, ordering_options = await asyncio.gather(
            self.production_service.get_overview(store_id=payload.store_id),
            self.ordering_service.get_selection_summary(
                store_id=payload.store_id,
                date_from=payload.business_date,
                date_to=payload.business_date,
            ),
            self.ordering_service.list_options(
                notification_entry=False,
                store_id=payload.store_id,
                skip_ai=True,
            ),
        )
        sales_status = self._build_sales_status(
            ordering_summary=ordering_summary, production_danger_count=production.danger_count
        )

        # 발주처별/메뉴별 다중 마감 시간 생성 로직
        deadlines = self._build_detailed_deadlines()

        # 가장 임박한 마감 시간 찾기 (완료되지 않았고 양수인 시간 중 최소값)
        active_deadlines = [d.remaining_minutes for d in deadlines if d.remaining_minutes >= 0]
        most_imminent_deadline = min(active_deadlines) if active_deadlines else 0

        stats = self._build_stats(
            danger_count=production.danger_count,
            ordering_deadline_minutes=most_imminent_deadline,
            alert_count=production.danger_count + (1 if ordering_summary.summary_status != "recommended_selected" else 0),
        )
        cards = self._build_cards(
            production=production,
            ordering_summary=ordering_summary,
            ordering_deadline_minutes=most_imminent_deadline,
            ordering_option_count=len(ordering_options.options),
            sales_status=sales_status,
        )

        return HomeOverviewResponse(
            updated_at=get_now().isoformat(timespec="seconds"),
            stats=stats,
            cards=cards,
            imminent_deadlines=deadlines,
        )

    @staticmethod
    def _to_iso_timestamp_from_hhmm(value: str | None) -> str | None:
        if not value or value == "-":
            return None
        try:
            hour_str, minute_str = value.split(":")
            hour = int(hour_str)
            minute = int(minute_str)
            base = get_now()
            target = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
        except (ValueError, TypeError):
            return None
        return target.isoformat(timespec="seconds")

    def _build_detailed_deadlines(self) -> list[HomeOrderingDeadline]:
        # 현재는 POC 목적이므로, 복수 발주처/메뉴 타입 시나리오에 맞는 하드코딩 데이터를 반환합니다.
        # 운영 환경에서는 발주처(Store-Supplier Mapping) 테이블에서 불러오게 됩니다.
        now = get_now()

        # 임의로 오늘 기준 14:00 (생지), 17:00 (완제품)으로 설정하여 남은 시간 계산
        raw_dough_time = now.replace(hour=14, minute=0, second=0, microsecond=0)
        finished_goods_time = now.replace(hour=17, minute=0, second=0, microsecond=0)

        raw_dough_remaining = int((raw_dough_time - now).total_seconds() / 60)
        finished_goods_remaining = int((finished_goods_time - now).total_seconds() / 60)

        return [
            HomeOrderingDeadline(
                supplier_name="평택 베이커리 공장",
                menu_type="베이커리류 (생지/반제품)",
                deadline_time="14:00",
                remaining_minutes=max(0, raw_dough_remaining),
                is_imminent=0 <= raw_dough_remaining <= 60,  # 1시간 이내 임박
                order_type="stock"
            ),
            HomeOrderingDeadline(
                supplier_name="본사 익일 물류센터",
                menu_type="완제품 (익일 배송)",
                deadline_time="17:00",
                remaining_minutes=max(0, finished_goods_remaining),
                is_imminent=0 <= finished_goods_remaining <= 60,
                order_type="finished_good"
            )
        ]

    async def get_schedule(self, store_id: str | None = None) -> ScheduleResponse:
        date_str = get_now().strftime("%Y-%m-%d")

        events_result, production_result, ordering_result = await asyncio.gather(
            self.repository.list_schedule_events(store_id=store_id, today=get_now().date()),
            self.production_service.get_overview(store_id=store_id),
            self.ordering_service.get_selection_summary(
                store_id=store_id, date_from=date_str, date_to=date_str
            ),
            return_exceptions=True,
        )

        if isinstance(events_result, Exception):
            logger.warning("home schedule 이벤트 조회 실패(store_id=%s): %s", store_id, events_result)
            events = []
        else:
            events = events_result

        notices = self._build_schedule_notices(events)

        try:
            todos = self._build_schedule_todos(
                production=None if isinstance(production_result, Exception) else production_result,
                ordering_summary=None if isinstance(ordering_result, Exception) else ordering_result,
                events=events,
            )
        except Exception as exc:
            logger.warning("home schedule todo 조회 실패(store_id=%s): %s", store_id, exc)
            todos = []

        source = "live:campaign+telecom" if events else "live:empty"
        return ScheduleResponse(
            updated_at=get_now().strftime("%Y-%m-%d %H:%M"),
            source=source,
            events=events,
            notices=notices,
            todos=todos,
        )

    @staticmethod
    def _build_schedule_notices(events: list[dict[str, str]]) -> list[ScheduleNotice]:
        notices: list[ScheduleNotice] = []
        for event in events[:6]:
            event_type = str(event.get("category") or event.get("type") or "notice")
            title = str(event.get("title") or "운영 안내")
            detail_type = str(event.get("type") or "")
            start_date = str(event.get("startDate") or event.get("start_date") or event.get("date") or "")
            end_date = str(event.get("endDate") or event.get("end_date") or event.get("date") or "")

            if event_type == "campaign":
                tone = "green"
            elif event_type == "telecom":
                tone = "blue"
            else:
                tone = "orange"

            notices.append(
                ScheduleNotice(
                    id=f"{event_type}-{event.get('date')}-{title}",
                    title=title,
                    category=event_type if event_type in {"campaign", "telecom", "notice"} else "notice",
                    type=detail_type,
                    startDate=start_date,
                    endDate=end_date,
                    tone=tone,
                )
            )
        return notices

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

    def _build_priority_actions(self, production, ordering_summary) -> list[HomePriorityAction]:
        danger_item = next(
            (item for item in production.items if item.status == "danger"),
            production.items[0] if production.items else None,
        )
        warning_item = next((item for item in production.items if item.status == "warning"), None)

        actions: list[HomePriorityAction] = []
        if danger_item is not None:
            actions.append(
                HomePriorityAction(
                    id=f"production-{danger_item.sku_id}",
                    type="production",
                    urgency="urgent",
                    badge_label=f"긴급 · 재고 소진 {danger_item.depletion_time}",
                    title=f"{danger_item.name} 생산 필요",
                    description=(
                        f"현재 {danger_item.current}개, 1시간 후 {danger_item.forecast}개 예상 · "
                        f"권장 생산 {danger_item.recommended}개"
                    ),
                    cta_label="생산하기",
                    cta_path="/production",
                    focus_section=danger_item.sku_id,
                    related_sku_id=danger_item.sku_id,
                    ai_reasoning=(
                        f"{danger_item.name} 현재 재고 {danger_item.current}개와 예측 재고 {danger_item.forecast}개 차이로 "
                        f"{danger_item.depletion_time} 전 소진 위험이 계산되었습니다."
                    ),
                    confidence_score=self._calculate_confidence_score(
                        danger_item.current, danger_item.forecast
                    ),
                    is_finished_good=False,
                    basis_data=HomePriorityActionBasisData(
                        selection_rule="first_danger_item",
                        sku_id=danger_item.sku_id,
                        name=danger_item.name,
                        current=danger_item.current,
                        forecast=danger_item.forecast,
                        recommended=danger_item.recommended,
                        depletion_time=danger_item.depletion_time,
                    ),
                )
            )

        actions.append(
            HomePriorityAction(
                id="ordering-deadline",
                type="ordering",
                urgency="important",
                badge_label="중요 · 주문 마감 임박",
                title="주문 마감 확인 필요",
                description=(
                    f"최근 주문 선택 {ordering_summary.total}건 · "
                    f"현재 상태: {'추천안 선택 완료' if ordering_summary.recommended_selected else '검토 필요'}"
                ),
                cta_label="주문 검토하기",
                cta_path="/ordering",
                focus_section="summary",
                ai_reasoning=(
                    f"최근 선택 {ordering_summary.total}건, 최근 7일 선택 {ordering_summary.recent_selection_count_7d}건을 기준으로 "
                    "마감 전 검토 필요도를 계산했습니다."
                ),
                confidence_score=self._calculate_ordering_confidence(ordering_summary.total),
                is_finished_good=False,
                basis_data=HomePriorityActionBasisData(
                    selection_rule="ordering_deadline_fixed",
                    summary_status=ordering_summary.summary_status,
                    recent_selection_count_7d=ordering_summary.recent_selection_count_7d,
                    total=ordering_summary.total,
                ),
            )
        )

        actions.append(
            HomePriorityAction(
                id=f"production-finished-{warning_item.sku_id if warning_item else 'hq'}",
                type="production",
                urgency="recommended",
                badge_label="권장 · 본사 납품 완제품",
                title=(
                    f"{warning_item.name} 추가 생산 제외"
                    if warning_item
                    else "완제품 납품 품목은 생산 제외"
                ),
                description=(
                    "본사 납품 완제품은 매장 생산 대상이 아니므로 생산 버튼을 비활성화합니다."
                ),
                cta_label="생산관리 보기",
                cta_path="/production",
                focus_section=warning_item.sku_id if warning_item else None,
                related_sku_id=warning_item.sku_id if warning_item else None,
                ai_reasoning=(
                    "매장 생산 대상이 아닌 품목 상태로 분류되어 생산 CTA를 비활성화했습니다."
                ),
                confidence_score=0.99,
                is_finished_good=True,
                basis_data=HomePriorityActionBasisData(
                    selection_rule="finished_goods_guidance",
                    sku_id=warning_item.sku_id if warning_item else None,
                    name=warning_item.name if warning_item else None,
                ),
            )
        )

        return actions[:3]

    @staticmethod
    def _build_stats(
        danger_count: int, ordering_deadline_minutes: int, alert_count: int
    ) -> list[HomeStatItem]:
        return [
            HomeStatItem(
                key="production_risk_count",
                label="품절 위험 상품",
                value=danger_count,
                unit="count",
                tone="danger" if danger_count > 0 else "success",
            ),
            HomeStatItem(
                key="ordering_deadline_minutes",
                label="주문 마감까지",
                value=ordering_deadline_minutes,
                unit="minutes",
                tone="primary",
            ),
            HomeStatItem(
                key="today_profit_estimate",
                label="오늘 운영 상태",
                value=("risk" if danger_count > 0 else "stable"),
                tone="default" if danger_count > 0 else "success",
            ),
            HomeStatItem(
                key="alert_count",
                label="알림 상태",
                value=alert_count,
                unit="count",
                tone="default",
            ),
        ]

    def _build_cards(
        self,
        production,
        ordering_summary,
        ordering_deadline_minutes: int,
        ordering_option_count: int,
        sales_status: dict[str, str],
    ) -> list[HomeSummaryCard]:
        danger_items = [item for item in production.items if item.status in {"danger", "warning"}][
            :2
        ]
        production_card = HomeSummaryCard(
            domain="production",
            title="생산 현황",
            description="실시간 재고 및 1시간 후 예측",
            highlights=[
                {
                    "type": "production_item",
                    "sku_id": item.sku_id,
                    "name": item.name,
                    "status": item.status,
                    "current": item.current,
                    "forecast": item.forecast,
                    "recommended": item.recommended,
                    "depletion_time": self._to_iso_timestamp_from_hhmm(item.depletion_time),
                }
                for item in danger_items
            ],
            metrics=[
                HomeCardMetric(
                    key="danger_count",
                    label="품절 위험",
                    value=production.danger_count,
                    unit="count",
                    tone="danger",
                ),
                HomeCardMetric(
                    key="production_lead_time_minutes",
                    label="생산 리드타임",
                    value=production.production_lead_time_minutes,
                    unit="minutes",
                    tone="primary",
                ),
            ],
            cta=HomeCta(label="생산관리 상세보기", path="/production"),
            prompts=self._get_domain_prompts(
                "production",
                store_id=getattr(production, "store_id", None),
                danger_items=danger_items,
            ),
            status_label="즉시 확인",
        )

        ordering_card = HomeSummaryCard(
            domain="ordering",
            title="주문 관리",
            description="주문 누락 방지 및 추천 검토",
            highlights=[
                {
                    "type": "ordering_summary",
                    "recommended_selected": ordering_summary.recommended_selected,
                    "summary_status": ordering_summary.summary_status,
                    "ordering_option_count": ordering_option_count,
                    "recent_selection_count_7d": ordering_summary.recent_selection_count_7d,
                    "selection_total": ordering_summary.total,
                }
            ],
            metrics=[
                HomeCardMetric(
                    key="ordering_deadline_minutes",
                    label="주문 마감",
                    value=ordering_deadline_minutes,
                    unit="minutes",
                    tone="primary",
                ),
                HomeCardMetric(
                    key="ordering_option_count",
                    label="추천 기준",
                    value=ordering_option_count,
                    unit="count",
                    tone="default",
                ),
            ],
            cta=HomeCta(label="주문 검토하기", path="/ordering"),
            prompts=self._get_domain_prompts(
                "ordering",
                store_id=getattr(ordering_summary, "store_id", None),
                ordering_option_count=ordering_option_count,
                ordering_deadline_minutes=ordering_deadline_minutes,
                ordering_selection_total=ordering_summary.total,
                ordering_recent_selection_count_7d=ordering_summary.recent_selection_count_7d,
                ordering_recommended_selected=ordering_summary.recommended_selected,
            ),
            status_label="검토 필요" if not ordering_summary.recommended_selected else "선택 완료",
            deadline_minutes=ordering_deadline_minutes,
            delivery_scheduled=ordering_summary.total > 0,
        )

        sales_card = HomeSummaryCard(
            domain="sales",
            title="손익 분석",
            description="현재 운영 데이터 기반 상태 요약",
            highlights=[
                {
                    "type": "sales_summary",
                    "production_danger_count": production.danger_count,
                    "ordering_selection_total": ordering_summary.total,
                    "recent_selection_count_7d": ordering_summary.recent_selection_count_7d,
                    "status_label": sales_status["status_label"],
                }
            ],
            metrics=[
                HomeCardMetric(
                    key="danger_count",
                    label="위험 SKU",
                    value=production.danger_count,
                    unit="count",
                    tone="danger" if production.danger_count else "success",
                ),
                HomeCardMetric(
                    key="ordering_selection_total",
                    label="주문 선택",
                    value=ordering_summary.total,
                    unit="count",
                    tone="default",
                ),
            ],
            cta=HomeCta(label="손익분석 상세보기", path="/sales"),
            prompts=self._get_domain_prompts(
                "sales",
                store_id=getattr(production, "store_id", None),
                production_danger_count=production.danger_count,
                ordering_selection_total=ordering_summary.total,
                ordering_recent_selection_count_7d=ordering_summary.recent_selection_count_7d,
                sales_status_label=sales_status["status_label"],
            ),
            status_label=sales_status["status_label"],
        )

        return [production_card, ordering_card, sales_card]

    def _get_domain_prompts(
        self,
        domain: str,
        store_id: str | None = None,
        danger_items: list | None = None,
        ordering_option_count: int | None = None,
        ordering_deadline_minutes: int | None = None,
        ordering_selection_total: int | None = None,
        ordering_recent_selection_count_7d: int | None = None,
        ordering_recommended_selected: bool | None = None,
        production_danger_count: int | None = None,
        sales_status_label: str | None = None,
    ) -> list[str]:
        if domain == "production":
            top_name = getattr(danger_items[0], "name", None) if danger_items else None
            top_recommended = (
                getattr(danger_items[0], "recommended", None) if danger_items else None
            )
            danger_count = len(danger_items or [])
            dynamic = [
                (
                    f"{top_name} 왜 {top_recommended}개 만들라는 거야?"
                    if top_name and top_recommended is not None
                    else "지금 당장 뭐부터 만들어야 해?"
                ),
                "지금 품절되면 얼마 날리는 거야?",
                (
                    f"품절 위험 메뉴 {danger_count}개 중 뭐가 제일 급해?"
                    if danger_count > 1
                    else "지금 당장 만들어야 할 메뉴가 뭐야?"
                ),
            ]
            return dynamic[:3]

        if domain == "ordering":
            dynamic = [
                (
                    "마감 전인데 어떤 거 골라야 해?"
                    if (ordering_deadline_minutes or 0) <= 30
                    else "지금 주문 어떤 안으로 하는 게 좋아?"
                ),
                f"마감 {ordering_deadline_minutes or 0}분 남았는데 지금 안 하면 어떻게 돼?",
                (
                    "지난번이랑 비슷하게 하면 돼?"
                    if ordering_selection_total and ordering_selection_total > 0
                    else "아직 주문 안 했는데 어떻게 해?"
                ),
            ]
            if ordering_recommended_selected is True:
                dynamic[0] = "이미 주문 선택했는데 바꿔야 할까?"
            return dynamic[:3]

        dynamic_sales = [
            f"오늘 장사 왜 '{sales_status_label or '주의'}'야?",
            f"품절 위험 메뉴가 {production_danger_count or 0}개인데 매출에 얼마나 영향 줘?",
            f"주문을 {ordering_selection_total or 0}건밖에 안 했는데 괜찮은 거야?",
        ]
        return dynamic_sales[:3]

    @staticmethod
    def _calculate_confidence_score(current: int, forecast: int) -> float:
        denominator = max(current + forecast, 1)
        return round(min(0.99, 0.55 + (abs(current - forecast) / denominator) * 0.4), 2)

    @staticmethod
    def _calculate_ordering_confidence(total_orders: int) -> float:
        return round(min(0.99, 0.5 + min(total_orders, 10) * 0.04), 2)

    @staticmethod
    def _build_sales_status(ordering_summary, production_danger_count: int) -> dict[str, str]:
        if production_danger_count > 0:
            return {
                "headline": f"생산 위험 SKU {production_danger_count}개가 운영 상태에 직접 영향을 주고 있습니다.",
                "detail": f"주문 선택 {ordering_summary.total}건, 최근 7일 선택 {ordering_summary.recent_selection_count_7d}건 기준으로 추가 확인이 필요합니다.",
                "status_label": "주의",
            }
        if ordering_summary.total > 0:
            return {
                "headline": "주문 선택 이력이 존재하며 생산 위험 SKU는 없습니다.",
                "detail": f"최근 선택 {ordering_summary.total}건이 기록되어 현재 운영 상태는 안정 범위입니다.",
                "status_label": "안정",
            }
        return {
            "headline": "운영 데이터가 제한적입니다.",
            "detail": "주문 선택 또는 생산 위험 데이터가 충분하지 않아 상세 손익 판단은 제한됩니다.",
            "status_label": "데이터 확인",
        }
