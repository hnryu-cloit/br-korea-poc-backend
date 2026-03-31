from app.repositories.ordering_repository import OrderingRepository
from app.schemas.ordering import (
    OrderingContextResponse,
    OrderingOptionsResponse,
    OrderSelectionRequest,
    OrderSelectionResponse,
)
from app.schemas.simulation import SimulationInput, SimulationResponse


class OrderingService:
    def __init__(self, repository: OrderingRepository) -> None:
        self.repository = repository

    async def list_options(self, notification_entry: bool = False) -> OrderingOptionsResponse:
        options = await self.repository.list_options()
        return OrderingOptionsResponse(
            deadline_minutes=20,
            notification_entry=notification_entry,
            focus_option_id="opt-a" if notification_entry else None,
            options=options,
        )

    async def get_notification_context(self, notification_id: int) -> OrderingContextResponse:
        context = await self.repository.get_notification_context(notification_id)
        return OrderingContextResponse(**context)

    async def save_selection(self, payload: OrderSelectionRequest) -> OrderSelectionResponse:
        saved = await self.repository.save_selection(payload.model_dump())
        return OrderSelectionResponse(saved=True, **saved)

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
