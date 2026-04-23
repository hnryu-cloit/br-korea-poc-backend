from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class DashboardHomeRequest(BaseModel):
    store_id: str | None = None
    business_date: str | None = None


class ScheduleEvent(BaseModel):
    date: str
    title: str
    category: Literal["campaign", "telecom", "notice"]
    type: str
    startDate: str
    endDate: str


class ScheduleTodoItem(BaseModel):
    id: str
    label: str
    recurring: bool = False


class ScheduleResponse(BaseModel):
    selected_date: str | None = None
    calendar_events: list[ScheduleEvent]
    daily_events: list[ScheduleEvent]
    todos: list[ScheduleTodoItem]


class DashboardNoticeItem(BaseModel):
    id: str
    name: str
    tag: str
    path: str | None = None


class DashboardNoticesResponse(BaseModel):
    items: list[DashboardNoticeItem]


class DashboardLowStockProduct(BaseModel):
    id: str
    name: str
    remaining_stock: int
    cta_path: str


class DashboardOrderDeadline(BaseModel):
    deadline_at: str
    cta_path: str


class DashboardAlertsResponse(BaseModel):
    low_stock_products: list[DashboardLowStockProduct]
    order_deadline: DashboardOrderDeadline | None = None


class DashboardProductionSummaryItem(BaseModel):
    name: str
    current_stock: int
    predicted_consumption_1h: int


class DashboardOrderingDeadlineItem(BaseModel):
    name: str
    deadline_time: str


class DashboardSalesTrendPoint(BaseModel):
    label: str
    value: int


class DashboardSalesOverview(BaseModel):
    monthly_sales: int
    today_sales: int
    current_hour_sales: int
    last_month_sales: int
    last_month_same_weekday_avg_sales: int
    last_month_same_hour_avg_sales: int
    monthly_sales_points: list[DashboardSalesTrendPoint]
    today_sales_points: list[DashboardSalesTrendPoint]
    current_hour_sales_points: list[DashboardSalesTrendPoint]


class DashboardSummaryCardBase(BaseModel):
    domain: Literal["production", "ordering", "sales"]
    title: str
    cta_path: str
    recommended_questions: list[str]


class DashboardProductionSummaryCard(DashboardSummaryCardBase):
    domain: Literal["production"] = "production"
    top_products: list[DashboardProductionSummaryItem]


class DashboardOrderingSummaryCard(DashboardSummaryCardBase):
    domain: Literal["ordering"] = "ordering"
    ai_order_basis: str
    ai_order_cta_path: str
    deadline_products: list[DashboardOrderingDeadlineItem]


class DashboardSalesSummaryCard(DashboardSummaryCardBase):
    domain: Literal["sales"] = "sales"
    sales_overview: DashboardSalesOverview


class DashboardSummaryCardsResponse(BaseModel):
    updated_at: str
    cards: list[
        DashboardProductionSummaryCard | DashboardOrderingSummaryCard | DashboardSalesSummaryCard
    ]
