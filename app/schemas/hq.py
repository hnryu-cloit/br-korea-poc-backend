from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class StoreOrderItem(BaseModel):
    store: str
    region: str
    option: str
    basis: str
    reason: str
    submitted_at: str
    status: Literal["normal", "review", "risk"]


class CoachingTip(BaseModel):
    store: str
    tip: str


class HQCoachingResponse(BaseModel):
    store_orders: list[StoreOrderItem]
    coaching_tips: list[CoachingTip]


class StoreInspectionItem(BaseModel):
    store: str
    region: str
    alert_response_rate: int
    production_registered: int
    production_total: int
    chance_loss_change: str
    status: Literal["compliant", "partial", "noncompliant"]


class HQInspectionResponse(BaseModel):
    items: list[StoreInspectionItem]
