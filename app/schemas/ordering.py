from typing import Optional

from pydantic import BaseModel


class OrderItem(BaseModel):
    name: str
    qty: int
    note: Optional[str] = None


class OrderOption(BaseModel):
    id: str
    label: str
    basis: str
    description: str
    recommended: bool
    items: list[OrderItem]
    notes: list[str]


class OrderingOptionsResponse(BaseModel):
    deadline_minutes: int
    notification_entry: bool = False
    focus_option_id: Optional[str] = None
    options: list[OrderOption]


class OrderingContextResponse(BaseModel):
    notification_id: int
    target_path: str
    focus_option_id: Optional[str] = None
    message: str


class OrderSelectionRequest(BaseModel):
    option_id: str
    reason: Optional[str] = None
    actor: str = "store_owner"


class OrderSelectionResponse(BaseModel):
    option_id: str
    reason: Optional[str] = None
    actor: str
    saved: bool
