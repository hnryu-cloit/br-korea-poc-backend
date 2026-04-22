from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ExplainabilityPayload(BaseModel):
    status: Literal["pending", "ready", "failed"] = "pending"
    trace_id: str
    actions: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    updated_at: str
    error_reason: str | None = None
