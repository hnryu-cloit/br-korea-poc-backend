from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class DomainPromptSettings(BaseModel):
    quick_prompts: list[str] = Field(default_factory=list)
    system_instruction: str = ""
    query_prefix_template: str = "[점포:{store_id}] [도메인:{domain}] {question}"


class PromptSettingsResponse(BaseModel):
    version: int = 1
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    updated_by: str = "system"
    domains: dict[str, DomainPromptSettings] = Field(default_factory=dict)


class PromptSettingsUpdateRequest(BaseModel):
    domains: dict[str, DomainPromptSettings]
    updated_by: str | None = None
