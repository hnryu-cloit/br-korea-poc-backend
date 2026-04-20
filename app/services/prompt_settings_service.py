from __future__ import annotations

from app.repositories.prompt_settings_repository import PromptSettingsRepository
from app.schemas.prompt_settings import (
    DomainPromptSettings,
    PromptSettingsResponse,
    PromptSettingsUpdateRequest,
)


class PromptSettingsService:
    def __init__(self, repository: PromptSettingsRepository) -> None:
        self.repository = repository

    def get_settings(self) -> PromptSettingsResponse:
        payload = self.repository.get()
        return PromptSettingsResponse(**payload)

    def update_settings(
        self, request: PromptSettingsUpdateRequest, default_updated_by: str = "hq_admin"
    ) -> PromptSettingsResponse:
        current = self.repository.get()
        next_payload = {
            **current,
            "domains": {key: value.model_dump() for key, value in request.domains.items()},
            "updated_by": request.updated_by or default_updated_by,
        }
        saved = self.repository.save(next_payload)
        return PromptSettingsResponse(**saved)

    def get_domain_settings(self, domain: str) -> DomainPromptSettings:
        settings = self.get_settings()
        return settings.domains.get(domain, settings.domains.get("sales", DomainPromptSettings()))

    def get_quick_prompts(self, domain: str) -> list[str]:
        return self.get_domain_settings(domain).quick_prompts

    def get_system_instruction(self, domain: str) -> str:
        return self.get_domain_settings(domain).system_instruction

    def build_ai_query(self, domain: str, question: str, store_id: str | None) -> str:
        if not store_id:
            return question
        domain_settings = self.get_domain_settings(domain)
        template = (
            domain_settings.query_prefix_template
            or "[점포:{store_id}] [도메인:{domain}] {question}"
        )
        try:
            return template.format(
                store_id=store_id,
                domain=domain,
                question=question,
            )
        except Exception:
            return f"[점포:{store_id}] [도메인:{domain}] {question}"
