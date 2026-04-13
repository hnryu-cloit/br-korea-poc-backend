from app.repositories.bootstrap_repository import BootstrapRepository


class BootstrapService:
    def __init__(self, repository: BootstrapRepository) -> None:
        self.repository = repository

    async def get_bootstrap(self) -> dict:
        return await self.repository.get_bootstrap()

    async def get_channel_drafts(self) -> dict:
        data = await self.get_bootstrap()
        drafts = data.get("channelDrafts") or {}
        return drafts if isinstance(drafts, dict) else {}

    async def get_review_checklist(self) -> list[dict]:
        data = await self.get_bootstrap()
        review_queue = data.get("reviewQueue") or []
        return review_queue if isinstance(review_queue, list) else []
