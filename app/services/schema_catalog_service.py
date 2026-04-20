from __future__ import annotations

from app.repositories.schema_catalog_repository import SchemaCatalogRepository
from app.schemas.schema_catalog import (
    SchemaCatalogTableDetail,
    SchemaCatalogTableListResponse,
    SchemaCatalogTableSummary,
)


class SchemaCatalogService:
    def __init__(self, repository: SchemaCatalogRepository) -> None:
        self.repository = repository

    async def list_tables(
        self,
        layer: str | None = None,
        preferred_only: bool = False,
    ) -> SchemaCatalogTableListResponse:
        items = await self.repository.list_tables(layer=layer, preferred_only=preferred_only)
        return SchemaCatalogTableListResponse(
            items=[SchemaCatalogTableSummary(**item) for item in items],
            total=len(items),
            filtered_layer=layer,
            preferred_only=preferred_only,
        )

    async def get_table_detail(self, table_name: str) -> SchemaCatalogTableDetail | None:
        detail = await self.repository.get_table_detail(table_name=table_name)
        if detail is None:
            return None
        return SchemaCatalogTableDetail(**detail)
