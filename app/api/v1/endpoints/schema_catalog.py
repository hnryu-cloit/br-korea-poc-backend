from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.deps import get_schema_catalog_service
from app.schemas.schema_catalog import SchemaCatalogTableDetail, SchemaCatalogTableListResponse
from app.services.schema_catalog_service import SchemaCatalogService

router = APIRouter(prefix="/data/schema", tags=["data"])


@router.get("/tables", response_model=SchemaCatalogTableListResponse)
async def list_schema_catalog_tables(
    layer: str | None = Query(default=None),
    preferred_only: bool = Query(default=False),
    service: SchemaCatalogService = Depends(get_schema_catalog_service),
) -> SchemaCatalogTableListResponse:
    return await service.list_tables(layer=layer, preferred_only=preferred_only)


@router.get("/tables/{table_name}", response_model=SchemaCatalogTableDetail)
async def get_schema_catalog_table(
    table_name: str,
    service: SchemaCatalogService = Depends(get_schema_catalog_service),
) -> SchemaCatalogTableDetail:
    detail = await service.get_table_detail(table_name=table_name)
    if detail is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"schema catalog entry not found: {table_name}",
        )
    return detail
