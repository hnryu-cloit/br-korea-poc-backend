from fastapi import APIRouter, Depends, Query

from app.core.deps import get_data_catalog_service
from app.schemas.data_catalog import DataCatalogResponse, DataPreviewResponse
from app.services.data_catalog_service import DataCatalogService

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/catalog", response_model=DataCatalogResponse)
async def get_data_catalog(
    service: DataCatalogService = Depends(get_data_catalog_service),
) -> DataCatalogResponse:
    return await service.list_catalog()


@router.get("/preview/{table_name}", response_model=DataPreviewResponse)
async def preview_table(
    table_name: str,
    limit: int = Query(default=20, ge=1, le=200),
    service: DataCatalogService = Depends(get_data_catalog_service),
) -> DataPreviewResponse:
    return await service.preview(table_name=table_name, limit=limit)
