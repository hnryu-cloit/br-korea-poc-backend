from app.repositories.data_catalog_repository import DataCatalogRepository
from app.schemas.data_catalog import DataCatalogResponse, DataPreviewResponse, DataTableSummary


class DataCatalogService:
    def __init__(self, repository: DataCatalogRepository, db_path: str) -> None:
        self.repository = repository
        self.db_path = db_path

    async def list_catalog(self) -> DataCatalogResponse:
        tables = await self.repository.list_tables()
        return DataCatalogResponse(
            db_path=self.db_path,
            tables=[DataTableSummary(**table) for table in tables],
        )

    async def preview(self, table_name: str, limit: int = 20) -> DataPreviewResponse:
        preview = await self.repository.preview_table(table_name=table_name, limit=limit)
        return DataPreviewResponse(**preview)
