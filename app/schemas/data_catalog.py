from typing import Any

from pydantic import BaseModel, Field


class DataTableSummary(BaseModel):
    table_name: str
    row_count: int
    source_files: list[str] = Field(default_factory=list)


class DataCatalogResponse(BaseModel):
    db_path: str
    tables: list[DataTableSummary]


class DataPreviewResponse(BaseModel):
    table_name: str
    columns: list[str]
    rows: list[dict[str, Any]]
