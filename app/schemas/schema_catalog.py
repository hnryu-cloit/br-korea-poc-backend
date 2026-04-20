from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SchemaCatalogTableSummary(BaseModel):
    table_name: str
    layer: str
    object_type: str
    domain: str
    description: str
    grain: str | None = None
    preferred_for_llm: bool = False
    is_sensitive: bool = False
    source_of_truth: str = "seed"


class SchemaCatalogColumn(BaseModel):
    column_name: str
    data_type: str
    ordinal_position: int
    description: str = ""
    semantic_role: str | None = None
    is_primary_key: bool = False
    is_filter_key: bool = False
    is_time_key: bool = False
    is_measure: bool = False
    is_sensitive: bool = False
    example_values: list[Any] = Field(default_factory=list)


class SchemaCatalogRelationship(BaseModel):
    from_table: str
    to_table: str
    relationship_type: str
    physical_fk: bool = False
    join_expression: str
    confidence: str = "logical"
    description: str = ""
    from_columns: list[str] = Field(default_factory=list)
    to_columns: list[str] = Field(default_factory=list)


class SchemaCatalogExample(BaseModel):
    use_case: str
    question: str
    sql_template: str | None = None
    notes: str | None = None


class SchemaCatalogTableDetail(SchemaCatalogTableSummary):
    columns: list[SchemaCatalogColumn] = Field(default_factory=list)
    relationships: list[SchemaCatalogRelationship] = Field(default_factory=list)
    examples: list[SchemaCatalogExample] = Field(default_factory=list)


class SchemaCatalogTableListResponse(BaseModel):
    items: list[SchemaCatalogTableSummary]
    total: int
    filtered_layer: str | None = None
    preferred_only: bool = False
