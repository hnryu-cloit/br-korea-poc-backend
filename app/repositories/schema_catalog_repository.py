from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table


class SchemaCatalogRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    @staticmethod
    def _normalize_json_array(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return []
            return parsed if isinstance(parsed, list) else []
        return []

    async def list_tables(
        self,
        layer: str | None = None,
        preferred_only: bool = False,
    ) -> list[dict[str, Any]]:
        if not self.engine or not has_table(self.engine, "schema_catalog_tables"):
            return []
        try:
            query = """
                SELECT
                    table_name,
                    layer,
                    object_type,
                    domain,
                    description,
                    grain,
                    preferred_for_llm,
                    is_sensitive,
                    source_of_truth
                FROM schema_catalog_tables
            """
            params: dict[str, Any] = {"preferred_only": preferred_only}
            conditions: list[str] = []
            if layer:
                conditions.append("layer = :layer")
                params["layer"] = layer
            if preferred_only:
                conditions.append("preferred_for_llm = TRUE")
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
            query += " ORDER BY layer, preferred_for_llm DESC, table_name"

            with self.engine.connect() as connection:
                rows = connection.execute(text(query), params).mappings().all()
                return [dict(row) for row in rows]
        except SQLAlchemyError:
            return []

    async def get_table_detail(self, table_name: str) -> dict[str, Any] | None:
        if not self.engine or not has_table(self.engine, "schema_catalog_tables"):
            return None
        try:
            with self.engine.connect() as connection:
                table_row = (
                    connection.execute(
                        text(
                            """
                            SELECT
                                table_name,
                                layer,
                                object_type,
                                domain,
                                description,
                                grain,
                                preferred_for_llm,
                                is_sensitive,
                                source_of_truth
                            FROM schema_catalog_tables
                            WHERE table_name = :table_name
                            """
                        ),
                        {"table_name": table_name},
                    )
                    .mappings()
                    .first()
                )
                if table_row is None:
                    return None

                column_rows = connection.execute(
                    text(
                        """
                        SELECT
                            column_name,
                            data_type,
                            ordinal_position,
                            description,
                            semantic_role,
                            is_primary_key,
                            is_filter_key,
                            is_time_key,
                            is_measure,
                            is_sensitive,
                            example_values_json
                        FROM schema_catalog_columns
                        WHERE table_name = :table_name
                        ORDER BY ordinal_position, column_name
                        """
                    ),
                    {"table_name": table_name},
                ).mappings()

                relationship_rows = connection.execute(
                    text(
                        """
                        SELECT
                            from_table,
                            to_table,
                            relationship_type,
                            physical_fk,
                            join_expression,
                            confidence,
                            description,
                            from_columns_json,
                            to_columns_json
                        FROM schema_catalog_relationships
                        WHERE from_table = :table_name OR to_table = :table_name
                        ORDER BY from_table, to_table, id
                        """
                    ),
                    {"table_name": table_name},
                ).mappings()

                example_rows = connection.execute(
                    text(
                        """
                        SELECT
                            use_case,
                            question,
                            sql_template,
                            notes
                        FROM schema_catalog_examples
                        WHERE table_name = :table_name
                        ORDER BY id
                        """
                    ),
                    {"table_name": table_name},
                ).mappings()

                detail = dict(table_row)
                detail["columns"] = [
                    {
                        **dict(row),
                        "example_values": self._normalize_json_array(row["example_values_json"]),
                    }
                    for row in column_rows
                ]
                detail["relationships"] = [
                    {
                        **dict(row),
                        "from_columns": self._normalize_json_array(row["from_columns_json"]),
                        "to_columns": self._normalize_json_array(row["to_columns_json"]),
                    }
                    for row in relationship_rows
                ]
                detail["examples"] = [dict(row) for row in example_rows]
                return detail
        except SQLAlchemyError:
            return None
