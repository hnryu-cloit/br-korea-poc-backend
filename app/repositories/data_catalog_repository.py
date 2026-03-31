from __future__ import annotations

from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class DataCatalogRepository:
    def __init__(self, engine: Engine | None) -> None:
        self.engine = engine

    async def list_tables(self) -> list[dict[str, Any]]:
        if self.engine is None:
            return []
        try:
            inspector = inspect(self.engine)
            table_names = sorted(
                table_name
                for table_name in inspector.get_table_names()
                if table_name != "schema_migrations"
            )

            summaries: list[dict[str, Any]] = []
            with self.engine.connect() as connection:
                for table_name in table_names:
                    row_count = connection.execute(
                        text(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
                    ).scalar_one()
                    columns = {column["name"] for column in inspector.get_columns(table_name)}
                    source_files: list[str] = []
                    if "source_file" in columns:
                        rows = connection.execute(
                            text(
                                f'SELECT DISTINCT source_file FROM "{table_name}" '
                                "WHERE source_file IS NOT NULL ORDER BY source_file"
                            )
                        ).mappings()
                        source_files = [row["source_file"] for row in rows]
                    summaries.append(
                        {
                            "table_name": table_name,
                            "row_count": int(row_count),
                            "source_files": source_files,
                        }
                    )
            return summaries
        except SQLAlchemyError:
            return []

    async def preview_table(self, table_name: str, limit: int) -> dict[str, Any]:
        if self.engine is None:
            return {"table_name": table_name, "columns": [], "rows": []}
        try:
            inspector = inspect(self.engine)
            columns = [column["name"] for column in inspector.get_columns(table_name)]
            with self.engine.connect() as connection:
                rows = connection.execute(
                    text(f'SELECT * FROM "{table_name}" LIMIT :limit'),
                    {"limit": limit},
                ).mappings()
                return {
                    "table_name": table_name,
                    "columns": columns,
                    "rows": [dict(row) for row in rows],
                }
        except SQLAlchemyError:
            return {"table_name": table_name, "columns": [], "rows": []}
