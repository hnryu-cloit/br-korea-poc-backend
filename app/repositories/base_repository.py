from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class BaseRepository:
    engine: Engine | None

    def _table_columns(self, table_name: str) -> dict[str, str]:
        if not self.engine:
            return {}
        try:
            return {
                column["name"].lower(): column["name"]
                for column in inspect(self.engine).get_columns(table_name)
            }
        except SQLAlchemyError:
            return {}
        except Exception:
            return {}

    @staticmethod
    def _pick_column(columns: dict[str, str], candidates: tuple[str, ...]) -> str | None:
        for candidate in candidates:
            column_name = columns.get(candidate.lower())
            if column_name:
                return column_name
        return None

    @staticmethod
    def _safe_int(value: object) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return 0