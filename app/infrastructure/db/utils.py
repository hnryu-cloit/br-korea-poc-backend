from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


def has_table(engine: Engine | None, table_name: str) -> bool:
    if engine is None:
        return False
    try:
        return inspect(engine).has_table(table_name)
    except SQLAlchemyError:
        return False
