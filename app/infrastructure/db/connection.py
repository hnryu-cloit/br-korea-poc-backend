from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine, make_url

from app.core.config import settings

_engine: Engine | None = None


def get_database_url() -> str:
    return settings.DATABASE_URL


def get_database_engine() -> Engine | None:
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(
                get_database_url(),
                future=True,
                pool_pre_ping=True,
            )
        except ModuleNotFoundError:
            return None
    return _engine


def get_safe_database_url() -> str:
    url = make_url(get_database_url())
    return str(
        URL.create(
            drivername=url.drivername,
            username=url.username,
            password="***" if url.password else None,
            host=url.host,
            port=url.port,
            database=url.database,
            query=url.query,
        )
    )
