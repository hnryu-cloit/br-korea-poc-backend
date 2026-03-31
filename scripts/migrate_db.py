from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url


def split_statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver is not installed. Install psycopg before running migrations.")
    migration_root = settings.migration_root

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        )
        applied = {
            row[0]
            for row in connection.execute(text("SELECT version FROM schema_migrations"))
        }

        for migration_path in sorted(migration_root.glob("*.sql")):
            version = migration_path.stem
            if version in applied:
                continue
            sql = migration_path.read_text(encoding="utf-8")
            for statement in split_statements(sql):
                connection.execute(text(statement))
            connection.execute(
                text("INSERT INTO schema_migrations(version, applied_at) VALUES (:version, :applied_at)"),
                {"version": version, "applied_at": datetime.now()},
            )

    print(f"Migrations applied to {get_safe_database_url()}")


if __name__ == "__main__":
    main()
