from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url

# 이 스크립트는 db/migrations 아래 SQL을 적용해 테이블/뷰를 만들고,
# resource 파일 적재는 수행하지 않는다.


def split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    dollar_tag: str | None = None
    idx = 0

    while idx < len(sql):
        ch = sql[idx]
        next_two = sql[idx : idx + 2]

        if dollar_tag is None and not in_single_quote and not in_double_quote and ch == "$":
            end_idx = sql.find("$", idx + 1)
            if end_idx != -1:
                candidate = sql[idx : end_idx + 1]
                if candidate.startswith("$") and candidate.endswith("$"):
                    dollar_tag = candidate
                    current.append(candidate)
                    idx = end_idx + 1
                    continue

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, idx):
                current.append(dollar_tag)
                idx += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(ch)
            idx += 1
            continue

        if ch == "'" and not in_double_quote:
            if in_single_quote and next_two == "''":
                current.append(next_two)
                idx += 2
                continue
            in_single_quote = not in_single_quote
            current.append(ch)
            idx += 1
            continue

        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
            idx += 1
            continue

        if ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            idx += 1
            continue

        current.append(ch)
        idx += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError(
            "PostgreSQL driver is not installed. Install psycopg before running migrations."
        )
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
            row[0] for row in connection.execute(text("SELECT version FROM schema_migrations"))
        }

        for migration_path in sorted(migration_root.glob("*.sql")):
            version = migration_path.stem
            if version in applied:
                continue
            sql = migration_path.read_text(encoding="utf-8")
            for statement in split_statements(sql):
                connection.execute(text(statement))
            connection.execute(
                text(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (:version, :applied_at)"
                ),
                {"version": version, "applied_at": datetime.now()},
            )

    print(f"Migrations applied to {get_safe_database_url()}")


if __name__ == "__main__":
    main()
