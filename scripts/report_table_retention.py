from __future__ import annotations
from _runner import run_main

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, os.path.abspath(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine


def _policy_path() -> Path:
    return Path(__file__).resolve().parents[1] / "app" / "config" / "db_table_retention_policy.json"


def _load_policy() -> dict[str, set[str]]:
    path = _policy_path()
    raw = json.loads(path.read_text(encoding="utf-8"))
    policy: dict[str, set[str]] = {}
    for group_name, values in raw.items():
        if not isinstance(group_name, str) or not isinstance(values, list):
            continue
        policy[group_name] = {str(value).strip() for value in values if str(value).strip()}
    return policy


def _load_public_tables() -> list[str]:
    engine = get_database_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
                """
            )
        ).scalars()
        return [str(row) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify current DB tables into keep/optional/cleanup groups."
    )
    parser.add_argument(
        "--only-cleanup",
        action="store_true",
        help="Print only cleanup candidates that currently exist in the DB.",
    )
    args = parser.parse_args()

    policy = _load_policy()
    public_tables = _load_public_tables()

    grouped: dict[str, list[str]] = defaultdict(list)
    known_tables = set()
    for group_name, table_names in policy.items():
        known_tables.update(table_names)
        for table_name in public_tables:
            if table_name in table_names:
                grouped[group_name].append(table_name)

    uncategorized = [table_name for table_name in public_tables if table_name not in known_tables]

    if args.only_cleanup:
        for table_name in grouped.get("cleanup_candidates", []):
            print(table_name)
        return

    print(f"[info] public tables: {len(public_tables)}")
    for group_name in (
        "runtime_essential",
        "runtime_optional",
        "optimization_marts",
        "ops_metadata",
        "cleanup_candidates",
    ):
        items = grouped.get(group_name, [])
        print(f"\n[{group_name}] {len(items)}")
        for table_name in items:
            print(f" - {table_name}")

    print(f"\n[uncategorized] {len(uncategorized)}")
    for table_name in uncategorized:
        print(f" - {table_name}")


if __name__ == "__main__":
    run_main(main)