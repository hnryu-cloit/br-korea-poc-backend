from __future__ import annotations

import os
import re
import sys

from sqlalchemy import text

# Add backend directory to sys.path to import config
sys.path.insert(0, os.path.abspath("br-korea-poc-backend"))
from app.infrastructure.db.connection import get_database_engine


def reset_db():
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver not installed.")

    # Drop all tables in public schema
    with engine.begin() as conn:
        print("Dropping all existing tables...")
        conn.execute(text("DROP SCHEMA public CASCADE;"))
        conn.execute(text("CREATE SCHEMA public;"))
        conn.execute(text("GRANT ALL ON SCHEMA public TO postgres;"))
        conn.execute(text("GRANT ALL ON SCHEMA public TO public;"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))

    # Execute the DDL file provided by the user
    ddl_path = "resources/data/POC_TABLE_DDL.sql"
    with open(ddl_path, encoding="utf-8") as f:
        ddl_sql = f.read()

    # Convert Oracle syntax to PostgreSQL
    ddl_sql = re.sub(r"(?i)\bVARCHAR2\b", "VARCHAR", ddl_sql)
    ddl_sql = re.sub(r"(?i)\bNUMBER\b", "NUMERIC", ddl_sql)

    print("Creating new tables from POC_TABLE_DDL.sql (with PostgreSQL adjustments)...")

    # Run each statement in autocommit mode so one failure doesn't abort everything
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for statement in ddl_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    print(f"Skipping statement due to error: {e.__class__.__name__}")

    print("Database reset complete.")


if __name__ == "__main__":
    reset_db()
