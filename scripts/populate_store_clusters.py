from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError(
            "PostgreSQL driver is not installed. Install psycopg before populating store clusters."
        )

    with engine.begin() as connection:
        connection.execute(text("DELETE FROM store_clusters"))
        connection.execute(
            text(
                """
                INSERT INTO store_clusters (
                    masked_stor_cd,
                    sido,
                    store_type,
                    cluster_id,
                    cluster_label,
                    updated_at
                )
                SELECT
                    masked_stor_cd,
                    NULLIF(TRIM(COALESCE(sido, '')), '') AS sido,
                    NULLIF(TRIM(COALESCE(store_type, '')), '') AS store_type,
                    CONCAT(
                        COALESCE(NULLIF(TRIM(COALESCE(sido, '')), ''), 'UNKNOWN'),
                        '|',
                        COALESCE(NULLIF(TRIM(COALESCE(store_type, '')), ''), 'UNKNOWN')
                    ) AS cluster_id,
                    CONCAT(
                        COALESCE(NULLIF(TRIM(COALESCE(sido, '')), ''), 'UNKNOWN'),
                        ' / ',
                        COALESCE(NULLIF(TRIM(COALESCE(store_type, '')), ''), 'UNKNOWN')
                    ) AS cluster_label,
                    :updated_at
                FROM raw_store_master
                WHERE NULLIF(TRIM(COALESCE(masked_stor_cd, '')), '') IS NOT NULL
                """
            ),
            {"updated_at": datetime.now()},
        )

    print(f"Store clusters populated on {get_safe_database_url()}")


if __name__ == "__main__":
    main()
