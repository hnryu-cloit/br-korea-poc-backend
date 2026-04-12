from __future__ import annotations

import argparse
import sys
from pathlib import Path
from pprint import pprint

from sqlalchemy import inspect, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url

# 이 스크립트는 migration이나 적재를 수행하지 않고, 현재 DB에 들어간
# raw/운영 테이블 상태만 조회한다.

def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect loaded PostgreSQL resource data")
    parser.add_argument("--table", help="Table name to preview")
    parser.add_argument("--limit", type=int, default=10, help="Row preview limit")
    args = parser.parse_args()

    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver is not installed. Install psycopg before inspecting data.")
    inspector = inspect(engine)

    with engine.connect() as connection:
        if args.table:
            rows = connection.execute(
                text(f'SELECT * FROM "{args.table}" LIMIT :limit'),
                {"limit": args.limit},
            ).mappings()
            for row in rows:
                pprint(dict(row))
            return

        print(get_safe_database_url())
        for table_name in sorted(inspector.get_table_names()):
            count = connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one()
            print(f"{table_name}: {count}")


if __name__ == "__main__":
    main()
