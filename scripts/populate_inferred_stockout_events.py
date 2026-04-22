from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.repositories.production_repository import ProductionRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate core_inferred_stockout_event from recorded stockout data and hourly sales inference."
    )
    parser.add_argument("--store-id", help="Optional store id filter, e.g. POC_001")
    parser.add_argument("--date-from", help="Optional start date filter (YYYYMMDD)")
    parser.add_argument("--date-to", help="Optional end date filter (YYYYMMDD)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver is not installed.")

    repository = ProductionRepository(engine=engine)
    targets = repository.list_stockout_event_targets(
        store_id=args.store_id,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    if not targets:
        print("No stockout event targets found.")
        return

    delete_sql = text(
        """
        DELETE FROM core_inferred_stockout_event
        WHERE masked_stor_cd = :store_id
          AND sale_dt = :sale_dt
        """
    )
    insert_sql = text(
        """
        INSERT INTO core_inferred_stockout_event(
            masked_stor_cd,
            sale_dt,
            item_cd,
            item_nm,
            is_stockout,
            stockout_hour,
            rule_type,
            source_table,
            open_hour,
            close_hour,
            zero_sales_window,
            evidence_start_hour,
            evidence_end_hour,
            generated_at
        ) VALUES (
            :masked_stor_cd,
            :sale_dt,
            :item_cd,
            :item_nm,
            :is_stockout,
            :stockout_hour,
            :rule_type,
            :source_table,
            :open_hour,
            :close_hour,
            :zero_sales_window,
            :evidence_start_hour,
            :evidence_end_hour,
            :generated_at
        )
        """
    )

    inserted_rows = 0
    with engine.begin() as connection:
        for store_id, sale_dt in targets:
            connection.execute(delete_sql, {"store_id": store_id, "sale_dt": sale_dt})
            events = repository.list_inferred_stockout_events(store_id=store_id, sale_date=sale_dt)
            if not events:
                continue

            generated_at = datetime.now()
            payload = []
            for event in events:
                row = dict(event)
                row["generated_at"] = generated_at
                payload.append(row)

            connection.execute(insert_sql, payload)
            inserted_rows += len(payload)

    print(
        f"Populated core_inferred_stockout_event on {get_safe_database_url()} "
        f"for {len(targets)} targets, inserted {inserted_rows} rows."
    )


if __name__ == "__main__":
    main()
