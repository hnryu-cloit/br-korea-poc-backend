from __future__ import annotations
from _runner import run_main

import argparse
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url


DEFAULT_START_DT = "20250311"
DEFAULT_END_DT = "20260310"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate store weather mart table")
    parser.add_argument("--start-date", default=DEFAULT_START_DT)
    parser.add_argument("--end-date", default=DEFAULT_END_DT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = str(args.start_date or DEFAULT_START_DT).replace("-", "")
    end_date = str(args.end_date or DEFAULT_END_DT).replace("-", "")

    engine = get_database_engine()
    if engine is None:
        raise RuntimeError(
            "PostgreSQL driver is not installed. Install psycopg before populating store weather mart."
        )

    updated_at = datetime.now()
    sql = text(
        """
        INSERT INTO mart_store_weather_daily (
            store_id,
            store_name,
            sido,
            weather_dt,
            weather_type,
            avg_temp_c,
            max_temp_c,
            min_temp_c,
            precipitation_mm,
            precipitation_probability_max,
            generated_at,
            updated_at
        )
        SELECT
            CAST(s.masked_stor_cd AS TEXT) AS store_id,
            NULLIF(TRIM(CAST(s.maked_stor_nm AS TEXT)), '') AS store_name,
            CAST(w.sido AS TEXT) AS sido,
            CAST(w.weather_dt AS TEXT) AS weather_dt,
            CASE
                WHEN COALESCE(w.precipitation_mm, 0) >= 5 THEN CASE WHEN COALESCE(w.avg_temp_c, 0) <= 0 THEN '눈' ELSE '비' END
                WHEN COALESCE(w.precipitation_mm, 0) > 0 THEN CASE WHEN COALESCE(w.avg_temp_c, 0) <= 1 THEN '진눈깨비' ELSE '흐리고 비' END
                WHEN COALESCE(w.avg_temp_c, 0) <= 0 THEN '흐림'
                ELSE '맑음'
            END AS weather_type,
            COALESCE(w.avg_temp_c, 0) AS avg_temp_c,
            w.max_temp_c AS max_temp_c,
            w.min_temp_c AS min_temp_c,
            COALESCE(w.precipitation_mm, 0) AS precipitation_mm,
            w.precipitation_probability_max AS precipitation_probability_max,
            :updated_at AS generated_at,
            :updated_at AS updated_at
        FROM raw_store_master s
        JOIN raw_weather_daily w
          ON CAST(w.sido AS TEXT) = CAST(s.sido AS TEXT)
        WHERE NULLIF(TRIM(CAST(s.masked_stor_cd AS TEXT)), '') IS NOT NULL
          AND NULLIF(TRIM(CAST(s.sido AS TEXT)), '') IS NOT NULL
          AND CAST(w.weather_dt AS TEXT) BETWEEN :start_date AND :end_date
        ON CONFLICT (store_id, weather_dt)
        DO UPDATE SET
            store_name = EXCLUDED.store_name,
            sido = EXCLUDED.sido,
            weather_type = EXCLUDED.weather_type,
            avg_temp_c = EXCLUDED.avg_temp_c,
            max_temp_c = EXCLUDED.max_temp_c,
            min_temp_c = EXCLUDED.min_temp_c,
            precipitation_mm = EXCLUDED.precipitation_mm,
            precipitation_probability_max = EXCLUDED.precipitation_probability_max,
            generated_at = EXCLUDED.generated_at,
            updated_at = EXCLUDED.updated_at
        """
    )

    cleanup_sql = text(
        """
        DELETE FROM mart_store_weather_daily
        WHERE weather_dt < :start_date OR weather_dt > :end_date
        """
    )

    with engine.begin() as connection:
        connection.execute(cleanup_sql, {"start_date": start_date, "end_date": end_date})
        result = connection.execute(
            sql,
            {
                "start_date": start_date,
                "end_date": end_date,
                "updated_at": updated_at,
            },
        )

    print(
        f"[ok] store weather mart populated range={start_date}~{end_date} "
        f"rows={result.rowcount or 0} db={get_safe_database_url()}"
    )


if __name__ == "__main__":
    run_main(main)