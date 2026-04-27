from __future__ import annotations
from _runner import run_main

import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine


STORE_ID = "POC_010"


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    delete_sql = text(
        """
        DELETE FROM mart_ordering_join_poc_010
        WHERE store_id = :store_id
        """
    )

    insert_sql = text(
        """
        INSERT INTO mart_ordering_join_poc_010 (
            store_id,
            store_name,
            sido,
            sigungu,
            business_date,
            dlv_dt,
            weather_date,
            weather_region,
            weather_type,
            weather_max_temperature_c,
            weather_min_temperature_c,
            weather_precipitation_probability,
            item_cd,
            item_nm,
            ord_qty,
            confrm_qty,
            ord_rec_qty,
            auto_ord_yn,
            ord_grp_nm,
            generated_at,
            updated_at
        )
        SELECT
            :store_id AS store_id,
            CAST(sm.maked_stor_nm AS TEXT) AS store_name,
            CAST(sm.sido AS TEXT) AS sido,
            CAST(sm.region AS TEXT) AS sigungu,
            REPLACE(CAST(o.dlv_dt AS TEXT), '-', '') AS business_date,
            REPLACE(CAST(o.dlv_dt AS TEXT), '-', '') AS dlv_dt,
            REPLACE(CAST(w.weather_dt AS TEXT), '-', '') AS weather_date,
            CAST(w.sido AS TEXT) AS weather_region,
            CASE
                WHEN COALESCE(w.precipitation_mm, 0) >= 10 THEN '비'
                WHEN COALESCE(w.precipitation_mm, 0) > 0 THEN '흐림'
                WHEN COALESCE(w.avg_temp_c, 0) >= 25 THEN '더움'
                WHEN COALESCE(w.avg_temp_c, 0) <= 0 THEN '추움'
                ELSE '맑음'
            END AS weather_type,
            CASE
                WHEN w.avg_temp_c IS NULL THEN NULL
                ELSE ROUND(CAST(w.avg_temp_c AS NUMERIC))
            END AS weather_max_temperature_c,
            CASE
                WHEN w.avg_temp_c IS NULL THEN NULL
                ELSE ROUND(CAST(w.avg_temp_c AS NUMERIC))
            END AS weather_min_temperature_c,
            CASE
                WHEN w.precipitation_mm IS NULL THEN NULL
                ELSE ROUND(CAST(w.precipitation_mm AS NUMERIC))
            END AS weather_precipitation_probability,
            CAST(o.item_cd AS TEXT) AS item_cd,
            CAST(o.item_nm AS TEXT) AS item_nm,
            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(o.ord_qty AS TEXT)), '')::numeric, 0))) AS ord_qty,
            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(o.confrm_qty AS TEXT)), '')::numeric, 0))) AS confrm_qty,
            ROUND(SUM(COALESCE(NULLIF(TRIM(CAST(o.ord_rec_qty AS TEXT)), '')::numeric, 0))) AS ord_rec_qty,
            MAX(CAST(o.auto_ord_yn AS TEXT)) AS auto_ord_yn,
            MAX(CAST(o.ord_grp_nm AS TEXT)) AS ord_grp_nm,
            NOW() AS generated_at,
            NOW() AS updated_at
        FROM raw_order_extract o
        LEFT JOIN raw_store_master sm
          ON sm.masked_stor_cd = :store_id
        LEFT JOIN raw_weather_daily w
          ON REPLACE(CAST(w.weather_dt AS TEXT), '-', '') = REPLACE(CAST(o.dlv_dt AS TEXT), '-', '')
         AND CAST(w.sido AS TEXT) = CAST(sm.sido AS TEXT)
        WHERE CAST(o.masked_stor_cd AS TEXT) = :store_id
        GROUP BY
            REPLACE(CAST(o.dlv_dt AS TEXT), '-', ''),
            REPLACE(CAST(w.weather_dt AS TEXT), '-', ''),
            CAST(w.sido AS TEXT),
            w.avg_temp_c,
            w.precipitation_mm,
            CAST(sm.maked_stor_nm AS TEXT),
            CAST(sm.sido AS TEXT),
            CAST(sm.region AS TEXT),
            CAST(o.item_cd AS TEXT),
            CAST(o.item_nm AS TEXT)
        ON CONFLICT (store_id, dlv_dt, item_nm)
        DO UPDATE SET
            store_name = EXCLUDED.store_name,
            sido = EXCLUDED.sido,
            sigungu = EXCLUDED.sigungu,
            business_date = EXCLUDED.business_date,
            weather_date = EXCLUDED.weather_date,
            weather_region = EXCLUDED.weather_region,
            weather_type = EXCLUDED.weather_type,
            weather_max_temperature_c = EXCLUDED.weather_max_temperature_c,
            weather_min_temperature_c = EXCLUDED.weather_min_temperature_c,
            weather_precipitation_probability = EXCLUDED.weather_precipitation_probability,
            item_cd = EXCLUDED.item_cd,
            ord_qty = EXCLUDED.ord_qty,
            confrm_qty = EXCLUDED.confrm_qty,
            ord_rec_qty = EXCLUDED.ord_rec_qty,
            auto_ord_yn = EXCLUDED.auto_ord_yn,
            ord_grp_nm = EXCLUDED.ord_grp_nm,
            updated_at = NOW()
        """
    )

    with engine.begin() as connection:
        connection.execute(delete_sql, {"store_id": STORE_ID})
        result = connection.execute(insert_sql, {"store_id": STORE_ID})

    print(f"[ok] mart_ordering_join_poc_010 populated rows={result.rowcount}")


if __name__ == "__main__":
    run_main(main)