"""mart_hourly_sales_pattern 적재 스크립트.

매장×요일×시간대 매출/주문 평균(최근 4주). raw_daily_store_channel.tmzon_div 사용.
"""

from __future__ import annotations
from _runner import run_main

import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


WINDOW_DAYS = 28


HOURLY_SQL = f"""
WITH window_bounds AS (
    SELECT MAX(sale_dt) AS max_dt
    FROM raw_daily_store_channel
    WHERE sale_dt ~ '^[0-9]{{8}}$'
),
filtered AS (
    SELECT
        masked_stor_cd AS store_id,
        sale_dt,
        TO_DATE(sale_dt, 'YYYYMMDD') AS d,
        CASE
            WHEN COALESCE(NULLIF(CAST(tmzon_div AS TEXT), ''), '') ~ '^[0-9]+$'
            THEN CAST(tmzon_div AS INTEGER) ELSE NULL
        END AS hour,
        CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC) AS amt,
        CAST(COALESCE(NULLIF(CAST(ord_cnt AS TEXT), ''), '0') AS NUMERIC) AS cnt
    FROM raw_daily_store_channel, window_bounds
    WHERE sale_dt ~ '^[0-9]{{8}}$'
      AND TO_DATE(sale_dt, 'YYYYMMDD')
        >= TO_DATE(window_bounds.max_dt, 'YYYYMMDD') - INTERVAL '{WINDOW_DAYS - 1} days'
)
SELECT
    f.store_id,
    EXTRACT(ISODOW FROM f.d)::INT - 1 AS dow,  -- 0=월 ~ 6=일
    f.hour,
    AVG(f.amt) AS avg_sale_amt,
    AVG(f.cnt) AS avg_ord_cnt,
    COUNT(DISTINCT f.sale_dt) AS sample_day_count,
    MIN(f.sale_dt) AS window_start_dt,
    MAX(f.sale_dt) AS window_end_dt
FROM filtered f
WHERE f.hour IS NOT NULL
GROUP BY f.store_id, dow, f.hour
"""


def fetch(engine, sql: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql)).mappings().all()]


def assign_peak_rank(rows: list[dict]) -> None:
    """매장×요일 내에서 매출 내림차순 순위 부여."""
    by_key: dict[tuple, list[dict]] = {}
    for r in rows:
        by_key.setdefault((r["store_id"], r["dow"]), []).append(r)
    for bucket in by_key.values():
        bucket.sort(key=lambda x: float(x["avg_sale_amt"] or 0), reverse=True)
        for idx, item in enumerate(bucket, start=1):
            item["peak_rank"] = idx


def upsert(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_hourly_sales_pattern (
            store_id, dow, hour, avg_sale_amt, avg_ord_cnt,
            sample_day_count, peak_rank, window_start_dt, window_end_dt, generated_at
        ) VALUES (
            :store_id, :dow, :hour, :avg_sale_amt, :avg_ord_cnt,
            :sample_day_count, :peak_rank, :window_start_dt, :window_end_dt, NOW()
        )
        ON CONFLICT (store_id, dow, hour) DO UPDATE SET
            avg_sale_amt = EXCLUDED.avg_sale_amt,
            avg_ord_cnt = EXCLUDED.avg_ord_cnt,
            sample_day_count = EXCLUDED.sample_day_count,
            peak_rank = EXCLUDED.peak_rank,
            window_start_dt = EXCLUDED.window_start_dt,
            window_end_dt = EXCLUDED.window_end_dt,
            generated_at = NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    logger.info("최근 %d일 시간대 패턴 집계 중...", WINDOW_DAYS)
    rows = fetch(engine, HOURLY_SQL)
    logger.info("rows=%d", len(rows))
    if not rows:
        logger.warning("집계 결과가 없습니다.")
        return

    payload = [
        {
            "store_id": r["store_id"],
            "dow": int(r["dow"]),
            "hour": int(r["hour"]),
            "avg_sale_amt": round(float(r["avg_sale_amt"] or 0), 2),
            "avg_ord_cnt": round(float(r["avg_ord_cnt"] or 0), 2),
            "sample_day_count": int(r["sample_day_count"] or 0),
            "peak_rank": 0,
            "window_start_dt": r.get("window_start_dt"),
            "window_end_dt": r.get("window_end_dt"),
        }
        for r in rows
    ]

    assign_peak_rank(payload)
    logger.info("upsert mart_hourly_sales_pattern...")
    upsert(engine, payload)
    logger.info("완료.")


if __name__ == "__main__":
    run_main(main)