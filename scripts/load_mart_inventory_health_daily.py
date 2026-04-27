"""mart_inventory_health_daily 적재 스크립트.

원천:
- core_stock_rate (재고율 ord_avg/sal_avg/stk_avg/stk_rt)
- core_stockout_time (품절 시각, is_stockout)
- raw_product_shelf_life (유통기한 일수)

inventory_status 분류:
- 품절: is_stockout = TRUE
- 과잉: stk_rt >= 0.35
- 부족: stk_rt < 0.05 (혹은 음수)
- 적정: 그 외

expiry_risk_level:
- 높음: shelf_life <= 1 AND stk_rt > 0.25
- 중간: stk_rt > 0.15
- 낮음: 그 외
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


# 마지막 N일치만 적재 (전체는 1M+ rows라 무겁다)
WINDOW_DAYS = 90


JOIN_SQL = f"""
WITH window_bounds AS (
    SELECT MAX(prc_dt) AS max_dt
    FROM core_stock_rate
    WHERE prc_dt ~ '^[0-9]{{8}}$'
),
filtered_stk AS (
    SELECT
        s.masked_stor_cd AS store_id,
        s.prc_dt,
        s.item_cd,
        s.item_nm,
        COALESCE(s.ord_avg, 0) AS ord_avg,
        COALESCE(s.sal_avg, 0) AS sal_avg,
        COALESCE(s.stk_avg, 0) AS stk_avg,
        COALESCE(s.stk_rt, 0) AS stk_rt
    FROM core_stock_rate s, window_bounds
    WHERE s.prc_dt ~ '^[0-9]{{8}}$'
      AND TO_DATE(s.prc_dt, 'YYYYMMDD')
        >= TO_DATE(window_bounds.max_dt, 'YYYYMMDD') - INTERVAL '{WINDOW_DAYS - 1} days'
)
SELECT
    f.store_id,
    f.prc_dt,
    f.item_cd,
    f.item_nm,
    f.ord_avg,
    f.sal_avg,
    f.stk_avg,
    f.stk_rt,
    COALESCE(so.is_stockout, FALSE) AS is_stockout,
    so.stockout_hour,
    COALESCE(NULLIF(TRIM(sl.shelf_life_days), ''), '1')::INT AS shelf_life_days
FROM filtered_stk f
LEFT JOIN core_stockout_time so
    ON so.masked_stor_cd = f.store_id
   AND so.prc_dt = f.prc_dt
   AND so.item_cd = f.item_cd
LEFT JOIN raw_product_shelf_life sl
    ON sl.item_cd = f.item_cd
"""


def fetch(engine, sql: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql)).mappings().all()]


def classify(row: dict) -> tuple[str, str]:
    is_stockout = bool(row["is_stockout"])
    stk_rt = float(row.get("stk_rt") or 0)
    shelf_life = int(row.get("shelf_life_days") or 1)

    if is_stockout or stk_rt < 0:
        status = "품절"
    elif stk_rt >= 0.35:
        status = "과잉"
    elif stk_rt < 0.05:
        status = "부족"
    else:
        status = "적정"

    if shelf_life <= 1 and stk_rt > 0.25:
        risk = "높음"
    elif stk_rt > 0.15:
        risk = "중간"
    else:
        risk = "낮음"

    return status, risk


def upsert(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_inventory_health_daily (
            store_id, prc_dt, item_cd, item_nm,
            ord_avg, sal_avg, stk_avg, stk_rt,
            is_stockout, stockout_hour, shelf_life_days,
            expiry_risk_level, inventory_status, generated_at
        ) VALUES (
            :store_id, :prc_dt, :item_cd, :item_nm,
            :ord_avg, :sal_avg, :stk_avg, :stk_rt,
            :is_stockout, :stockout_hour, :shelf_life_days,
            :expiry_risk_level, :inventory_status, NOW()
        )
        ON CONFLICT (store_id, prc_dt, item_cd, item_nm) DO UPDATE SET
            ord_avg = EXCLUDED.ord_avg,
            sal_avg = EXCLUDED.sal_avg,
            stk_avg = EXCLUDED.stk_avg,
            stk_rt = EXCLUDED.stk_rt,
            is_stockout = EXCLUDED.is_stockout,
            stockout_hour = EXCLUDED.stockout_hour,
            shelf_life_days = EXCLUDED.shelf_life_days,
            expiry_risk_level = EXCLUDED.expiry_risk_level,
            inventory_status = EXCLUDED.inventory_status,
            generated_at = NOW()
        """
    )
    with engine.begin() as conn:
        chunk = 5000
        for i in range(0, len(rows), chunk):
            conn.execute(sql, rows[i : i + chunk])


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    logger.info("최근 %d일 재고 건전성 집계 중...", WINDOW_DAYS)
    rows = fetch(engine, JOIN_SQL)
    logger.info("rows=%d", len(rows))
    if not rows:
        logger.warning("집계 결과가 없습니다.")
        return

    payload = []
    for row in rows:
        status, risk = classify(row)
        payload.append(
            {
                "store_id": row["store_id"],
                "prc_dt": row["prc_dt"],
                "item_cd": row["item_cd"] or "",
                "item_nm": row["item_nm"],
                "ord_avg": round(float(row["ord_avg"] or 0), 4),
                "sal_avg": round(float(row["sal_avg"] or 0), 4),
                "stk_avg": round(float(row["stk_avg"] or 0), 4),
                "stk_rt": round(float(row["stk_rt"] or 0), 4),
                "is_stockout": bool(row["is_stockout"]),
                "stockout_hour": row.get("stockout_hour"),
                "shelf_life_days": int(row["shelf_life_days"] or 1),
                "expiry_risk_level": risk,
                "inventory_status": status,
            }
        )

    counts: dict[str, int] = {}
    for r in payload:
        counts[r["inventory_status"]] = counts.get(r["inventory_status"], 0) + 1
    logger.info("status distribution: %s", counts)

    logger.info("upsert mart_inventory_health_daily (%d rows)...", len(payload))
    upsert(engine, payload)
    logger.info("완료.")


if __name__ == "__main__":
    run_main(main)