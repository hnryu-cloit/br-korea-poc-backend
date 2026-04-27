"""mart_campaign_effect_daily 적재 스크립트.

캠페인 진행 기간(start_dt~fnsh_dt) × 적용 품목(raw_campaign_item.item_cd)을
core_daily_item_sales와 조인하여 일별 매출/할인/참여 매장수를 산출한다.

baseline_sales_avg = 캠페인 시작 직전 14일 동안의 적용 품목 일평균 매출.
sales_lift_ratio = (캠페인 일자 매출 - baseline) / baseline.
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


CAMPAIGN_SQL = """
SELECT
    m.cpi_cd,
    MAX(m.cpi_nm) AS cpi_nm,
    MAX(m.start_dt) AS start_dt,
    MAX(m.fnsh_dt) AS fnsh_dt,
    MAX(i.cpi_dc_type_nm) AS cpi_dc_type_nm,
    MAX(NULLIF(TRIM(i.dc_rate_amt), '')::NUMERIC) AS discount_value,
    array_agg(DISTINCT i.item_cd) FILTER (WHERE TRIM(COALESCE(i.item_cd, '')) <> '') AS item_cds
FROM raw_campaign_master m
JOIN raw_campaign_item i ON i.cpi_cd = m.cpi_cd
WHERE COALESCE(m.prgrs_status_nm, '') = '확정'
  AND COALESCE(i.use_yn, '1') IN ('1', 'Y')
  AND TRIM(COALESCE(m.start_dt, '')) <> ''
  AND TRIM(COALESCE(m.fnsh_dt, '')) <> ''
GROUP BY m.cpi_cd
HAVING COUNT(DISTINCT i.item_cd) > 0
"""


SALES_BY_ITEM_DAY_SQL = """
SELECT
    item_cd,
    sale_dt,
    SUM(sale_amt) AS sale_amt,
    SUM(sale_qty) AS sale_qty,
    SUM(COALESCE(dc_amt, 0) + COALESCE(enuri_amt, 0)) AS dc_amt,
    COUNT(DISTINCT masked_stor_cd) AS store_count
FROM core_daily_item_sales
WHERE sale_qty > 0
  AND TRIM(COALESCE(item_cd, '')) <> ''
GROUP BY item_cd, sale_dt
"""


def fetch(engine, sql: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql)).mappings().all()]


def index_sales(rows: list[dict]) -> dict[tuple[str, str], dict]:
    return {(r["item_cd"], r["sale_dt"]): r for r in rows}


def daterange_yyyymmdd(start: str, end: str) -> list[str]:
    from datetime import datetime, timedelta

    try:
        s = datetime.strptime(start, "%Y%m%d")
        e = datetime.strptime(end, "%Y%m%d")
    except ValueError:
        return []
    if s > e:
        return []
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return out


def shift_yyyymmdd(value: str, days: int) -> str:
    from datetime import datetime, timedelta

    return (datetime.strptime(value, "%Y%m%d") + timedelta(days=days)).strftime("%Y%m%d")


def upsert(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_campaign_effect_daily (
            cpi_cd, cpi_nm, sale_dt, applicable_item_count,
            total_sales_during, total_dc_amt, total_qty,
            participating_store_count, baseline_sales_avg, sales_lift_ratio,
            cpi_dc_type_nm, discount_value, generated_at
        ) VALUES (
            :cpi_cd, :cpi_nm, :sale_dt, :applicable_item_count,
            :total_sales_during, :total_dc_amt, :total_qty,
            :participating_store_count, :baseline_sales_avg, :sales_lift_ratio,
            :cpi_dc_type_nm, :discount_value, NOW()
        )
        ON CONFLICT (cpi_cd, sale_dt) DO UPDATE SET
            cpi_nm = EXCLUDED.cpi_nm,
            applicable_item_count = EXCLUDED.applicable_item_count,
            total_sales_during = EXCLUDED.total_sales_during,
            total_dc_amt = EXCLUDED.total_dc_amt,
            total_qty = EXCLUDED.total_qty,
            participating_store_count = EXCLUDED.participating_store_count,
            baseline_sales_avg = EXCLUDED.baseline_sales_avg,
            sales_lift_ratio = EXCLUDED.sales_lift_ratio,
            cpi_dc_type_nm = EXCLUDED.cpi_dc_type_nm,
            discount_value = EXCLUDED.discount_value,
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

    logger.info("캠페인 메타 조회 중...")
    campaigns = fetch(engine, CAMPAIGN_SQL)
    logger.info("campaigns=%d", len(campaigns))

    logger.info("일별 품목 매출 조회 중...")
    sales_rows = fetch(engine, SALES_BY_ITEM_DAY_SQL)
    sales_index = index_sales(sales_rows)
    logger.info("sales rows=%d", len(sales_rows))

    payload: list[dict] = []
    for c in campaigns:
        item_cds = c.get("item_cds") or []
        item_cds = [str(x) for x in item_cds if x]
        if not item_cds:
            continue
        start_dt = str(c["start_dt"]).replace("-", "")
        fnsh_dt = str(c["fnsh_dt"]).replace("-", "")
        if len(start_dt) != 8 or len(fnsh_dt) != 8:
            continue

        # 캠페인 종료가 9999면 데이터 한계까지만 산출
        if fnsh_dt > "20260331":
            fnsh_dt = "20260331"
        if start_dt > fnsh_dt:
            continue

        # baseline = 시작 직전 14일 평균 매출(전체 적용 품목 합)
        baseline_from = shift_yyyymmdd(start_dt, -14)
        baseline_to = shift_yyyymmdd(start_dt, -1)
        baseline_amts: list[float] = []
        for d in daterange_yyyymmdd(baseline_from, baseline_to):
            day_total = 0.0
            for ic in item_cds:
                row = sales_index.get((ic, d))
                if row:
                    day_total += float(row["sale_amt"] or 0)
            baseline_amts.append(day_total)
        baseline_avg = (
            sum(baseline_amts) / len(baseline_amts) if baseline_amts else 0.0
        )

        for d in daterange_yyyymmdd(start_dt, fnsh_dt):
            day_sales = 0.0
            day_qty = 0.0
            day_dc = 0.0
            store_set: set = set()
            applicable: int = 0
            for ic in item_cds:
                row = sales_index.get((ic, d))
                if not row:
                    continue
                day_sales += float(row["sale_amt"] or 0)
                day_qty += float(row["sale_qty"] or 0)
                day_dc += float(row["dc_amt"] or 0)
                store_set.add(int(row["store_count"] or 0))
                applicable += 1

            if day_sales == 0 and day_qty == 0:
                continue

            lift = (
                round((day_sales - baseline_avg) / baseline_avg, 4)
                if baseline_avg > 0
                else 0.0
            )
            payload.append(
                {
                    "cpi_cd": str(c["cpi_cd"]),
                    "cpi_nm": c.get("cpi_nm"),
                    "sale_dt": d,
                    "applicable_item_count": applicable,
                    "total_sales_during": round(day_sales, 2),
                    "total_dc_amt": round(day_dc, 2),
                    "total_qty": round(day_qty, 2),
                    "participating_store_count": max(store_set) if store_set else 0,
                    "baseline_sales_avg": round(baseline_avg, 2),
                    "sales_lift_ratio": lift,
                    "cpi_dc_type_nm": c.get("cpi_dc_type_nm"),
                    "discount_value": (
                        float(c["discount_value"]) if c.get("discount_value") is not None else None
                    ),
                }
            )

    logger.info("upsert mart_campaign_effect_daily (%d rows)...", len(payload))
    upsert(engine, payload)
    logger.info("완료.")


if __name__ == "__main__":
    run_main(main)