"""mart_payment_mix_daily 적재 스크립트.

원천: raw_daily_store_pay_way

매장×일자×결제수단(pay_way_cd) 단위로 결제금액/건수를 합산하고 일별 share_ratio를 산출.
- is_delivery_channel = (pay_way_cd = '18')
- is_discount_channel = (pay_way_cd in ('03','19'))
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


PAY_MIX_SQL = """
SELECT
    masked_stor_cd AS store_id,
    sale_dt,
    COALESCE(pay_way_cd, '') AS pay_way_cd,
    MAX(pay_way_cd_nm) AS pay_way_cd_nm,
    SUM(NULLIF(TRIM(pay_amt), '')::NUMERIC) AS pay_amt,
    COUNT(*) AS pay_count,
    SUM(NULLIF(TRIM(rtn_pay_amt), '')::NUMERIC) AS rtn_pay_amt
FROM raw_daily_store_pay_way
WHERE sale_dt ~ '^[0-9]{8}$'
GROUP BY masked_stor_cd, sale_dt, pay_way_cd
"""


def fetch(engine, sql: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql)).mappings().all()]


def upsert(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_payment_mix_daily (
            store_id, sale_dt, pay_way_cd, pay_way_cd_nm,
            pay_amt, pay_count, rtn_pay_amt, share_ratio,
            is_delivery_channel, is_discount_channel, generated_at
        ) VALUES (
            :store_id, :sale_dt, :pay_way_cd, :pay_way_cd_nm,
            :pay_amt, :pay_count, :rtn_pay_amt, :share_ratio,
            :is_delivery_channel, :is_discount_channel, NOW()
        )
        ON CONFLICT (store_id, sale_dt, pay_way_cd) DO UPDATE SET
            pay_way_cd_nm = EXCLUDED.pay_way_cd_nm,
            pay_amt = EXCLUDED.pay_amt,
            pay_count = EXCLUDED.pay_count,
            rtn_pay_amt = EXCLUDED.rtn_pay_amt,
            share_ratio = EXCLUDED.share_ratio,
            is_delivery_channel = EXCLUDED.is_delivery_channel,
            is_discount_channel = EXCLUDED.is_discount_channel,
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

    logger.info("결제 믹스 집계 중...")
    rows = fetch(engine, PAY_MIX_SQL)
    logger.info("rows=%d", len(rows))
    if not rows:
        logger.warning("집계 결과가 없습니다.")
        return

    # share_ratio = 매장×일자 단위 비중
    by_day_total: dict[tuple[str, str], float] = {}
    for r in rows:
        key = (r["store_id"], r["sale_dt"])
        by_day_total[key] = by_day_total.get(key, 0.0) + float(r["pay_amt"] or 0)

    payload: list[dict] = []
    for r in rows:
        key = (r["store_id"], r["sale_dt"])
        day_total = by_day_total[key]
        amt = float(r["pay_amt"] or 0)
        share = round(amt / day_total, 4) if day_total > 0 else 0.0
        cd = str(r["pay_way_cd"] or "")
        payload.append(
            {
                "store_id": r["store_id"],
                "sale_dt": r["sale_dt"],
                "pay_way_cd": cd,
                "pay_way_cd_nm": r.get("pay_way_cd_nm"),
                "pay_amt": round(amt, 2),
                "pay_count": int(r["pay_count"] or 0),
                "rtn_pay_amt": round(float(r["rtn_pay_amt"] or 0), 2),
                "share_ratio": share,
                "is_delivery_channel": cd == "18",
                "is_discount_channel": cd in ("03", "19"),
            }
        )

    logger.info("upsert mart_payment_mix_daily (%d rows)...", len(payload))
    upsert(engine, payload)
    logger.info("완료.")


if __name__ == "__main__":
    run_main(main)