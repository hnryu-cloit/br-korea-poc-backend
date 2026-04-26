"""mart_store_daily_kpi 적재 스크립트.

원천:
- raw_daily_store_channel: 매출/주문/채널/시간대
- raw_daily_store_pay_way: 매장 결제 건수(pay_way_cd != '18')
- core_daily_item_sales × mart_item_category_master: 카테고리별 매출, 1위 메뉴
- raw_weather_daily: 날씨 (매장 sido 기준)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


CHANNEL_KPI_SQL = """
WITH normalized AS (
    SELECT
        masked_stor_cd AS store_id,
        sale_dt,
        COALESCE(ho_chnl_div, '') AS chnl_div,
        CASE
            WHEN COALESCE(NULLIF(CAST(tmzon_div AS TEXT), ''), '') ~ '^[0-9]+$'
            THEN CAST(tmzon_div AS INTEGER) ELSE NULL
        END AS hour_div,
        CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC) AS amt,
        CAST(COALESCE(NULLIF(CAST(ord_cnt AS TEXT), ''), '0') AS NUMERIC) AS cnt
    FROM raw_daily_store_channel
)
SELECT
    store_id,
    sale_dt,
    SUM(amt) AS total_sales,
    SUM(cnt) AS total_orders,
    SUM(CASE WHEN chnl_div LIKE '%배달%' THEN amt ELSE 0 END) AS delivery_sales,
    SUM(CASE WHEN chnl_div LIKE '%픽업%' OR chnl_div LIKE '%포장%' OR chnl_div LIKE '%투고%' THEN cnt ELSE 0 END) AS takeout_orders,
    SUM(CASE WHEN hour_div <= 15 THEN amt ELSE 0 END) AS lunch_sales,
    SUM(CASE WHEN hour_div <= 15 THEN cnt ELSE 0 END) AS lunch_orders,
    SUM(CASE WHEN hour_div > 15 AND hour_div < 17 THEN amt ELSE 0 END) AS swing_sales,
    SUM(CASE WHEN hour_div > 15 AND hour_div < 17 THEN cnt ELSE 0 END) AS swing_orders,
    SUM(CASE WHEN hour_div >= 17 THEN amt ELSE 0 END) AS dinner_sales,
    SUM(CASE WHEN hour_div >= 17 THEN cnt ELSE 0 END) AS dinner_orders
FROM normalized
GROUP BY store_id, sale_dt
"""

PAY_COUNT_SQL = """
SELECT
    masked_stor_cd AS store_id,
    sale_dt,
    COUNT(*) FILTER (WHERE COALESCE(pay_way_cd, '') <> '18') AS in_store_pay_count
FROM raw_daily_store_pay_way
GROUP BY masked_stor_cd, sale_dt
"""

CATEGORY_SALES_SQL = """
WITH item_with_cat AS (
    SELECT
        s.masked_stor_cd AS store_id,
        s.sale_dt,
        s.item_nm,
        s.sale_amt,
        c.is_coffee,
        c.is_drink,
        c.is_food
    FROM core_daily_item_sales s
    LEFT JOIN mart_item_category_master c ON c.item_nm = s.item_nm
    WHERE s.sale_qty > 0
)
SELECT
    store_id,
    sale_dt,
    SUM(CASE WHEN is_coffee THEN sale_amt ELSE 0 END) AS coffee_sales,
    SUM(CASE WHEN is_drink THEN sale_amt ELSE 0 END) AS drink_sales,
    SUM(CASE WHEN is_food THEN sale_amt ELSE 0 END) AS food_sales,
    COUNT(DISTINCT item_nm) AS item_count
FROM item_with_cat
GROUP BY store_id, sale_dt
"""

TOP_ITEM_SQL = """
SELECT DISTINCT ON (s.masked_stor_cd, s.sale_dt)
    s.masked_stor_cd AS store_id,
    s.sale_dt,
    s.item_nm AS top_item_nm,
    s.sale_amt AS top_item_sales
FROM core_daily_item_sales s
WHERE s.sale_qty > 0
ORDER BY s.masked_stor_cd, s.sale_dt, s.sale_amt DESC NULLS LAST
"""

WEATHER_SQL = """
SELECT
    sm.masked_stor_cd AS store_id,
    w.weather_dt AS sale_dt,
    AVG(w.avg_temp_c) AS avg_temp_c,
    AVG(w.precipitation_mm) AS precipitation_mm
FROM raw_weather_daily w
JOIN raw_store_master sm ON sm.sido = w.sido
GROUP BY sm.masked_stor_cd, w.weather_dt
"""


def fetch(engine, sql: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql)).mappings().all()]


def index_by_key(rows: list[dict], *keys: str) -> dict[tuple, dict]:
    result: dict[tuple, dict] = {}
    for r in rows:
        key = tuple(r[k] for k in keys)
        result[key] = r
    return result


def upsert(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_store_daily_kpi (
            store_id, sale_dt,
            total_sales, total_orders, delivery_sales, takeout_orders, in_store_pay_count,
            lunch_avg_ticket, swing_avg_ticket, dinner_avg_ticket,
            coffee_sales, drink_sales, food_sales, coffee_attach_ratio,
            top_item_nm, top_item_sales,
            avg_temp_c, precipitation_mm, item_count, generated_at
        ) VALUES (
            :store_id, :sale_dt,
            :total_sales, :total_orders, :delivery_sales, :takeout_orders, :in_store_pay_count,
            :lunch_avg_ticket, :swing_avg_ticket, :dinner_avg_ticket,
            :coffee_sales, :drink_sales, :food_sales, :coffee_attach_ratio,
            :top_item_nm, :top_item_sales,
            :avg_temp_c, :precipitation_mm, :item_count, NOW()
        )
        ON CONFLICT (store_id, sale_dt) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            total_orders = EXCLUDED.total_orders,
            delivery_sales = EXCLUDED.delivery_sales,
            takeout_orders = EXCLUDED.takeout_orders,
            in_store_pay_count = EXCLUDED.in_store_pay_count,
            lunch_avg_ticket = EXCLUDED.lunch_avg_ticket,
            swing_avg_ticket = EXCLUDED.swing_avg_ticket,
            dinner_avg_ticket = EXCLUDED.dinner_avg_ticket,
            coffee_sales = EXCLUDED.coffee_sales,
            drink_sales = EXCLUDED.drink_sales,
            food_sales = EXCLUDED.food_sales,
            coffee_attach_ratio = EXCLUDED.coffee_attach_ratio,
            top_item_nm = EXCLUDED.top_item_nm,
            top_item_sales = EXCLUDED.top_item_sales,
            avg_temp_c = EXCLUDED.avg_temp_c,
            precipitation_mm = EXCLUDED.precipitation_mm,
            item_count = EXCLUDED.item_count,
            generated_at = NOW()
        """
    )
    with engine.begin() as conn:
        # 청크 단위 upsert로 RAM 부담 감소
        chunk_size = 5000
        for i in range(0, len(rows), chunk_size):
            conn.execute(sql, rows[i : i + chunk_size])


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    logger.info("채널 KPI 집계 중...")
    channel_rows = fetch(engine, CHANNEL_KPI_SQL)
    logger.info("channel_rows=%d", len(channel_rows))

    logger.info("매장 결제 건수 집계 중...")
    pay_rows = fetch(engine, PAY_COUNT_SQL)
    pay_index = index_by_key(pay_rows, "store_id", "sale_dt")

    logger.info("카테고리 매출 집계 중...")
    cat_rows = fetch(engine, CATEGORY_SALES_SQL)
    cat_index = index_by_key(cat_rows, "store_id", "sale_dt")

    logger.info("일별 1위 메뉴 집계 중...")
    top_rows = fetch(engine, TOP_ITEM_SQL)
    top_index = index_by_key(top_rows, "store_id", "sale_dt")

    logger.info("날씨 집계 중...")
    weather_rows = fetch(engine, WEATHER_SQL)
    weather_index = index_by_key(weather_rows, "store_id", "sale_dt")

    payload = []
    for row in channel_rows:
        key = (row["store_id"], row["sale_dt"])
        total_sales = float(row["total_sales"] or 0)
        total_orders = float(row["total_orders"] or 0)

        def avg_ticket(amt: float, cnt: float) -> float:
            return round(amt / cnt, 2) if cnt > 0 else 0.0

        cat = cat_index.get(key, {})
        coffee_sales = float(cat.get("coffee_sales") or 0)
        drink_sales = float(cat.get("drink_sales") or 0)
        food_sales = float(cat.get("food_sales") or 0)
        coffee_attach_ratio = (
            round((coffee_sales / total_sales) * 100, 4) if total_sales > 0 else 0
        )

        top = top_index.get(key, {})
        weather = weather_index.get(key, {})
        pay = pay_index.get(key, {})

        payload.append(
            {
                "store_id": row["store_id"],
                "sale_dt": row["sale_dt"],
                "total_sales": round(total_sales, 2),
                "total_orders": round(total_orders, 2),
                "delivery_sales": round(float(row["delivery_sales"] or 0), 2),
                "takeout_orders": round(float(row["takeout_orders"] or 0), 2),
                "in_store_pay_count": int(pay.get("in_store_pay_count") or 0),
                "lunch_avg_ticket": avg_ticket(
                    float(row["lunch_sales"] or 0), float(row["lunch_orders"] or 0)
                ),
                "swing_avg_ticket": avg_ticket(
                    float(row["swing_sales"] or 0), float(row["swing_orders"] or 0)
                ),
                "dinner_avg_ticket": avg_ticket(
                    float(row["dinner_sales"] or 0), float(row["dinner_orders"] or 0)
                ),
                "coffee_sales": round(coffee_sales, 2),
                "drink_sales": round(drink_sales, 2),
                "food_sales": round(food_sales, 2),
                "coffee_attach_ratio": coffee_attach_ratio,
                "top_item_nm": top.get("top_item_nm"),
                "top_item_sales": round(float(top.get("top_item_sales") or 0), 2),
                "avg_temp_c": (
                    round(float(weather["avg_temp_c"]), 2)
                    if weather.get("avg_temp_c") is not None
                    else None
                ),
                "precipitation_mm": (
                    round(float(weather["precipitation_mm"]), 2)
                    if weather.get("precipitation_mm") is not None
                    else None
                ),
                "item_count": int(cat.get("item_count") or 0),
            }
        )

    logger.info("upsert mart_store_daily_kpi (%d rows)...", len(payload))
    upsert(engine, payload)
    logger.info("완료.")


if __name__ == "__main__":
    main()
