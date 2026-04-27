"""mart_product_price_daily / mart_product_price_master 적재 스크립트

원천:
- core_daily_item_sales: 전 지점 일별 판매가 집계
- raw_campaign_master + raw_campaign_item: 진행중 캠페인 매칭

is_promotion 판정 기준:
- 일별 net_price < 평상시(mode list_price) × 0.95 (5%↑ 할인된 날)
- 또는 raw_campaign_master.start_dt~fnsh_dt 범위 내 + raw_campaign_item.item_cd 매칭
"""

from __future__ import annotations
from _runner import run_main

import logging
import sys
from collections import Counter
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


PROMOTION_DISCOUNT_THRESHOLD = 0.05  # 평상시 정가 대비 5%↑ 할인 시 프로모션
LATEST_WINDOW_DAYS = 30


def fetch_daily_rows(engine) -> list[dict]:
    """전 지점 통합 일별 가격 집계 — core_daily_item_sales 기반"""
    sql = text(
        """
        SELECT
            COALESCE(NULLIF(TRIM(item_cd), ''), item_nm) AS item_cd,
            item_nm,
            sale_dt AS price_dt,
            ROUND(SUM(sale_amt) / NULLIF(SUM(sale_qty), 0), 2) AS list_price,
            ROUND(SUM(actual_sale_amt) / NULLIF(SUM(sale_qty), 0), 2) AS net_price,
            ROUND(SUM(COALESCE(dc_amt, 0) + COALESCE(enuri_amt, 0)) / NULLIF(SUM(sale_qty), 0), 2) AS discount_amount,
            ROUND(
                (SUM(COALESCE(dc_amt, 0) + COALESCE(enuri_amt, 0)) / NULLIF(SUM(sale_amt), 0)) * 100,
                4
            ) AS discount_rate,
            COUNT(DISTINCT masked_stor_cd) AS sample_store_count,
            SUM(sale_qty) AS sample_qty
        FROM core_daily_item_sales
        WHERE sale_qty > 0
          AND item_nm IS NOT NULL
          AND TRIM(item_nm) <> ''
        GROUP BY item_cd, item_nm, sale_dt
        HAVING SUM(sale_qty) > 0
        """
    )
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(sql).mappings().all()]


def fetch_campaign_periods(engine) -> list[dict]:
    """확정 상태 캠페인 기간 + 적용 품목 매핑"""
    sql = text(
        """
        SELECT
            m.cpi_cd,
            m.start_dt,
            m.fnsh_dt,
            i.item_cd
        FROM raw_campaign_master m
        JOIN raw_campaign_item i ON i.cpi_cd = m.cpi_cd
        WHERE COALESCE(m.prgrs_status_nm, '') = '확정'
          AND COALESCE(i.use_yn, 'Y') = 'Y'
          AND TRIM(COALESCE(i.item_cd, '')) <> ''
          AND TRIM(COALESCE(m.start_dt, '')) <> ''
          AND TRIM(COALESCE(m.fnsh_dt, '')) <> ''
        """
    )
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(sql).mappings().all()]


def index_campaigns(rows: list[dict]) -> dict[str, list[tuple[str, str, str]]]:
    """item_cd → [(cpi_cd, start_dt, fnsh_dt), ...]"""
    index: dict[str, list[tuple[str, str, str]]] = {}
    for row in rows:
        item_cd = str(row.get("item_cd") or "").strip()
        if not item_cd:
            continue
        index.setdefault(item_cd, []).append(
            (
                str(row["cpi_cd"]),
                str(row["start_dt"]).replace("-", ""),
                str(row["fnsh_dt"]).replace("-", ""),
            )
        )
    return index


def matched_campaigns_for(
    item_cd: str, price_dt: str, index: dict[str, list[tuple[str, str, str]]]
) -> list[str]:
    bucket = index.get(item_cd, [])
    matched: list[str] = []
    for cpi_cd, start_dt, fnsh_dt in bucket:
        if start_dt <= price_dt <= fnsh_dt:
            matched.append(cpi_cd)
    return matched


def compute_master_rows(daily_rows: list[dict]) -> list[dict]:
    """mart_product_price_master용 품목별 1행 집계"""
    by_item: dict[tuple[str, str], list[dict]] = {}
    for row in daily_rows:
        key = (str(row["item_cd"]), str(row["item_nm"]))
        by_item.setdefault(key, []).append(row)

    master_rows: list[dict] = []
    if not daily_rows:
        return master_rows

    max_dt = max(str(r["price_dt"]) for r in daily_rows)
    cutoff_dt = _shift_yyyymmdd(max_dt, -LATEST_WINDOW_DAYS)

    for (item_cd, item_nm), rows in by_item.items():
        list_prices = [float(r["list_price"]) for r in rows if r["list_price"] is not None]
        if not list_prices:
            continue
        # 평상시 정가 = 가장 빈번한(mode) list_price
        rounded = [round(price, 0) for price in list_prices]
        regular_list_price = float(Counter(rounded).most_common(1)[0][0])

        # 평상시 net_price = is_promotion=False 일자 평균
        non_promo_net = [
            float(r["net_price"]) for r in rows if r.get("net_price") is not None and not r["_is_promotion"]
        ]
        regular_net_price = (
            sum(non_promo_net) / len(non_promo_net) if non_promo_net else regular_list_price
        )

        recent = [r for r in rows if str(r["price_dt"]) >= cutoff_dt]
        latest_list = [float(r["list_price"]) for r in recent if r["list_price"] is not None]
        latest_net = [float(r["net_price"]) for r in recent if r["net_price"] is not None]
        latest_list_price = sum(latest_list) / len(latest_list) if latest_list else regular_list_price
        latest_net_price = sum(latest_net) / len(latest_net) if latest_net else regular_net_price

        # 가격 변동: 일별 list_price의 고유값(반올림 단위) 수
        unique_prices = sorted(set(rounded))
        price_change_count = max(0, len(unique_prices) - 1)

        # 마지막 가격 변동: 직전 일자와 list_price가 다른 가장 최근 일자
        sorted_rows = sorted(rows, key=lambda r: str(r["price_dt"]))
        last_change_dt: str | None = None
        for prev, curr in zip(sorted_rows, sorted_rows[1:]):
            if round(float(prev["list_price"] or 0), 0) != round(float(curr["list_price"] or 0), 0):
                last_change_dt = str(curr["price_dt"])

        active_promotion_count = sum(1 for r in recent if r["_matched_campaign_codes"])

        master_rows.append(
            {
                "item_cd": item_cd,
                "item_nm": item_nm,
                "regular_list_price": round(regular_list_price, 2),
                "regular_net_price": round(regular_net_price, 2),
                "latest_list_price": round(latest_list_price, 2),
                "latest_net_price": round(latest_net_price, 2),
                "price_change_count": price_change_count,
                "last_price_change_dt": last_change_dt,
                "active_promotion_count": active_promotion_count,
                "sample_day_count": len(rows),
            }
        )
    return master_rows


def _shift_yyyymmdd(dt_str: str, delta_days: int) -> str:
    from datetime import datetime, timedelta

    base = datetime.strptime(dt_str, "%Y%m%d")
    return (base + timedelta(days=delta_days)).strftime("%Y%m%d")


def annotate_promotion_flags(
    daily_rows: list[dict], campaign_index: dict[str, list[tuple[str, str, str]]]
) -> None:
    """1차 패스: 캠페인 매칭으로 is_promotion 판정 (mode 기반은 2차 패스)"""
    by_item: dict[tuple[str, str], list[dict]] = {}
    for row in daily_rows:
        by_item.setdefault((str(row["item_cd"]), str(row["item_nm"])), []).append(row)

    for (item_cd, item_nm), rows in by_item.items():
        list_prices = [round(float(r["list_price"] or 0), 0) for r in rows if r["list_price"] is not None]
        regular_price = (
            float(Counter(list_prices).most_common(1)[0][0]) if list_prices else 0.0
        )
        for row in rows:
            matched = matched_campaigns_for(item_cd, str(row["price_dt"]), campaign_index)
            net_price = float(row["net_price"] or 0)
            price_drop = (
                regular_price > 0 and net_price > 0
                and (regular_price - net_price) / regular_price >= PROMOTION_DISCOUNT_THRESHOLD
            )
            row["_matched_campaign_codes"] = matched
            row["_is_promotion"] = bool(matched) or bool(price_drop)


def upsert_daily_rows(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_product_price_daily (
            item_cd, item_nm, price_dt, list_price, net_price,
            discount_amount, discount_rate, sample_store_count, sample_qty,
            is_promotion, matched_campaign_codes, generated_at
        ) VALUES (
            :item_cd, :item_nm, :price_dt, :list_price, :net_price,
            :discount_amount, :discount_rate, :sample_store_count, :sample_qty,
            :is_promotion, :matched_campaign_codes, NOW()
        )
        ON CONFLICT (item_cd, item_nm, price_dt) DO UPDATE SET
            list_price = EXCLUDED.list_price,
            net_price = EXCLUDED.net_price,
            discount_amount = EXCLUDED.discount_amount,
            discount_rate = EXCLUDED.discount_rate,
            sample_store_count = EXCLUDED.sample_store_count,
            sample_qty = EXCLUDED.sample_qty,
            is_promotion = EXCLUDED.is_promotion,
            matched_campaign_codes = EXCLUDED.matched_campaign_codes,
            generated_at = NOW()
        """
    )
    payload = [
        {
            "item_cd": r["item_cd"],
            "item_nm": r["item_nm"],
            "price_dt": r["price_dt"],
            "list_price": r["list_price"] or 0,
            "net_price": r["net_price"] or 0,
            "discount_amount": r["discount_amount"] or 0,
            "discount_rate": r["discount_rate"] or 0,
            "sample_store_count": int(r["sample_store_count"] or 0),
            "sample_qty": r["sample_qty"] or 0,
            "is_promotion": bool(r["_is_promotion"]),
            "matched_campaign_codes": r["_matched_campaign_codes"],
        }
        for r in rows
    ]
    with engine.begin() as conn:
        conn.execute(sql, payload)


def upsert_master_rows(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_product_price_master (
            item_cd, item_nm, regular_list_price, regular_net_price,
            latest_list_price, latest_net_price, price_change_count,
            last_price_change_dt, active_promotion_count, sample_day_count, updated_at
        ) VALUES (
            :item_cd, :item_nm, :regular_list_price, :regular_net_price,
            :latest_list_price, :latest_net_price, :price_change_count,
            :last_price_change_dt, :active_promotion_count, :sample_day_count, NOW()
        )
        ON CONFLICT (item_cd, item_nm) DO UPDATE SET
            regular_list_price = EXCLUDED.regular_list_price,
            regular_net_price = EXCLUDED.regular_net_price,
            latest_list_price = EXCLUDED.latest_list_price,
            latest_net_price = EXCLUDED.latest_net_price,
            price_change_count = EXCLUDED.price_change_count,
            last_price_change_dt = EXCLUDED.last_price_change_dt,
            active_promotion_count = EXCLUDED.active_promotion_count,
            sample_day_count = EXCLUDED.sample_day_count,
            updated_at = NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    logger.info("일별 판매가 집계 조회 중...")
    daily_rows = fetch_daily_rows(engine)
    logger.info("daily_rows=%d", len(daily_rows))

    logger.info("캠페인 기간 인덱싱 중...")
    campaign_rows = fetch_campaign_periods(engine)
    campaign_index = index_campaigns(campaign_rows)
    logger.info("campaign items=%d", len(campaign_index))

    annotate_promotion_flags(daily_rows, campaign_index)
    promo_count = sum(1 for r in daily_rows if r["_is_promotion"])
    logger.info("is_promotion=True 일자 수=%d / %d", promo_count, len(daily_rows))

    logger.info("mart_product_price_master 집계 중...")
    master_rows = compute_master_rows(daily_rows)
    logger.info("master_rows=%d", len(master_rows))

    logger.info("mart_product_price_daily upsert...")
    upsert_daily_rows(engine, daily_rows)
    logger.info("mart_product_price_master upsert...")
    upsert_master_rows(engine, master_rows)
    logger.info("완료.")


if __name__ == "__main__":
    run_main(main)