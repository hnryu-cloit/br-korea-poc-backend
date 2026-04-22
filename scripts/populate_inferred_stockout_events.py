from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.infrastructure.db.utils import has_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate core_inferred_stockout_event from recorded stockout data and hourly sales inference."
    )
    parser.add_argument("--store-id", help="Optional store id filter, e.g. POC_001")
    parser.add_argument("--date-from", help="Optional start date filter (YYYYMMDD)")
    parser.add_argument("--date-to", help="Optional end date filter (YYYYMMDD)")
    return parser.parse_args()


def build_scope(store_col: str, date_col: str, args: argparse.Namespace) -> tuple[str, dict[str, object]]:
    clauses: list[str] = []
    params: dict[str, object] = {}
    if args.store_id:
        clauses.append(f"{store_col} = :store_id")
        params["store_id"] = args.store_id
    if args.date_from:
        clauses.append(f"{date_col} >= :date_from")
        params["date_from"] = args.date_from
    if args.date_to:
        clauses.append(f"{date_col} <= :date_to")
        params["date_to"] = args.date_to
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def main() -> None:
    args = parse_args()
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver is not installed.")
    if not has_table(engine, "core_inferred_stockout_event"):
        raise RuntimeError("core_inferred_stockout_event table does not exist. Run migrations first.")

    has_store_hours = has_table(engine, "store_operating_hours")
    operating_hours_join = (
        "LEFT JOIN store_operating_hours soh ON soh.masked_stor_cd = h.masked_stor_cd"
        if has_store_hours
        else ""
    )
    open_hour_expr = (
        "CASE WHEN soh.open_hour IS NOT NULL AND soh.close_hour IS NOT NULL AND soh.close_hour >= soh.open_hour THEN soh.open_hour ELSE 8 END"
        if has_store_hours
        else "8"
    )
    close_hour_expr = (
        "CASE WHEN soh.open_hour IS NOT NULL AND soh.close_hour IS NOT NULL AND soh.close_hour >= soh.open_hour THEN soh.close_hour ELSE 21 END"
        if has_store_hours
        else "21"
    )

    delete_scope, params = build_scope("masked_stor_cd", "sale_dt", args)
    recorded_scope, recorded_params = build_scope("st.masked_stor_cd", "st.prc_dt", args)
    inferred_scope, inferred_params = build_scope("h.masked_stor_cd", "h.sale_dt", args)
    recorded_item_scope, recorded_item_params = build_scope("masked_stor_cd", "prc_dt", args)

    delete_sql = text(
        f"""
        DELETE FROM core_inferred_stockout_event
        WHERE 1 = 1
        {delete_scope}
        """
    )

    recorded_insert_sql = text(
        f"""
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
            evidence_end_hour
        )
        SELECT
            st.masked_stor_cd,
            st.prc_dt AS sale_dt,
            st.item_cd,
            COALESCE(NULLIF(TRIM(st.item_nm), ''), st.item_cd) AS item_nm,
            TRUE AS is_stockout,
            st.stockout_hour,
            'raw_stockout_time' AS rule_type,
            'core_stockout_time' AS source_table,
            {open_hour_expr} AS open_hour,
            {close_hour_expr} AS close_hour,
            NULL AS zero_sales_window,
            st.stockout_hour AS evidence_start_hour,
            st.stockout_hour AS evidence_end_hour
        FROM core_stockout_time st
        {'LEFT JOIN store_operating_hours soh ON soh.masked_stor_cd = st.masked_stor_cd' if has_store_hours else ''}
        WHERE st.is_stockout = TRUE
          AND st.stockout_hour IS NOT NULL
          {recorded_scope}
        """
    )

    inferred_insert_sql = text(
        f"""
        WITH hourly_agg AS (
            SELECT
                h.masked_stor_cd,
                h.sale_dt,
                COALESCE(NULLIF(TRIM(h.item_cd), ''), NULLIF(TRIM(h.item_nm), '')) AS item_cd,
                COALESCE(NULLIF(TRIM(h.item_nm), ''), NULLIF(TRIM(h.item_cd), '')) AS item_nm,
                CAST(h.tmzon_div AS INT) AS hour,
                SUM(COALESCE(h.sale_qty, 0)) AS sale_qty
            FROM core_hourly_item_sales h
            WHERE h.tmzon_div ~ '^[0-9]{{2}}$'
              {inferred_scope}
            GROUP BY
                h.masked_stor_cd,
                h.sale_dt,
                COALESCE(NULLIF(TRIM(h.item_cd), ''), NULLIF(TRIM(h.item_nm), '')),
                COALESCE(NULLIF(TRIM(h.item_nm), ''), NULLIF(TRIM(h.item_cd), '')),
                CAST(h.tmzon_div AS INT)
        ),
        item_scope AS (
            SELECT
                h.masked_stor_cd,
                h.sale_dt,
                h.item_cd,
                MAX(h.item_nm) AS item_nm,
                MIN({open_hour_expr}) AS open_hour,
                MIN({close_hour_expr}) AS close_hour
            FROM hourly_agg h
            {operating_hours_join}
            GROUP BY
                h.masked_stor_cd,
                h.sale_dt,
                h.item_cd
        ),
        hour_grid AS (
            SELECT
                s.masked_stor_cd,
                s.sale_dt,
                s.item_cd,
                s.item_nm,
                s.open_hour,
                s.close_hour,
                gs.hour
            FROM item_scope s
            JOIN LATERAL generate_series(s.open_hour, s.close_hour) AS gs(hour) ON TRUE
        ),
        scored AS (
            SELECT
                g.masked_stor_cd,
                g.sale_dt,
                g.item_cd,
                g.item_nm,
                g.open_hour,
                g.close_hour,
                g.hour,
                COALESCE(h.sale_qty, 0) AS sale_qty
            FROM hour_grid g
            LEFT JOIN hourly_agg h
                ON h.masked_stor_cd = g.masked_stor_cd
               AND h.sale_dt = g.sale_dt
               AND h.item_cd = g.item_cd
               AND h.hour = g.hour
        ),
        last_positive AS (
            SELECT
                s.masked_stor_cd,
                s.sale_dt,
                s.item_cd,
                MAX(s.item_nm) AS item_nm,
                MIN(s.open_hour) AS open_hour,
                MIN(s.close_hour) AS close_hour,
                MAX(CASE WHEN s.sale_qty > 0 THEN s.hour END) AS last_positive_hour
            FROM scored s
            GROUP BY
                s.masked_stor_cd,
                s.sale_dt,
                s.item_cd
        ),
        recorded_items AS (
            SELECT DISTINCT
                masked_stor_cd,
                prc_dt AS sale_dt,
                item_cd
            FROM core_stockout_time
            WHERE item_cd IS NOT NULL
              {recorded_item_scope}
        ),
        candidates AS (
            SELECT
                a.masked_stor_cd,
                a.sale_dt,
                a.item_cd,
                a.item_nm,
                a.open_hour,
                a.close_hour,
                a.last_positive_hour
            FROM last_positive a
            LEFT JOIN recorded_items r
                ON r.masked_stor_cd = a.masked_stor_cd
               AND r.sale_dt = a.sale_dt
               AND r.item_cd = a.item_cd
            WHERE r.item_cd IS NULL
              AND a.last_positive_hour IS NOT NULL
              AND a.close_hour - a.last_positive_hour >= 3
        )
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
            evidence_end_hour
        )
        SELECT
            c.masked_stor_cd,
            c.sale_dt,
            c.item_cd,
            c.item_nm,
            TRUE AS is_stockout,
            c.last_positive_hour + 1 AS stockout_hour,
            'hourly_zero_sales_3h' AS rule_type,
            'core_hourly_item_sales' AS source_table,
            c.open_hour,
            c.close_hour,
            3 AS zero_sales_window,
            c.last_positive_hour + 1 AS evidence_start_hour,
            LEAST(c.last_positive_hour + 3, c.close_hour) AS evidence_end_hour
        FROM candidates c
        """
    )

    with engine.begin() as connection:
        connection.execute(delete_sql, params)
        recorded_result = connection.execute(recorded_insert_sql, recorded_params)
        inferred_result = connection.execute(inferred_insert_sql, inferred_params | recorded_item_params)

    recorded_count = recorded_result.rowcount if recorded_result.rowcount is not None else -1
    inferred_count = inferred_result.rowcount if inferred_result.rowcount is not None else -1
    print(
        f"Populated core_inferred_stockout_event on {get_safe_database_url()} "
        f"(recorded={recorded_count}, inferred={inferred_count})."
    )


if __name__ == "__main__":
    main()
