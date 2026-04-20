from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.infrastructure.db.utils import has_table


class HomeRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def list_schedule_events(
        self, store_id: str | None = None, today: date | None = None, window_days: int = 90
    ) -> list[dict[str, str]]:
        if not self.engine:
            return []

        today_date = today or date.today()
        today_str = today_date.strftime("%Y%m%d")
        window_end_str = (today_date + timedelta(days=window_days)).strftime("%Y%m%d")
        events: list[dict[str, str]] = []

        with self.engine.connect() as conn:
            if has_table(self.engine, "raw_campaign_master"):
                events.extend(
                    self._query_campaign_events(
                        conn=conn, store_id=store_id, start=today_str, end=window_end_str
                    )
                )
            if has_table(self.engine, "raw_telecom_discount_policy"):
                events.extend(
                    self._query_telecom_events(
                        conn=conn, store_id=store_id, start=today_str, end=window_end_str
                    )
                )
        return sorted(events, key=lambda event: event["date"])

    def _query_campaign_events(
        self,
        conn,
        store_id: str | None,
        start: str,
        end: str,
    ) -> list[dict[str, str]]:
        if not self.engine:
            return []
        campaign_columns = {
            column["name"].lower() for column in inspect(self.engine).get_columns("raw_campaign_master")
        }
        campaign_store_col = next(
            (
                column
                for column in ("masked_stor_cd", "store_id", "stor_cd")
                if column in campaign_columns
            ),
            None,
        )
        campaign_store_filter = (
            f" AND {campaign_store_col} = :store_id" if store_id and campaign_store_col else ""
        )
        campaign_params: dict[str, str] = {"start": start, "end": end}
        if store_id and campaign_store_col:
            campaign_params["store_id"] = store_id

        rows = (
            conn.execute(
                text(
                    f"""
                SELECT cpi_nm, start_dt, fnsh_dt, cpi_kind_nm
                FROM raw_campaign_master
                WHERE start_dt <= :end AND fnsh_dt >= :start
                  AND use_yn = '1'
                  AND fnsh_dt < '99991221'
                  {campaign_store_filter}
                ORDER BY fnsh_dt
                LIMIT 20
            """
                ),
                campaign_params,
            )
            .mappings()
            .all()
        )

        events: list[dict[str, str]] = []
        for row in rows:
            start_dt = str(row["start_dt"] or "")
            end_dt = str(row["fnsh_dt"] or "")
            display_date = end_dt if start_dt < start else start_dt
            events.append(
                {
                    "date": display_date,
                    "title": str(row["cpi_nm"] or "캠페인"),
                    "type": "campaign",
                    "description": f"{row['cpi_kind_nm'] or ''} · {start_dt} ~ {end_dt}",
                }
            )
        return events

    def _query_telecom_events(
        self,
        conn,
        store_id: str | None,
        start: str,
        end: str,
    ) -> list[dict[str, str]]:
        if not self.engine:
            return []
        telecom_columns = {
            column["name"].lower()
            for column in inspect(self.engine).get_columns("raw_telecom_discount_policy")
        }
        telecom_store_col = next(
            (
                column
                for column in ("masked_stor_cd", "store_id", "stor_cd")
                if column in telecom_columns
            ),
            None,
        )
        telecom_store_filter = (
            f" AND {telecom_store_col} = :store_id" if store_id and telecom_store_col else ""
        )
        telecom_params: dict[str, str] = {"start": start, "end": end}
        if store_id and telecom_store_col:
            telecom_params["store_id"] = store_id

        rows = (
            conn.execute(
                text(
                    f"""
                SELECT pay_dc_nm, start_dt, fnsh_dt, pay_dc_grp_type_nm
                FROM raw_telecom_discount_policy
                WHERE start_dt <= :end AND fnsh_dt >= :start
                  AND use_yn = '1'
                  AND fnsh_dt < '99991221'
                  {telecom_store_filter}
                ORDER BY fnsh_dt
                LIMIT 10
            """
                ),
                telecom_params,
            )
            .mappings()
            .all()
        )

        events: list[dict[str, str]] = []
        for row in rows:
            start_dt = str(row["start_dt"] or "")
            end_dt = str(row["fnsh_dt"] or "")
            display_date = end_dt if start_dt < start else start_dt
            events.append(
                {
                    "date": display_date,
                    "title": str(row["pay_dc_nm"] or "통신사 할인"),
                    "type": "telecom",
                    "description": f"{row['pay_dc_grp_type_nm'] or ''} · {start_dt} ~ {end_dt}",
                }
            )
        return events
