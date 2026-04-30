from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.infrastructure.db.utils import has_table


_DASHBOARD_DOMAIN_AGENT_MAP = {
    "production": "생산 관리",
    "ordering": "주문 관리",
    "sales": "매출 관리",
}

_DASHBOARD_QUESTION_PRIORITIES = {
    "production": [
        "오늘 품목별 폐기 수량을 판매 수량으로 나눈 폐기율 높은 품목 알려줘",
        "품절이 자주 발생하는 특정 시간대가 있어?",
        "최근 30일 중 폐기가 집중된 요일은?",
    ],
    "ordering": [
        "최근 30일 발주 수량이 들쭉날쭉한 품목 있어?",
        "주문 마감 전 알림 기준으로 전주/전전주/전월 동요일 3가지 추천량을 보여줘",
        "각 옵션별 총량과 품목별 상세 수량을 같이 보여줘",
    ],
    "sales": [
        "오늘 총매출과 일평균 매출 알려줘",
        "오늘 상품 매출 상위 10개 알려줘",
        "오늘 시간대별 매출 피크 알려줘",
    ],
}

_DASHBOARD_QUESTION_DISPLAY_MAP = {
    "production": {
        "오늘 품목별 폐기 수량을 판매 수량으로 나눈 폐기율 높은 품목 알려줘": "오늘 폐기율 높은 품목은?",
        "품절이 자주 발생하는 특정 시간대가 있어?": "품절 잦은 시간대 알려줘",
        "최근 30일 중 폐기가 집중된 요일은?": "최근 30일 폐기 집중 요일은?",
        "오늘 품절 발생 시각이 가장 빠른 품목 알려줘": "오늘 가장 빨리 품절되는 품목 알려줘.",
        "오늘 시간대별 판매 속도 대비 현재 재고 부족 위험 품목": "오늘 판매 속도 대비 재고가 부족할 위험이 있는 품목 알려줘.",
    },
    "ordering": {
        "최근 30일 발주 수량이 들쭉날쭉한 품목 있어?": "발주 변동성 큰 품목 알려줘",
        "주문 마감 전 알림 기준으로 전주/전전주/전월 동요일 3가지 추천량을 보여줘": "이번 주 추천 발주량 알려줘",
        "각 옵션별 총량과 품목별 상세 수량을 같이 보여줘": "옵션별 총량과 상세 수량 알려줘",
    },
    "sales": {
        "오늘 총매출과 일평균 매출 알려줘": "오늘 총매출과 일평균 매출 알려줘.",
        "오늘 상품 매출 상위 10개 알려줘": "오늘 상품 매출 상위 품목 알려줘.",
        "오늘 시간대별 매출 피크 알려줘": "오늘 매출이 가장 높은 시간대 알려줘.",
    },
}


class HomeRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def list_schedule_events(
        self,
        store_id: str | None = None,
        today: date | None = None,
        window_days: int = 90,
        limit: int | None = 20,
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
        sorted_events = sorted(events, key=lambda event: (event["date"], event["category"], event["title"]))
        if limit is None or limit <= 0:
            return sorted_events
        return sorted_events[:limit]

    async def get_dashboard_recommended_questions(
        self,
        target_date: date | None = None,
    ) -> dict[str, list[str]]:
        effective_date = target_date or date.today()
        year_month = effective_date.strftime("%Y%m")
        fallback = self._build_dashboard_recommended_questions()

        if not self.engine or not has_table(self.engine, "dashboard_recommended_questions"):
            return {
                domain: [
                    self._to_dashboard_display_question(domain, str(entry["question"]))
                    for entry in questions[:3]
                ]
                for domain, questions in fallback.items()
            }

        with self.engine.begin() as conn:
            rows = (
                conn.execute(
                    text(
                        """
                        SELECT domain, rank_no, question
                        FROM dashboard_recommended_questions
                        WHERE year_month = :year_month
                        ORDER BY domain, rank_no
                        """
                    ),
                    {"year_month": year_month},
                )
                .mappings()
                .all()
            )
            grouped: dict[str, list[str]] = {}
            for row in rows:
                domain = str(row["domain"])
                grouped.setdefault(domain, []).append(
                    self._to_dashboard_display_question(domain, str(row["question"]))
                )

            if all(len(grouped.get(domain, [])) >= 3 for domain in _DASHBOARD_DOMAIN_AGENT_MAP):
                return {domain: grouped[domain][:3] for domain in _DASHBOARD_DOMAIN_AGENT_MAP}

            conn.execute(
                text(
                    """
                    DELETE FROM dashboard_recommended_questions
                    WHERE year_month = :year_month
                    """
                ),
                {"year_month": year_month},
            )
            for domain, questions in fallback.items():
                for rank_no, question in enumerate(questions[:3], start=1):
                    conn.execute(
                        text(
                            """
                            INSERT INTO dashboard_recommended_questions (
                                year_month,
                                domain,
                                rank_no,
                                question,
                                source_agent,
                                source_question_no,
                                source_count,
                                updated_at
                            ) VALUES (
                                :year_month,
                                :domain,
                                :rank_no,
                                :question,
                                :source_agent,
                                :source_question_no,
                                :source_count,
                                NOW()
                            )
                            """
                        ),
                        {
                            "year_month": year_month,
                            "domain": domain,
                            "rank_no": rank_no,
                            "question": self._to_dashboard_display_question(
                                domain, str(question["question"])
                            ),
                            "source_agent": question["source_agent"],
                            "source_question_no": question["source_question_no"],
                            "source_count": question["source_count"],
                        },
                    )

        return {
            domain: [
                self._to_dashboard_display_question(domain, str(entry["question"]))
                for entry in questions[:3]
            ]
            for domain, questions in fallback.items()
        }

    @staticmethod
    def _golden_queries_csv_path() -> Path:
        return Path(__file__).resolve().parents[2] / "docs" / "golden-queries-store-owner.csv"

    @classmethod
    def _build_dashboard_recommended_questions(cls) -> dict[str, list[dict[str, str | int]]]:
        path = cls._golden_queries_csv_path()
        if not path.exists():
            return cls._build_dashboard_question_fallback()

        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            if len(fieldnames) < 6:
                return cls._build_dashboard_question_fallback()
            question_no_key = fieldnames[0]
            agent_key = fieldnames[2]
            question_key = fieldnames[3]
            available_key = fieldnames[5]

            grouped: dict[str, dict[str, dict[str, str | int]]] = {
                domain: {} for domain in _DASHBOARD_DOMAIN_AGENT_MAP
            }
            for row in reader:
                if str(row.get(available_key) or "").strip() != "\u2705":
                    continue
                agent = str(row.get(agent_key) or "").strip()
                question = str(row.get(question_key) or "").strip()
                if not question:
                    continue
                domain = next(
                    (
                        domain_name
                        for domain_name, agent_name in _DASHBOARD_DOMAIN_AGENT_MAP.items()
                        if agent == agent_name
                    ),
                    None,
                )
                if not domain:
                    continue
                existing = grouped[domain].get(question)
                if existing is None:
                    grouped[domain][question] = {
                        "question": question,
                        "source_agent": agent,
                        "source_question_no": str(row.get(question_no_key) or ""),
                        "source_count": 1,
                    }
                else:
                    existing["source_count"] = int(existing["source_count"]) + 1

        ranked: dict[str, list[dict[str, str | int]]] = {}
        for domain, by_question in grouped.items():
            entries = list(by_question.values())
            if not entries:
                ranked[domain] = cls._build_dashboard_question_fallback()[domain]
                continue
            priority_order = {
                question: index
                for index, question in enumerate(_DASHBOARD_QUESTION_PRIORITIES.get(domain, []))
            }
            entries.sort(
                key=lambda entry: (
                    priority_order.get(str(entry["question"]), 999),
                    -int(entry["source_count"]),
                    str(entry["source_question_no"]),
                )
            )
            ranked[domain] = entries[:3]
        return ranked

    @staticmethod
    def _build_dashboard_question_fallback() -> dict[str, list[dict[str, str | int]]]:
        fallback: dict[str, list[dict[str, str | int]]] = {}
        for domain, questions in _DASHBOARD_QUESTION_PRIORITIES.items():
            fallback[domain] = [
                {
                    "question": question,
                    "source_agent": _DASHBOARD_DOMAIN_AGENT_MAP[domain],
                    "source_question_no": "",
                    "source_count": 0,
                }
                for question in questions[:3]
            ]
        return fallback

    @staticmethod
    def _to_dashboard_display_question(domain: str, question: str) -> str:
        mapped = _DASHBOARD_QUESTION_DISPLAY_MAP.get(domain, {}).get(question)
        if mapped:
            return mapped
        return question

    @staticmethod
    def _to_iso_date(value: str) -> str:
        normalized = "".join(ch for ch in value if ch.isdigit())
        if len(normalized) == 8:
            return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}"
        return value

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
                ORDER BY fnsh_dt, cpi_nm
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
                    "date": self._to_iso_date(display_date),
                    "title": str(row["cpi_nm"] or "캠페인"),
                    "category": "campaign",
                    "type": str(row["cpi_kind_nm"] or ""),
                    "startDate": self._to_iso_date(start_dt),
                    "endDate": self._to_iso_date(end_dt),
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
                WITH ranked AS (
                    SELECT
                        pay_dc_nm,
                        start_dt,
                        fnsh_dt,
                        pay_dc_grp_type_nm,
                        ROW_NUMBER() OVER (
                            PARTITION BY pay_dc_nm
                            ORDER BY fnsh_dt ASC, start_dt DESC
                        ) AS rn
                    FROM raw_telecom_discount_policy
                    WHERE start_dt <= :end AND fnsh_dt >= :start
                      AND fnsh_dt < '99991221'
                      {telecom_store_filter}
                )
                SELECT pay_dc_nm, start_dt, fnsh_dt, pay_dc_grp_type_nm
                FROM ranked
                WHERE rn = 1
                ORDER BY fnsh_dt, pay_dc_nm
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
                    "date": self._to_iso_date(display_date),
                    "title": str(row["pay_dc_nm"] or "통신사 할인"),
                    "category": "telecom",
                    "type": str(row["pay_dc_grp_type_nm"] or ""),
                    "startDate": self._to_iso_date(start_dt),
                    "endDate": self._to_iso_date(end_dt),
                }
            )
        return events
