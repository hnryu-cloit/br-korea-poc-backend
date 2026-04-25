from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

logger = logging.getLogger(__name__)


class InsightRepositoryMixin:
    engine: Engine | None
    async def get_insights(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        insights: dict = {
            "filtered_store_id": store_id,
            "filtered_date_from": date_from,
            "filtered_date_to": date_to,
        }
        campaign_context = self._fetch_campaign_context()
        if campaign_context:
            insights["campaign_seasonality"] = self._build_campaign_insight(campaign_context)
        if not self.engine:
            raise RuntimeError("sales database engine is not configured")

        try:
            # 시간대 운영 코칭: raw_daily_store_item_tmzon 직접 사용
            peak_hours = self._fetch_peak_hours_from_tmzon(
                store_id=store_id, date_from=date_from, date_to=date_to
            )
            if peak_hours:
                insights["peak_hours"] = peak_hours

            # 채널 전환 인사이트: raw_daily_store_channel 직접 사용
            channel_mix = self._fetch_channel_mix_from_online(
                store_id=store_id, date_from=date_from, date_to=date_to
            )
            if channel_mix:
                insights["channel_mix"] = channel_mix

            # 결제/할인 민감도: raw 테이블 직접 조회
            payment_mix = self._fetch_payment_mix_insight(
                store_id=store_id, date_from=date_from, date_to=date_to
            )
            if payment_mix:
                insights["payment_mix"] = payment_mix

            # 메뉴 믹스 추천: raw_daily_store_item 직접 사용
            menu_mix = self._fetch_menu_mix_from_item(
                store_id=store_id, date_from=date_from, date_to=date_to
            )
            if menu_mix:
                insights["menu_mix"] = menu_mix
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to build insights (store_id=%s, date_from=%s, date_to=%s): %s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            raise RuntimeError("매출 인사이트 조회 중 DB 오류가 발생했습니다.") from exc

        # 실데이터가 일부만 존재해도 화면이 전부 실패하지 않도록 섹션별 안내 응답을 채운다.
        required_sections = ("peak_hours", "channel_mix", "payment_mix", "menu_mix")
        fallback_titles = {
            "peak_hours": "시간대 운영 코칭",
            "channel_mix": "채널 전환 인사이트",
            "payment_mix": "결제/할인 민감도",
            "menu_mix": "메뉴 믹스 추천",
        }
        for section in required_sections:
            if insights.get(section):
                continue
            insights[section] = self._build_missing_section(fallback_titles[section])

        return insights

    @staticmethod
    def _build_missing_section(title: str) -> dict:
        return {
            "title": title,
            "summary": "현재 조건에서 실데이터가 충분하지 않아 점검이 필요합니다.",
            "metrics": [
                {
                    "label": "데이터 상태",
                    "value": "부족",
                    "detail": "조회 기간 또는 점포 조건을 변경해 다시 확인해 주세요.",
                }
            ],
            "actions": [
                "조회 기간을 넓혀 다시 확인해 주세요.",
                "점포를 변경해 데이터 적재 상태를 확인해 주세요.",
            ],
            "status": "review",
        }

    def _fetch_peak_hours_from_tmzon(
        self, store_id: str | None, date_from: str | None, date_to: str | None
    ) -> dict | None:
        """raw_daily_store_item_tmzon 기반 시간대별 매출 분석"""
        where_clause, params = self._build_filters(
            "masked_stor_cd", "sale_dt", store_id, date_from, date_to
        )
        try:
            with self.engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            CAST(tmzon_div AS INTEGER) AS tmzon_div,
                            SUM(CAST(COALESCE(NULLIF(sale_amt, ''), '0') AS NUMERIC)) AS sale_amt,
                            SUM(CAST(COALESCE(NULLIF(sale_qty, ''), '0') AS NUMERIC)) AS sale_qty
                        FROM raw_daily_store_item_tmzon
                        {where_clause}
                        GROUP BY CAST(tmzon_div AS INTEGER)
                        ORDER BY sale_amt DESC
                        LIMIT 3
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to fetch peak hours from tmzon (store_id=%s, date_from=%s, date_to=%s): %s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            return None

        if not rows:
            return None

        lead = rows[0]
        hour_label = f"{int(lead['tmzon_div']):02d}"
        metric_items = [
            {
                "label": f"{int(row['tmzon_div']):02d}시",
                "value": f"{int(float(row['sale_amt'] or 0)):,}원",
                "detail": f"판매 {int(float(row['sale_qty'] or 0)):,}개",
            }
            for row in rows
        ]
        return {
            "title": "시간대 운영 코칭",
            "summary": f"{hour_label}시가 가장 강한 시간대입니다. 해당 시간 전 생산·진열을 완료해 주세요.",
            "metrics": metric_items,
            "actions": [
                f"{hour_label}시 이전에 핵심 상품 생산·진열을 완료해 주세요.",
                "저조 시간대에는 세트 제안이나 음료 동시 노출을 강화해 주세요.",
            ],
            "status": "active",
        }

    def _fetch_channel_mix_from_online(
        self, store_id: str | None, date_from: str | None, date_to: str | None
    ) -> dict | None:
        """raw_daily_store_channel 기반 채널별 매출 분석"""
        where_clause, params = self._build_filters(
            "masked_stor_cd", "sale_dt", store_id, date_from, date_to
        )
        try:
            with self.engine.connect() as connection:
                channel_rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            COALESCE(NULLIF(ho_chnl_div, ''), '기타') AS channel_div,
                            SUM(CAST(COALESCE(NULLIF(CAST(sale_amt AS TEXT), ''), '0') AS NUMERIC)) AS sale_amt,
                            SUM(CAST(COALESCE(NULLIF(CAST(ord_cnt AS TEXT), ''), '0') AS NUMERIC)) AS ord_cnt
                        FROM raw_daily_store_channel
                        {where_clause}
                        GROUP BY COALESCE(NULLIF(ho_chnl_div, ''), '기타')
                        ORDER BY sale_amt DESC
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to fetch channel mix from online (store_id=%s, date_from=%s, date_to=%s): %s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            return None

        if not channel_rows:
            return None

        total_amt = sum(float(row["sale_amt"] or 0) for row in channel_rows)
        metric_items = [
            {
                "label": str(row["channel_div"]),
                "value": (
                    f"{round(float(row['sale_amt'] or 0) / total_amt * 100, 1):.1f}%"
                    if total_amt > 0
                    else "0%"
                ),
                "detail": f"매출 {int(float(row['sale_amt'] or 0)):,}원 / 주문 {int(float(row['ord_cnt'] or 0)):,}건",
            }
            for row in channel_rows[:3]
        ]
        top_channel = str(channel_rows[0]["channel_div"])
        return {
            "title": "채널 전환 인사이트",
            "summary": f"{top_channel} 채널 비중이 가장 높습니다. 채널별 집중 시간대 차이를 확인해 주세요.",
            "metrics": metric_items,
            "actions": [
                "온라인 강세 시간대에는 배달/픽업 전용 구성을 노출해 주세요.",
                "오프라인 강세 시간대에는 회전율 중심으로 진열 우선순위를 유지해 주세요.",
            ],
            "status": "active",
        }

    def _fetch_menu_mix_from_item(
        self, store_id: str | None, date_from: str | None, date_to: str | None
    ) -> dict | None:
        """raw_daily_store_item 기반 상품 믹스 분석 — Top6, 매출비중, 객단가"""
        where_clause, params = self._build_filters(
            "masked_stor_cd", "sale_dt", store_id, date_from, date_to
        )
        try:
            with self.engine.connect() as connection:
                top_rows = (
                    connection.execute(
                        text(
                            f"""
                        SELECT
                            item_nm,
                            sale_qty,
                            sale_amt,
                            ROUND(sale_amt * 100.0 / NULLIF(total_sale_amt, 0), 1) AS share_pct,
                            CASE WHEN sale_qty > 0
                                 THEN ROUND(sale_amt / sale_qty, 0)
                                 ELSE 0 END AS avg_price
                        FROM (
                            SELECT
                                COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '기타') AS item_nm,
                                SUM(CAST(COALESCE(NULLIF(sale_qty, ''), '0') AS NUMERIC)) AS sale_qty,
                                SUM(CAST(COALESCE(NULLIF(sale_amt, ''), '0') AS NUMERIC)) AS sale_amt,
                                SUM(SUM(CAST(COALESCE(NULLIF(sale_amt, ''), '0') AS NUMERIC))) OVER () AS total_sale_amt
                            FROM raw_daily_store_item
                            {where_clause}
                            GROUP BY COALESCE(NULLIF(TRIM(CAST(item_nm AS TEXT)), ''), '기타')
                        ) sub
                        ORDER BY sale_amt DESC
                        LIMIT 6
                        """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to fetch menu mix from item (store_id=%s, date_from=%s, date_to=%s): %s",
                store_id,
                date_from,
                date_to,
                exc,
            )
            return None

        if not top_rows:
            return None

        top1_name = str(top_rows[0]["item_nm"])
        top1_share = float(top_rows[0]["share_pct"] or 0)

        metric_items = [
            {
                "label": str(row["item_nm"]),
                "value": f"{float(row['share_pct'] or 0):.1f}%",
                "detail": f"객단가 {int(float(row['avg_price'] or 0)):,}원 · 판매 {int(float(row['sale_qty'] or 0)):,}개",
            }
            for row in top_rows
        ]
        concentration_label = "집중형" if top1_share > 50 else "균형형" if top1_share < 30 else "편중형"
        metric_items.append({
            "label": "히트상품 집중도",
            "value": f"{top1_share:.1f}% ({concentration_label})",
            "detail": f"1위 {top1_name} 매출 비중 기준",
        })

        top2_name = str(top_rows[1]["item_nm"]) if len(top_rows) > 1 else None
        bottom_name = str(top_rows[-1]["item_nm"]) if len(top_rows) >= 3 else None

        actions: list[str] = []
        if top1_share > 40:
            actions.append(
                f"{top1_name}이 전체 매출의 {top1_share:.0f}%를 차지합니다. 2~3위 상품 세트 구성으로 의존도를 완화하세요."
            )
        else:
            actions.append(
                f"히트상품 {top1_name} 중심으로 음료·사이드 세트 구성을 추가해 객단가를 높이세요."
            )
        if top2_name:
            actions.append(
                f"{top1_name}과 {top2_name}를 세트로 구성하면 객단가 향상 효과를 기대할 수 있습니다."
            )
        if bottom_name:
            actions.append(
                f"하위 상품 {bottom_name}은 피크 외 시간대 노출 테스트 후 진열 정리를 검토하세요."
            )

        return {
            "title": "메뉴 믹스 추천",
            "summary": (
                f"{top1_name} 중심 매출 구조 (비중 {top1_share:.0f}%). "
                "상품별 매출 비중과 객단가를 기반으로 세트 구성과 저매출 상품 정리를 검토하세요."
            ),
            "metrics": metric_items,
            "actions": actions,
            "status": "active",
        }

    def _build_filters(
        self,
        column_store: str,
        column_date: str,
        store_id: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> tuple[str, dict]:
        clauses: list[str] = []
        params: dict[str, str] = {}
        if store_id:
            clauses.append(f"{column_store} = :store_id")
            params["store_id"] = store_id
        if date_from:
            clauses.append(f"{column_date} >= :date_from")
            params["date_from"] = date_from.replace("-", "")
        if date_to:
            clauses.append(f"{column_date} <= :date_to")
            params["date_to"] = date_to.replace("-", "")
        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _fetch_payment_mix_insight(
        self, store_id: str | None, date_from: str | None, date_to: str | None
    ) -> dict | None:
        rows: list[dict] = []
        discount_ratio = 0.0
        total_amt = 0.0
        if has_table(self.engine, "raw_daily_store_pay_way"):
            where_clause, params = self._build_filters(
                "masked_stor_cd", "sale_dt", store_id, date_from, date_to
            )
            with self.engine.connect() as connection:
                rows = [
                    dict(row)
                    for row in connection.execute(
                        text(
                            f"""
                            SELECT
                                COALESCE(NULLIF(pay_way_cd_nm, ''), NULLIF(pay_way_cd, ''), '기타') AS payment_label,
                                COALESCE(NULLIF(pay_way_cd, ''), '기타') AS payment_code,
                                SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) AS payment_amt
                            FROM raw_daily_store_pay_way
                            {where_clause}
                            GROUP BY
                                COALESCE(NULLIF(pay_way_cd_nm, ''), NULLIF(pay_way_cd, ''), '기타'),
                                COALESCE(NULLIF(pay_way_cd, ''), '기타')
                            ORDER BY SUM(COALESCE(NULLIF(pay_amt, '')::numeric, 0)) DESC
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                ]

            total_amt = sum(float(row["payment_amt"] or 0) for row in rows)
            discount_amt = sum(
                float(row["payment_amt"] or 0)
                for row in rows
                if row["payment_code"] in {"03", "19"}
            )
            discount_ratio = 0 if total_amt == 0 else round(discount_amt / total_amt * 100, 1)

        reference_date = self._resolve_payment_reference_date(
            store_id=store_id, date_from=date_from, date_to=date_to
        )
        discount_context = self._fetch_discount_program_context(reference_date)
        if not rows and not discount_context:
            return None

        metric_items = []
        if rows:
            for row in rows[:2]:
                ratio = (
                    0
                    if total_amt == 0
                    else round(float(row["payment_amt"] or 0) / total_amt * 100, 1)
                )
                metric_items.append(
                    {
                        "label": str(row["payment_label"]),
                        "value": f"{ratio:.1f}%",
                        "detail": f"결제금액 {int(float(row['payment_amt'] or 0)):,}원",
                    }
                )
            metric_items.append(
                {
                    "label": "할인 결제 비중",
                    "value": f"{discount_ratio:.1f}%",
                    "detail": "제휴할인 + 캠페인할인결제 기준",
                }
            )

        actions = [
            "상위 결제수단과 연결된 프로모션 성과를 함께 비교해 주세요.",
            "특정 할인수단 비중이 높아질 때는 객단가 방어 상품을 함께 제안해 주세요.",
        ]
        summary = "결제수단 비중과 할인 프로그램 구성을 함께 점검할 수 있습니다."

        if rows:
            summary = f"{rows[0]['payment_label']} 비중이 가장 높고 결제수단 편중 여부를 운영 관점에서 점검할 수 있습니다."

        if discount_context:
            active_count = int(discount_context.get("active_settlement_count") or 0)
            top_settlement_name = self._first_text(discount_context.get("top_settlement_name"))
            top_settlement_method = self._first_text(discount_context.get("top_settlement_method"))
            top_telecom_name = self._first_text(discount_context.get("top_telecom_name"))
            top_telecom_target = self._first_text(discount_context.get("top_telecom_target"))
            top_telecom_item_count = int(discount_context.get("top_telecom_item_count") or 0)

            if active_count or top_telecom_name:
                summary += " "
                if active_count:
                    summary += f"정산 기준 정보상 현재 활성 할인 기준은 {active_count}건이며 "
                    if top_settlement_name:
                        summary += f"대표 기준은 {top_settlement_name}"
                        if top_settlement_method:
                            summary += f"({top_settlement_method})"
                        summary += "입니다."
                if top_telecom_name:
                    if active_count:
                        summary += " "
                    summary += f"통신사 제휴 정책 기준 대표 프로그램은 {top_telecom_name}"
                    if top_telecom_target:
                        summary += f"({top_telecom_target})"
                    summary += "입니다."

            if top_settlement_name:
                metric_items.append(
                    {
                        "label": "활성 정산 기준",
                        "value": top_settlement_name,
                        "detail": top_settlement_method or "정산 기준 정보 기준",
                    }
                )
            elif active_count:
                metric_items.append(
                    {
                        "label": "활성 정산 기준",
                        "value": f"{active_count}건",
                        "detail": "정산 기준 정보 기준",
                    }
                )

            if top_telecom_name:
                telecom_detail = top_telecom_target or "통신사 제휴 할인"
                if top_telecom_item_count:
                    telecom_detail = f"{telecom_detail} / 대상 상품 {top_telecom_item_count}개"
                metric_items.append(
                    {
                        "label": "대표 제휴 할인",
                        "value": top_telecom_name,
                        "detail": telecom_detail,
                    }
                )
                actions[0] = f"{top_telecom_name} 반응을 상위 결제수단과 함께 비교해 주세요."
                if top_telecom_item_count:
                    actions.append(
                        "제휴 대상 상품 구성과 실제 판매 상위 상품이 맞는지 함께 점검해 주세요."
                    )

        return {
            "title": "결제/할인 민감도",
            "summary": summary,
            "metrics": metric_items[:5],
            "actions": actions[:3],
            "status": "active" if discount_context else "normal",
        }
