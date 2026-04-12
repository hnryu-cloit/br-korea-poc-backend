from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table

_STUB = [
    {"id": "sig-1", "title": "강남권 배달 급감", "metric": "배달 건수", "value": "312건", "change": "-21%", "trend": "down", "priority": "high", "region": "강남구·서초구", "insight": "배달앱 노출 순위 하락으로 점심 시간대 주문 집중 감소. 즉시 광고 입찰가 조정 권고."},
    {"id": "sig-2", "title": "T-day 재방문율 상승", "metric": "재방문율", "value": "34%", "change": "+12.4%", "trend": "up", "priority": "medium", "region": "전체", "insight": "T-day 참여 고객의 1주 내 재방문율이 증가. 리타겟팅 쿠폰 발송 효과 검토 필요."},
    {"id": "sig-3", "title": "마포권 커피 동반 구매 증가", "metric": "커피+도넛 세트 전환율", "value": "62%", "change": "+8.4%", "trend": "up", "priority": "medium", "region": "마포구", "insight": "묶음 메뉴 상시 편성 검토. 타 지역 적용 시 유사 효과 예상."},
    {"id": "sig-4", "title": "송파권 찬스 로스 증가", "metric": "찬스 로스", "value": "+18%", "change": "+18%", "trend": "down", "priority": "high", "region": "송파구", "insight": "생산 알림 미대응 매장 집중. 본사 코칭 및 매장 방문 점검 필요."},
]


class SignalsRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def list_signals(self) -> list[dict]:
        if not self.engine:
            return _STUB
        try:
            items: list[dict] = []
            if has_table(self.engine, "core_channel_sales") and has_table(self.engine, "core_store_master"):
                items.extend(self._get_channel_signals())
            if has_table(self.engine, "production_registrations") and has_table(self.engine, "core_store_master"):
                items.extend(self._get_production_signals())
            if has_table(self.engine, "core_daily_item_sales") and has_table(self.engine, "core_store_master"):
                items.extend(self._get_menu_signals())
            if has_table(self.engine, "raw_settlement_master") and has_table(self.engine, "raw_telecom_discount_policy"):
                items.extend(self._get_discount_signals())
            if items:
                priority_rank = {"high": 0, "medium": 1, "low": 2}
                return sorted(items, key=lambda item: (priority_rank.get(item["priority"], 3), item["id"]))[:6]
        except SQLAlchemyError:
            pass
        return _STUB

    @staticmethod
    def _normalize_date(value: str | None) -> str:
        if not value:
            return date.today().strftime("%Y%m%d")
        return value.replace("-", "")

    def _resolve_reference_date(self) -> str:
        if not has_table(self.engine, "raw_daily_store_pay_way"):
            return self._normalize_date(None)
        with self.engine.connect() as connection:
            row = connection.execute(text("SELECT MAX(sale_dt) AS max_sale_dt FROM raw_daily_store_pay_way")).mappings().first()
        if not row or not row["max_sale_dt"]:
            return self._normalize_date(None)
        return str(row["max_sale_dt"])

    def _get_channel_signals(self) -> list[dict]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    WITH daily AS (
                        SELECT
                            c.masked_stor_cd,
                            MAX(sm.region) AS region,
                            sale_dt,
                            SUM(CASE WHEN c.ho_chnl_div LIKE '온라인%' THEN c.ord_cnt ELSE 0 END) AS online_orders
                        FROM core_channel_sales c
                        LEFT JOIN core_store_master sm
                          ON c.masked_stor_cd = sm.masked_stor_cd
                        GROUP BY c.masked_stor_cd, sale_dt
                    ),
                    ranked AS (
                        SELECT
                            masked_stor_cd,
                            region,
                            sale_dt,
                            online_orders,
                            ROW_NUMBER() OVER (PARTITION BY masked_stor_cd ORDER BY sale_dt DESC) AS rn
                        FROM daily
                    )
                    SELECT
                        region,
                        SUM(CASE WHEN rn <= 7 THEN online_orders ELSE 0 END) AS recent_orders,
                        SUM(CASE WHEN rn > 7 AND rn <= 14 THEN online_orders ELSE 0 END) AS prior_orders
                    FROM ranked
                    GROUP BY region
                    HAVING SUM(CASE WHEN rn > 7 AND rn <= 14 THEN online_orders ELSE 0 END) > 0
                    ORDER BY ((SUM(CASE WHEN rn <= 7 THEN online_orders ELSE 0 END) - SUM(CASE WHEN rn > 7 AND rn <= 14 THEN online_orders ELSE 0 END)) / NULLIF(SUM(CASE WHEN rn > 7 AND rn <= 14 THEN online_orders ELSE 0 END), 0)) ASC
                    LIMIT 2
                    """
                )
            ).mappings().all()
        items = []
        for index, row in enumerate(rows, start=1):
            recent = float(row["recent_orders"] or 0)
            prior = float(row["prior_orders"] or 0)
            if prior <= 0:
                continue
            delta = round(((recent - prior) / prior) * 100, 1)
            if delta >= -5:
                continue
            priority = "high" if delta <= -15 else "medium"
            items.append(
                {
                    "id": f"sig-channel-{index}",
                    "title": f"{row['region'] or '전체'} 배달 주문 감소",
                    "metric": "배달 건수",
                    "value": f"{int(round(recent)):,}건",
                    "change": f"{delta:+.1f}%",
                    "trend": "down",
                    "priority": priority,
                    "region": row["region"] or "전체",
                    "insight": "최근 7일 온라인 주문이 직전 7일보다 감소했습니다. 점심 시간대 온라인 노출과 쿠폰 집행 여부를 우선 점검해 주세요.",
                }
            )
        return items

    def _get_production_signals(self) -> list[dict]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT
                        COALESCE(sm.region, pr.store_id, '전체') AS region,
                        COUNT(*) AS registration_count,
                        COALESCE(SUM(pr.qty), 0) AS total_qty
                    FROM production_registrations pr
                    LEFT JOIN core_store_master sm
                      ON pr.store_id = sm.masked_stor_cd
                    GROUP BY COALESCE(sm.region, pr.store_id, '전체')
                    ORDER BY COUNT(*) DESC, COALESCE(SUM(pr.qty), 0) DESC
                    LIMIT 2
                    """
                )
            ).mappings().all()
        items = []
        for index, row in enumerate(rows, start=1):
            count = int(row["registration_count"] or 0)
            if count <= 0:
                continue
            items.append(
                {
                    "id": f"sig-production-{index}",
                    "title": f"{row['region']} 생산 대응 집중",
                    "metric": "생산 등록 건수",
                    "value": f"{count}건",
                    "change": f"+{int(row['total_qty'] or 0)}개",
                    "trend": "down",
                    "priority": "high" if count >= 3 else "medium",
                    "region": row["region"],
                    "insight": "생산 등록이 집중된 지역입니다. 위험 SKU 대응이 잦은지 점포별 운영 패턴을 함께 확인해 주세요.",
                }
            )
        return items

    def _get_menu_signals(self) -> list[dict]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    WITH region_sales AS (
                        SELECT
                            COALESCE(sm.region, '전체') AS region,
                            SUM(CASE WHEN di.item_nm LIKE '%커피%' OR di.item_nm LIKE '%아메리카노%' THEN di.net_sale_amt ELSE 0 END) AS coffee_sales,
                            SUM(di.net_sale_amt) AS total_sales
                        FROM core_daily_item_sales di
                        LEFT JOIN core_store_master sm
                          ON di.masked_stor_cd = sm.masked_stor_cd
                        GROUP BY COALESCE(sm.region, '전체')
                    )
                    SELECT
                        region,
                        coffee_sales,
                        total_sales,
                        CASE WHEN total_sales = 0 THEN 0 ELSE ROUND(coffee_sales / total_sales * 100, 1) END AS coffee_ratio
                    FROM region_sales
                    WHERE total_sales > 0
                    ORDER BY coffee_ratio DESC
                    LIMIT 2
                    """
                )
            ).mappings().all()
        items = []
        for index, row in enumerate(rows, start=1):
            ratio = float(row["coffee_ratio"] or 0)
            if ratio <= 0:
                continue
            items.append(
                {
                    "id": f"sig-menu-{index}",
                    "title": f"{row['region']} 커피 동반 구매 강세",
                    "metric": "커피 동반 구매율",
                    "value": f"{ratio:.1f}%",
                    "change": f"+{ratio:.1f}%p",
                    "trend": "up",
                    "priority": "medium" if ratio >= 15 else "low",
                    "region": row["region"],
                    "insight": "커피 계열 비중이 높은 지역입니다. 도넛-음료 묶음 제안을 강화하면 객단가 개선 여지가 있습니다.",
                }
            )
        return items

    def _get_discount_signals(self) -> list[dict]:
        target_date = self._resolve_reference_date()
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    WITH active_settlement AS (
                        SELECT
                            pay_dc_ty_cd_nm,
                            COUNT(*) AS row_count
                        FROM raw_settlement_master
                        WHERE COALESCE(use_yn, '0') = '1'
                          AND COALESCE(start_dt, '00000000') <= :target_date
                          AND COALESCE(fnsh_dt, '99999999') >= :target_date
                        GROUP BY pay_dc_ty_cd_nm
                    ),
                    active_policy AS (
                        SELECT
                            p.pay_dc_nm,
                            p.dc_apply_trgt_nm,
                            COUNT(*) AS row_count,
                            COALESCE(MAX(item_count.item_count), 0) AS item_count
                        FROM raw_telecom_discount_policy p
                        LEFT JOIN (
                            SELECT
                                pay_dc_cd,
                                COUNT(*) AS item_count
                            FROM raw_telecom_discount_item
                            WHERE COALESCE(use_yn, '1') = '1'
                            GROUP BY pay_dc_cd
                        ) item_count
                          ON p.pay_dc_cd = item_count.pay_dc_cd
                        WHERE COALESCE(p.use_yn, '0') = '1'
                          AND COALESCE(p.start_dt, '00000000') <= :target_date
                          AND COALESCE(p.fnsh_dt, '99999999') >= :target_date
                        GROUP BY p.pay_dc_nm, p.dc_apply_trgt_nm
                    )
                    SELECT
                        COALESCE((SELECT SUM(row_count) FROM active_settlement), 0) AS active_settlement_count,
                        COALESCE((SELECT pay_dc_ty_cd_nm FROM active_settlement ORDER BY row_count DESC, pay_dc_ty_cd_nm LIMIT 1), '') AS top_settlement_name,
                        COALESCE((SELECT pay_dc_nm FROM active_policy ORDER BY row_count DESC, pay_dc_nm LIMIT 1), '') AS top_policy_name,
                        COALESCE((SELECT dc_apply_trgt_nm FROM active_policy ORDER BY row_count DESC, pay_dc_nm LIMIT 1), '') AS top_policy_target,
                        COALESCE((SELECT item_count FROM active_policy ORDER BY row_count DESC, pay_dc_nm LIMIT 1), 0) AS top_policy_item_count
                    """
                ),
                {"target_date": target_date},
            ).mappings().first()

        if not row:
            return []

        active_settlement_count = int(row["active_settlement_count"] or 0)
        top_settlement_name = row["top_settlement_name"] or ""
        top_policy_name = row["top_policy_name"] or ""
        top_policy_target = row["top_policy_target"] or ""
        top_policy_item_count = int(row["top_policy_item_count"] or 0)

        if not active_settlement_count and not top_policy_name:
            return []

        title_target = f"{top_policy_name} 운영 점검" if top_policy_name else "제휴 할인 운영 점검"
        value = f"{active_settlement_count}건"
        change = f"+{top_policy_item_count}개" if top_policy_item_count else "+0개"
        insight = "정산 기준 정보와 제휴 할인 정책을 같이 점검해 주세요."
        if top_policy_name:
            insight = f"{top_policy_name} 정책이 활성 상태입니다."
            if top_policy_target:
                insight += f" 적용 범위는 {top_policy_target}"
            if top_policy_item_count:
                insight += f"이며 대상 상품은 {top_policy_item_count}개입니다."
            else:
                insight += "입니다."
            if top_settlement_name:
                insight += f" 정산 기준의 대표 항목은 {top_settlement_name}입니다."

        return [
            {
                "id": "sig-discount-1",
                "title": title_target,
                "metric": "활성 할인 기준",
                "value": value,
                "change": change,
                "trend": "up",
                "priority": "medium",
                "region": "전체",
                "insight": insight,
            }
        ]
