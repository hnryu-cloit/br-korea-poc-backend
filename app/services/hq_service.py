from __future__ import annotations

from app.repositories.hq_repository import HQRepository
from app.schemas.hq import (
    CoachingTip,
    HQCoachingResponse,
    HQInspectionResponse,
    StoreInspectionItem,
    StoreOrderItem,
)


class HQService:
    def __init__(self, repository: HQRepository, ordering_service: object | None = None) -> None:
        self.repository = repository
        self.ordering_service = ordering_service

    async def get_coaching(self) -> HQCoachingResponse:
        rows = await self.repository.list_coaching_rows()
        store_orders: list[StoreOrderItem] = []
        coaching_tips: list[CoachingTip] = []

        for row in rows:
            option_id = str(row.get("option_id") or "")
            option_label = option_id or "-"
            basis = "-"
            reason = str(row.get("reason") or "-")
            submitted_at = str(row.get("submitted_at") or "-")
            order_count_7d = int(row.get("order_count_7d") or 0)
            recommended_count_7d = int(row.get("recommended_count_7d") or 0)
            production_count_7d = int(row.get("production_count_7d") or 0)
            fallback_status = str(row.get("status") or "")
            if (
                order_count_7d == 0
                and production_count_7d == 0
                and fallback_status in {"normal", "review", "risk"}
            ):
                status = fallback_status
            else:
                status = self._resolve_status(
                    option_id=option_id,
                    reason=reason,
                    order_count_7d=order_count_7d,
                    recommended_count_7d=recommended_count_7d,
                    production_count_7d=production_count_7d,
                )

            store_orders.append(
                StoreOrderItem(
                    store=str(row.get("store") or "-"),
                    region=str(row.get("region") or "전체"),
                    option=option_label,
                    basis=basis,
                    reason=reason,
                    submitted_at=submitted_at,
                    status=status,
                )
            )

            tip = self._build_tip(
                store=str(row.get("store") or "-"),
                status=status,
                option_label=option_label,
                basis=basis,
                order_count_7d=order_count_7d,
                recommended_count_7d=recommended_count_7d,
                production_count_7d=production_count_7d,
                campaign_sales_ratio=float(row.get("campaign_sales_ratio") or 0),
            )
            if tip:
                coaching_tips.append(CoachingTip(store=str(row.get("store") or "-"), tip=tip))

        return HQCoachingResponse(
            store_orders=store_orders[:5],
            coaching_tips=coaching_tips[:2],
        )

    async def get_inspection(self) -> HQInspectionResponse:
        rows = await self.repository.list_inspection_rows()
        items: list[StoreInspectionItem] = []

        for row in rows:
            order_count_7d = int(row.get("order_count_7d") or 0)
            recommended_count_7d = int(row.get("recommended_count_7d") or 0)
            production_count_7d = int(row.get("production_count_7d") or 0)
            production_qty_7d = float(row.get("production_qty_7d") or 0)
            production_qty_prev_7d = float(row.get("production_qty_prev_7d") or 0)
            alert_response_rate = self._calculate_alert_response_rate(
                order_count_7d=order_count_7d,
                recommended_count_7d=recommended_count_7d,
                production_count_7d=production_count_7d,
            )
            production_total = max(4, order_count_7d + production_count_7d)
            chance_loss_change = self.repository._format_percentage_delta(
                production_qty_7d, production_qty_prev_7d
            )
            fallback_status = str(row.get("status") or "")
            if (
                order_count_7d == 0
                and production_count_7d == 0
                and fallback_status in {"compliant", "partial", "noncompliant"}
            ):
                status = fallback_status
            else:
                status = self._resolve_inspection_status(
                    alert_response_rate=alert_response_rate,
                    production_registered=production_count_7d,
                    production_total=production_total,
                    chance_loss_change=chance_loss_change,
                )

            items.append(
                StoreInspectionItem(
                    store=str(row.get("store") or "-"),
                    region=str(row.get("region") or "전체"),
                    alert_response_rate=alert_response_rate,
                    production_registered=production_count_7d,
                    production_total=production_total,
                    chance_loss_change=chance_loss_change,
                    status=status,
                )
            )

        return HQInspectionResponse(items=items[:5])

    @staticmethod
    def _resolve_status(
        *,
        option_id: str,
        reason: str,
        order_count_7d: int,
        recommended_count_7d: int,
        production_count_7d: int,
    ) -> str:
        if not option_id or order_count_7d == 0:
            return "risk"
        if recommended_count_7d >= max(1, order_count_7d):
            return "normal"
        if not reason or reason == "-":
            return "risk"
        if production_count_7d == 0:
            return "risk"
        return "review"

    @staticmethod
    def _build_tip(
        *,
        store: str,
        status: str,
        option_label: str,
        basis: str,
        order_count_7d: int,
        recommended_count_7d: int,
        production_count_7d: int,
        campaign_sales_ratio: float,
    ) -> str | None:
        if status == "risk":
            if order_count_7d == 0:
                return "주문 선택 이력이 없습니다. 마감 전 추천안을 먼저 확인하세요."
            if production_count_7d == 0:
                return "주문 선택은 있었지만 생산 등록이 없습니다. 본사 확인이 필요합니다."
            return f"{store}의 주문 선택이 기준과 맞지 않습니다. {basis} 기준과 선택 내역을 다시 검토하세요."
        if status == "review":
            return (
                f"{option_label} 기준과 다른 선택이 확인되었습니다. "
                f"사유를 점주와 재확인해 주세요."
            )
        if production_count_7d > order_count_7d and campaign_sales_ratio >= 10:
            return "생산 등록 대비 주문 선택이 보수적으로 보입니다. 캠페인 영향과 주문 마감을 함께 점검하세요."
        if recommended_count_7d >= max(1, order_count_7d):
            return "추천안 선택 비율이 높습니다. 현재 운영 방식 유지가 가능합니다."
        return None

    @staticmethod
    def _calculate_alert_response_rate(
        *, order_count_7d: int, recommended_count_7d: int, production_count_7d: int
    ) -> int:
        if order_count_7d <= 0:
            return 0
        response_rate = round((recommended_count_7d / order_count_7d) * 100)
        if production_count_7d > 0 and response_rate < 100:
            response_rate = min(100, response_rate + min(20, production_count_7d * 3))
        return max(0, min(100, response_rate))

    @staticmethod
    def _resolve_inspection_status(
        *,
        alert_response_rate: int,
        production_registered: int,
        production_total: int,
        chance_loss_change: str,
    ) -> str:
        if (
            alert_response_rate >= 90
            and production_registered >= max(1, int(round(production_total * 0.75)))
            and chance_loss_change.startswith("-")
        ):
            return "compliant"
        if alert_response_rate >= 70 or production_registered > 0:
            return "partial"
        return "noncompliant"
