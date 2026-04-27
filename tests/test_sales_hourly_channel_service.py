from __future__ import annotations

import asyncio

from app.services.sales_service import SalesService


class _HourlyChannelRepoStub:
    async def get_hourly_channel(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        return [
            {"hour": 9, "channel": "오프라인", "sales": 1000, "qty": 2},
            {"hour": 9, "channel": "온라인-픽업", "sales": 500, "qty": 1},
            {"hour": 9, "channel": "온라인-배달", "sales": 300, "qty": 1},
        ]


def test_get_hourly_channel_counts_pickup_in_offline_bucket() -> None:
    service = SalesService(repository=_HourlyChannelRepoStub())

    response = asyncio.run(
        service.get_hourly_channel(store_id="POC_010", date_from="2026-03-01", date_to="2026-03-07")
    )

    target = next(item for item in response.items if item.hour == 9)
    assert target.offline_sales == 1500
    assert target.offline_qty == 3
    assert target.delivery_sales == 300
    assert target.delivery_qty == 1
