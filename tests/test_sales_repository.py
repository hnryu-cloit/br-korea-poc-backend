from __future__ import annotations

import asyncio

from app.repositories import sales_repository as sales_repository_module
from app.repositories.sales_repository import SalesRepository


class _FakeResult:
    def __init__(self, first=None, all_rows=None):
        self._first = first
        self._all_rows = all_rows or []

    def mappings(self):
        return self

    def first(self):
        return self._first

    def all(self):
        return self._all_rows


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        if "SELECT MAX(sale_dt) AS max_dt" in sql:
            return _FakeResult(first={"max_dt": "20260310"})
        if "WHERE sale_dt = :max_dt" in sql and "net_revenue" in sql:
            return _FakeResult(first={"revenue": 610100, "net_revenue": 554629})
        if "WITH recent_dates AS" in sql:
            return _FakeResult(
                all_rows=[
                    {"sale_dt": "20260309", "revenue": 100, "net_revenue": 90},
                    {"sale_dt": "20260310", "revenue": 200, "net_revenue": 180},
                ]
            )
        if "GROUP BY item_nm" in sql:
            return _FakeResult(
                all_rows=[
                    {"item_nm": "Plain", "sales": 300, "qty": 10},
                    {"item_nm": "Choco", "sales": 250, "qty": 8},
                ]
            )
        raise AssertionError(f"Unexpected SQL executed: {sql}")


class _FakeEngine:
    def connect(self):
        return _FakeConnection()


def test_get_summary_uses_net_sale_amount_for_today_net_revenue(monkeypatch) -> None:
    monkeypatch.setattr(sales_repository_module, "has_table", lambda engine, table_name: False)

    repository = SalesRepository(engine=_FakeEngine())

    result = asyncio.run(repository.get_summary(store_id="POC_001"))

    assert result["today_revenue"] == 610100.0
    assert result["today_net_revenue"] == 554629.0
    assert result["weekly_data"][0]["net_revenue"] == 90.0
