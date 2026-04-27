from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.services.ordering_service import OrderingService


class _DummyOrderingRepository:
    def is_known_store(self, store_id: str) -> bool:
        return store_id == "POC_002"

    def uses_ordering_join_table(self, store_id: str) -> bool:
        return False

    def get_history_filtered(
        self,
        *,
        store_id: str,
        limit: int | None,
        date_from: str | None,
        date_to: str | None,
        item_nm: str | None,
        is_auto: bool | None,
        reference_datetime=None,
    ) -> dict:
        return {
            "items": [
                {
                    "item_nm": "ChocoMuffin",
                    "dlv_dt": "2026-04-22",
                    "ord_qty": 28,
                    "confrm_qty": 19,
                    "is_auto": False,
                    "ord_grp_nm": "manual",
                },
                {
                    "item_nm": "ChocoMuffin",
                    "dlv_dt": "2026-04-15",
                    "ord_qty": 16,
                    "confrm_qty": 16,
                    "is_auto": False,
                    "ord_grp_nm": "manual",
                },
                {
                    "item_nm": "ChocoMuffin",
                    "dlv_dt": "2026-04-08",
                    "ord_qty": 16,
                    "confrm_qty": 16,
                    "is_auto": False,
                    "ord_grp_nm": "manual",
                },
                {
                    "item_nm": "ChocoMuffin",
                    "dlv_dt": "2026-04-01",
                    "ord_qty": 16,
                    "confrm_qty": 16,
                    "is_auto": False,
                    "ord_grp_nm": "manual",
                },
                {
                    "item_nm": "ChocoMuffin",
                    "dlv_dt": "2026-03-10",
                    "ord_qty": 4,
                    "confrm_qty": 4,
                    "is_auto": False,
                    "ord_grp_nm": "manual",
                },
                {
                    "item_nm": "ZeroChange",
                    "dlv_dt": "2026-04-22",
                    "ord_qty": 10,
                    "confrm_qty": 10,
                    "is_auto": True,
                    "ord_grp_nm": "auto",
                },
                {
                    "item_nm": "ZeroChange",
                    "dlv_dt": "2026-04-15",
                    "ord_qty": 10,
                    "confrm_qty": 10,
                    "is_auto": True,
                    "ord_grp_nm": "auto",
                },
            ],
            "auto_rate": 0.67,
            "manual_rate": 0.33,
            "total_count": 6,
        }


class _DateFilteringOrderingRepository(_DummyOrderingRepository):
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_history_filtered(
        self,
        *,
        store_id: str,
        limit: int | None,
        date_from: str | None,
        date_to: str | None,
        item_nm: str | None,
        is_auto: bool | None,
        reference_datetime=None,
    ) -> dict:
        self.calls.append(
            {
                "store_id": store_id,
                "limit": limit,
                "date_from": date_from,
                "date_to": date_to,
                "item_nm": item_nm,
                "is_auto": is_auto,
            }
        )
        payload = super().get_history_filtered(
            store_id=store_id,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
            item_nm=item_nm,
            is_auto=is_auto,
            reference_datetime=reference_datetime,
        )
        filtered_items = payload["items"]
        if date_from:
            filtered_items = [item for item in filtered_items if str(item["dlv_dt"]) >= date_from]
        if date_to:
            filtered_items = [item for item in filtered_items if str(item["dlv_dt"]) <= date_to]
        return {
            **payload,
            "items": filtered_items[:limit] if limit is not None else filtered_items,
            "total_count": len(filtered_items[:limit] if limit is not None else filtered_items),
        }


class _JoinTableOrderingRepository(_DummyOrderingRepository):
    def is_known_store(self, store_id: str) -> bool:
        return store_id == "POC_010"

    def uses_ordering_join_table(self, store_id: str) -> bool:
        return store_id == "POC_010"


class _DummyAIClient:
    def __init__(self) -> None:
        self.calls = 0

    async def generate_ordering_history_insights(
        self,
        *,
        store_id: str,
        filters: dict[str, object],
        history_items: list[dict[str, object]],
        summary_stats: dict[str, object],
    ) -> dict | None:
        self.calls += 1
        return {
            "kpis": [
                {"key": "auto_rate", "label": "자동 발주 비율", "value": "67.0%", "tone": "primary"},
                {"key": "manual_rate", "label": "수동 발주 비율", "value": "33.0%", "tone": "warning"},
            ],
            "anomalies": [
                {
                    "id": "anomaly-1",
                    "severity": "high",
                    "kind": "ordering_drop",
                    "message": "카카오후로스티드 품목의 발주량이 평균 대비 75% 급감하였습니다.",
                    "recommended_action": "판매 추세를 반영해 다음 주문량을 재조정하세요.",
                    "related_items": ["카카오후로스티드"],
                }
            ],
            "top_changed_items": [
                {"item_nm": "ShouldNotUseAIValue", "avg_ord_qty": 1.0, "latest_ord_qty": 99, "change_ratio": 99.0}
            ],
            "sources": ["operations_guide:ordering", "ordering_history"],
            "retrieved_contexts": ["주문 마감 2시간 전 변동 품목 점검"],
            "confidence": 0.85,
        }


def test_ordering_history_insights_success_from_ai_uses_deterministic_changed_items() -> None:
    ai_client = _DummyAIClient()
    service = OrderingService(
        repository=_DummyOrderingRepository(),
        ai_client=ai_client,
    )
    cache_path = Path(__file__).resolve().parents[1] / "data" / "ordering-history-insights-test-cache.json"
    if cache_path.exists():
        cache_path.unlink()
    service.history_insights_cache_path = cache_path

    response = asyncio.run(service.get_history_insights(store_id="POC_002"))

    assert len(response.kpis) >= 1
    assert len(response.anomalies) >= 1
    assert response.sources
    assert response.confidence == pytest.approx(0.85)
    assert len(response.top_changed_items) == 1
    assert response.top_changed_items[0].item_nm == "ChocoMuffin"
    assert response.top_changed_items[0].change_ratio == pytest.approx(0.75)
    assert ai_client.calls == 1


def test_ordering_history_insights_uses_prior_4_weeks_average_only() -> None:
    service = OrderingService(
        repository=_DummyOrderingRepository(),
        ai_client=_DummyAIClient(),
    )

    data = service.repository.get_history_filtered(
        store_id="POC_002",
        limit=200,
        date_from=None,
        date_to=None,
        item_nm=None,
        is_auto=None,
    )

    from app.schemas.ordering import OrderingHistoryItem

    summary = service._build_history_summary_stats(
        items=[OrderingHistoryItem(**item) for item in data["items"]],
        total_count=data["total_count"],
        auto_rate=data["auto_rate"],
        manual_rate=data["manual_rate"],
    )

    changed_items = summary["top_changed_items_preview"]
    assert changed_items[0]["item_nm"] == "ChocoMuffin"
    assert changed_items[0]["avg_ord_qty"] == pytest.approx(16.0)
    assert changed_items[0]["latest_ord_qty"] == 28


def test_ordering_history_insights_uses_previous_4_weeks_even_for_short_date_range() -> None:
    repository = _DateFilteringOrderingRepository()
    service = OrderingService(
        repository=repository,
        ai_client=_DummyAIClient(),
    )

    response = asyncio.run(
        service.get_history_insights(
            store_id="POC_002",
            date_from="2026-04-22",
            date_to="2026-04-22",
        )
    )

    assert len(repository.calls) == 2
    assert repository.calls[0]["date_from"] == "2026-04-22"
    assert repository.calls[1]["date_from"] == "2026-03-25"
    assert repository.calls[1]["date_to"] == "2026-04-22"
    assert len(response.top_changed_items) == 1
    assert response.top_changed_items[0].item_nm == "ChocoMuffin"
    assert response.top_changed_items[0].avg_ord_qty == pytest.approx(16.0)
    assert response.top_changed_items[0].latest_ord_qty == 28


def test_ordering_history_insights_unknown_store() -> None:
    service = OrderingService(repository=_DummyOrderingRepository(), ai_client=_DummyAIClient())
    with pytest.raises(ValueError):
        asyncio.run(service.get_history_insights(store_id="INVALID"))


def test_ordering_history_insights_ai_payload_missing() -> None:
    class _EmptyAIClient:
        async def generate_ordering_history_insights(self, **_: object) -> dict | None:
            return None

    service = OrderingService(repository=_DummyOrderingRepository(), ai_client=_EmptyAIClient())
    with pytest.raises(RuntimeError):
        asyncio.run(service.get_history_insights(store_id="POC_002"))


def test_ordering_history_insights_reuses_cached_anomalies() -> None:
    ai_client = _DummyAIClient()
    service = OrderingService(
        repository=_DummyOrderingRepository(),
        ai_client=ai_client,
    )
    cache_path = Path(__file__).resolve().parents[1] / "data" / "ordering-history-insights-test-cache.json"
    if cache_path.exists():
        cache_path.unlink()
    service.history_insights_cache_path = cache_path

    first = asyncio.run(service.get_history_insights(store_id="POC_002"))
    second = asyncio.run(service.get_history_insights(store_id="POC_002"))

    assert ai_client.calls == 1
    assert first.anomalies[0].message == second.anomalies[0].message
    assert first.anomalies[0].severity == second.anomalies[0].severity


def test_ordering_history_insights_sorts_anomalies_by_severity() -> None:
    class _SortingAIClient:
        async def generate_ordering_history_insights(self, **_: object) -> dict | None:
            return {
                "kpis": [],
                "anomalies": [
                    {
                        "id": "low-1",
                        "severity": "low",
                        "kind": "ordering_low",
                        "message": "Low severity anomaly",
                        "recommended_action": "Monitor",
                        "related_items": [],
                    },
                    {
                        "id": "high-1",
                        "severity": "high",
                        "kind": "ordering_high",
                        "message": "High severity anomaly",
                        "recommended_action": "Act now",
                        "related_items": [],
                    },
                    {
                        "id": "medium-1",
                        "severity": "medium",
                        "kind": "ordering_medium",
                        "message": "Medium severity anomaly",
                        "recommended_action": "Review",
                        "related_items": [],
                    },
                ],
                "top_changed_items": [],
                "sources": [],
                "retrieved_contexts": [],
                "confidence": 0.7,
            }

    service = OrderingService(
        repository=_DummyOrderingRepository(),
        ai_client=_SortingAIClient(),
    )
    cache_path = Path(__file__).resolve().parents[1] / "data" / "ordering-history-insights-sort-test-cache.json"
    if cache_path.exists():
        cache_path.unlink()
    service.history_insights_cache_path = cache_path

    response = asyncio.run(service.get_history_insights(store_id="POC_002"))

    assert [anomaly.severity for anomaly in response.anomalies] == ["high", "medium", "low"]


def test_ordering_history_insights_caps_anomalies_at_four() -> None:
    class _ManyAnomaliesAIClient:
        async def generate_ordering_history_insights(self, **_: object) -> dict | None:
            return {
                "kpis": [
                    {"key": "auto_rate", "label": "Auto", "value": "50%", "tone": "primary"},
                ],
                "anomalies": [
                    {
                        "id": f"high-{idx}",
                        "severity": "high",
                        "kind": "ordering_high",
                        "message": f"High severity anomaly {idx}",
                        "recommended_action": "Act now",
                        "related_items": [],
                    }
                    for idx in range(5)
                ],
                "top_changed_items": [],
                "sources": [],
                "retrieved_contexts": [],
                "confidence": 0.7,
            }

    service = OrderingService(
        repository=_DummyOrderingRepository(),
        ai_client=_ManyAnomaliesAIClient(),
    )
    cache_path = Path(__file__).resolve().parents[1] / "data" / "ordering-history-insights-cap-test-cache.json"
    if cache_path.exists():
        cache_path.unlink()
    service.history_insights_cache_path = cache_path

    response = asyncio.run(service.get_history_insights(store_id="POC_002"))

    assert len(response.anomalies) == 4
    assert [anomaly.id for anomaly in response.anomalies] == ["high-0", "high-1", "high-2", "high-3"]


def test_deterministic_history_insights_creates_multiple_change_anomalies() -> None:
    summary_stats = {
        "auto_rate": 0.5,
        "manual_rate": 0.5,
        "avg_order_qty": 12.0,
        "confirm_gap_count": 0,
        "top_changed_items_preview": [
            {"item_nm": "A", "avg_ord_qty": 10.0, "latest_ord_qty": 16, "change_ratio": 0.6},
            {"item_nm": "B", "avg_ord_qty": 20.0, "latest_ord_qty": 27, "change_ratio": 0.35},
            {"item_nm": "C", "avg_ord_qty": 15.0, "latest_ord_qty": 12, "change_ratio": -0.2},
            {"item_nm": "D", "avg_ord_qty": 30.0, "latest_ord_qty": 34, "change_ratio": 0.1333},
        ],
    }

    response = OrderingService._build_deterministic_history_insights(
        store_id="POC_010",
        summary_stats=summary_stats,
    )

    assert [anomaly.id for anomaly in response.anomalies] == [
        "top-changed-item-1",
        "top-changed-item-2",
        "top-changed-item-3",
    ]
    assert [anomaly.severity for anomaly in response.anomalies] == ["high", "medium", "low"]
    assert response.anomalies[0].message == "A 발주량이 평균 10.0개에서 최근 16개로 60.0% 증가했습니다."
    assert response.anomalies[2].message == "C 발주량이 평균 15.0개에서 최근 12개로 20.0% 감소했습니다."


def test_ordering_history_insights_skips_ai_for_join_table_store() -> None:
    ai_client = _DummyAIClient()
    service = OrderingService(
        repository=_JoinTableOrderingRepository(),
        ai_client=ai_client,
    )

    response = asyncio.run(service.get_history_insights(store_id="POC_010"))

    assert ai_client.calls == 0
    assert response.confidence == pytest.approx(0.95)
    assert response.sources == ["ordering_history_summary_stats"]
    assert len(response.kpis) == 3
    assert len(response.top_changed_items) == 1
    assert response.top_changed_items[0].item_nm == "ChocoMuffin"
