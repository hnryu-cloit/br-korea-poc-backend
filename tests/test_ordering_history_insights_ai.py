from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.services.ordering_service import OrderingService


class _DummyOrderingRepository:
    def is_known_store(self, store_id: str) -> bool:
        return store_id == "POC_002"

    def get_history_filtered(
        self,
        *,
        store_id: str,
        limit: int,
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
