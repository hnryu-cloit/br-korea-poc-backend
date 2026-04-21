from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

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
    ) -> dict:
        return {
            "items": [
                {
                    "item_nm": "초코링",
                    "dlv_dt": "2026-04-22",
                    "ord_qty": 28,
                    "confrm_qty": 19,
                    "is_auto": False,
                    "ord_grp_nm": "수동",
                }
            ],
            "auto_rate": 0.67,
            "manual_rate": 0.33,
            "total_count": 1,
        }


class _DummyAIClient:
    async def generate_ordering_history_insights(
        self,
        *,
        store_id: str,
        filters: dict[str, object],
        history_items: list[dict[str, object]],
        summary_stats: dict[str, object],
    ) -> dict | None:
        return {
            "kpis": [
                {"key": "auto_rate", "label": "자동 발주 비율", "value": "67.0%", "tone": "primary"},
                {"key": "manual_rate", "label": "수동 발주 비율", "value": "33.0%", "tone": "warning"},
            ],
            "anomalies": [
                {
                    "id": "anomaly-1",
                    "severity": "high",
                    "kind": "확정 편차",
                    "message": "주요 품목 확정 편차가 큽니다.",
                    "recommended_action": "전일 판매 추세 반영 후 주문량 재확인",
                    "related_items": ["초코링"],
                }
            ],
            "top_changed_items": [
                {"item_nm": "초코링", "avg_ord_qty": 16.2, "latest_ord_qty": 28, "change_ratio": 0.7284}
            ],
            "sources": ["operations_guide:ordering", "ordering_history"],
            "retrieved_contexts": ["주문 마감 2시간 전 변동 품목 점검"],
            "confidence": 0.85,
        }


def test_ordering_history_insights_success_from_ai() -> None:
    service = OrderingService(
        repository=_DummyOrderingRepository(),
        ai_client=_DummyAIClient(),
    )
    response = asyncio.run(service.get_history_insights(store_id="POC_002"))
    assert len(response.kpis) >= 1
    assert len(response.anomalies) >= 1
    assert response.sources
    assert response.confidence == pytest.approx(0.85)


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
