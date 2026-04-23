from __future__ import annotations

import asyncio

from app.schemas.sales import SalesQueryRequest
from app.services.sales_service import SalesService


class _RepoStub:
    async def get_campaign_effect(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        prompt_hint: str | None = None,
    ) -> dict:
        assert store_id == "POC_010"
        assert date_to == "2026-03-05"
        assert "티데이" in (prompt_hint or "")
        return {
            "analysis_mode": "telecom_tday",
            "campaign_code": "13",
            "campaign_name": "SKT Tday",
            "promotion_period_sales": 120000.0,
            "usage_amount": 8400.0,
            "usage_ratio_pct": 7.0,
            "periods": [
                {
                    "label": "프로모션 기간",
                    "start_date": "2026-02-19",
                    "end_date": "2026-02-25",
                    "revenue": 120000.0,
                }
            ],
            "product_mix": [
                {"item_nm": "아메리카노", "share_pct": 32.5},
                {"item_nm": "글레이즈드", "share_pct": 18.0},
                {"item_nm": "카페라떼", "share_pct": 12.3},
            ],
            "comparison": {
                "basis": "peer_average_prior_similar",
                "message": "해당 매장에서 이전 유사 프로모션 정보가 없어 타 매장의 평균치와 비교합니다.",
                "benchmark_sales": 110000.0,
                "benchmark_usage_ratio_pct": 6.2,
                "sales_change_pct": 9.1,
                "usage_ratio_gap_pct": 0.8,
            },
        }

    async def get_query_response(self, prompt: str) -> dict:
        raise AssertionError("T-Day prompt should not fall back to generic query response")


def test_sales_query_uses_tday_specific_route() -> None:
    service = SalesService(repository=_RepoStub())

    result = asyncio.run(
        service.query(
            SalesQueryRequest(
                prompt="이번 티데이 프로모션은 전체적으로 어땠어?",
                store_id="POC_010",
                business_date="2026-03-05",
            )
        )
    )

    assert result.processing_route == "repository_tday"
    assert "타 매장의 평균치와 비교합니다" in result.text
    assert any("상품별 매출 비중 상위" in evidence for evidence in result.evidence)
    assert result.agent_trace is not None
    assert result.agent_trace.intent == "tday_promotion_analysis"
