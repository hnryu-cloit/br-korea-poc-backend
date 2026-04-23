"""
백엔드 AIServiceClient 통합 테스트
- httpx를 mock하여 AI 서비스 API 계약을 검증합니다.
- 실제 AI 서비스 없이 실행 가능합니다.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from app.services.ai_client import AIServiceClient

AI_BASE_URL = "http://localhost:8001"
TOKEN = "test-token"


@pytest.fixture
def client() -> AIServiceClient:
    return AIServiceClient(base_url=AI_BASE_URL, token=TOKEN)


# ── query_sales ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_query_sales_success(client: AIServiceClient) -> None:
    stub = {
        "answer": {
            "text": "배달 매출이 14% 감소했습니다.",
            "evidence": ["배달앱 노출 하락"],
            "actions": ["광고 입찰가 조정"],
        },
        "source_data_period": "최근 1개월",
        "request_context": {
            "store_id": "POC_001",
            "business_date": "2026-03-05",
            "business_time": "14:00",
            "prompt": "諛곕떖 留ㅼ텧 遺꾩꽍?댁쨾",
            "domain": "sales",
        },
        "grounding": {
            "keywords": ["諛곕떖", "留ㅼ텧"],
            "intent": "channel sales analysis",
            "relevant_tables": ["raw_daily_store_channel"],
            "sql": "SELECT 1",
            "row_count": 1,
        },
        "queried_period": {"type": "date", "value": "20260305"},
        "channel_analysis": {},
        "profit_simulation": {},
    }
    respx.post(f"{AI_BASE_URL}/sales/query").mock(return_value=httpx.Response(200, json=stub))

    result = await client.query_sales("배달 매출 분석해줘", store_id="POC_001")

    assert result is not None
    assert result["text"] == stub["answer"]["text"]
    assert result["evidence"] == stub["answer"]["evidence"]


@pytest.mark.asyncio
@respx.mock
async def test_query_sales_returns_none_on_server_error(client: AIServiceClient) -> None:
    respx.post(f"{AI_BASE_URL}/sales/query").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    result = await client.query_sales("배달 분석", store_id="POC_001")
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_query_sales_returns_none_on_connection_error(client: AIServiceClient) -> None:
    respx.post(f"{AI_BASE_URL}/sales/query").mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    result = await client.query_sales("배달 분석", store_id="POC_001")
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_query_sales_sends_bearer_token(client: AIServiceClient) -> None:
    stub = {
        "answer": {"text": "ok", "evidence": [], "actions": []},
        "source_data_period": "최근 1개월",
        "channel_analysis": {},
        "profit_simulation": {},
    }
    route = respx.post(f"{AI_BASE_URL}/sales/query").mock(
        return_value=httpx.Response(200, json=stub)
    )

    await client.query_sales(
        "테스트",
        store_id="POC_001",
        domain="sales",
        business_date="2026-03-05",
    )

    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Request-Id"]
    assert json.loads(request.content)["query"] == "테스트"
    assert json.loads(request.content)["store_id"] == "POC_001"
    assert json.loads(request.content)["domain"] == "sales"
    assert json.loads(request.content)["business_date"] == "2026-03-05"


# ── predict_production ────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_predict_production_success(client: AIServiceClient) -> None:
    stub = {
        "sku": "SKU_001",
        "predicted_stock_1h": 8.5,
        "risk_detected": True,
        "stockout_expected_at": "14:45",
        "alert_message": "1시간 이내 품절 위험. 지금 생산을 시작하세요.",
        "confidence": 0.91,
    }
    respx.post(f"{AI_BASE_URL}/management/production/predict").mock(
        return_value=httpx.Response(200, json=stub)
    )

    result = await client.predict_production(
        sku="SKU_001",
        current_stock=12,
        history=[{"timestamp": "2024-01-01T12:00:00", "stock": 20, "production": 0, "sales": 8}],
        pattern_4w=[0.9, 1.1],
    )

    assert result is not None
    assert result["sku"] == "SKU_001"
    assert result["risk_detected"] is True
    assert result["alert_message"] != ""


@pytest.mark.asyncio
@respx.mock
async def test_predict_production_returns_none_on_error(client: AIServiceClient) -> None:
    respx.post(f"{AI_BASE_URL}/management/production/predict").mock(
        return_value=httpx.Response(401, text="Unauthorized")
    )
    result = await client.predict_production("SKU_X", 0, [], [])
    assert result is None


# ── recommend_ordering ────────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_recommend_ordering_success(client: AIServiceClient) -> None:
    stub = {
        "options": [
            {"name": "전주 동요일 기준", "recommended_quantity": 120, "priority": 1},
            {"name": "전전주 동요일 기준", "recommended_quantity": 115, "priority": 2},
            {"name": "전월 동요일 기준", "recommended_quantity": 108, "priority": 3},
        ],
        "reasoning": "지난주 패턴이 최신 수요와 가장 유사합니다.",
        "guardrail_note": "최종 주문 결정은 점주의 권한입니다.",
    }
    respx.post(f"{AI_BASE_URL}/management/ordering/recommend").mock(
        return_value=httpx.Response(200, json=stub)
    )

    result = await client.recommend_ordering(
        store_id="POC_001",
        current_date="2024-01-15",
        is_campaign=False,
        is_holiday=False,
    )

    assert result is not None
    assert len(result["options"]) == 3
    assert result["options"][0]["priority"] == 1
    assert "reasoning" in result


@pytest.mark.asyncio
@respx.mock
async def test_recommend_ordering_campaign_flag_passed(client: AIServiceClient) -> None:
    stub = {
        "options": [{"name": "전주 동요일 기준", "recommended_quantity": 150, "priority": 1}],
        "reasoning": "캠페인 기간 수요 증가 반영.",
        "guardrail_note": "최종 주문 결정은 점주의 권한입니다.",
    }
    route = respx.post(f"{AI_BASE_URL}/management/ordering/recommend").mock(
        return_value=httpx.Response(200, json=stub)
    )

    await client.recommend_ordering(
        store_id="POC_001",
        current_date="2024-01-15",
        is_campaign=True,
    )

    import json

    request_body = json.loads(route.calls.last.request.content)
    assert request_body["is_campaign"] is True


@pytest.mark.asyncio
@respx.mock
async def test_recommend_ordering_returns_none_on_error(client: AIServiceClient) -> None:
    respx.post(f"{AI_BASE_URL}/management/ordering/recommend").mock(
        return_value=httpx.Response(503, text="Service Unavailable")
    )
    result = await client.recommend_ordering("POC_001", "2024-01-15")
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_generate_market_insights_success(client: AIServiceClient) -> None:
    stub = {
        "executive_summary": "요약",
        "key_insights": [],
        "risk_warnings": [],
        "action_plan": [],
        "branch_scoreboard": [],
        "report_markdown": "# report",
        "evidence_refs": ["estimated_sales_summary.monthly_estimated_sales"],
        "audience": "store_owner",
        "source": "ai",
        "trace_id": "trace-1",
    }
    route = respx.post(f"{AI_BASE_URL}/analytics/market/insights").mock(
        return_value=httpx.Response(200, json=stub)
    )

    result = await client.generate_market_insights(
        audience="store_owner",
        scope={"store_id": "POC_001"},
        market_data={"estimated_sales_summary": {"monthly_estimated_sales": 1000000}},
    )

    assert result is not None
    assert result["executive_summary"] == "요약"
    request = route.calls.last.request
    assert request.headers["X-Request-Id"]
