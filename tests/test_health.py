import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.repositories.analytics_repository import AnalyticsRepository
from app.repositories.bootstrap_repository import BootstrapRepository
from app.repositories.hq_repository import HQRepository
from app.repositories.signals_repository import SignalsRepository
from app.schemas.production import ProductionSimulationResponse
from app.services.bootstrap_service import BootstrapService
from app.services.hq_service import HQService


client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_simulation_preview() -> None:
    response = client.post(
        "/api/simulation/preview",
        json={
            "promotion_name": "봄 이벤트",
            "promo_price": 149000,
            "list_price": 220000,
            "procedure_cost": 42000,
            "expected_leads": 30,
            "close_rate": 0.4,
            "upsell_rate": 0.2,
            "average_upsell_revenue": 80000,
            "repeat_visit_rate": 0.1,
            "repeat_visit_revenue": 100000,
            "ad_budget": 1000000,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["promotion_name"] == "봄 이벤트"
    assert payload["expected_patients"] == 12.0
    assert "projected_profit" in payload


def test_review_checklist() -> None:
    response = client.get("/api/review/checklist")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_ordering_options() -> None:
    response = client.get("/api/ordering/options?notification_entry=true")
    assert response.status_code == 200
    payload = response.json()
    assert payload["notification_entry"] is True
    assert isinstance(payload["options"], list)


def test_ordering_context() -> None:
    response = client.get("/api/ordering/context/2")
    assert response.status_code == 200
    payload = response.json()
    assert payload["target_path"] == "/ordering"
    assert payload["notification_id"] == 2


def test_ordering_alerts() -> None:
    response = client.get("/api/ordering/alerts?before_minutes=20")
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["alerts"], list)
    if payload["alerts"]:
        assert payload["alerts"][0]["target_path"] == "/ordering"


def test_ordering_selection_save() -> None:
    response = client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-a", "reason": "알림에서 바로 선택", "actor": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["saved"], bool)
    assert payload["option_id"] == "opt-a"


def test_ordering_selection_history() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-c", "reason": "히스토리 테스트", "actor": "store_owner", "store_id": "gangnam"},
    )
    response = client.get("/api/ordering/selections/history?limit=5")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 0
    if payload["items"]:
        assert payload["items"][0]["option_id"]
        assert "selected_at" in payload["items"][0]


def test_ordering_selection_summary() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-a", "reason": "요약 테스트", "actor": "store_owner", "store_id": "gangnam"},
    )
    response = client.get("/api/ordering/selections/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 0
    if payload["latest"] is not None:
        assert payload["latest"]["option_id"] == "opt-a"
    assert payload["summary_status"] in {"recommended_selected", "custom_selected", "empty"}


def test_ordering_selection_summary_filters_by_store() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-b", "reason": "점포 필터 테스트", "actor": "store_owner", "store_id": "jamsil"},
    )
    response = client.get("/api/ordering/selections/summary?store_id=jamsil")
    assert response.status_code == 200
    payload = response.json()
    assert payload["filtered_store_id"] == "jamsil"
    if payload["latest"] is not None:
        assert payload["latest"]["store_id"] == "jamsil"


def test_ordering_selection_history_filters_by_date_range() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-a", "reason": "기간 테스트", "actor": "store_owner", "store_id": "gangnam"},
    )
    matched = client.get("/api/ordering/selections/history?date_from=2026-03-31&date_to=2026-03-31")
    assert matched.status_code == 200
    matched_payload = matched.json()
    assert matched_payload["total"] >= 0
    assert matched_payload["filtered_date_from"] == "2026-03-31"
    assert matched_payload["filtered_date_to"] == "2026-03-31"

    missed = client.get("/api/ordering/selections/history?date_to=2026-03-30")
    assert missed.status_code == 200
    missed_payload = missed.json()
    assert missed_payload["total"] == 0


def test_ordering_selection_summary_filters_by_date_range() -> None:
    response = client.get("/api/ordering/selections/summary?date_from=2026-04-01")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 0
    assert payload["filtered_date_from"] == "2026-04-01"
    assert payload["summary_status"] == "empty"


def test_home_overview() -> None:
    response = client.get("/api/home/overview")
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["priority_actions"], list)
    if payload["priority_actions"]:
        assert "ai_reasoning" in payload["priority_actions"][0]
        assert "confidence_score" in payload["priority_actions"][0]
        assert "is_finished_good" in payload["priority_actions"][0]
    ordering_card = next(card for card in payload["cards"] if card["domain"] == "ordering")
    assert isinstance(ordering_card["delivery_scheduled"], bool)


def test_production_overview() -> None:
    response = client.get("/api/production/overview")
    assert response.status_code == 200
    payload = response.json()
    assert payload["refresh_interval_minutes"] == 5
    assert isinstance(payload["summary_stats"], list)
    assert isinstance(payload["alerts"], list)

def test_production_items() -> None:
    response = client.get("/api/production/items?page=1&page_size=20&store_id=gangnam")
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["items"], list)
    assert "pagination" in payload


def test_production_item_detail() -> None:
    list_response = client.get("/api/production/items?page=1&page_size=20")
    assert list_response.status_code == 200
    items = list_response.json()["items"]
    assert items

    sku_id = items[0]["sku_id"]
    response = client.get(f"/api/production/items/{sku_id}?store_id=gangnam")
    assert response.status_code == 200
    payload = response.json()
    assert payload["sku_id"] == sku_id
    assert "recommended_qty" in payload


def test_production_registration() -> None:
    response = client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-1", "qty": 40, "registered_by": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["feedback_type"] == "chance_loss_reduced"


def test_production_registration_history() -> None:
    client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-2", "qty": 24, "registered_by": "store_operator", "store_id": "gangnam"},
    )
    response = client.get("/api/production/registrations/history?limit=5")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 0
    if payload["items"]:
        assert payload["items"][0]["sku_id"]
        assert "registered_at" in payload["items"][0]


def test_production_registration_summary() -> None:
    client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-3", "qty": 18, "registered_by": "store_owner", "store_id": "gangnam"},
    )
    response = client.get("/api/production/registrations/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 0
    if payload["latest"] is not None:
        assert payload["latest"]["sku_id"] == "sku-3"
    assert payload["affected_sku_count"] >= 0
    assert payload["summary_status"] in {"active", "empty"}


def test_production_simulation_routes() -> None:
    request_body = {
        "store_id": "gangnam",
        "item_id": "sku-1",
        "simulation_date": "2026-04-13",
        "lead_time_hour": 1,
        "margin_rate": 0.3,
    }

    for path in ("/api/production/simulation", "/api/v1/production/simulation"):
        response = client.post(path, json=request_body)
        assert response.status_code == 200
        payload = response.json()
        assert payload["metadata"]["store_id"] == "gangnam"
        assert payload["metadata"]["source"] in {"repository", "ai-fastapi-contract"}
        assert len(payload["time_series_data"]) >= 1
        assert len(payload["actions_timeline"]) >= 0


def test_production_simulation_response_accepts_ai_contract_aliases() -> None:
    response = ProductionSimulationResponse.model_validate(
        {
            "metadata": {"store_id": "gangnam", "item_id": "sku-1", "date": "2026-04-13"},
            "summary_metrics": {
                "additional_sales_qty": 12.0,
                "additional_profit_amt": 18000,
                "additional_waste_qty": 2.0,
                "additional_waste_cost": 1400,
                "net_profit_change": 16600,
                "performance_status": "POSITIVE",
                "chance_loss_reduction": 4500.0,
            },
            "chart_data": [
                {"time": "08:00", "actual_stock": 40.0, "ai_guided_stock": 52.0},
            ],
            "action_timeline": ["[10:00] AI 추천으로 20개 추가 생산"],
        }
    )
    assert len(response.time_series_data) == 1
    assert len(response.actions_timeline) == 1


def test_production_registration_summary_filters_by_store() -> None:
    client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-4", "qty": 12, "registered_by": "store_owner", "store_id": "jamsil"},
    )
    response = client.get("/api/production/registrations/summary?store_id=jamsil")
    assert response.status_code == 200
    payload = response.json()
    assert payload["filtered_store_id"] == "jamsil"
    if payload["latest"] is not None:
        assert payload["latest"]["store_id"] == "jamsil"
    assert payload["affected_sku_count"] >= 0


def test_production_registration_history_filters_by_date_range() -> None:
    client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-1", "qty": 10, "registered_by": "store_owner", "store_id": "gangnam"},
    )
    matched = client.get("/api/production/registrations/history?date_from=2026-03-31&date_to=2026-03-31")
    assert matched.status_code == 200
    matched_payload = matched.json()
    assert matched_payload["total"] >= 0
    assert matched_payload["filtered_date_from"] == "2026-03-31"
    assert matched_payload["filtered_date_to"] == "2026-03-31"

    missed = client.get("/api/production/registrations/history?date_to=2026-03-30")
    assert missed.status_code == 200
    missed_payload = missed.json()
    assert missed_payload["total"] == 0


def test_production_registration_summary_filters_by_date_range() -> None:
    response = client.get("/api/production/registrations/summary?date_from=2026-04-01")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 0
    assert payload["filtered_date_from"] == "2026-04-01"
    assert payload["summary_status"] == "empty"


def test_sales_prompts() -> None:
    response = client.get("/api/sales/prompts")
    assert response.status_code == 200
    assert len(response.json()) >= 10


def test_sales_query() -> None:
    response = client.post(
        "/api/sales/query",
        json={"prompt": "이번 주 배달 건수가 지난주보다 줄어든 원인을 알려줘"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "evidence" in payload
    assert payload["comparison"] is None
    assert payload["query_type"] == "data_lookup"
    assert payload["processing_route"] in {"stub_repository", "ai_proxy"}


def test_sales_query_blocks_sensitive_prompt_for_store_role() -> None:
    response = client.post(
        "/api/sales/query",
        json={"prompt": "이번 달 이익률과 원가를 알려줘"},
        headers={"X-User-Role": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["blocked"] is True
    assert payload["processing_route"] == "policy_block"
    assert payload["masked_fields"] == ["profitability"]


def test_sales_insights() -> None:
    response = client.get("/api/sales/insights?store_id=gangnam&date_from=2026-03-01&date_to=2026-03-31")
    assert response.status_code == 200
    payload = response.json()
    assert payload["filtered_store_id"] == "gangnam"
    assert payload["filtered_date_from"] == "2026-03-01"
    assert payload["peak_hours"]["title"] == "시간대 운영 코칭"
    assert len(payload["channel_mix"]["metrics"]) >= 1
    assert len(payload["payment_mix"]["actions"]) >= 1
    assert len(payload["menu_mix"]["metrics"]) >= 1
    assert payload["payment_mix"]["status"] in {"active", "review"}


def test_audit_logs_require_hq_role_and_return_recent_events() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-b", "reason": "테스트 로그", "actor": "store_owner"},
    )
    client.post(
        "/api/sales/query",
        json={"prompt": "이번 주 배달 건수가 지난주보다 줄어든 원인을 알려줘"},
        headers={"X-User-Role": "hq_operator"},
    )

    default_access = client.get("/api/audit/logs")
    assert default_access.status_code == 200

    allowed = client.get("/api/audit/logs?limit=5", headers={"X-User-Role": "hq_operator"})
    assert allowed.status_code == 200
    payload = allowed.json()
    assert payload["total"] >= 2
    assert any(item["domain"] == "ordering" for item in payload["items"])
    assert any(item["domain"] == "sales" for item in payload["items"])


def test_data_catalog_endpoint_returns_response_shape() -> None:
    response = client.get("/api/data/catalog")
    assert response.status_code == 200
    payload = response.json()
    assert "db_path" in payload
    assert "tables" in payload


def test_notifications_endpoint_returns_response_shape() -> None:
    response = client.get("/api/notifications")
    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert "unread_count" in payload
    if payload["items"]:
        assert "title" in payload["items"][0]
        assert "category" in payload["items"][0]


def test_analytics_metrics_endpoint_returns_response_shape() -> None:
    response = client.get("/api/analytics/metrics")
    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert isinstance(payload["items"], list)
    if payload["items"]:
        assert {"label", "value", "change", "trend", "detail"} <= set(payload["items"][0].keys())


def test_signals_endpoint_returns_response_shape() -> None:
    response = client.get("/api/signals")
    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert "high_count" in payload
    assert isinstance(payload["items"], list)
    assert payload["high_count"] >= 0
    if payload["items"]:
        assert {"id", "title", "metric", "value", "change", "trend", "priority", "region", "insight"} <= set(payload["items"][0].keys())


def test_bootstrap_endpoint_returns_minimal_payload() -> None:
    response = client.get("/api/bootstrap")
    assert response.status_code == 200
    payload = response.json()
    assert payload["product"] == ""
    assert payload["summary"] == ""
    assert payload["users"] == []
    assert payload["goals"] == []
    assert payload["policies"] == []
    assert payload["features"] == {}


def test_channels_endpoint_returns_empty_drafts_without_bootstrap_data() -> None:
    response = client.get("/api/channels/drafts")
    assert response.status_code == 200
    assert response.json() == {}


@pytest.mark.asyncio
async def test_repository_and_service_empty_fallbacks_without_engine() -> None:
    analytics_items = await AnalyticsRepository().get_metrics()
    signals_items = await SignalsRepository().list_signals()
    coaching_rows = await HQRepository().list_coaching_rows()
    inspection_rows = await HQRepository().list_inspection_rows()
    bootstrap_payload = await BootstrapRepository().get_bootstrap()
    bootstrap_service = BootstrapService(BootstrapRepository())
    hq_service = HQService(HQRepository(), ordering_service=None)

    assert analytics_items == []
    assert signals_items == []
    assert coaching_rows == []
    assert inspection_rows == []
    assert bootstrap_payload == {
        "product": "",
        "summary": "",
        "users": [],
        "goals": [],
        "policies": [],
        "features": {},
    }
    assert await bootstrap_service.get_channel_drafts() == {}
    assert await bootstrap_service.get_review_checklist() == []

    coaching_response = await hq_service.get_coaching()
    inspection_response = await hq_service.get_inspection()
    assert coaching_response.store_orders == []
    assert coaching_response.coaching_tips == []
    assert inspection_response.items == []


# ── 역할 기반 접근 제어 테스트 ────────────────────────────────────────────

def test_hq_coaching_forbidden_for_store_role() -> None:
    response = client.get("/api/hq/coaching", headers={"X-User-Role": "store_owner"})
    assert response.status_code == 403


def test_hq_inspection_forbidden_for_store_role() -> None:
    response = client.get("/api/hq/inspection", headers={"X-User-Role": "store_owner"})
    assert response.status_code == 403


def test_hq_coaching_allowed_for_hq_admin() -> None:
    response = client.get("/api/hq/coaching", headers={"X-User-Role": "hq_admin"})
    assert response.status_code == 200


def test_hq_inspection_allowed_for_hq_operator() -> None:
    response = client.get("/api/hq/inspection", headers={"X-User-Role": "hq_operator"})
    assert response.status_code == 200


def test_audit_logs_forbidden_without_valid_role() -> None:
    # 알 수 없는 역할은 audit/logs 접근 불가
    response = client.get("/api/audit/logs", headers={"X-User-Role": "unknown_role"})
    assert response.status_code == 403


def test_audit_logs_allowed_for_hq_admin() -> None:
    response = client.get("/api/audit/logs", headers={"X-User-Role": "hq_admin"})
    assert response.status_code == 200


# ── 민감 질의 차단 회귀 테스트 ───────────────────────────────────────────

def test_sales_query_blocks_sensitive_fields_for_store_role() -> None:
    # store_owner 역할은 순이익·원가 질의가 차단됨
    response = client.post(
        "/api/sales/query",
        json={"prompt": "전 매장 순이익과 원가율을 알려줘"},
        headers={"X-User-Role": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["blocked"] is True
    assert payload["processing_route"] == "policy_block"


def test_sales_query_not_blocked_for_general_question() -> None:
    response = client.post(
        "/api/sales/query",
        json={"prompt": "이번 주 배달 건수 현황을 알려줘"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["blocked"] is False


# ── 주문 마감 시간 계산 테스트 ────────────────────────────────────────────

def test_ordering_deadline_returns_expected_shape() -> None:
    response = client.get("/api/ordering/deadline?store_id=gangnam")
    assert response.status_code == 200
    payload = response.json()
    assert payload["store_id"] == "gangnam"
    assert payload["deadline"] == "14:00"
    assert "minutes_remaining" in payload
    assert isinstance(payload["minutes_remaining"], int)
    assert isinstance(payload["is_urgent"], bool)
    assert isinstance(payload["is_passed"], bool)


def test_ordering_deadline_without_store_id() -> None:
    response = client.get("/api/ordering/deadline")
    assert response.status_code == 200
    payload = response.json()
    assert payload["store_id"] == "default"
    assert payload["minutes_remaining"] >= 0


# ── 생산 등록 폼 테스트 ───────────────────────────────────────────────────

def test_production_registration_form_returns_items() -> None:
    response = client.get("/api/production/registrations/form")
    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert "generated_at" in payload
    assert isinstance(payload["items"], list)
    if payload["items"]:
        item = payload["items"][0]
        assert "sku_id" in item
        assert "recommended_qty" in item
        assert "current_stock" in item
        assert "forecast_stock_1h" in item


def test_production_registration_form_with_store_id() -> None:
    response = client.get("/api/production/registrations/form?store_id=gangnam")
    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload


# ── 공통 예외 처리 테스트 ─────────────────────────────────────────────────

def test_unknown_route_returns_404() -> None:
    response = client.get("/api/nonexistent-endpoint")
    assert response.status_code == 404
