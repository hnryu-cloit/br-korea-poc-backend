from fastapi.testclient import TestClient

from app.main import app


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
    assert len(response.json()) >= 1


def test_ordering_options() -> None:
    response = client.get("/api/ordering/options?notification_entry=true")
    assert response.status_code == 200
    payload = response.json()
    assert payload["notification_entry"] is True
    assert payload["focus_option_id"] == "opt-a"
    assert len(payload["options"]) == 3


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
    assert len(payload["alerts"]) == 1
    assert payload["alerts"][0]["target_path"] == "/ordering"
    assert payload["alerts"][0]["focus_option_id"] == "opt-a"


def test_ordering_selection_save() -> None:
    response = client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-a", "reason": "알림에서 바로 선택", "actor": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["saved"] is True
    assert payload["option_id"] == "opt-a"


def test_ordering_selection_history() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-c", "reason": "히스토리 테스트", "actor": "store_owner", "store_id": "gangnam"},
    )
    response = client.get("/api/ordering/selections/history?limit=5")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 1
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
    assert payload["total"] >= 1
    assert payload["latest"]["option_id"] == "opt-a"
    assert payload["recommended_selected"] is True
    assert "store_owner" in payload["recent_actor_roles"]
    assert payload["recent_selection_count_7d"] >= 1
    assert payload["option_counts"]["opt-a"] >= 1
    assert payload["summary_status"] == "recommended_selected"


def test_ordering_selection_summary_filters_by_store() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-b", "reason": "점포 필터 테스트", "actor": "store_owner", "store_id": "jamsil"},
    )
    response = client.get("/api/ordering/selections/summary?store_id=jamsil")
    assert response.status_code == 200
    payload = response.json()
    assert payload["filtered_store_id"] == "jamsil"
    assert payload["latest"]["store_id"] == "jamsil"
    assert payload["option_counts"]["opt-b"] >= 1


def test_ordering_selection_history_filters_by_date_range() -> None:
    client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-a", "reason": "기간 테스트", "actor": "store_owner", "store_id": "gangnam"},
    )
    matched = client.get("/api/ordering/selections/history?date_from=2026-03-31&date_to=2026-03-31")
    assert matched.status_code == 200
    matched_payload = matched.json()
    assert matched_payload["total"] >= 1
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


def test_production_overview() -> None:
    response = client.get("/api/production/overview")
    assert response.status_code == 200
    payload = response.json()
    assert payload["production_lead_time_minutes"] == 60
    assert len(payload["items"]) >= 1


def test_production_alerts() -> None:
    response = client.get("/api/production/alerts")
    assert response.status_code == 200
    payload = response.json()
    assert payload["lead_time_minutes"] == 60
    assert len(payload["alerts"]) >= 1
    assert payload["alerts"][0]["push_title"]


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
    assert payload["total"] >= 1
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
    assert payload["total"] >= 1
    assert payload["latest"]["sku_id"] == "sku-3"
    assert payload["total_registered_qty"] >= 18
    assert "store_owner" in payload["recent_registered_by"]
    assert payload["recent_registration_count_7d"] >= 1
    assert payload["recent_registered_qty_7d"] >= 18
    assert payload["affected_sku_count"] >= 1
    assert payload["summary_status"] == "active"


def test_production_registration_summary_filters_by_store() -> None:
    client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-4", "qty": 12, "registered_by": "store_owner", "store_id": "jamsil"},
    )
    response = client.get("/api/production/registrations/summary?store_id=jamsil")
    assert response.status_code == 200
    payload = response.json()
    assert payload["filtered_store_id"] == "jamsil"
    assert payload["latest"]["store_id"] == "jamsil"
    assert payload["affected_sku_count"] >= 1


def test_production_registration_history_filters_by_date_range() -> None:
    client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-1", "qty": 10, "registered_by": "store_owner", "store_id": "gangnam"},
    )
    matched = client.get("/api/production/registrations/history?date_from=2026-03-31&date_to=2026-03-31")
    assert matched.status_code == 200
    matched_payload = matched.json()
    assert matched_payload["total"] >= 1
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
    assert payload["comparison"] is not None
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

    forbidden = client.get("/api/audit/logs")
    assert forbidden.status_code == 403

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
    assert len(payload["items"]) >= 1
    assert {"label", "value", "change", "trend", "detail"} <= set(payload["items"][0].keys())


def test_signals_endpoint_returns_response_shape() -> None:
    response = client.get("/api/signals")
    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert "high_count" in payload
    assert len(payload["items"]) >= 1
    assert {"id", "title", "metric", "value", "change", "trend", "priority", "region", "insight"} <= set(payload["items"][0].keys())
