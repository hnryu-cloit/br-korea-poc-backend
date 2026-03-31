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


def test_ordering_selection_save() -> None:
    response = client.post(
        "/api/ordering/selections",
        json={"option_id": "opt-a", "reason": "알림에서 바로 선택", "actor": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["saved"] is True
    assert payload["option_id"] == "opt-a"


def test_production_overview() -> None:
    response = client.get("/api/production/overview")
    assert response.status_code == 200
    payload = response.json()
    assert payload["production_lead_time_minutes"] == 60
    assert len(payload["items"]) >= 1


def test_production_registration() -> None:
    response = client.post(
        "/api/production/registrations",
        json={"sku_id": "sku-1", "qty": 40, "registered_by": "store_owner"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["feedback_type"] == "chance_loss_reduced"


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
