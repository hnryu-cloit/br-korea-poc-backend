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
