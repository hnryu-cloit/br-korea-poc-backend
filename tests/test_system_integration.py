from __future__ import annotations

import sys
import types
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.core.deps import get_production_service as backend_get_production_service
from app.main import app as backend_app
from app.repositories.production_repository import ProductionRepository
from app.schemas.production import ProductionSimulationResponse
from app.services.ai_client import AIServiceClient
from app.services.production_service import ProductionService


BACKEND_ROOT = Path(__file__).resolve().parents[1]
AI_ROOT = BACKEND_ROOT.parent / "br-korea-poc-ai"
AI_TOKEN = "test-token"


def _install_ai_import_stubs() -> None:
    """AI FastAPI 앱을 테스트 환경에서 import할 수 있도록 경량 stub을 주입한다."""

    if "dotenv" not in sys.modules:
        dotenv_module = types.ModuleType("dotenv")
        dotenv_module.load_dotenv = lambda *args, **kwargs: None  # type: ignore[assignment]
        sys.modules["dotenv"] = dotenv_module

    if "common.logger" not in sys.modules:
        common_logger_module = types.ModuleType("common.logger")

        def _init_logger(*args: object, **kwargs: object) -> object:
            import logging

            return logging.getLogger("br-korea-poc-ai-test")

        def _timefn(func):
            return func

        common_logger_module.init_logger = _init_logger  # type: ignore[attr-defined]
        common_logger_module.timefn = _timefn  # type: ignore[attr-defined]
        sys.modules["common.logger"] = common_logger_module

    if "google" not in sys.modules:
        google_module = types.ModuleType("google")
        google_module.__path__ = []  # type: ignore[attr-defined]
        sys.modules["google"] = google_module
    else:
        google_module = sys.modules["google"]

    if "google.genai" not in sys.modules:
        genai_module = types.ModuleType("google.genai")
        genai_module.__path__ = []  # type: ignore[attr-defined]

        class _DummyModels:
            def embed_content(self, **_: object) -> object:
                return types.SimpleNamespace(embeddings=[types.SimpleNamespace(values=[])])

            def generate_content(self, **_: object) -> object:
                return types.SimpleNamespace(
                    candidates=[
                        types.SimpleNamespace(
                            content=types.SimpleNamespace(
                                parts=[types.SimpleNamespace(text="{}")],
                            )
                        )
                    ]
                )

        class _DummyFiles:
            def upload(self, **_: object) -> object:
                return types.SimpleNamespace()

        class _DummyClient:
            def __init__(self, api_key: str | None = None) -> None:
                self.api_key = api_key
                self.models = _DummyModels()
                self.files = _DummyFiles()

        genai_types = types.ModuleType("google.genai.types")

        class _DummyPart:
            @classmethod
            def from_bytes(cls, data: bytes, mime_type: str) -> dict[str, object]:
                return {"data": data, "mime_type": mime_type}

            @classmethod
            def from_text(cls, text: str) -> dict[str, str]:
                return {"text": text}

        class _DummyContent:
            def __init__(self, role: str, parts: list[object]) -> None:
                self.role = role
                self.parts = parts

        genai_types.Part = _DummyPart
        genai_types.Content = _DummyContent
        genai_module.Client = _DummyClient
        genai_module.types = genai_types
        sys.modules["google.genai"] = genai_module
        sys.modules["google.genai.types"] = genai_types
        google_module.genai = genai_module

    if "PIL" not in sys.modules:
        pil_module = types.ModuleType("PIL")
        pil_module.__path__ = []  # type: ignore[attr-defined]
        pil_image_module = types.ModuleType("PIL.Image")
        pil_module.Image = pil_image_module
        sys.modules["PIL"] = pil_module
        sys.modules["PIL.Image"] = pil_image_module

    if "api.config" not in sys.modules:
        api_config = types.ModuleType("api.config")

        class _DummySettings:
            def __init__(self, **kwargs: object) -> None:
                for k, v in kwargs.items():
                    setattr(self, k, v)
            APP_NAME: str = "br-korea-poc-ai-stub"
            AI_SERVICE_TOKEN: str = AI_TOKEN

        api_config.Settings = _DummySettings  # type: ignore[attr-defined]
        api_config.get_settings = lambda: _DummySettings()  # type: ignore[attr-defined]
        sys.modules["api.config"] = api_config

    if "api.main" not in sys.modules:
        api_main = types.ModuleType("api.main")
        from fastapi import FastAPI
        api_main.app = FastAPI()  # type: ignore[attr-defined]
        sys.modules["api.main"] = api_main

    if "schemas.management" not in sys.modules:
        schemas_management = types.ModuleType("schemas.management")

        class _DummyResponse:
            def __init__(self, **kwargs: object) -> None:
                pass

        schemas_management.OrderingRecommendResponse = _DummyResponse  # type: ignore[attr-defined]
        schemas_management.ProductionPredictResponse = _DummyResponse  # type: ignore[attr-defined]
        sys.modules["schemas.management"] = schemas_management

    if "schemas.contracts" not in sys.modules:
        schemas_contracts = types.ModuleType("schemas.contracts")

        class _DummyResponse:
            def __init__(self, **kwargs: object) -> None:
                pass

        schemas_contracts.ChartDataPoint = _DummyResponse  # type: ignore[attr-defined]
        schemas_contracts.SimulationReportResponse = _DummyResponse  # type: ignore[attr-defined]
        sys.modules["schemas.contracts"] = schemas_contracts

    if "api.dependencies" not in sys.modules:
        api_dependencies = types.ModuleType("api.dependencies")
        from api.config import Settings as AISettings

        def _dummy_gemini_client() -> object:
            return object()

        def _dummy_rag_service() -> object:
            return object()

        def _dummy_orchestrator() -> object:
            return object()

        def _dummy_sales_analyzer() -> object:
            return object()

        def _dummy_channel_payment_analyzer() -> object:
            return object()

        def _dummy_production_service() -> object:
            return object()

        def _dummy_ordering_service() -> object:
            return object()

        def _dummy_sales_service() -> object:
            return object()

        async def _verify_token() -> bool:
            return True

        api_dependencies.get_settings = lambda: AISettings(AI_SERVICE_TOKEN=AI_TOKEN)  # type: ignore[attr-defined]
        api_dependencies.verify_token = _verify_token  # type: ignore[attr-defined]
        api_dependencies.get_gemini_client = _dummy_gemini_client  # type: ignore[attr-defined]
        api_dependencies.get_rag_service = _dummy_rag_service  # type: ignore[attr-defined]
        api_dependencies.get_orchestrator = _dummy_orchestrator  # type: ignore[attr-defined]
        api_dependencies.get_sales_analyzer = _dummy_sales_analyzer  # type: ignore[attr-defined]
        api_dependencies.get_channel_payment_analyzer = _dummy_channel_payment_analyzer  # type: ignore[attr-defined]
        api_dependencies.get_production_service = _dummy_production_service  # type: ignore[attr-defined]
        api_dependencies.get_ordering_service = _dummy_ordering_service  # type: ignore[attr-defined]
        api_dependencies.get_sales_service = _dummy_sales_service  # type: ignore[attr-defined]
        sys.modules["api.dependencies"] = api_dependencies

    if "pipeline.run" not in sys.modules:
        pipeline_run = types.ModuleType("pipeline.run")

        async def _run_pipeline(prompt: str, context: dict | None = None) -> dict[str, object]:
            return {"prompt": prompt, "context": context or {}}

        pipeline_run.run_pipeline = _run_pipeline  # type: ignore[attr-defined]
        sys.modules["pipeline.run"] = pipeline_run

    service_module_names = [
        "services.sales_analyzer",
        "services.channel_payment_analyzer",
        "services.rag_service",
        "services.orchestrator",
        "services.production_service",
        "services.ordering_service",
        "services.sales_service",
    ]
    service_exports = {
        "services.sales_analyzer": "SalesAnalyzer",
        "services.channel_payment_analyzer": "ChannelPaymentAnalyzer",
        "services.rag_service": "RAGService",
        "services.orchestrator": "AgentOrchestrator",
        "services.production_service": "ProductionService",
        "services.ordering_service": "OrderingService",
        "services.sales_service": "SalesService",
    }
    for module_name in service_module_names:
        if module_name in sys.modules:
            continue
        module = types.ModuleType(module_name)

        class _DummyService:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

        exported_name = service_exports[module_name]
        setattr(module, exported_name, _DummyService)
        sys.modules[module_name] = module


_install_ai_import_stubs()

if str(AI_ROOT) not in sys.path:
    sys.path.insert(0, str(AI_ROOT))

from api.config import Settings as AISettings
from api.config import get_settings as ai_get_settings
from api.dependencies import get_ordering_service as ai_get_ordering_service
from api.dependencies import get_production_service as ai_get_production_service
from api.main import app as ai_app
from schemas.management import OrderingRecommendResponse, ProductionPredictResponse
from schemas.contracts import ChartDataPoint, SimulationReportResponse as AIContractSimulationReportResponse


class InProcessAIServiceClient(AIServiceClient):
    def __init__(self, ai_app_instance, token: str) -> None:
        super().__init__(base_url="http://ai.local", token=token)
        self._ai_app = ai_app_instance
        self.last_request: dict[str, object] | None = None

    async def _post(self, path: str, body: dict) -> dict | None:
        transport = httpx.ASGITransport(app=self._ai_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://ai.local") as client:
            response = await client.post(path, json=body, headers=self._headers)
        self.last_request = {"path": path, "body": body}
        response.raise_for_status()
        return response.json()


class FakeAIProductionService:
    def __init__(self) -> None:
        self.last_payload: dict[str, object] | None = None

    def get_simulation_report(self, payload, inventory_df, production_df, sales_df):
        self.last_payload = {
            "store_id": payload.store_id,
            "item_id": payload.item_id,
            "inventory_rows": len(inventory_df),
            "production_rows": len(production_df),
            "sales_rows": len(sales_df),
        }
        return AIContractSimulationReportResponse(
            metadata={
                "store_id": payload.store_id,
                "item_id": payload.item_id,
                "date": payload.simulation_date,
                "source": "ai-fastapi-contract",
            },
            summary_metrics={
                "additional_sales_qty": 12.0,
                "additional_profit_amt": 18000,
                "additional_waste_qty": 2.0,
                "additional_waste_cost": 1400,
                "net_profit_change": 16600,
                "performance_status": "POSITIVE",
                "chance_loss_reduction": 4500.0,
            },
            time_series_data=[
                ChartDataPoint(time="08:00", actual_stock=40.0, ai_guided_stock=52.0),
                ChartDataPoint(time="10:00", actual_stock=34.0, ai_guided_stock=46.0),
            ],
            actions_timeline=[
                "[10:00] AI 추천으로 20개 추가 생산",
                "[14:00] AI 추천으로 15개 추가 생산",
            ],
        )

    def predict_stock(self, sku, current_stock, history, pattern_4w):
        return ProductionPredictResponse(
            sku=sku,
            predicted_stock_1h=max(float(current_stock) - 4.0, 0.0),
            risk_detected=True,
            stockout_expected_at="1시간 이내",
            alert_message="1시간 이내 품절 위험입니다. 즉시 생산 여부를 확인하세요.",
            confidence=0.84,
        )


class FakeAIOrderingService:
    def recommend_ordering(self, payload):
        return OrderingRecommendResponse(
            options=[
                {"name": "전주 동요일 기준", "recommended_quantity": 120, "priority": 1},
                {"name": "전전주 동요일 기준", "recommended_quantity": 112, "priority": 2},
                {"name": "전월 동요일 기준", "recommended_quantity": 108, "priority": 3},
            ],
            reasoning=f"{getattr(payload, 'store_id', 'store')} 최근 패턴 기준 추천입니다.",
        )


@pytest.fixture(scope="module")
def ai_client() -> TestClient:
    ai_app.dependency_overrides[ai_get_settings] = lambda: AISettings(AI_SERVICE_TOKEN=AI_TOKEN)
    ai_app.dependency_overrides[ai_get_production_service] = lambda: FakeAIProductionService()
    ai_app.dependency_overrides[ai_get_ordering_service] = lambda: FakeAIOrderingService()
    with TestClient(ai_app) as client:
        yield client
    ai_app.dependency_overrides.clear()


def test_home_overview_matches_frontend_contract() -> None:
    client = TestClient(backend_app)

    response = client.get("/api/home/overview")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {"updated_at", "priority_actions", "stats", "cards", "imminent_deadlines"}
    assert isinstance(payload["priority_actions"], list)
    assert len(payload["stats"]) == 4
    assert len(payload["cards"]) == 3

    if payload["priority_actions"]:
        first_action = payload["priority_actions"][0]
        assert "ai_reasoning" in first_action
        assert "confidence_score" in first_action
        assert "is_finished_good" in first_action

    ordering_card = next(card for card in payload["cards"] if card["domain"] == "ordering")
    assert isinstance(ordering_card["delivery_scheduled"], bool)


def test_ai_fastapi_simulation_route_returns_contract(ai_client: TestClient) -> None:
    response = ai_client.post(
        "/api/production/simulation",
        json={
            "store_id": "POC_001",
            "item_id": "sku-1",
            "simulation_date": "2026-04-13",
            "lead_time_hour": 1,
            "margin_rate": 0.3,
            "inventory_data": [
                {
                    "MASKED_STOR_CD": "POC_001",
                    "ITEM_CD": "sku-1",
                    "ITEM_NM": "스트로베리 필드",
                    "STOCK_QTY": 24,
                    "SALE_QTY": 3,
                    "STOCK_DT": "20260413",
                }
            ],
            "production_data": [
                {
                    "MASKED_STOR_CD": "POC_001",
                    "ITEM_CD": "sku-1",
                    "ITEM_NM": "스트로베리 필드",
                    "PROD_QTY": 40,
                    "PROD_DT": "20260413",
                    "PROD_DGRE": "1",
                    "SALE_PRC": 1500,
                    "ITEM_COST": 700,
                }
            ],
            "sales_data": [
                {
                    "MASKED_STOR_CD": "POC_001",
                    "ITEM_CD": "sku-1",
                    "ITEM_NM": "스트로베리 필드",
                    "SALE_QTY": 3,
                    "SALE_DT": "20260413",
                    "TMZON_DIV": "08",
                }
            ],
        },
        headers={"Authorization": f"Bearer {AI_TOKEN}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["metadata"]["source"] == "ai-fastapi-contract"
    assert len(payload["time_series_data"]) == 2
    assert len(payload["actions_timeline"]) == 2


def test_ai_fastapi_management_alias_routes_are_compatible(ai_client: TestClient) -> None:
    predict_response = ai_client.post(
        "/management/production/predict",
        json={
            "sku": "sku-1",
            "current_stock": 24,
            "history": [{"timestamp": "2026-04-13T09:00:00", "stock": 24, "production": 0, "sales": 6}],
            "pattern_4w": [1.1, 0.0],
        },
        headers={"Authorization": f"Bearer {AI_TOKEN}"},
    )
    assert predict_response.status_code == 200
    assert predict_response.json()["risk_detected"] is True

    ordering_response = ai_client.post(
        "/management/ordering/recommend",
        json={
            "store_id": "POC_001",
            "current_date": "2026-04-13",
            "is_campaign": True,
            "is_holiday": False,
        },
        headers={"Authorization": f"Bearer {AI_TOKEN}"},
    )
    assert ordering_response.status_code == 200
    ordering_payload = ordering_response.json()
    assert len(ordering_payload["options"]) == 3
    assert ordering_payload["options"][0]["priority"] == 1


def test_backend_simulation_round_trip_uses_ai_fastapi_app(ai_client: TestClient) -> None:
    assert ai_client is not None

    in_process_ai_client = InProcessAIServiceClient(ai_app, token=AI_TOKEN)
    backend_service = ProductionService(
        repository=ProductionRepository(engine=None),
        ai_client=in_process_ai_client,
    )
    backend_app.dependency_overrides[backend_get_production_service] = lambda: backend_service

    try:
        response = TestClient(backend_app).post(
            "/api/production/simulation",
            json={
                "store_id": "POC_001",
                "item_id": "sku-1",
                "simulation_date": "2026-04-13",
                "lead_time_hour": 1,
                "margin_rate": 0.3,
            },
        )
    finally:
        backend_app.dependency_overrides.pop(backend_get_production_service, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["metadata"]["store_id"] == "POC_001"
    assert payload["metadata"]["source"] == "ai-fastapi-contract"
    assert len(payload["time_series_data"]) == 2
    assert len(payload["actions_timeline"]) == 2

    validated = ProductionSimulationResponse.model_validate(payload)
    assert len(validated.time_series_data) == 2
    assert in_process_ai_client.last_request is not None
    assert in_process_ai_client.last_request["path"] == "/api/production/simulation"
    assert in_process_ai_client.last_request["body"]["store_id"] == "POC_001"
