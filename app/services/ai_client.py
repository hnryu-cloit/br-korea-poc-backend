from __future__ import annotations

import logging
from uuid import uuid4

import httpx
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger(__name__)


class AIServiceClient:
    def __init__(self, base_url: str, token: str = "") -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token

    def _build_headers(self, request_id: str) -> dict[str, str]:
        headers = {"X-Request-Id": request_id}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    @staticmethod
    def _extract_error_contract(response: httpx.Response) -> dict | None:
        try:
            payload = response.json()
        except ValueError:
            return None
        detail = payload.get("detail") if isinstance(payload, dict) else None
        if not isinstance(detail, dict):
            return None
        error_code = detail.get("error_code")
        message = detail.get("message")
        if isinstance(error_code, str) and isinstance(message, str):
            return detail
        return None

    async def _post(self, path: str, body: dict, timeout: float = 30.0) -> dict | None:
        url = f"{self._base_url}{path}"
        request_id = uuid4().hex
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    url,
                    json=jsonable_encoder(body),
                    headers=self._build_headers(request_id),
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            detail = self._extract_error_contract(exc.response) or {}
            logger.warning(
                "AI 서비스 오류 (HTTP %s): %s request_id=%s trace_id=%s error_code=%s retryable=%s",
                exc.response.status_code,
                url,
                request_id,
                detail.get("trace_id"),
                detail.get("error_code"),
                detail.get("retryable"),
            )
            return None
        except httpx.RequestError as exc:
            logger.warning("AI 서비스 연결 실패: request_id=%s error=%s", request_id, exc)
            return None

    async def query_sales(
        self,
        prompt: str,
        store_id: str,
        domain: str | None = None,
        system_instruction: str | None = None,
    ) -> dict | None:
        """AI 서비스에 매출 분석 쿼리를 요청합니다. 실패 시 None을 반환합니다."""
        normalized_store_id = store_id.strip() if isinstance(store_id, str) else ""
        if not normalized_store_id:
            raise ValueError("store_id is required")

        body: dict[str, object] = {"store_id": normalized_store_id, "query": prompt}
        if domain:
            body["domain"] = domain
        if system_instruction:
            body["system_instruction"] = system_instruction
        result = await self._post("/sales/query", body, timeout=90.0)
        if result is None:
            return None

        if "text" in result and "actions" in result:
            return result

        answer = result.get("answer") or {}
        return {
            "text": answer.get("text", ""),
            "evidence": answer.get("evidence", []),
            "actions": answer.get("actions", []),
            "store_context": "",
            "data_source": "ai",
            "comparison_basis": result.get("source_data_period", ""),
            "calculation_date": "",
            "comparison": None,
            "blocked": False,
            "masked_fields": [],
            "confidence_score": 1.0,
            "semantic_logic": None,
            "sources": [],
            "visual_data": {
                "channel_analysis": result.get("channel_analysis"),
                "profit_simulation": result.get("profit_simulation"),
            },
        }

    async def suggest_sales_prompts(
        self,
        store_id: str,
        domain: str,
        context_prompts: list[dict],
        system_instruction: str | None = None,
    ) -> list[dict] | None:
        body: dict[str, object] = {
            "store_id": store_id,
            "domain": domain,
            "context_prompts": context_prompts,
        }
        if system_instruction:
            body["system_instruction"] = system_instruction
        result = await self._post("/sales/prompts/suggest", body)
        if result is None:
            return None
        prompts = result.get("prompts")
        if isinstance(prompts, list):
            return prompts
        return None

    async def predict_production(
        self,
        sku: str,
        current_stock: int,
        history: list[dict],
        pattern_4w: list[float],
    ) -> dict | None:
        """AI 서비스에 생산 위험 예측을 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post(
            "/management/production/predict",
            {
                "sku": sku,
                "current_stock": current_stock,
                "history": history,
                "pattern_4w": pattern_4w,
            },
        )

    async def recommend_ordering(
        self,
        store_id: str,
        current_date: str,
        is_campaign: bool = False,
        is_holiday: bool = False,
    ) -> dict | None:
        """AI 서비스에 주문 추천을 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post(
            "/management/ordering/recommend",
            {
                "store_id": store_id,
                "current_date": current_date,
                "is_campaign": is_campaign,
                "is_holiday": is_holiday,
            },
        )

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        url = f"{self._base_url}{path}"
        request_id = uuid4().hex
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    url,
                    params=params,
                    headers=self._build_headers(request_id),
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            detail = self._extract_error_contract(exc.response) or {}
            logger.warning(
                "AI 서비스 오류 (HTTP %s): %s request_id=%s trace_id=%s error_code=%s retryable=%s",
                exc.response.status_code,
                url,
                request_id,
                detail.get("trace_id"),
                detail.get("error_code"),
                detail.get("retryable"),
            )
            return None
        except httpx.RequestError as exc:
            logger.warning("AI 서비스 연결 실패: request_id=%s error=%s", request_id, exc)
            return None

    async def get_production_push_alerts(self, store_id: str | None) -> list[dict]:
        """AI 서비스에서 생산 PUSH 알림 페이로드 목록을 조회합니다."""
        if not store_id:
            return []
        result = await self._get("/api/production/alerts/push", params={"store_id": store_id})
        if result is None:
            return []
        return result.get("alerts", [])

    async def get_ordering_deadline_alert(self, store_id: str) -> dict | None:
        """AI 서비스에서 주문 마감 알림 정보를 조회합니다."""
        return await self._get("/api/ordering/deadline-alerts", params={"store_id": store_id})

    async def get_ordering_deadline_alerts_batch(self, store_ids: list[str]) -> list[dict]:
        """AI 서비스에서 여러 매장의 주문 마감 알림 정보를 일괄 조회합니다."""
        normalized_store_ids = [store_id.strip() for store_id in store_ids if store_id and store_id.strip()]
        if not normalized_store_ids:
            return []
        result = await self._post(
            "/api/ordering/deadline-alerts/batch",
            {"store_ids": normalized_store_ids},
        )
        if result is None:
            return []
        items = result.get("items")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
        return []

    async def get_contract_info(self) -> dict | None:
        """AI 서비스 계약 버전 메타정보를 조회합니다."""
        return await self._get("/meta/contract")

    async def run_simulation(
        self,
        store_id: str,
        item_id: str,
        simulation_date: str,
        lead_time_hour: int,
        margin_rate: float,
        inventory_data: list[dict],
        production_data: list[dict],
        sales_data: list[dict],
    ) -> dict | None:
        """AI 서비스에 생산 시뮬레이션을 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post(
            "/api/production/simulation",
            {
                "store_id": store_id,
                "item_id": item_id,
                "simulation_date": simulation_date,
                "lead_time_hour": lead_time_hour,
                "margin_rate": margin_rate,
                "inventory_data": inventory_data,
                "production_data": production_data,
                "sales_data": sales_data,
            },
        )
