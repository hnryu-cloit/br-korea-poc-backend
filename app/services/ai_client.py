from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class AIServiceClient:
    def __init__(self, base_url: str, token: str = "") -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token

    @property
    def _headers(self) -> dict[str, str]:
        if self._token:
            return {"Authorization": f"Bearer {self._token}"}
        return {}

    async def _post(self, path: str, body: dict) -> dict | None:
        url = f"{self._base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, json=body, headers=self._headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("AI 서비스 오류 (HTTP %s): %s", exc.response.status_code, url)
            return None
        except httpx.RequestError as exc:
            logger.warning("AI 서비스 연결 실패: %s", exc)
            return None

    async def query_sales(self, payload: dict) -> dict | None:
        """AI 서비스에 매출 분석 쿼리를 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post("/api/sales/query", payload)

    async def run_production_simulation(
        self,
        payload: dict,
        inventory_data: list[dict],
        production_data: list[dict],
        sales_data: list[dict],
        store_production_data: list[dict],
    ) -> dict | None:
        """AI 서비스에 생산 가이드 시뮬레이션을 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post(
            "/api/production/simulation",
            {
                "payload": payload,
                "inventory_data": inventory_data,
                "production_data": production_data,
                "sales_data": sales_data,
                "store_production_data": store_production_data,
            },
        )

    async def get_home_dashboard(
        self,
        inventory_data: list[dict],
        production_data: list[dict],
        sales_data: list[dict],
        store_production_data: list[dict],
    ) -> dict | None:
        """AI 서비스에 홈 대시보드 통합 분석을 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post(
            "/api/home/overview",
            {
                "inventory_data": inventory_data,
                "production_data": production_data,
                "sales_data": sales_data,
                "store_production_data": store_production_data,
            },
        )

    async def recommend_ordering(
        self,
        payload: dict,
    ) -> dict | None:
        """AI 서비스에 주문 추천을 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post("/api/ordering/recommend", payload)
