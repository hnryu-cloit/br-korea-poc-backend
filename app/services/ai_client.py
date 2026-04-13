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

    async def query_sales(self, prompt: str) -> dict | None:
        """AI 서비스에 매출 분석 쿼리를 요청합니다. 실패 시 None을 반환합니다."""
        return await self._post("/sales/query", {"prompt": prompt})

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