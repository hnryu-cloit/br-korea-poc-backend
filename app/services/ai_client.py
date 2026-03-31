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

    async def query_sales(self, prompt: str) -> dict | None:
        """AI 서비스에 매출 분석 쿼리를 요청합니다. 실패 시 None을 반환합니다."""
        url = f"{self._base_url}/sales/query"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, json={"prompt": prompt}, headers=self._headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("AI 서비스 오류 (HTTP %s): %s", exc.response.status_code, url)
            return None
        except httpx.RequestError as exc:
            logger.warning("AI 서비스 연결 실패: %s", exc)
            return None