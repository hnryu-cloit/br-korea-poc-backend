from __future__ import annotations

import logging
import re
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
        business_date: str | None = None,
        business_time: str | None = None,
        system_instruction: str | None = None,
    ) -> dict | None:
        """AI 서비스에 매출 분석 쿼리를 요청합니다. 실패 시 None을 반환합니다."""
        normalized_store_id = store_id.strip() if isinstance(store_id, str) else ""
        if not normalized_store_id:
            raise ValueError("store_id is required")

        body: dict[str, object] = {"store_id": normalized_store_id, "query": prompt}
        if domain:
            body["domain"] = domain
        if business_date:
            body["business_date"] = business_date
        if business_time:
            body["business_time"] = business_time
        if system_instruction:
            body["system_instruction"] = system_instruction
        result = await self._post("/sales/query", body, timeout=90.0)
        if result is None:
            return None
        answer = result.get("answer") if isinstance(result.get("answer"), dict) else {}
        if not answer:
            answer = {
                "text": result.get("text", ""),
                "evidence": result.get("evidence", []),
                "actions": result.get("actions", []),
            }

        grounding = result.get("grounding") if isinstance(result.get("grounding"), dict) else {}
        if not grounding:
            grounding = {
                "keywords": result.get("keywords", []),
                "intent": result.get("intent"),
                "relevant_tables": result.get("relevant_tables", []),
                "sql": result.get("sql"),
                "row_count": result.get("row_count", 0),
            }

        request_context = result.get("request_context")
        if not isinstance(request_context, dict):
            request_context = {
                "store_id": normalized_store_id,
                "business_date": business_date or "2026-03-05",
                "business_time": business_time,
                "prompt": prompt,
                "domain": domain or "sales",
            }

        return {
            "text": answer.get("text", ""),
            "evidence": answer.get("evidence", []),
            "actions": answer.get("actions", []),
            "answer": answer,
            "request_context": request_context,
            "agent_trace": {
                "keywords": grounding.get("keywords", []),
                "intent": grounding.get("intent"),
                "relevant_tables": grounding.get("relevant_tables", []),
                "sql": grounding.get("sql"),
                "queried_period": result.get("queried_period"),
                "row_count": grounding.get("row_count", 0),
            },
            "store_context": "",
            "data_source": "ai",
            "comparison_basis": result.get("source_data_period", ""),
            "calculation_date": "",
            "comparison": None,
            "blocked": False,
            "masked_fields": result.get("masked_fields", []),
            "confidence_score": result.get("confidence_score", 1.0),
            "semantic_logic": None,
            "sources": [],
            "visual_data": {
                "channel_analysis": result.get("channel_analysis"),
                "profit_simulation": result.get("profit_simulation"),
            },
            "data_lineage": result.get("data_lineage", []),
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
        store_id: str | None = None,
    ) -> dict | None:
        """AI 서비스에 생산 위험 예측을 요청합니다. 실패 시 None을 반환합니다."""
        body: dict = {
            "sku": sku,
            "current_stock": current_stock,
            "history": history,
            "pattern_4w": pattern_4w,
        }
        if store_id:
            body["store_id"] = store_id
        return await self._post("/management/production/predict", body)

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

    async def generate_market_insights(
        self,
        *,
        audience: str,
        scope: dict[str, object],
        market_data: dict[str, object],
        branch_snapshots: list[dict[str, object]] | None = None,
        store_name: str | None = None,
    ) -> dict | None:
        """AI 서비스에 상권 인사이트 생성을 요청합니다."""
        return await self._post(
            "/analytics/market/insights",
            {
                "audience": audience,
                "scope": scope,
                "market_data": market_data,
                "branch_snapshots": branch_snapshots or [],
                "store_name": store_name,
            },
            timeout=20.0,
        )

    async def generate_ordering_history_insights(
        self,
        *,
        store_id: str,
        filters: dict[str, object],
        history_items: list[dict[str, object]],
        summary_stats: dict[str, object],
    ) -> dict | None:
        """AI 서비스에 발주 이력 이상징후 인사이트 생성을 요청합니다."""
        return await self._post(
            "/analytics/ordering/history/insights",
            {
                "store_id": store_id,
                "filters": filters,
                "history_items": history_items,
                "summary_stats": summary_stats,
            },
            timeout=60.0,
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

    async def generate_grounded_explanation(
        self,
        *,
        store_id: str,
        topic: str,
        evidence_items: list[dict[str, str]],
    ) -> dict[str, object] | None:
        """근거 ID를 포함한 설명 문장을 생성합니다.

        - 성공 조건: 설명 본문에 [E1] 형태 근거 인용이 최소 1개 존재
        - 인용된 ID는 입력 evidence_items의 id 집합 내에 존재해야 함
        """
        if not evidence_items:
            return None

        evidence_lines = "\n".join(
            [
                f"{item.get('id', '')}: {item.get('label', '')} | {item.get('value', '')} | {item.get('calculation', '')}"
                for item in evidence_items
            ]
        )
        prompt = (
            f"[요청]\n{topic} 결과를 점주가 이해할 수 있게 2~3문장으로 요약해줘.\n"
            "[규칙]\n"
            "1) 문장마다 반드시 근거 ID를 [E1], [E2] 형태로 인용\n"
            "2) 아래 근거 목록에 없는 ID는 사용 금지\n"
            "3) 수치를 임의 생성하지 말고 근거 목록 수치만 사용\n\n"
            f"[근거 목록]\n{evidence_lines}"
        )
        result = await self.query_sales(
            prompt=prompt,
            store_id=store_id,
            domain="production",
            system_instruction="근거 ID 인용을 강제하고 수치를 왜곡하지 마세요.",
        )
        if not result:
            return None

        text = str(result.get("text") or "").strip()
        if not text:
            return None

        cited_ids = sorted(set(re.findall(r"\[(E\d+)\]", text)))
        valid_ids = {str(item.get("id") or "") for item in evidence_items}
        filtered_ids = [evid for evid in cited_ids if evid in valid_ids]
        if not filtered_ids:
            return None

        return {"text": text, "citations": filtered_ids}
