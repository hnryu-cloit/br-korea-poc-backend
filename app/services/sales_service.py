from __future__ import annotations

from typing import Optional

from app.repositories.sales_repository import SalesRepository
from app.schemas.sales import SalesComparison, SalesInsightsResponse, SalesPrompt, SalesQueryRequest, SalesQueryResponse
from app.services.ai_client import AIServiceClient
from app.services.audit_service import AuditService

_COMPARISON_KEYWORDS = ["배달", "매출", "전년 동월", "채널"]
_FAQ_KEYWORDS = ["무엇", "어떻게", "왜", "설명", "가이드"]
_DATA_LOOKUP_KEYWORDS = ["조회", "건수", "수치", "비율", "얼마", "몇"]
_SENSITIVE_KEYWORDS = ["이익", "이익률", "원가", "손익", "마진", "타점포", "점포 성과"]
_HQ_ROLES = {"hq_operator", "hq_planner"}

_STUB_COMPARISON = SalesComparison(
    store="강남역점",
    peer_group="유사 상권 10개 점포 평균",
    summary="강남역점은 배달 비중과 앱 전환율이 비교군보다 낮고, 오전 매장 방문 매출은 더 높습니다.",
    metrics=[
        {"label": "배달 매출 비중", "store_value": "22%", "peer_value": "29%"},
        {"label": "앱 쿠폰 사용률", "store_value": "22%", "peer_value": "31%"},
        {"label": "오전 매장 방문 매출", "store_value": "58%", "peer_value": "49%"},
    ],
)


class SalesService:
    def __init__(
        self,
        repository: SalesRepository,
        ai_client: Optional[AIServiceClient] = None,
        audit_service: Optional[AuditService] = None,
    ) -> None:
        self.repository = repository
        self.ai_client = ai_client
        self.audit_service = audit_service

    async def list_prompts(self) -> list[SalesPrompt]:
        prompts = await self.repository.list_prompts()
        return [SalesPrompt(**prompt) for prompt in prompts]

    async def query(self, payload: SalesQueryRequest, actor_role: str = "store_owner") -> SalesQueryResponse:
        query_type = self._classify_query(payload.prompt)
        if query_type == "sensitive_request" and actor_role not in _HQ_ROLES:
            response = SalesQueryResponse(
                text="요청하신 내용에는 민감정보가 포함되어 있어 현재 권한으로는 조회할 수 없습니다.",
                evidence=["민감정보 정책이 적용되었습니다.", "점포 손익·원가·이익률 정보는 본사 권한에서만 조회할 수 있습니다."],
                actions=["민감정보를 제외한 운영 지표로 다시 질문해 주세요.", "본사 운영/기획 권한으로 재시도해 주세요."],
                query_type=query_type,
                processing_route="policy_block",
                blocked=True,
                masked_fields=["profitability"],
            )
            if self.audit_service:
                await self.audit_service.record(
                    domain="sales",
                    event_type="sales_query_blocked",
                    actor_role=actor_role,
                    route="policy_block",
                    outcome="blocked",
                    message="민감정보 질의를 차단했습니다.",
                    metadata={"prompt": payload.prompt, "query_type": query_type},
                )
            return response

        ai_result: Optional[dict] = None
        processing_route = "stub_repository"
        if self.ai_client:
            ai_result = await self.ai_client.query_sales(payload.prompt)
            processing_route = "ai_proxy"

        if ai_result is not None:
            response = ai_result
        else:
            response = await self.repository.get_query_response(payload.prompt)

        comparison = None
        if any(keyword in payload.prompt for keyword in _COMPARISON_KEYWORDS):
            comparison = _STUB_COMPARISON

        result = SalesQueryResponse(
            comparison=comparison,
            query_type=query_type,
            processing_route=processing_route,
            **response,
        )
        if self.audit_service:
            await self.audit_service.record(
                domain="sales",
                event_type="sales_query_completed",
                actor_role=actor_role,
                route=processing_route,
                outcome="success",
                message="매출 질의를 처리했습니다.",
                metadata={"prompt": payload.prompt, "query_type": query_type, "comparison": comparison is not None},
            )
        return result

    async def get_insights(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> SalesInsightsResponse:
        payload = await self.repository.get_insights(store_id=store_id, date_from=date_from, date_to=date_to)
        return SalesInsightsResponse(**payload)

    def _classify_query(self, prompt: str) -> str:
        if any(keyword in prompt for keyword in _SENSITIVE_KEYWORDS):
            return "sensitive_request"
        if any(keyword in prompt for keyword in _DATA_LOOKUP_KEYWORDS):
            return "data_lookup"
        if any(keyword in prompt for keyword in _FAQ_KEYWORDS):
            return "faq"
        return "analysis"
