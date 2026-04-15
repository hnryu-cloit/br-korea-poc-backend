from __future__ import annotations

import re
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

# 한국 전화번호 패턴 (010-XXXX-XXXX, 0X0-XXXXXXXX 등 변형 포함)
_PHONE_RE = re.compile(r"0\d{1,2}[-.\s]?\d{3,4}[-.\s]?\d{4}")


def _mask_pii(text: str) -> tuple[str, list[str]]:
    """텍스트에서 PII를 마스킹하고 마스킹된 필드 목록을 반환"""
    masked_fields: list[str] = []
    masked = text
    if _PHONE_RE.search(text):
        masked = _PHONE_RE.sub("***-****-****", text)
        masked_fields.append("phone_number")
    return masked, masked_fields

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
        # 1. PII 마스킹 (전화번호 등)
        safe_prompt, pii_masked_fields = _mask_pii(payload.prompt)

        query_type = self._classify_query(safe_prompt)
        if query_type == "sensitive_request" and actor_role not in _HQ_ROLES:
            response = SalesQueryResponse(
                text="요청하신 내용에는 민감정보가 포함되어 있어 현재 권한으로는 조회할 수 없습니다.",
                evidence=["민감정보 정책이 적용되었습니다.", "점포 손익·원가·이익률 정보는 본사 권한에서만 조회할 수 있습니다."],
                actions=["민감정보를 제외한 운영 지표로 다시 질문해 주세요.", "본사 운영/기획 권한으로 재시도해 주세요."],
                query_type=query_type,
                processing_route="policy_block",
                blocked=True,
                masked_fields=pii_masked_fields + ["profitability"],
            )
            if self.audit_service:
                await self.audit_service.record(
                    domain="sales",
                    event_type="sales_query_blocked",
                    actor_role=actor_role,
                    route="policy_block",
                    outcome="blocked",
                    message="민감정보 질의를 차단했습니다.",
                    metadata={"prompt": safe_prompt, "query_type": query_type},
                )
            return response

        # 2. AI 또는 repository 조회 (마스킹된 쿼리 사용)
        ai_result: Optional[dict] = None
        processing_route = "stub_repository"
        if self.ai_client:
            ai_result = await self.ai_client.query_sales(safe_prompt)

        if ai_result is not None:
            response = ai_result
            processing_route = "ai_proxy"
        else:
            response = await self.repository.get_query_response(safe_prompt)

        # 3. PII 마스킹 필드 병합
        existing_masked = response.pop("masked_fields", []) if isinstance(response, dict) else []
        merged_masked = pii_masked_fields + [f for f in existing_masked if f not in pii_masked_fields]

        comparison_val = response.pop("comparison", None) if isinstance(response, dict) else None
        result = SalesQueryResponse(
            comparison=comparison_val,
            query_type=query_type,
            processing_route=processing_route,
            masked_fields=merged_masked,
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
                metadata={"prompt": safe_prompt, "query_type": query_type, "comparison": False},
            )
        return result

    async def get_insights(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> SalesInsightsResponse:
        payload = await self.repository.get_insights(store_id=store_id, date_from=date_from, date_to=date_to)
        for key in ("peak_hours", "channel_mix", "payment_mix", "menu_mix", "campaign_seasonality"):
            section = payload.get(key)
            if not section:
                continue
            section["status"] = "active" if section.get("status") == "active" else "review"
        return SalesInsightsResponse(**payload)

    def _classify_query(self, prompt: str) -> str:
        if any(keyword in prompt for keyword in _SENSITIVE_KEYWORDS):
            return "sensitive_request"
        if any(keyword in prompt for keyword in _DATA_LOOKUP_KEYWORDS):
            return "data_lookup"
        if any(keyword in prompt for keyword in _FAQ_KEYWORDS):
            return "faq"
        return "analysis"
