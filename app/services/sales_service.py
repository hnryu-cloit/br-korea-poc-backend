from __future__ import annotations

import logging
import re
from textwrap import dedent
from typing import Optional

logger = logging.getLogger(__name__)

from app.repositories.sales_repository import SalesRepository
from app.schemas.sales import (
    SalesCampaignEffectResponse,
    SalesInsightsResponse,
    SalesAnswer,
    SalesQueryAgentTrace,
    SalesPrompt,
    SalesQueryRequest,
    SalesQueryRequestContext,
    SalesQueryResponse,
    SalesSummaryResponse,
)
from app.services.ai_client import AIServiceClient
from app.services.audit_service import AuditService
from app.services.explainability_service import (
    create_failed_payload,
    create_ready_payload,
    ensure_non_empty_actions_evidence,
)
from app.services.prompt_settings_service import PromptSettingsService

_COMPARISON_KEYWORDS = ["배달", "매출", "전년 동월", "채널"]
_FAQ_KEYWORDS = ["무엇", "어떻게", "왜", "설명", "가이드"]
_DATA_LOOKUP_KEYWORDS = ["조회", "건수", "수치", "비율", "얼마", "몇"]
_SENSITIVE_KEYWORDS = ["이익", "이익률", "원가", "손익", "마진", "타점포", "점포 성과"]
_HQ_ROLES = {"hq_admin", "hq_operator", "hq_planner"}
_FLOATING_CHAT_REQUIRED_INSTRUCTION = """
당신은 점주 운영 보조 AI다.
- 단순 데이터 요약이 아니라 질문 맥락에 맞는 실행 가능한 인사이트를 제시한다.
- 모든 응답과 알림에는 점주가 즉시 할 수 있는 Action을 포함한다.
- 수치 제안은 과거 데이터 또는 예측 모델 근거를 evidence로 제시한다.
- 매장 맞춤형 답변을 제공한다.
- 재고/생산 답변은 1시간 후 예측 오차 허용범위(±10%)와 찬스 로스 방지 알림 시점 근거를 포함한다.
- 출력은 설명(text), 출처/근거(evidence), 후속 액션(actions), 추가 예상질문 3개(follow_up_questions)를 포함한다.
""".strip()

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
        ai_client: AIServiceClient | None = None,
        audit_service: AuditService | None = None,
        prompt_settings_service: PromptSettingsService | None = None,
    ) -> None:
        self.repository = repository
        self.ai_client = ai_client
        self.audit_service = audit_service
        self.prompt_settings_service = prompt_settings_service

    @staticmethod
    def _build_request_context(
        payload: SalesQueryRequest,
        safe_prompt: str,
    ) -> SalesQueryRequestContext:
        return SalesQueryRequestContext(
            store_id=payload.store_id,
            business_date=payload.business_date,
            business_time=payload.business_time,
            prompt=safe_prompt,
            domain=payload.domain,
        )

    @staticmethod
    def _sale_dt_to_day_label(dt_str: str) -> str:
        """YYYYMMDD 형식 날짜 문자열을 요일 라벨로 변환"""
        _DAY_LABELS = ["월", "화", "수", "목", "금", "토", "일"]
        if len(dt_str) == 8:
            try:
                from datetime import datetime as _dt
                return _DAY_LABELS[_dt.strptime(dt_str, "%Y%m%d").weekday()]
            except ValueError:
                return dt_str[-2:]
        return dt_str

    @staticmethod
    def _is_tday_prompt(prompt: str) -> bool:
        lowered = prompt.lower()
        return "티데이" in prompt or "tday" in lowered

    @staticmethod
    def _format_currency(value: float | int | None) -> str:
        amount = float(value or 0)
        return f"{int(round(amount)):,}원"

    def _build_tday_query_response(
        self,
        *,
        payload: dict,
        request_context: SalesQueryRequestContext,
    ) -> dict:
        period = next(iter(payload.get("periods", [])), {})
        period_text = " ~ ".join(
            [text for text in [period.get("start_date"), period.get("end_date")] if text]
        ) or "프로모션 기간 정보 없음"
        product_mix = payload.get("product_mix", []) or []
        top_mix = ", ".join(
            f"{item.get('item_nm')} {float(item.get('share_pct') or 0):.1f}%"
            for item in product_mix[:3]
            if item.get("item_nm")
        ) or "상품별 매출 비중 데이터 없음"
        comparison = payload.get("comparison") or {}
        comparison_note = str(comparison.get("message") or "")
        sales_change_pct = comparison.get("sales_change_pct")
        usage_gap_pct = comparison.get("usage_ratio_gap_pct")
        comparison_line = ""
        if sales_change_pct is not None:
            comparison_line = f" 비교 기준 대비 전체 매출은 {float(sales_change_pct):+.1f}%입니다."
            if usage_gap_pct is not None:
                comparison_line += f" 티데이 사용 금액 비중 차이는 {float(usage_gap_pct):+.1f}%p입니다."

        text = (
            f"{payload.get('campaign_name') or '티데이'} 프로모션은 {period_text} 동안 진행됐습니다. "
            f"이 기간 전체 매출은 {self._format_currency(payload.get('promotion_period_sales'))}이고, "
            f"티데이 사용 금액은 {self._format_currency(payload.get('usage_amount'))}로 "
            f"전체 매출 대비 {float(payload.get('usage_ratio_pct') or 0):.1f}% 수준입니다. "
            f"상품별 매출 비중은 {top_mix} 순입니다."
            f"{(' ' + comparison_note) if comparison_note else ''}{comparison_line}"
        )
        evidence = [
            f"프로모션 기간: {period_text}",
            f"전체 매출: {self._format_currency(payload.get('promotion_period_sales'))}",
            f"티데이 사용 금액 비중: {float(payload.get('usage_ratio_pct') or 0):.1f}%",
            f"상품별 매출 비중 상위: {top_mix}",
        ]
        if comparison_note:
            evidence.append(comparison_note)
        actions = [
            "상위 상품 비중이 높은 SKU를 다음 티데이 프로모션 진열 우선순위에 반영하세요.",
            "티데이 사용 금액 비중이 낮으면 행사 기간 결제 안내 문구와 직원 안내 동선을 점검하세요.",
        ]
        return {
            "text": text,
            "evidence": evidence,
            "actions": actions,
            "answer": {
                "text": text,
                "evidence": evidence,
                "actions": actions,
            },
            "request_context": request_context.model_dump(),
            "agent_trace": {
                "keywords": ["티데이", "프로모션"],
                "intent": "tday_promotion_analysis",
                "relevant_tables": [
                    "raw_telecom_discount_policy",
                    "raw_daily_store_pay_way",
                    "raw_daily_store_item",
                ],
                "sql": None,
                "queried_period": {
                    "date_from": period.get("start_date"),
                    "date_to": period.get("end_date"),
                },
                "row_count": len(product_mix),
            },
        }

    async def list_prompts(
        self,
        domain: str = "sales",
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[SalesPrompt]:
        try:
            context_prompts = await self.repository.list_prompts(
                store_id=store_id, date_from=date_from, date_to=date_to
            )
        except Exception:
            context_prompts = self._build_prompt_fallbacks(store_id=store_id)
        if not context_prompts:
            context_prompts = self._build_prompt_fallbacks(store_id=store_id)

        if not self.ai_client:
            return [SalesPrompt(**prompt) for prompt in context_prompts]

        system_instruction = (
            self.prompt_settings_service.get_system_instruction(domain)
            if self.prompt_settings_service
            else None
        )
        ai_prompts = await self.ai_client.suggest_sales_prompts(
            store_id=store_id,
            domain=domain,
            context_prompts=context_prompts,
            system_instruction=system_instruction,
        )
        if not ai_prompts:
            return [SalesPrompt(**prompt) for prompt in context_prompts]
        return [SalesPrompt(**prompt) for prompt in ai_prompts]

    def _build_prompt_fallbacks(self, store_id: str | None = None) -> list[dict[str, str]]:
        store = store_id or "POC_001"
        return [
            {"label": "기간 매출 변동 요인", "category": "매출", "prompt": f"{store} 최근 매출 변동 요인을 설명해줘"},
            {"label": "채널 수익성 비교", "category": "채널", "prompt": f"{store} 배달/매장 채널 수익성을 비교해줘"},
            {"label": "결제수단 믹스 점검", "category": "결제", "prompt": f"{store} 결제수단별 매출 기여도를 알려줘"},
            {"label": "피크타임 운영 액션", "category": "운영", "prompt": f"{store} 피크시간 운영 개선 액션 3가지를 제안해줘"},
            {"label": "인기 상품 유지 전략", "category": "상품", "prompt": f"{store} 인기 상품 매출을 유지할 운영 전략을 제안해줘"},
            {"label": "재고 리스크 점검", "category": "생산", "prompt": f"{store} 품절 리스크가 높은 SKU를 점검해줘"},
            {"label": "할인 정책 효과", "category": "할인", "prompt": f"{store} 할인 정책이 매출과 마진에 미친 영향을 분석해줘"},
            {"label": "주간 코칭 리포트", "category": "코칭", "prompt": f"{store} 이번 주 운영 코칭 포인트를 요약해줘"},
            {"label": "주문 마감 체크", "category": "주문", "prompt": f"{store} 주문 마감 전 점검 항목을 정리해줘"},
            {"label": "다음주 우선 과제", "category": "운영", "prompt": f"{store} 다음 주 우선 실행 과제 3가지를 제안해줘"},
        ]

    def _build_query_system_instruction(self, domain: str) -> str:
        configured = (
            self.prompt_settings_service.get_system_instruction(domain)
            if self.prompt_settings_service
            else ""
        )
        configured = str(configured or "").strip()
        if configured:
            return f"{_FLOATING_CHAT_REQUIRED_INSTRUCTION}\n\n{configured}"
        return _FLOATING_CHAT_REQUIRED_INSTRUCTION

    @staticmethod
    def _default_follow_up_questions(prompt: str) -> list[str]:
        compact_prompt = prompt.strip() or "현재 질의"
        return [
            f"{compact_prompt}를 최근 7일 기준으로 다시 보여줘",
            "이 결과의 근거 테이블과 기간을 요약해줘",
            "지금 당장 실행할 액션 3가지를 우선순위로 알려줘",
        ]

    async def query(self, payload: SalesQueryRequest, actor_role: str = "store_owner") -> SalesQueryResponse:
        # 1. PII 마스킹 (전화번호 등)
        safe_prompt, pii_masked_fields = _mask_pii(payload.prompt)
        request_context = self._build_request_context(payload, safe_prompt)

        query_type = self._classify_query(safe_prompt)
        if query_type == "sensitive_request" and actor_role not in _HQ_ROLES:
            blocked_text = "요청하신 내용에는 민감정보가 포함되어 있어 현재 권한으로는 조회할 수 없습니다."
            blocked_evidence = [
                "민감정보 정책이 적용되었습니다.",
                "점포 손익·원가·이익률 정보는 본사 권한에서만 조회할 수 있습니다.",
            ]
            blocked_actions = [
                "민감정보를 제외한 운영 지표로 다시 질문해 주세요.",
                "본사 운영/기획 권한으로 재시도해 주세요.",
            ]
            response = SalesQueryResponse(
                text=blocked_text,
                evidence=blocked_evidence,
                actions=blocked_actions,
                answer=SalesAnswer(
                    text=blocked_text,
                    evidence=blocked_evidence,
                    actions=blocked_actions,
                ),
                request_context=request_context,
                agent_trace=SalesQueryAgentTrace(intent="sensitive", row_count=0),
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
        processing_route = "repository"
        if self._is_tday_prompt(safe_prompt):
            tday_payload = await self.repository.get_campaign_effect(
                store_id=payload.store_id,
                date_to=payload.business_date,
                prompt_hint=safe_prompt,
            )
            response = self._build_tday_query_response(
                payload=tday_payload,
                request_context=request_context,
            )
            processing_route = "repository_tday"
        else:
            ai_result: Optional[dict] = None
            if self.ai_client:
                system_instruction = self._build_query_system_instruction(payload.domain or "sales")
                ai_result = await self.ai_client.query_sales(
                    safe_prompt,
                    store_id=payload.store_id,
                    domain=payload.domain,
                    business_date=payload.business_date,
                    business_time=payload.business_time,
                    system_instruction=system_instruction,
                    page_context=payload.page_context,
                    card_context_key=payload.card_context_key,
                    store_name=payload.store_name,
                    user_role=payload.user_role,
                    conversation_history=(
                        [e.model_dump() for e in payload.conversation_history]
                        if payload.conversation_history
                        else None
                    ),
                )

            if ai_result is not None:
                response = ai_result
                processing_route = "ai_proxy"
            else:
                response = await self.repository.get_query_response(safe_prompt)

        if isinstance(response, dict):
            answer_payload = response.get("answer") if isinstance(response.get("answer"), dict) else None
            if answer_payload is None:
                answer_payload = {
                    "text": response.get("text", ""),
                    "evidence": response.get("evidence", []),
                    "actions": response.get("actions", []),
                    "follow_up_questions": response.get("follow_up_questions", []),
                }
            response["answer"] = answer_payload
            response.setdefault("text", answer_payload.get("text", ""))
            response.setdefault("evidence", answer_payload.get("evidence", []))
            response.setdefault("actions", answer_payload.get("actions", []))
            response.setdefault("follow_up_questions", answer_payload.get("follow_up_questions", []))
            response.setdefault("request_context", request_context.model_dump())
            response.setdefault(
                "agent_trace",
                {
                    "keywords": [],
                    "intent": None,
                    "relevant_tables": [],
                    "sql": None,
                    "queried_period": None,
                    "row_count": 0,
                },
            )

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
        ensured_actions, ensured_evidence = ensure_non_empty_actions_evidence(
            result.actions,
            result.evidence,
            fallback_action=f"{payload.store_id} 매장의 오늘 핵심 지표 1개를 선택해 즉시 점검하세요.",
            fallback_evidence=f"{payload.store_id} 매장 질의 결과를 기준으로 생성한 기본 근거입니다.",
        )
        result.actions = ensured_actions
        result.evidence = ensured_evidence
        if not result.follow_up_questions:
            result.follow_up_questions = self._default_follow_up_questions(safe_prompt)
        if result.request_context is None:
            result.request_context = request_context
        if result.answer is None:
            result.answer = SalesAnswer(
                text=result.text,
                evidence=result.evidence,
                actions=result.actions,
                follow_up_questions=result.follow_up_questions,
            )
        elif not result.answer.follow_up_questions:
            result.answer.follow_up_questions = result.follow_up_questions
        if result.agent_trace is None:
            result.agent_trace = SalesQueryAgentTrace(row_count=0)
        if self.audit_service:
            matched_query_id = result.agent_trace.matched_query_id if result.agent_trace else None
            match_score = result.agent_trace.match_score if result.agent_trace else None
            await self.audit_service.record(
                domain="sales",
                event_type="sales_query_completed",
                actor_role=actor_role,
                route=processing_route,
                outcome="success",
                message="매출 질의를 처리했습니다.",
                metadata={
                    "prompt": safe_prompt,
                    "query_type": query_type,
                    "comparison": False,
                    "matched_query_id": matched_query_id,
                    "match_score": match_score,
                },
            )
        return result

    async def enrich_sales_query_explainability(
        self,
        *,
        trace_id: str,
        store_id: str,
        prompt: str,
        base_text: str,
        base_actions: list[str],
        base_evidence: list[str],
    ) -> None:
        fallback_action = (
            f"{store_id} 매장 응답을 먼저 실행하고, 필요 시 동일 질의를 기준일시 유지 상태로 재조회하세요."
        )
        fallback_evidence = f"{store_id} 매장 응답 본문을 기반으로 보강 근거를 생성했습니다."
        if not self.ai_client:
            create_ready_payload(
                trace_id,
                actions=base_actions,
                evidence=base_evidence,
            )
            return

        enrichment_prompt = dedent(
            f"""
            다음 매출 응답을 보강하세요.
            - 액션은 점주가 즉시 수행 가능한 문장 2~3개
            - 근거는 수치/기간/비교 기준을 포함한 문장 2~3개
            - 반드시 JSON: {{"actions":["..."],"evidence":["..."]}}

            [점포] {store_id}
            [사용자 질문] {prompt}
            [현재 응답] {base_text}
            [기존 액션] {'; '.join(base_actions)}
            [기존 근거] {'; '.join(base_evidence)}
            """
        ).strip()

        try:
            enriched = await self.ai_client.query_sales(
                prompt=enrichment_prompt,
                store_id=store_id,
                domain="sales",
                system_instruction="JSON 형식으로 actions/evidence만 반환하세요.",
            )
            actions = base_actions
            evidence = base_evidence
            if isinstance(enriched, dict):
                candidate_actions = enriched.get("actions")
                candidate_evidence = enriched.get("evidence")
                actions, evidence = ensure_non_empty_actions_evidence(
                    candidate_actions if isinstance(candidate_actions, list) else base_actions,
                    candidate_evidence if isinstance(candidate_evidence, list) else base_evidence,
                    fallback_action=fallback_action,
                    fallback_evidence=fallback_evidence,
                )
            create_ready_payload(
                trace_id,
                actions=actions,
                evidence=evidence,
            )
        except Exception as exc:  # noqa: BLE001
            create_failed_payload(
                trace_id,
                actions=base_actions,
                evidence=base_evidence,
                error_reason=f"sales_enrichment_failed:{type(exc).__name__}",
            )

    async def get_insights(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> SalesInsightsResponse:
        if not store_id:
            raise ValueError("store_id is required")
        payload = await self.repository.get_insights(store_id=store_id, date_from=date_from, date_to=date_to)
        for key in ("peak_hours", "channel_mix", "payment_mix", "menu_mix", "campaign_seasonality"):
            section = payload.get(key)
            if not section:
                continue
            section["status"] = "active" if section.get("status") == "active" else "review"
        response = SalesInsightsResponse(**payload)
        insight_actions: list[str] = []
        insight_evidence: list[str] = []
        for section in (
            response.peak_hours,
            response.channel_mix,
            response.payment_mix,
            response.menu_mix,
            response.campaign_seasonality,
        ):
            if not section:
                continue
            insight_actions.extend(section.actions or [])
            for metric in section.metrics or []:
                insight_evidence.append(f"{section.title} - {metric.label}: {metric.value}")
        response.explainability = create_ready_payload(
            trace_id=f"sales-insights-{store_id or 'all'}",
            actions=insight_actions,
            evidence=insight_evidence,
        )
        return response

    async def get_summary(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> SalesSummaryResponse:
        payload = await self.repository.get_summary(store_id=store_id, date_from=date_from, date_to=date_to)
        payload["weekly_data"] = [
            {
                "day": self._sale_dt_to_day_label(item["sale_dt"]),
                "revenue": item["revenue"],
                "net_revenue": item["net_revenue"],
            }
            for item in payload.get("weekly_data", [])
        ]
        response = SalesSummaryResponse(**payload)
        response.explainability = create_ready_payload(
            trace_id=f"sales-summary-{store_id or 'all'}",
            actions=[
                "오늘 매출/순이익 변동이 큰 SKU 1개를 선정해 재고와 판매속도를 즉시 점검하세요.",
                "주간 데이터 기준으로 저성과 품목의 프로모션 또는 진열 위치를 조정하세요.",
            ],
            evidence=[
                f"오늘 매출: {response.today_revenue:,.0f}",
                f"오늘 순매출: {response.today_net_revenue:,.0f}",
                f"평균 마진율: {response.avg_margin_rate:.2f}",
            ],
        )
        return response

    async def get_dashboard_overview(
        self,
        store_id: str | None = None,
        business_date: str | None = None,
        reference_datetime=None,
    ) -> dict[str, int | list[dict[str, int | str]]]:
        return await self.repository.get_dashboard_overview(
            store_id=store_id,
            business_date=business_date,
            reference_datetime=reference_datetime,
        )

    async def get_campaign_effect(
        self,
        store_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> SalesCampaignEffectResponse:
        if not store_id:
            raise ValueError("store_id is required")
        payload = await self.repository.get_campaign_effect(
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
        periods = payload.get("periods", [])
        if not periods:
            raise LookupError("캠페인 효과 실데이터가 없습니다.")

        metrics = [
            {
                "label": "캠페인 코드",
                "value": str(payload.get("campaign_code") or "-"),
                "detail": str(payload.get("campaign_name") or "-"),
            },
            {
                "label": "할인 비용",
                "value": f"{int(float(payload.get('discount_cost') or 0)):,}원",
                "detail": "raw_daily_store_cpi_tmzon 기준",
            },
            {
                "label": "순 업리프트",
                "value": f"{int(float(payload.get('uplift_revenue') or 0)):,}원",
                "detail": "캠페인 전/중 매출 비교",
            },
            {
                "label": "ROI",
                "value": f"{float(payload.get('roi_pct') or 0):.1f}%",
                "detail": "((업리프트-비용)/비용) * 100",
            },
        ]

        before = next((period for period in periods if period.get("label") == "캠페인 전"), None)
        during = next((period for period in periods if period.get("label") == "캠페인 중"), None)
        after = next((period for period in periods if period.get("label") == "캠페인 후"), None)
        default_summary = (
            f"캠페인 전 {int(float((before or {}).get('revenue') or 0)):,}원 대비 "
            f"캠페인 중 {int(float((during or {}).get('revenue') or 0)):,}원을 기록했습니다. "
            f"캠페인 후 매출은 {int(float((after or {}).get('revenue') or 0)):,}원입니다."
        )
        default_actions = [
            "ROI가 낮으면 할인강도 대신 번들/세트 구성을 우선 검토하세요.",
            "캠페인 전/중/후 기간의 채널 믹스와 결제수단 반응을 함께 점검하세요.",
        ]
        summary, actions = await self._generate_campaign_effect_narrative(
            store_id=store_id,
            payload=payload,
            default_summary=default_summary,
            default_actions=default_actions,
        )
        return SalesCampaignEffectResponse(
            title="캠페인 효과 분석",
            summary=summary,
            metrics=metrics,
            actions=actions,
            explainability=create_ready_payload(
                trace_id=f"sales-campaign-{store_id or 'all'}",
                actions=actions,
                evidence=[f"{metric['label']}: {metric['value']}" for metric in metrics],
            ),
        )

    async def _apply_ai_summaries_to_insights(
        self,
        *,
        payload: dict,
        store_id: str,
        date_from: str | None,
        date_to: str | None,
    ) -> None:
        if not self.ai_client:
            logger.warning(
                "AI client is not configured for sales insights summary (store_id=%s). Using repository summaries.",
                store_id,
            )
            return

        sections: list[tuple[str, dict]] = []
        for key in ("peak_hours", "channel_mix", "payment_mix", "menu_mix"):
            section = payload.get(key)
            if isinstance(section, dict):
                sections.append((key, section))
        if not sections:
            logger.warning(
                "No insight sections found to summarize (store_id=%s, date_from=%s, date_to=%s).",
                store_id,
                date_from,
                date_to,
            )
            return

        sections_payload = {
            key: {
                "title": section.get("title") or "",
                "summary": section.get("summary") or "",
                "metrics": section.get("metrics") or [],
                "actions": section.get("actions") or [],
            }
            for key, section in sections
        }

        result = await self.ai_client.summarize_sales_insights(
            store_id=store_id,
            sections=sections_payload,
            date_from=date_from,
            date_to=date_to,
        )
        if not result:
            logger.warning(
                "AI returned empty sales insights summary (store_id=%s, date_from=%s, date_to=%s).",
                store_id,
                date_from,
                date_to,
            )
            return

        required_keys = {"peak_hours", "channel_mix", "payment_mix", "menu_mix"}
        missing = required_keys - set(k for k in result if result.get(k))
        if missing:
            logger.warning(
                "AI summary missing required keys (store_id=%s, missing=%s). Keeping repository summaries.",
                store_id,
                sorted(missing),
            )
            return

        for key, section in sections:
            if result.get(key):
                section["summary"] = result[key]

    async def _generate_campaign_effect_narrative(
        self,
        *,
        store_id: str,
        payload: dict,
        default_summary: str,
        default_actions: list[str],
    ) -> tuple[str, list[str]]:
        if not self.ai_client:
            logger.warning(
                "AI client is not configured for campaign narrative (store_id=%s). Using default narrative.",
                store_id,
            )
            return default_summary, default_actions
        result = await self.ai_client.generate_campaign_narrative(
            store_id=store_id,
            campaign_data=payload,
        )
        if not result:
            logger.warning(
                "AI returned empty campaign narrative (store_id=%s). Using default narrative.",
                store_id,
            )
            return default_summary, default_actions

        summary = str(result.get("summary") or "").strip()
        action1 = str(result.get("action1") or "").strip()
        action2 = str(result.get("action2") or "").strip()
        if not summary or not action1 or not action2:
            logger.warning(
                "AI campaign narrative missing fields (store_id=%s, summary=%s, action1=%s, action2=%s). Using default narrative.",
                store_id,
                bool(summary),
                bool(action1),
                bool(action2),
            )
            return default_summary, default_actions
        return summary, [action1, action2]

    def _classify_query(self, prompt: str) -> str:
        if any(keyword in prompt for keyword in _SENSITIVE_KEYWORDS):
            return "sensitive_request"
        if any(keyword in prompt for keyword in _DATA_LOOKUP_KEYWORDS):
            return "data_lookup"
        if any(keyword in prompt for keyword in _FAQ_KEYWORDS):
            return "faq"
        return "analysis"
