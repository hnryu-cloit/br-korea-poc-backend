from __future__ import annotations

import re
from textwrap import dedent
from typing import Optional

from app.repositories.sales_repository import SalesRepository
from app.schemas.sales import (
    SalesCampaignEffectResponse,
    SalesInsightsResponse,
    SalesPrompt,
    SalesQueryRequest,
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
        processing_route = "repository"
        if self.ai_client:
            ai_result = await self.ai_client.query_sales(safe_prompt, store_id=payload.store_id)

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
        ensured_actions, ensured_evidence = ensure_non_empty_actions_evidence(
            result.actions,
            result.evidence,
            fallback_action=f"{payload.store_id} 매장의 오늘 핵심 지표 1개를 선택해 즉시 점검하세요.",
            fallback_evidence=f"{payload.store_id} 매장 질의 결과를 기준으로 생성한 기본 근거입니다.",
        )
        result.actions = ensured_actions
        result.evidence = ensured_evidence
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
        await self._apply_ai_summaries_to_insights(
            payload=payload,
            store_id=store_id,
            date_from=date_from,
            date_to=date_to,
        )
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
            raise RuntimeError("매출 인사이트 요약용 AI 클라이언트가 설정되지 않았습니다.")

        sections: list[tuple[str, dict]] = []
        for key in ("peak_hours", "channel_mix", "payment_mix", "menu_mix"):
            section = payload.get(key)
            if isinstance(section, dict):
                sections.append((key, section))
        if not sections:
            raise LookupError("매출 인사이트 실데이터가 부족합니다.")

        section_lines: list[str] = []
        for key, section in sections:
            metrics = section.get("metrics") or []
            metric_line = ", ".join(
                f"{item.get('label')}={item.get('value')}" for item in metrics if isinstance(item, dict)
            )
            actions = section.get("actions") or []
            action_line = "; ".join(str(item) for item in actions)
            section_lines.append(
                f"- {key}: title={section.get('title')} | summary={section.get('summary')} | "
                f"metrics=[{metric_line}] | actions=[{action_line}]"
            )

        prompt = dedent(
            f"""
            아래 매출 인사이트 섹션을 기반으로, 각 섹션 summary를 1문장씩 다시 작성하세요.
            출력 형식:
            peak_hours::...
            channel_mix::...
            payment_mix::...
            menu_mix::...

            규칙:
            1) 입력 데이터 외 새로운 수치/사실 생성 금지
            2) 섹션당 1문장, 한국어, 실무 실행 관점
            3) 형식이 다르면 실패로 간주

            점포: {store_id}
            기간: {date_from or "미지정"} ~ {date_to or "미지정"}
            섹션 데이터:
            {chr(10).join(section_lines)}
            """
        ).strip()
        result = await self.ai_client.query_sales(
            prompt=prompt,
            store_id=store_id,
            domain="sales",
            system_instruction="입력 데이터에 근거한 요약만 작성하고 지정된 포맷을 정확히 지키세요.",
        )
        if not result:
            raise RuntimeError("AI 섹션 요약 생성에 실패했습니다.")
        text = str(result.get("text") or "").strip()
        parsed: dict[str, str] = {}
        for line in text.splitlines():
            if "::" not in line:
                continue
            key, value = line.split("::", 1)
            key = key.strip()
            value = value.strip()
            if key and value:
                parsed[key] = value
        required_keys = {"peak_hours", "channel_mix", "payment_mix", "menu_mix"}
        if not required_keys.issubset(parsed.keys()):
            raise RuntimeError("AI 섹션 요약 포맷이 올바르지 않습니다.")

        for key, section in sections:
            section["summary"] = parsed.get(key, section.get("summary", ""))

    async def _generate_campaign_effect_narrative(
        self,
        *,
        store_id: str,
        payload: dict,
        default_summary: str,
        default_actions: list[str],
    ) -> tuple[str, list[str]]:
        if not self.ai_client:
            raise RuntimeError("캠페인 인사이트 생성용 AI 클라이언트가 설정되지 않았습니다.")

        prompt = dedent(
            f"""
            아래 캠페인 실데이터를 바탕으로 summary 1문장과 action 2개를 생성하세요.
            출력 형식:
            summary::...
            action1::...
            action2::...

            규칙:
            1) 입력 수치만 사용하고 임의 생성 금지
            2) action은 실행형 문장
            3) 지정된 키를 모두 포함

            점포: {store_id}
            campaign_code: {payload.get("campaign_code")}
            campaign_name: {payload.get("campaign_name")}
            discount_cost: {payload.get("discount_cost")}
            uplift_revenue: {payload.get("uplift_revenue")}
            roi_pct: {payload.get("roi_pct")}
            periods: {payload.get("periods")}
            """
        ).strip()
        result = await self.ai_client.query_sales(
            prompt=prompt,
            store_id=store_id,
            domain="sales",
            system_instruction="입력 실데이터 근거로만 생성하고 형식을 정확히 지키세요.",
        )
        if not result:
            raise RuntimeError("AI 캠페인 서술 생성에 실패했습니다.")
        text = str(result.get("text") or "").strip()
        parsed: dict[str, str] = {}
        for line in text.splitlines():
            if "::" not in line:
                continue
            key, value = line.split("::", 1)
            key = key.strip()
            value = value.strip()
            if key and value:
                parsed[key] = value
        summary = parsed.get("summary", "").strip()
        action1 = parsed.get("action1", "").strip()
        action2 = parsed.get("action2", "").strip()
        if not summary or not action1 or not action2:
            raise RuntimeError("AI 캠페인 서술 포맷이 올바르지 않습니다.")
        return summary, [action1, action2]

    def _classify_query(self, prompt: str) -> str:
        if any(keyword in prompt for keyword in _SENSITIVE_KEYWORDS):
            return "sensitive_request"
        if any(keyword in prompt for keyword in _DATA_LOOKUP_KEYWORDS):
            return "data_lookup"
        if any(keyword in prompt for keyword in _FAQ_KEYWORDS):
            return "faq"
        return "analysis"
