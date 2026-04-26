from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from app.core.ttl_cache import TTLMemoryCache
from app.schemas.explainability import ExplainabilityPayload

_CACHE = TTLMemoryCache(max_size=4096)
_TTL_SECONDS = 600


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_non_empty_actions_evidence(
    actions: list[str] | None,
    evidence: list[str] | None,
    *,
    fallback_action: str = "오늘 운영 우선순위를 확인하고 즉시 실행 항목 1개를 먼저 처리하세요.",
    fallback_evidence: str = "현재 응답 기준 데이터 요약을 근거로 도출한 권장 조치입니다.",
) -> tuple[list[str], list[str]]:
    normalized_actions = [str(item).strip() for item in (actions or []) if str(item).strip()]
    normalized_evidence = [str(item).strip() for item in (evidence or []) if str(item).strip()]
    if not normalized_actions:
        normalized_actions = [fallback_action]
    if not normalized_evidence:
        normalized_evidence = [fallback_evidence]
    return normalized_actions, normalized_evidence


def build_trace_id(prefix: str = "exp") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def save_payload(trace_id: str, payload: ExplainabilityPayload, ttl_seconds: int = _TTL_SECONDS) -> None:
    _CACHE.set(trace_id, payload.model_dump(), ttl_sec=ttl_seconds)


def create_pending_payload(
    trace_id: str,
    actions: list[str] | None = None,
    evidence: list[str] | None = None,
) -> ExplainabilityPayload:
    norm_actions, norm_evidence = ensure_non_empty_actions_evidence(actions, evidence)
    payload = ExplainabilityPayload(
        status="pending",
        trace_id=trace_id,
        actions=norm_actions,
        evidence=norm_evidence,
        updated_at=_now_str(),
    )
    save_payload(trace_id, payload)
    return payload


def create_ready_payload(
    trace_id: str,
    *,
    actions: list[str] | None = None,
    evidence: list[str] | None = None,
) -> ExplainabilityPayload:
    norm_actions, norm_evidence = ensure_non_empty_actions_evidence(actions, evidence)
    cached = _CACHE.get(trace_id)
    if isinstance(cached, dict):
        cached_actions = cached.get("actions") if isinstance(cached.get("actions"), list) else []
        cached_evidence = cached.get("evidence") if isinstance(cached.get("evidence"), list) else []
        if (
            cached.get("status") == "ready"
            and cached_actions == norm_actions
            and cached_evidence == norm_evidence
        ):
            return ExplainabilityPayload(**cached)
    payload = ExplainabilityPayload(
        status="ready",
        trace_id=trace_id,
        actions=norm_actions,
        evidence=norm_evidence,
        updated_at=_now_str(),
    )
    save_payload(trace_id, payload)
    return payload


def create_failed_payload(
    trace_id: str,
    *,
    actions: list[str] | None = None,
    evidence: list[str] | None = None,
    error_reason: str | None = None,
) -> ExplainabilityPayload:
    norm_actions, norm_evidence = ensure_non_empty_actions_evidence(actions, evidence)
    payload = ExplainabilityPayload(
        status="failed",
        trace_id=trace_id,
        actions=norm_actions,
        evidence=norm_evidence,
        updated_at=_now_str(),
        error_reason=error_reason or "enrichment_failed",
    )
    save_payload(trace_id, payload)
    return payload


def get_payload(trace_id: str) -> ExplainabilityPayload:
    cached = _CACHE.get(trace_id)
    if isinstance(cached, dict):
        return ExplainabilityPayload(**cached)
    return create_failed_payload(
        trace_id,
        actions=["화면을 새로고침한 뒤 동일 조건으로 다시 조회하세요."],
        evidence=["보강 캐시가 만료되어 최신 근거를 다시 생성해야 합니다."],
        error_reason="expired",
    )


def payload_from_dict(data: dict[str, Any] | None, trace_id: str) -> ExplainabilityPayload:
    actions = None
    evidence = None
    if isinstance(data, dict):
        actions = data.get("actions") if isinstance(data.get("actions"), list) else None
        evidence = data.get("evidence") if isinstance(data.get("evidence"), list) else None
    return create_pending_payload(trace_id, actions=actions, evidence=evidence)
