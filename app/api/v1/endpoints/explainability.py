from __future__ import annotations

from fastapi import APIRouter

from app.schemas.explainability import ExplainabilityPayload
from app.services.explainability_service import get_payload

router = APIRouter(prefix="/explainability", tags=["explainability"])


@router.get("/{trace_id}", response_model=ExplainabilityPayload)
async def get_explainability(trace_id: str) -> ExplainabilityPayload:
    return get_payload(trace_id)
