from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["health"])


@router.get("", response_model=dict[str, str])
async def health() -> dict[str, str]:
    return {"status": "ok"}