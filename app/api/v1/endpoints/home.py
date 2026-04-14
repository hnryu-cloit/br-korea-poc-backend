from fastapi import APIRouter, Depends
from app.core.deps import get_production_service
from app.services.production_service import ProductionService

router = APIRouter(prefix="/home", tags=["home"])

@router.post("/overview")
async def get_home_summary(
    store_id: str = "POC_030",      # 시연용 기본값
    target_date: str = "2026-04-06", # 시연용 기본값
    service: ProductionService = Depends(get_production_service),
):
    """
    [FE 연동] 홈 대시보드 통합 요약 정보 조회
    내부적으로 AI 서버를 호출하여 3대 에이전트의 핵심 인사이트를 반환합니다.
    """
    return await service.get_home_overview(store_id=store_id, target_date=target_date)
