from typing import Optional
from fastapi import APIRouter, Depends, Query
from app.core.deps import get_production_service
from app.services.production_service import ProductionService

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

@router.get("/overview")
async def get_dashboard_overview(
    store_id: str = Query(default="POC_030"),
    target_date: str = Query(default="2026-04-06"),
    service: ProductionService = Depends(get_production_service),
):
    """
    [FE 연동] 홈 대시보드 통합 요약 정보 (Priority Actions, Stats, Cards, Insights 모두 포함)
    """
    data = await service.get_home_overview(store_id=store_id, target_date=target_date)
    
    # 프론트엔드 스키마(단일 객체 형태)에 맞게 Flatten
    overview = data.get("overview", {})
    # AI 서버는 { "cards": [...] } 로 주지만 프론트엔드는 바로 배열을 원할 수 있음
    cards_obj = data.get("cards", {})
    cards_list = cards_obj.get("cards", []) if isinstance(cards_obj, dict) else []
    
    # 프론트엔드 타입에 맞게 변환 (highlights와 prompts를 string 배열로 변경)
    for card in cards_list:
        if "highlights" in card:
            card["highlights"] = [
                f"[{h.get('tone', 'info')}] {h.get('title', '')} - {h.get('description', '')}" if isinstance(h, dict) else str(h) 
                for h in card["highlights"]
            ]
        if "prompts" in card:
            card["prompts"] = [
                p.get("label", "") if isinstance(p, dict) else str(p)
                for p in card["prompts"]
            ]
            
    insights_obj = data.get("insights", {})
    insights_list = insights_obj.get("insights", []) if isinstance(insights_obj, dict) else []
    
    return {
        "updated_at": overview.get("updated_at", ""),
        "priority_actions": overview.get("priority_actions", []),
        "stats": overview.get("stats", []),
        "cards": cards_list,
        "insights": insights_list,
        "notifications": []
    }

@router.get("/cards")
async def get_dashboard_cards(
    store_id: str = Query(default="POC_030"),
    target_date: str = Query(default="2026-04-06"),
    service: ProductionService = Depends(get_production_service),
):
    """
    [FE 연동] 홈 대시보드 중단 카드 정보 (3대 에이전트 상세)
    """
    data = await service.get_home_overview(store_id=store_id, target_date=target_date)
    return data.get("cards")

@router.get("/insights")
async def get_dashboard_insights(
    store_id: str = Query(default="POC_030"),
    target_date: str = Query(default="2026-04-06"),
    service: ProductionService = Depends(get_production_service),
):
    """
    [FE 연동] 홈 대시보드 하단 인사이트 정보
    """
    data = await service.get_home_overview(store_id=store_id, target_date=target_date)
    return data.get("insights")
