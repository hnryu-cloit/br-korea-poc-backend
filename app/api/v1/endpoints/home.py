from fastapi import APIRouter, Depends
from app.core.deps import get_production_service
from app.services.production_service import ProductionService

router = APIRouter(prefix="/home", tags=["home"])

@router.post("/overview")
async def get_home_summary_post(
    store_id: str = "POC_030",      # 시연용 기본값
    target_date: str = "2026-04-06", # 시연용 기본값
    service: ProductionService = Depends(get_production_service),
):
    """
    [FE 연동] 홈 대시보드 통합 요약 정보 조회 (POST)
    """
    data = await service.get_home_overview(store_id=store_id, target_date=target_date)
    
    overview = data.get("overview", {})
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

@router.get("/overview")
async def get_home_summary_get(
    store_id: str = Query(default="POC_030"),
    target_date: str = Query(default="2026-04-06", alias="business_date"),
    service: ProductionService = Depends(get_production_service),
):
    """
    [FE 연동] 홈 대시보드 통합 요약 정보 조회 (GET)
    """
    data = await service.get_home_overview(store_id=store_id, target_date=target_date)
    
    overview = data.get("overview", {})
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
