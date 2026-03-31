from app.repositories.sales_repository import SalesRepository
from app.schemas.sales import SalesComparison, SalesPrompt, SalesQueryRequest, SalesQueryResponse


class SalesService:
    def __init__(self, repository: SalesRepository) -> None:
        self.repository = repository

    async def list_prompts(self) -> list[SalesPrompt]:
        prompts = await self.repository.list_prompts()
        return [SalesPrompt(**prompt) for prompt in prompts]

    async def query(self, payload: SalesQueryRequest) -> SalesQueryResponse:
        response = await self.repository.get_query_response(payload.prompt)
        comparison = None
        if any(keyword in payload.prompt for keyword in ["배달", "매출", "전년 동월", "채널"]):
            comparison = SalesComparison(
                store="강남역점",
                peer_group="유사 상권 10개 점포 평균",
                summary="강남역점은 배달 비중과 앱 전환율이 비교군보다 낮고, 오전 매장 방문 매출은 더 높습니다.",
                metrics=[
                    {"label": "배달 매출 비중", "store_value": "22%", "peer_value": "29%"},
                    {"label": "앱 쿠폰 사용률", "store_value": "22%", "peer_value": "31%"},
                    {"label": "오전 매장 방문 매출", "store_value": "58%", "peer_value": "49%"},
                ],
            )
        return SalesQueryResponse(comparison=comparison, **response)
