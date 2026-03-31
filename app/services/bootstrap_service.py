from app.repositories.bootstrap_repository import BootstrapRepository


class BootstrapService:
    def __init__(self, repository: BootstrapRepository) -> None:
        self.repository = repository

    async def get_bootstrap(self) -> dict:
        return await self.repository.get_bootstrap()

    async def get_channel_drafts(self) -> dict:
        data = await self.get_bootstrap()
        drafts = data.get("channelDrafts")
        if drafts:
            return drafts

        return {
            "push": {
                "format": "push",
                "headline": "주문 마감 20분 전입니다",
                "body": "추천 옵션 3개와 근거를 확인하고 오늘 발주를 마무리하세요.",
                "cta": "주문 추천 보기",
            },
            "kakao": {
                "format": "kakao",
                "headline": "매출 분석 요약",
                "body": "배달 주문 감소 원인과 실행 액션 2개를 확인하세요.",
                "cta": "분석 결과 보기",
            },
        }

    async def get_review_checklist(self) -> list[dict]:
        data = await self.get_bootstrap()
        review_queue = data.get("reviewQueue")
        if review_queue:
            return review_queue

        return [
            {
                "stage": "주문 추천 검증",
                "owner": "POC 운영 담당",
                "status": "in_review",
                "notes": "추천 옵션 3안과 점주 최종 선택 저장 흐름 점검",
            },
            {
                "stage": "매출 질의 검증",
                "owner": "데이터 분석 담당",
                "status": "ready",
                "notes": "추천 질문 세트와 매장 맞춤 비교 응답 확인",
            },
        ]
