from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/signals", tags=["signals"])


class SalesSignal(BaseModel):
    id: str
    title: str
    metric: str
    value: str
    change: str
    trend: str
    priority: str
    region: str
    insight: str


class SignalsResponse(BaseModel):
    items: list[SalesSignal]
    high_count: int


_STUB: list[SalesSignal] = [
    SalesSignal(id="sig-1", title="강남권 배달 급감", metric="배달 건수", value="312건", change="-21%", trend="down", priority="high", region="강남구·서초구", insight="배달앱 노출 순위 하락으로 점심 시간대 주문 집중 감소. 즉시 광고 입찰가 조정 권고."),
    SalesSignal(id="sig-2", title="T-day 재방문율 상승", metric="재방문율", value="34%", change="+12.4%", trend="up", priority="medium", region="전체", insight="T-day 참여 고객의 1주 내 재방문율이 증가. 리타겟팅 쿠폰 발송 효과 검토 필요."),
    SalesSignal(id="sig-3", title="마포권 커피 동반 구매 증가", metric="커피+도넛 세트 전환율", value="62%", change="+8.4%", trend="up", priority="medium", region="마포구", insight="묶음 메뉴 상시 편성 검토. 타 지역 적용 시 유사 효과 예상."),
    SalesSignal(id="sig-4", title="송파권 찬스 로스 증가", metric="찬스 로스", value="+18%", change="+18%", trend="down", priority="high", region="송파구", insight="생산 알림 미대응 매장 집중. SV 코칭 및 매장 방문 점검 필요."),
    SalesSignal(id="sig-5", title="앱 쿠폰 사용률 하락", metric="앱 쿠폰 사용률", value="22%", change="-16%p", trend="down", priority="medium", region="전체", insight="쿠폰 소진 이후 재발급 미진행. 마케터 쿠폰 캠페인 재설정 검토."),
    SalesSignal(id="sig-6", title="오전 홀 비중 안정", metric="홀 방문 비중", value="58%", change="+0%", trend="flat", priority="low", region="전체", insight="오전 홀 트래픽 안정적. 배달 비중 확대 여지 있음."),
]


@router.get("", response_model=SignalsResponse)
async def list_signals() -> SignalsResponse:
    return SignalsResponse(
        items=_STUB,
        high_count=sum(1 for s in _STUB if s.priority == "high"),
    )