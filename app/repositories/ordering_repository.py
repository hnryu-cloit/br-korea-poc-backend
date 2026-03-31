ORDER_OPTIONS = [
    {
        "id": "opt-a",
        "label": "지난주 같은 요일",
        "basis": "3월 24일(월) 기준",
        "description": "가장 최근 데이터 기준이에요. 오늘 날씨와 비슷해 무난한 선택입니다.",
        "recommended": True,
        "items": [
            {"name": "스트로베리 필드", "qty": 120, "note": "캠페인으로 8% 더 팔림"},
            {"name": "글레이즈드", "qty": 96, "note": None},
            {"name": "올드패션", "qty": 80, "note": None},
            {"name": "초코 트위스트", "qty": 72, "note": None},
        ],
        "notes": ["지난주 캠페인으로 도넛 주문이 좀 많았어요", "오후 배달은 조금 줄었어요"],
    },
    {
        "id": "opt-b",
        "label": "2주 전 같은 요일",
        "basis": "3월 17일(월) 기준",
        "description": "행사나 이벤트 영향이 없는 평상시 기준이에요. 넉넉하지 않지만 안전해요.",
        "recommended": False,
        "items": [
            {"name": "스트로베리 필드", "qty": 108, "note": None},
            {"name": "글레이즈드", "qty": 88, "note": None},
            {"name": "올드패션", "qty": 76, "note": None},
            {"name": "초코 트위스트", "qty": 68, "note": None},
        ],
        "notes": ["행사 없었던 날 기준이라 안정적이에요", "재고가 남을 위험이 가장 낮아요"],
    },
    {
        "id": "opt-c",
        "label": "지난달 같은 요일",
        "basis": "2월 24일(월) 기준",
        "description": "한 달 전 같은 요일 기준이에요. 배달 주문이 지금보다 많았던 시기예요.",
        "recommended": False,
        "items": [
            {"name": "스트로베리 필드", "qty": 132, "note": None},
            {"name": "글레이즈드", "qty": 104, "note": None},
            {"name": "올드패션", "qty": 88, "note": None},
            {"name": "초코 트위스트", "qty": 80, "note": None},
        ],
        "notes": ["배달 주문이 지금보다 12% 더 많았어요", "커피 같이 구매가 많았어요"],
    },
]


class OrderingRepository:
    saved_selections: list[dict] = []

    async def list_options(self) -> list[dict]:
        return ORDER_OPTIONS

    async def get_notification_context(self, notification_id: int) -> dict:
        return {
            "notification_id": notification_id,
            "target_path": "/ordering",
            "focus_option_id": "opt-a",
            "message": "주문 추천 3개 옵션이 준비되었습니다. 추천 옵션부터 확인하세요.",
        }

    async def save_selection(self, payload: dict) -> dict:
        self.saved_selections.append(payload)
        return payload
