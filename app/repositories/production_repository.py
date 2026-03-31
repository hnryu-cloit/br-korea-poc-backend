PRODUCTION_ITEMS = [
    {
        "sku_id": "sku-1",
        "name": "스트로베리 필드",
        "current": 24,
        "forecast": 3,
        "status": "danger",
        "depletion_time": "15:05",
        "recommended": 40,
        "prod1": "08:10 / 52개",
        "prod2": "14:20 / 40개",
    },
    {
        "sku_id": "sku-2",
        "name": "올드패션",
        "current": 18,
        "forecast": 6,
        "status": "danger",
        "depletion_time": "15:22",
        "recommended": 36,
        "prod1": "08:00 / 48개",
        "prod2": "14:10 / 36개",
    },
    {
        "sku_id": "sku-3",
        "name": "크림 필드",
        "current": 12,
        "forecast": 4,
        "status": "danger",
        "depletion_time": "15:18",
        "recommended": 32,
        "prod1": "08:30 / 44개",
        "prod2": "14:30 / 32개",
    },
    {
        "sku_id": "sku-4",
        "name": "글레이즈드",
        "current": 42,
        "forecast": 22,
        "status": "safe",
        "depletion_time": "-",
        "recommended": 0,
        "prod1": "08:05 / 60개",
        "prod2": "14:00 / 48개",
    },
]


class ProductionRepository:
    saved_registrations: list[dict] = []

    async def list_items(self) -> list[dict]:
        return PRODUCTION_ITEMS

    async def save_registration(self, payload: dict) -> dict:
        self.saved_registrations.append(payload)
        return payload
