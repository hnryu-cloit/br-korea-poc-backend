from __future__ import annotations

_STORE_MART_KEYS: dict[tuple[str, str], str] = {
    ("analytics", "daily_table"): "analytics_daily",
    ("analytics", "deadline_table"): "analytics_deadline",
    ("analytics", "hourly_table"): "analytics_hourly",
    ("production", "inventory_status_table"): "production_inventory_status",
    ("production", "waste_monthly_table"): "production_waste_monthly",
}


def get_store_mart_table(store_id: str | None, domain: str, key: str) -> str | None:
    if not store_id:
        return None
    suffix = _STORE_MART_KEYS.get((domain, key))
    if not suffix:
        return None
    return f"mart_{store_id.lower()}_{suffix}"
