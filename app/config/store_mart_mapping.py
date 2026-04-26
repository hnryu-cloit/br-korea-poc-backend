from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path


_LEGACY_STORE_MART_KEYS: dict[tuple[str, str], str] = {
    ("analytics", "daily_table"): "analytics_daily",
    ("analytics", "deadline_table"): "analytics_deadline",
    ("analytics", "hourly_table"): "analytics_hourly",
    ("ordering", "options_join_table"): "ordering_join",
    ("production", "inventory_status_table"): "production_inventory_status",
    ("production", "waste_daily_table"): "production_waste_daily",
    ("production", "waste_monthly_table"): "production_waste_monthly",
}
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _mapping_path() -> Path:
    return Path(__file__).resolve().with_name("store_mart_mappings.json")


@lru_cache(maxsize=1)
def load_store_mart_mappings() -> dict[str, dict[str, dict[str, str]]]:
    path = _mapping_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}

    normalized: dict[str, dict[str, dict[str, str]]] = {}
    for store_id, family_mapping in raw.items():
        if not isinstance(store_id, str) or not isinstance(family_mapping, dict):
            continue
        store_key = store_id.strip().upper()
        normalized_families: dict[str, dict[str, str]] = {}
        for family_name, mapping in family_mapping.items():
            if not isinstance(family_name, str) or not isinstance(mapping, dict):
                continue
            table_mapping: dict[str, str] = {}
            for key, value in mapping.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    continue
                identifier = value.strip()
                if not _VALID_IDENTIFIER.match(identifier):
                    continue
                table_mapping[key.strip()] = identifier
            if table_mapping:
                normalized_families[family_name.strip()] = table_mapping
        if normalized_families:
            normalized[store_key] = normalized_families
    return normalized


def get_store_mart_family(store_id: str | None, domain: str) -> dict[str, str]:
    if not store_id or not domain:
        return {}
    return dict(load_store_mart_mappings().get(store_id.strip().upper(), {}).get(domain.strip(), {}))


def has_store_mart_mapping(store_id: str | None, domain: str, key: str) -> bool:
    if not store_id or not domain or not key:
        return False
    return key.strip() in get_store_mart_family(store_id, domain)


def get_store_mart_table(store_id: str | None, domain: str, key: str) -> str | None:
    if not store_id or not domain or not key:
        return None

    mapped = get_store_mart_family(store_id, domain).get(key.strip())
    if mapped:
        return mapped

    suffix = _LEGACY_STORE_MART_KEYS.get((domain, key))
    if not suffix:
        return None
    return f"mart_{store_id.lower()}_{suffix}"
