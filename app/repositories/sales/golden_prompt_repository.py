from __future__ import annotations

import csv
from pathlib import Path


_DOMAIN_AGENT_MAP = {
    "production": "생산 관리",
    "ordering": "주문 관리",
    "sales": "매출 관리",
}


def _golden_queries_csv_path() -> Path:
    return Path(__file__).resolve().parents[3] / "docs" / "golden-queries.csv"


def list_golden_prompts(domain: str | None = None) -> list[dict[str, str]]:
    path = _golden_queries_csv_path()
    if not path.exists():
        return []

    if domain is None:
        target_agent: str | None = None
    else:
        target_agent = _DOMAIN_AGENT_MAP.get(domain)
        if target_agent is None:
            return []

    prompts: list[dict[str, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            label = (row.get("표시라벨") or "").strip()
            if not label:
                continue
            agent = (row.get("에이전트") or "").strip()
            if target_agent and agent != target_agent:
                continue
            prompt_text = (row.get("질문") or "").strip()
            if not prompt_text:
                continue
            prompts.append(
                {
                    "label": label,
                    "category": agent or "골든쿼리",
                    "prompt": prompt_text,
                }
            )
    return prompts
