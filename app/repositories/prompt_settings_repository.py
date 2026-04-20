from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_PROMPT_SETTINGS: dict[str, Any] = {
    "version": 1,
    "updated_at": "2026-04-17T00:00:00",
    "updated_by": "system",
    "domains": {
        "production": {
            "quick_prompts": [
                "지금 당장 뭐부터 만들어야 해?",
                "지금 품절되면 얼마 날리는 거야?",
                "오늘 생산 타이밍을 언제로 잡는 게 좋아?",
            ],
            "system_instruction": (
                "당신은 생산관리 코치입니다. 점포별 재고/생산/판매 데이터 근거만 사용하고, "
                "즉시 실행할 수 있는 생산 액션을 우선 제시하세요."
            ),
            "query_prefix_template": "[점포:{store_id}] [생산관리] {question}",
        },
        "ordering": {
            "quick_prompts": [
                "지금 주문 어떤 안으로 하는 게 좋아?",
                "지금 주문 안 하면 어떻게 돼?",
                "지난번 주문이랑 비교해서 이번에 바뀐 게 있어?",
            ],
            "system_instruction": (
                "당신은 주문관리 코치입니다. 주문 누락 방지와 마감 대응을 최우선으로 답변하세요."
            ),
            "query_prefix_template": "[점포:{store_id}] [주문관리] {question}",
        },
        "sales": {
            "quick_prompts": [
                "오늘 장사 잘 된 거야 안 된 거야?",
                "이번 달 이 페이스면 얼마 남아?",
                "매출 올리려면 지금 당장 뭘 바꾸면 돼?",
            ],
            "system_instruction": (
                "당신은 손익분석 코치입니다. 실제 조회 데이터에 없는 수치를 추측하지 말고, "
                "근거와 실행 액션을 함께 제시하세요."
            ),
            "query_prefix_template": "[점포:{store_id}] [손익분석] {question}",
        },
    },
}


class PromptSettingsRepository:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path

    def get(self) -> dict[str, Any]:
        if not self.file_path.exists():
            initial = deepcopy(DEFAULT_PROMPT_SETTINGS)
            self._write(initial)
            return initial

        try:
            payload = json.loads(self.file_path.read_text(encoding="utf-8"))
            return self._merge_with_defaults(payload)
        except Exception:
            fallback = deepcopy(DEFAULT_PROMPT_SETTINGS)
            self._write(fallback)
            return fallback

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        merged = self._merge_with_defaults(payload)
        merged["updated_at"] = datetime.utcnow().isoformat()
        self._write(merged)
        return merged

    def _write(self, payload: dict[str, Any]) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _merge_with_defaults(payload: dict[str, Any] | None) -> dict[str, Any]:
        merged = deepcopy(DEFAULT_PROMPT_SETTINGS)
        if not isinstance(payload, dict):
            return merged

        for key in ("version", "updated_at", "updated_by"):
            if key in payload:
                merged[key] = payload[key]

        input_domains = payload.get("domains")
        if not isinstance(input_domains, dict):
            return merged

        for domain, default_domain_payload in merged["domains"].items():
            override = input_domains.get(domain)
            if not isinstance(override, dict):
                continue
            if isinstance(override.get("quick_prompts"), list):
                merged["domains"][domain]["quick_prompts"] = [
                    str(item).strip() for item in override["quick_prompts"] if str(item).strip()
                ][:5] or default_domain_payload["quick_prompts"]
            if isinstance(override.get("system_instruction"), str):
                merged["domains"][domain]["system_instruction"] = override[
                    "system_instruction"
                ].strip()
            if isinstance(override.get("query_prefix_template"), str):
                merged["domains"][domain]["query_prefix_template"] = override[
                    "query_prefix_template"
                ].strip()

        return merged
