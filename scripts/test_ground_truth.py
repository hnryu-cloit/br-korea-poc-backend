"""
agent-ground-truth-questions.csv 기반 DB 쿼리 검증 스크립트 (L1)

CSV의 쿼리문을 실제 DB에서 실행하고 기댓값(응답)과 비교합니다.
결과는 콘솔 출력과 함께 docs/test_ground_truth_result.csv 로 저장됩니다.

사용법:
    python scripts/test_ground_truth.py
    python scripts/test_ground_truth.py --store_id POC_001
    python scripts/test_ground_truth.py --store_id POC_001 --date 2025-12-04
    python scripts/test_ground_truth.py --verbose
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from app.infrastructure.db.connection import get_database_engine

CSV_PATH = Path(__file__).resolve().parents[2] / "docs" / "agent-ground-truth-questions.csv"
OUTPUT_PATH = Path(__file__).resolve().parents[2] / "docs" / "test_ground_truth_result.csv"

PASS = "[PASS]"
FAIL = "[FAIL]"
APP  = "[APP]"   # 앱 레벨 후처리 필요 (DB 결과 != 최종 답변)
ERROR = "[ERROR]"

OUTPUT_FIELDS = [
    "no",
    "일치여부",       # O / X / APP / ERROR
    "지점코드",
    "조회일자",
    "질문",
    "키워드",
    "조회테이블",
    "기댓값(DB응답)",
    "실제DB값",
    "LLM예상답변",
    "비고",           # 오류/APP 사유
    "쿼리문",         # 열람용 (Excel에서 숨김 가능)
]

# 앱 레벨 처리가 필요한 쿼리 식별 문자열
_APP_LEVEL_MARKERS = [
    "/* 3 consecutive zero-sales hours rule applied in app */",
]


def _norm_val(val: Any) -> str:
    """단일 DB 값 → 정규화된 문자열"""
    if val is None:
        return ""
    if isinstance(val, Decimal):
        val = float(val)
    if isinstance(val, float):
        if val == int(val):
            return str(int(val))
        return str(round(val, 1))
    s = str(val).strip()
    # YYYYMMDD → YYYY-MM-DD (날짜 형식 통일)
    if re.match(r"^\d{8}$", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def _norm_expected(expected: str) -> str:
    """기댓값 정규화: 숫자의 천 단위 쉼표 제거"""
    parts = [p.strip() for p in expected.split(" | ")]
    return " | ".join(re.sub(r"(?<=\d),(?=\d)", "", p) for p in parts)


def normalize_result(rows: list[dict], sql: str) -> tuple[str, bool]:
    """
    DB 결과 → (정규화 문자열, is_app_level)
    - 다중 행 반환 또는 APP_LEVEL_MARKER 포함 시 is_app_level=True
    """
    is_app = any(marker in sql for marker in _APP_LEVEL_MARKERS)
    if is_app:
        return f"[rows={len(rows)}]", True

    if not rows:
        return "", False

    if len(rows) > 1:
        # 다중 행 = 앱 레벨 후처리 필요 (예: 주간 비교)
        return f"[rows={len(rows)}]", True

    values = [_norm_val(v) for v in rows[0].values()]
    if len(values) == 1:
        return values[0], False
    return " | ".join(values), False


def run_tests(store_id_filter: str | None, date_filter: str | None, verbose: bool) -> None:
    engine = get_database_engine()
    if engine is None:
        print("DB connection failed: psycopg driver not found.")
        sys.exit(1)

    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}")
        sys.exit(1)

    with open(CSV_PATH, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    if store_id_filter:
        rows = [r for r in rows if r["지점코드(혹은 지점명)"].startswith(store_id_filter)]
    if date_filter:
        rows = [r for r in rows if r["조회 일자(가상)"] == date_filter]

    if not rows:
        print("No matching rows.")
        return

    total = len(rows)
    passed = failed = errored = app_level = 0
    result_records: list[dict] = []

    print(f"\n{'='*70}")
    print(f"  Test count: {total}")
    if store_id_filter:
        print(f"  store_id filter: {store_id_filter}")
    if date_filter:
        print(f"  date filter: {date_filter}")
    print(f"{'='*70}\n")

    with engine.connect() as conn:
        for i, row in enumerate(rows, 1):
            store    = row["지점코드(혹은 지점명)"]
            date     = row["조회 일자(가상)"]
            question = row["질문"]
            keyword  = row.get("키워드", "")
            table    = row["조회 대상 테이블"]
            query    = row["쿼리문"]
            expected = str(row["응답"]).strip()
            llm_ans  = row.get("LLM이 점주에게 보여줄 답변", "")
            expected_norm = _norm_expected(expected)

            label = f"[{i:02d}] {store} | {date} | {question}"
            record: dict = {
                "no": i,
                "일치여부": "",
                "지점코드": store,
                "조회일자": date,
                "질문": question,
                "키워드": keyword,
                "조회테이블": table,
                "기댓값(DB응답)": expected,
                "실제DB값": "",
                "LLM예상답변": llm_ans,
                "비고": "",
                "쿼리문": query,
            }

            try:
                db_rows = [dict(r) for r in conn.execute(text(query)).mappings().all()]
                actual, is_app = normalize_result(db_rows, query)

                if is_app:
                    status = APP
                    app_level += 1
                    record["일치여부"] = "APP"
                    record["비고"] = "앱 레벨 후처리 필요"
                elif actual == expected_norm:
                    status = PASS
                    passed += 1
                    record["일치여부"] = "O"
                else:
                    status = FAIL
                    failed += 1
                    record["일치여부"] = "X"

                record["실제DB값"] = actual

                print(f"{status} {label}")
                if verbose or status == FAIL:
                    print(f"       table    : {table}")
                    print(f"       expected : {expected_norm}")
                    print(f"       actual   : {actual}")
                    if status == FAIL:
                        print(f"       query    : {query}")
                    print()

            except Exception as e:
                errored += 1
                record["일치여부"] = "ERROR"
                record["비고"] = str(e)
                print(f"{ERROR} {label}")
                print(f"       error : {e}")
                print()

            result_records.append(record)

    # 요약 행 추가
    result_records.append({
        "no": "",
        "일치여부": f"PASS={passed} / FAIL={failed} / APP={app_level} / ERROR={errored}",
        "지점코드": "[요약]",
        "조회일자": "",
        "질문": f"전체 {total}건",
        "키워드": "", "조회테이블": "",
        "기댓값(DB응답)": "", "실제DB값": "",
        "LLM예상답변": "", "비고": "", "쿼리문": "",
    })

    with open(OUTPUT_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(result_records)

    print(f"\n{'='*70}")
    print(f"  Result: total {total} | PASS {passed} | FAIL {failed} | APP {app_level} | ERROR {errored}")
    print(f"  Saved : {OUTPUT_PATH}")
    print(f"{'='*70}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="ground-truth CSV query validation (L1)")
    parser.add_argument("--store_id", help="store_id filter (e.g. POC_001)")
    parser.add_argument("--date", help="date filter (e.g. 2025-12-04)")
    parser.add_argument("--verbose", action="store_true", help="print details for PASS items too")
    args = parser.parse_args()

    run_tests(args.store_id, args.date, args.verbose)


if __name__ == "__main__":
    main()
