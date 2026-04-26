"""서울 열린데이터광장 OpenAPI에서 상권분석 분기 데이터를 받아 raw_seoul_market_* 테이블에 적재합니다.

사용 예시:
    SEOUL_OPENAPI_KEY=xxxx python scripts/load_seoul_market_from_api.py --year 2024 --quarters 1,2,3,4

데이터셋:
    - VwsmTrdarSelngQ : 우리마을가게 상권분석서비스(상권-추정매출, 분기) → raw_seoul_market_sales
    - VwsmTrdarFlpopW : 우리마을가게 상권분석서비스(상권-유동인구, 분기) → raw_seoul_market_floating_population

OpenAPI 호출 형식:
    http://openapi.seoul.go.kr:8088/{KEY}/json/{SERVICE}/{START}/{END}/{STDR_YYQUARTER}/

페이지네이션은 START_INDEX/END_INDEX 1000행 단위 반복.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import urllib.request
import urllib.parse
import json

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine

OPENAPI_BASE = "http://openapi.seoul.go.kr:8088"
PAGE_SIZE = 1000
MAX_RETRY = 3
RETRY_BACKOFF_SEC = 2.0

SALES_INSERT_SQL = text(
    """
    INSERT INTO raw_seoul_market_sales(
        base_year, base_quarter, area_type_code, area_type_name, area_code, area_name,
        service_code, service_name, monthly_sales_amount, monthly_sales_count,
        weekend_sales_ratio, weekday_sales_amount, weekend_sales_amount,
        monday_sales_amount, tuesday_sales_amount, wednesday_sales_amount, thursday_sales_amount,
        friday_sales_amount, saturday_sales_amount, sunday_sales_amount,
        time_00_06_sales_amount, time_06_11_sales_amount, time_11_14_sales_amount,
        time_14_17_sales_amount, time_17_21_sales_amount, time_21_24_sales_amount,
        male_sales_amount, female_sales_amount,
        age10_sales_amount, age20_sales_amount, age30_sales_amount,
        age40_sales_amount, age50_sales_amount, age60_plus_sales_amount,
        loaded_at, source_file
    ) VALUES (
        :base_year, :base_quarter, :area_type_code, :area_type_name, :area_code, :area_name,
        :service_code, :service_name, :monthly_sales_amount, :monthly_sales_count,
        :weekend_sales_ratio, :weekday_sales_amount, :weekend_sales_amount,
        :monday_sales_amount, :tuesday_sales_amount, :wednesday_sales_amount, :thursday_sales_amount,
        :friday_sales_amount, :saturday_sales_amount, :sunday_sales_amount,
        :time_00_06_sales_amount, :time_06_11_sales_amount, :time_11_14_sales_amount,
        :time_14_17_sales_amount, :time_17_21_sales_amount, :time_21_24_sales_amount,
        :male_sales_amount, :female_sales_amount,
        :age10_sales_amount, :age20_sales_amount, :age30_sales_amount,
        :age40_sales_amount, :age50_sales_amount, :age60_plus_sales_amount,
        :loaded_at, :source_file
    )
    """
)

FLOATING_INSERT_SQL = text(
    """
    INSERT INTO raw_seoul_market_floating_population(
        base_year, base_quarter, area_type_code, area_type_name, area_code, area_name,
        total_population, male_population, female_population,
        age10_population, age20_population, age30_population,
        age40_population, age50_population, age60_plus_population,
        time_slot1_population, time_slot2_population, time_slot3_population,
        time_slot4_population, time_slot5_population, time_slot6_population,
        monday_population, tuesday_population, wednesday_population,
        thursday_population, friday_population, saturday_population, sunday_population,
        loaded_at, source_file
    ) VALUES (
        :base_year, :base_quarter, :area_type_code, :area_type_name, :area_code, :area_name,
        :total_population, :male_population, :female_population,
        :age10_population, :age20_population, :age30_population,
        :age40_population, :age50_population, :age60_plus_population,
        :time_slot1_population, :time_slot2_population, :time_slot3_population,
        :time_slot4_population, :time_slot5_population, :time_slot6_population,
        :monday_population, :tuesday_population, :wednesday_population,
        :thursday_population, :friday_population, :saturday_population, :sunday_population,
        :loaded_at, :source_file
    )
    """
)


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    text_value = str(value).strip().replace(",", "")
    if text_value == "":
        return None
    try:
        return float(text_value)
    except ValueError:
        return None


def _to_int(value: Any) -> int:
    number = _to_number(value)
    return int(number or 0)


def _parse_quarter_code(stdr: Any) -> tuple[int, int]:
    """STDR_YYQUARTER_CD(예: '20241') → (2024, 1)."""
    code = str(stdr or "").strip()
    if len(code) >= 5 and code[:4].isdigit() and code[4].isdigit():
        return int(code[:4]), int(code[4])
    return 0, 0


def _http_get_json(url: str) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                payload = resp.read().decode("utf-8")
            return json.loads(payload)
        except Exception as exc:
            last_exc = exc
            time.sleep(RETRY_BACKOFF_SEC * attempt)
    raise RuntimeError(f"OpenAPI 호출 실패 (재시도 {MAX_RETRY}회): {last_exc}")


def _iter_api_rows(api_key: str, service: str, target_quarter: str) -> Iterable[dict[str, Any]]:
    start = 1
    total = None
    while True:
        end = start + PAGE_SIZE - 1
        url = f"{OPENAPI_BASE}/{api_key}/json/{service}/{start}/{end}/{target_quarter}/"
        body = _http_get_json(url)
        block = body.get(service, {})
        if total is None:
            total = int(block.get("list_total_count") or 0)
            if total == 0:
                return
        rows = block.get("row") or []
        for row in rows:
            yield row
        if end >= total:
            return
        start = end + 1


def _build_quarter_codes(year: int, quarters: list[int]) -> list[tuple[str, int, int]]:
    return [(f"{year}{q}", year, q) for q in quarters]


def _delete_existing(conn: Any, table: str, year: int, quarters: list[int]) -> None:
    conn.execute(
        text(f"DELETE FROM {table} WHERE base_year = :y AND base_quarter = ANY(:qs)"),
        {"y": year, "qs": quarters},
    )


def _flush(conn: Any, sql: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.execute(sql, rows)
    n = len(rows)
    rows.clear()
    return n


def _build_sales_payload(row: dict[str, Any], loaded_at: datetime, source: str) -> dict[str, Any]:
    base_year, base_quarter = _parse_quarter_code(row.get("STDR_YYQUARTER_CD"))
    return {
        "base_year": base_year,
        "base_quarter": base_quarter,
        "area_type_code": str(row.get("TRDAR_SE_CD") or ""),
        "area_type_name": str(row.get("TRDAR_SE_CD_NM") or ""),
        "area_code": str(row.get("TRDAR_CD") or ""),
        "area_name": str(row.get("TRDAR_CD_NM") or ""),
        "service_code": str(row.get("SVC_INDUTY_CD") or ""),
        "service_name": str(row.get("SVC_INDUTY_CD_NM") or ""),
        "monthly_sales_amount": _to_number(row.get("THSMON_SELNG_AMT")),
        "monthly_sales_count": _to_number(row.get("THSMON_SELNG_CO")),
        "weekend_sales_ratio": _to_number(row.get("WK_SELNG_RT")),
        "weekday_sales_amount": _to_number(row.get("MDWK_SELNG_AMT")),
        "weekend_sales_amount": _to_number(row.get("WKEND_SELNG_AMT")),
        "monday_sales_amount": _to_number(row.get("MON_SELNG_AMT")),
        "tuesday_sales_amount": _to_number(row.get("TUES_SELNG_AMT")),
        "wednesday_sales_amount": _to_number(row.get("WED_SELNG_AMT")),
        "thursday_sales_amount": _to_number(row.get("THUR_SELNG_AMT")),
        "friday_sales_amount": _to_number(row.get("FRI_SELNG_AMT")),
        "saturday_sales_amount": _to_number(row.get("SAT_SELNG_AMT")),
        "sunday_sales_amount": _to_number(row.get("SUN_SELNG_AMT")),
        "time_00_06_sales_amount": _to_number(row.get("TMZON_00_06_SELNG_AMT")),
        "time_06_11_sales_amount": _to_number(row.get("TMZON_06_11_SELNG_AMT")),
        "time_11_14_sales_amount": _to_number(row.get("TMZON_11_14_SELNG_AMT")),
        "time_14_17_sales_amount": _to_number(row.get("TMZON_14_17_SELNG_AMT")),
        "time_17_21_sales_amount": _to_number(row.get("TMZON_17_21_SELNG_AMT")),
        "time_21_24_sales_amount": _to_number(row.get("TMZON_21_24_SELNG_AMT")),
        "male_sales_amount": _to_number(row.get("ML_SELNG_AMT")),
        "female_sales_amount": _to_number(row.get("FML_SELNG_AMT")),
        "age10_sales_amount": _to_number(row.get("AGRDE_10_SELNG_AMT")),
        "age20_sales_amount": _to_number(row.get("AGRDE_20_SELNG_AMT")),
        "age30_sales_amount": _to_number(row.get("AGRDE_30_SELNG_AMT")),
        "age40_sales_amount": _to_number(row.get("AGRDE_40_SELNG_AMT")),
        "age50_sales_amount": _to_number(row.get("AGRDE_50_SELNG_AMT")),
        "age60_plus_sales_amount": _to_number(row.get("AGRDE_60_ABOVE_SELNG_AMT")),
        "loaded_at": loaded_at,
        "source_file": source,
    }


def _build_floating_payload(row: dict[str, Any], loaded_at: datetime, source: str) -> dict[str, Any]:
    base_year, base_quarter = _parse_quarter_code(row.get("STDR_YYQUARTER_CD"))
    return {
        "base_year": base_year,
        "base_quarter": base_quarter,
        "area_type_code": str(row.get("TRDAR_SE_CD") or ""),
        "area_type_name": str(row.get("TRDAR_SE_CD_NM") or ""),
        "area_code": str(row.get("TRDAR_CD") or ""),
        "area_name": str(row.get("TRDAR_CD_NM") or ""),
        "total_population": _to_number(row.get("TOT_FLPOP_CO")),
        "male_population": _to_number(row.get("ML_FLPOP_CO")),
        "female_population": _to_number(row.get("FML_FLPOP_CO")),
        "age10_population": _to_number(row.get("AGRDE_10_FLPOP_CO")),
        "age20_population": _to_number(row.get("AGRDE_20_FLPOP_CO")),
        "age30_population": _to_number(row.get("AGRDE_30_FLPOP_CO")),
        "age40_population": _to_number(row.get("AGRDE_40_FLPOP_CO")),
        "age50_population": _to_number(row.get("AGRDE_50_FLPOP_CO")),
        "age60_plus_population": _to_number(row.get("AGRDE_60_ABOVE_FLPOP_CO")),
        "time_slot1_population": _to_number(row.get("TMZON_1_FLPOP_CO")),
        "time_slot2_population": _to_number(row.get("TMZON_2_FLPOP_CO")),
        "time_slot3_population": _to_number(row.get("TMZON_3_FLPOP_CO")),
        "time_slot4_population": _to_number(row.get("TMZON_4_FLPOP_CO")),
        "time_slot5_population": _to_number(row.get("TMZON_5_FLPOP_CO")),
        "time_slot6_population": _to_number(row.get("TMZON_6_FLPOP_CO")),
        "monday_population": _to_number(row.get("MON_FLPOP_CO")),
        "tuesday_population": _to_number(row.get("TUES_FLPOP_CO")),
        "wednesday_population": _to_number(row.get("WED_FLPOP_CO")),
        "thursday_population": _to_number(row.get("THUR_FLPOP_CO")),
        "friday_population": _to_number(row.get("FRI_FLPOP_CO")),
        "saturday_population": _to_number(row.get("SAT_FLPOP_CO")),
        "sunday_population": _to_number(row.get("SUN_FLPOP_CO")),
        "loaded_at": loaded_at,
        "source_file": source,
    }


def load_dataset(
    engine: Any,
    api_key: str,
    service: str,
    table: str,
    insert_sql: Any,
    payload_builder,
    year: int,
    quarters: list[int],
    chunk_size: int,
) -> int:
    loaded_at = datetime.now()
    inserted_total = 0
    with engine.begin() as conn:
        _delete_existing(conn, table, year, quarters)
        for stdr_code, _y, _q in _build_quarter_codes(year, quarters):
            source = f"openapi:{service}:{stdr_code}"
            buffer: list[dict[str, Any]] = []
            row_count = 0
            for row in _iter_api_rows(api_key, service, stdr_code):
                buffer.append(payload_builder(row, loaded_at, source))
                row_count += 1
                if len(buffer) >= chunk_size:
                    inserted_total += _flush(conn, insert_sql, buffer)
            inserted_total += _flush(conn, insert_sql, buffer)
            print(f"  [{service} / {stdr_code}] api_rows={row_count}")
    return inserted_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="서울 열린데이터광장 OpenAPI에서 상권분석 분기 데이터를 raw_seoul_market_* 테이블에 적재합니다."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=int(os.environ.get("SEOUL_OPENAPI_TARGET_YEAR") or datetime.now().year - 1),
        help="대상 연도 (기본값: 직전 해)",
    )
    parser.add_argument(
        "--quarters",
        type=str,
        default="1,2,3,4",
        help="대상 분기 (콤마 구분, 예: 1,2,3,4)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="배치 INSERT 크기",
    )
    parser.add_argument(
        "--datasets",
        type=str,
        default="sales,floating",
        help="적재 대상 (sales / floating / sales,floating)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key = os.environ.get("SEOUL_OPENAPI_KEY")
    if not api_key:
        raise RuntimeError(
            "SEOUL_OPENAPI_KEY 환경변수가 필요합니다. "
            "https://data.seoul.go.kr 인증키 발급 후 설정하세요."
        )

    quarters = [int(q.strip()) for q in args.quarters.split(",") if q.strip()]
    if not quarters:
        raise RuntimeError("--quarters 인자를 1개 이상 지정하세요.")

    targets = [name.strip() for name in args.datasets.split(",") if name.strip()]

    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL 드라이버가 설치되어 있지 않습니다.")

    print(f"[seoul-openapi] year={args.year} quarters={quarters} datasets={targets}")

    if "sales" in targets:
        inserted = load_dataset(
            engine=engine,
            api_key=api_key,
            service="VwsmTrdarSelngQ",
            table="raw_seoul_market_sales",
            insert_sql=SALES_INSERT_SQL,
            payload_builder=_build_sales_payload,
            year=args.year,
            quarters=quarters,
            chunk_size=args.chunk_size,
        )
        print(f"  sales inserted: {inserted}")

    if "floating" in targets:
        inserted = load_dataset(
            engine=engine,
            api_key=api_key,
            service="VwsmTrdarFlpopW",
            table="raw_seoul_market_floating_population",
            insert_sql=FLOATING_INSERT_SQL,
            payload_builder=_build_floating_payload,
            year=args.year,
            quarters=quarters,
            chunk_size=args.chunk_size,
        )
        print(f"  floating inserted: {inserted}")

    print("[seoul-openapi] 완료")


if __name__ == "__main__":
    main()