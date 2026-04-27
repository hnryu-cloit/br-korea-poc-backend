from __future__ import annotations
from _runner import run_main

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="reference/dss_eda_seoul_market_area-main/data CSV를 raw_seoul_market_* 테이블에 적재합니다."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="배치 insert 크기 (기본값: 5000)",
    )
    return parser.parse_args()


def _open_csv(path: Path):
    for encoding in ("cp949", "euc-kr", "utf-8-sig", "utf-8"):
        try:
            return path.open("r", encoding=encoding, newline="", errors="strict")
        except UnicodeDecodeError:
            continue
    return path.open("r", encoding="utf-8", newline="", errors="replace")


def _clean_key(value: str) -> str:
    return value.strip().replace('"', "")


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


def _iter_rows(path: Path):
    with _open_csv(path) as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            return
        reader.fieldnames = [_clean_key(name or "") for name in reader.fieldnames]
        for row in reader:
            yield {_clean_key(k): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def _flush_rows(connection: Any, sql: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    connection.execute(sql, rows)
    count = len(rows)
    rows.clear()
    return count


def load_sales(connection: Any, source_path: Path, chunk_size: int) -> int:
    insert_sql = text(
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

    loaded_at = datetime.now()
    payload_rows: list[dict[str, Any]] = []
    inserted = 0

    for row in _iter_rows(source_path):
        payload_rows.append(
            {
                "base_year": _to_int(row.get("기준_년_코드")),
                "base_quarter": _to_int(row.get("기준_분기_코드")),
                "area_type_code": row.get("상권_구분_코드") or "",
                "area_type_name": row.get("상권_구분_코드_명") or "",
                "area_code": row.get("상권_코드") or "",
                "area_name": row.get("상권_코드_명") or "",
                "service_code": row.get("서비스_업종_코드") or "",
                "service_name": row.get("서비스_업종_코드_명") or "",
                "monthly_sales_amount": _to_number(row.get("당월_매출_금액")),
                "monthly_sales_count": _to_number(row.get("당월_매출_건수")),
                "weekend_sales_ratio": _to_number(row.get("주말_매출_비율")),
                "weekday_sales_amount": _to_number(row.get("주중_매출_금액")),
                "weekend_sales_amount": _to_number(row.get("주말_매출_금액")),
                "monday_sales_amount": _to_number(row.get("월요일_매출_금액")),
                "tuesday_sales_amount": _to_number(row.get("화요일_매출_금액")),
                "wednesday_sales_amount": _to_number(row.get("수요일_매출_금액")),
                "thursday_sales_amount": _to_number(row.get("목요일_매출_금액")),
                "friday_sales_amount": _to_number(row.get("금요일_매출_금액")),
                "saturday_sales_amount": _to_number(row.get("토요일_매출_금액")),
                "sunday_sales_amount": _to_number(row.get("일요일_매출_금액")),
                "time_00_06_sales_amount": _to_number(row.get("시간대_00~06_매출_금액")),
                "time_06_11_sales_amount": _to_number(row.get("시간대_06~11_매출_금액")),
                "time_11_14_sales_amount": _to_number(row.get("시간대_11~14_매출_금액")),
                "time_14_17_sales_amount": _to_number(row.get("시간대_14~17_매출_금액")),
                "time_17_21_sales_amount": _to_number(row.get("시간대_17~21_매출_금액")),
                "time_21_24_sales_amount": _to_number(row.get("시간대_21~24_매출_금액")),
                "male_sales_amount": _to_number(row.get("남성_매출_금액")),
                "female_sales_amount": _to_number(row.get("여성_매출_금액")),
                "age10_sales_amount": _to_number(row.get("연령대_10_매출_금액")),
                "age20_sales_amount": _to_number(row.get("연령대_20_매출_금액")),
                "age30_sales_amount": _to_number(row.get("연령대_30_매출_금액")),
                "age40_sales_amount": _to_number(row.get("연령대_40_매출_금액")),
                "age50_sales_amount": _to_number(row.get("연령대_50_매출_금액")),
                "age60_plus_sales_amount": _to_number(row.get("연령대_60_이상_매출_금액")),
                "loaded_at": loaded_at,
                "source_file": str(source_path.relative_to(settings.project_root)),
            }
        )
        if len(payload_rows) >= chunk_size:
            inserted += _flush_rows(connection, insert_sql, payload_rows)

    inserted += _flush_rows(connection, insert_sql, payload_rows)
    return inserted


def load_floating_population(connection: Any, source_path: Path, chunk_size: int) -> int:
    insert_sql = text(
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

    loaded_at = datetime.now()
    payload_rows: list[dict[str, Any]] = []
    inserted = 0

    for row in _iter_rows(source_path):
        payload_rows.append(
            {
                "base_year": _to_int(row.get("기준 년코드")),
                "base_quarter": _to_int(row.get("기준_분기_코드")),
                "area_type_code": row.get("상권_구분_코드") or "",
                "area_type_name": row.get("상권_구분_코드_명") or "",
                "area_code": row.get("상권_코드") or "",
                "area_name": row.get("상권_코드_명") or "",
                "total_population": _to_number(row.get("총_유동인구_수")),
                "male_population": _to_number(row.get("남성_유동인구_수")),
                "female_population": _to_number(row.get("여성_유동인구_수")),
                "age10_population": _to_number(row.get("연령대_10_유동인구_수")),
                "age20_population": _to_number(row.get("연령대_20_유동인구_수")),
                "age30_population": _to_number(row.get("연령대_30_유동인구_수")),
                "age40_population": _to_number(row.get("연령대_40_유동인구_수")),
                "age50_population": _to_number(row.get("연령대_50_유동인구_수")),
                "age60_plus_population": _to_number(row.get("연령대_60_이상_유동인구_수")),
                "time_slot1_population": _to_number(row.get("시간대_1_유동인구_수")),
                "time_slot2_population": _to_number(row.get("시간대_2_유동인구_수")),
                "time_slot3_population": _to_number(row.get("시간대_3_유동인구_수")),
                "time_slot4_population": _to_number(row.get("시간대_4_유동인구_수")),
                "time_slot5_population": _to_number(row.get("시간대_5_유동인구_수")),
                "time_slot6_population": _to_number(row.get("시간대_6_유동인구_수")),
                "monday_population": _to_number(row.get("월요일_유동인구_수")),
                "tuesday_population": _to_number(row.get("화요일_유동인구_수")),
                "wednesday_population": _to_number(row.get("수요일_유동인구_수")),
                "thursday_population": _to_number(row.get("목요일_유동인구_수")),
                "friday_population": _to_number(row.get("금요일_유동인구_수")),
                "saturday_population": _to_number(row.get("토요일_유동인구_수")),
                "sunday_population": _to_number(row.get("일요일_유동인구_수")),
                "loaded_at": loaded_at,
                "source_file": str(source_path.relative_to(settings.project_root)),
            }
        )
        if len(payload_rows) >= chunk_size:
            inserted += _flush_rows(connection, insert_sql, payload_rows)

    inserted += _flush_rows(connection, insert_sql, payload_rows)
    return inserted


def main() -> None:
    args = parse_args()
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver is not installed. Install psycopg before loading data.")

    data_root = settings.project_root / "reference/dss_eda_seoul_market_area-main/data"
    csv_files = sorted(data_root.glob("*.csv"))
    sales_files = [path for path in csv_files if "2019" in path.name]
    pop_files = [path for path in csv_files if path not in sales_files]
    if not sales_files or not pop_files:
        raise FileNotFoundError(f"reference 데이터 파일을 찾을 수 없습니다: {data_root}")

    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE raw_seoul_market_sales"))
        connection.execute(text("TRUNCATE TABLE raw_seoul_market_floating_population"))

        sales_count = 0
        for sales_path in sales_files:
            sales_count += load_sales(connection, sales_path, args.chunk_size)

        pop_count = 0
        for pop_path in pop_files:
            pop_count += load_floating_population(connection, pop_path, args.chunk_size)

    print(
        "Market reference data ingestion completed. "
        f"sales_rows={sales_count}, floating_rows={pop_count}, db={get_safe_database_url()}"
    )


if __name__ == "__main__":
    run_main(main)