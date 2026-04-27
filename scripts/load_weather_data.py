from __future__ import annotations
from _runner import run_main

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_START_DATE = "2025-03-11"
DEFAULT_END_DATE = "2026-03-10"

SIDO_COORDINATES: dict[str, tuple[float, float]] = {
    "서울특별시": (37.5665, 126.9780),
    "경기도": (37.4138, 127.5183),
    "인천광역시": (37.4563, 126.7052),
    "강원도": (37.8228, 128.1555),
    "충청북도": (36.6358, 127.4914),
    "충청남도": (36.6588, 126.6728),
    "대전광역시": (36.3504, 127.3845),
    "세종특별자치시": (36.4800, 127.2890),
    "전라북도": (35.7175, 127.1530),
    "전라남도": (34.8679, 126.9910),
    "광주광역시": (35.1595, 126.8526),
    "경상북도": (36.4919, 128.8889),
    "경상남도": (35.4606, 128.2132),
    "대구광역시": (35.8714, 128.6014),
    "울산광역시": (35.5384, 129.3114),
    "부산광역시": (35.1796, 129.0756),
    "제주특별자치도": (33.4996, 126.5312),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="외부 날씨 API(Open-Meteo)에서 일별 평균기온/강수량을 수집해 raw_weather_daily에 적재합니다."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE,
        help="수집 시작일(YYYY-MM-DD), 기본값: 30일 전",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=DEFAULT_END_DATE,
        help="수집 종료일(YYYY-MM-DD), 기본값: 어제",
    )
    parser.add_argument(
        "--sido",
        type=str,
        nargs="*",
        default=[],
        help="특정 시도만 수집하고 싶을 때 공백 구분으로 입력 (예: 서울 경기)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="외부 API 호출 타임아웃(초), 기본값: 20",
    )
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def fetch_weather_rows(
    *,
    client: httpx.Client,
    sido: str,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
) -> list[dict[str, object]]:
    response = client.get(
        OPEN_METEO_ARCHIVE_URL,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": "temperature_2m,precipitation",
            "timezone": "Asia/Seoul",
        },
    )
    response.raise_for_status()
    payload = response.json()
    hourly = payload.get("hourly") or {}
    hours = hourly.get("time") or []
    temperatures = hourly.get("temperature_2m") or []
    precipitations = hourly.get("precipitation") or []

    daily_buckets: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "temp_sum": 0.0,
            "temp_count": 0.0,
            "temp_max": float("-inf"),
            "temp_min": float("inf"),
            "precipitation_sum": 0.0,
            "precipitation_hours": 0.0,
            "hour_count": 0.0,
        }
    )
    for hour, temp, rain in zip(hours, temperatures, precipitations):
        hour_text = str(hour)
        day_text = hour_text.split("T", 1)[0]
        bucket = daily_buckets[day_text]
        bucket["hour_count"] += 1.0
        if temp is not None:
            temp_value = float(temp)
            bucket["temp_sum"] += temp_value
            bucket["temp_count"] += 1.0
            bucket["temp_max"] = max(bucket["temp_max"], temp_value)
            bucket["temp_min"] = min(bucket["temp_min"], temp_value)
        if rain is not None:
            rain_value = float(rain)
            bucket["precipitation_sum"] += rain_value
            if rain_value > 0:
                bucket["precipitation_hours"] += 1.0

    rows: list[dict[str, object]] = []
    for day_text in sorted(daily_buckets):
        bucket = daily_buckets[day_text]
        day_date = parse_date(day_text)
        avg_temp_c = (
            bucket["temp_sum"] / bucket["temp_count"] if bucket["temp_count"] else 0.0
        )
        max_temp_c = bucket["temp_max"] if bucket["temp_count"] else avg_temp_c
        min_temp_c = bucket["temp_min"] if bucket["temp_count"] else avg_temp_c
        precipitation_probability_max = (
            int(round((bucket["precipitation_hours"] / bucket["hour_count"]) * 100))
            if bucket["hour_count"]
            else 0
        )
        rows.append(
            {
                "weather_dt": day_date.strftime("%Y%m%d"),
                "sido": sido,
                "avg_temp_c": round(avg_temp_c, 2),
                "max_temp_c": round(max_temp_c, 2),
                "min_temp_c": round(min_temp_c, 2),
                "precipitation_mm": round(bucket["precipitation_sum"], 2),
                "precipitation_probability_max": precipitation_probability_max,
                "source_provider": "open-meteo-hourly-aggregated",
                "loaded_at": datetime.now(),
            }
        )
    return rows


def upsert_rows(rows: list[dict[str, object]]) -> int:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("PostgreSQL driver is not installed. Install psycopg before loading data.")

    upsert_sql = text(
        """
        INSERT INTO raw_weather_daily(
            weather_dt, sido, avg_temp_c, max_temp_c, min_temp_c, precipitation_mm,
            precipitation_probability_max, source_provider, loaded_at
        ) VALUES (
            :weather_dt, :sido, :avg_temp_c, :max_temp_c, :min_temp_c, :precipitation_mm,
            :precipitation_probability_max, :source_provider, :loaded_at
        )
        ON CONFLICT (weather_dt, sido)
        DO UPDATE SET
            avg_temp_c = EXCLUDED.avg_temp_c,
            max_temp_c = EXCLUDED.max_temp_c,
            min_temp_c = EXCLUDED.min_temp_c,
            precipitation_mm = EXCLUDED.precipitation_mm,
            precipitation_probability_max = EXCLUDED.precipitation_probability_max,
            source_provider = EXCLUDED.source_provider,
            loaded_at = EXCLUDED.loaded_at
        """
    )

    with engine.begin() as connection:
        if rows:
            connection.execute(upsert_sql, rows)
    return len(rows)


def main() -> None:
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    requested_sidos = args.sido or list(SIDO_COORDINATES.keys())
    unknown_sidos = [item for item in requested_sidos if item not in SIDO_COORDINATES]
    if unknown_sidos:
        raise ValueError(f"지원하지 않는 시도: {', '.join(unknown_sidos)}")

    total_rows = 0
    with httpx.Client(timeout=args.timeout) as client:
        for sido in requested_sidos:
            lat, lon = SIDO_COORDINATES[sido]
            rows = fetch_weather_rows(
                client=client,
                sido=sido,
                latitude=lat,
                longitude=lon,
                start_date=start_date,
                end_date=end_date,
            )
            inserted = upsert_rows(rows)
            total_rows += inserted
            print(f"[{sido}] upsert {inserted} rows")

    print(
        f"Weather data ingestion completed. total_rows={total_rows}, "
        f"range={start_date.isoformat()}~{end_date.isoformat()}, db={get_safe_database_url()}"
    )


if __name__ == "__main__":
    run_main(main)