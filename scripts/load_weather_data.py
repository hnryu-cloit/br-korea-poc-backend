from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

SIDO_COORDINATES: dict[str, tuple[float, float]] = {
    "서울": (37.5665, 126.9780),
    "경기": (37.4138, 127.5183),
    "인천": (37.4563, 126.7052),
    "강원": (37.8228, 128.1555),
    "충북": (36.6358, 127.4914),
    "충남": (36.6588, 126.6728),
    "대전": (36.3504, 127.3845),
    "세종": (36.4800, 127.2890),
    "전북": (35.7175, 127.1530),
    "전남": (34.8679, 126.9910),
    "광주": (35.1595, 126.8526),
    "경북": (36.4919, 128.8889),
    "경남": (35.4606, 128.2132),
    "대구": (35.8714, 128.6014),
    "울산": (35.5384, 129.3114),
    "부산": (35.1796, 129.0756),
    "제주": (33.4996, 126.5312),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="외부 날씨 API(Open-Meteo)에서 일별 평균기온/강수량을 수집해 raw_weather_daily에 적재합니다."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=(date.today() - timedelta(days=30)).isoformat(),
        help="수집 시작일(YYYY-MM-DD), 기본값: 30일 전",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=(date.today() - timedelta(days=1)).isoformat(),
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
            "daily": "temperature_2m_mean,precipitation_sum",
            "timezone": "Asia/Seoul",
        },
    )
    response.raise_for_status()
    payload = response.json()
    daily = payload.get("daily") or {}
    days = daily.get("time") or []
    temperatures = daily.get("temperature_2m_mean") or []
    precipitations = daily.get("precipitation_sum") or []

    rows: list[dict[str, object]] = []
    for day, temp, rain in zip(days, temperatures, precipitations):
        day_date = parse_date(str(day))
        rows.append(
            {
                "weather_dt": day_date.strftime("%Y%m%d"),
                "sido": sido,
                "avg_temp_c": float(temp or 0),
                "precipitation_mm": float(rain or 0),
                "source_provider": "open-meteo",
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
            weather_dt, sido, avg_temp_c, precipitation_mm, source_provider, loaded_at
        ) VALUES (
            :weather_dt, :sido, :avg_temp_c, :precipitation_mm, :source_provider, :loaded_at
        )
        ON CONFLICT (weather_dt, sido)
        DO UPDATE SET
            avg_temp_c = EXCLUDED.avg_temp_c,
            precipitation_mm = EXCLUDED.precipitation_mm,
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
    main()
