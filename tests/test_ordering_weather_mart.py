from __future__ import annotations

import asyncio

from sqlalchemy import create_engine, text

from app.repositories.ordering_repository import OrderingRepository


def _build_repository() -> OrderingRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE mart_store_weather_daily (
                    store_id TEXT NOT NULL,
                    store_name TEXT,
                    sido TEXT NOT NULL,
                    weather_dt TEXT NOT NULL,
                    weather_type TEXT NOT NULL,
                    avg_temp_c REAL NOT NULL,
                    max_temp_c REAL,
                    min_temp_c REAL,
                    precipitation_mm REAL NOT NULL,
                    precipitation_probability_max INTEGER,
                    PRIMARY KEY (store_id, weather_dt)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE raw_weather_daily (
                    weather_dt TEXT NOT NULL,
                    sido TEXT NOT NULL,
                    avg_temp_c REAL NOT NULL,
                    precipitation_mm REAL NOT NULL,
                    PRIMARY KEY (weather_dt, sido)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE raw_store_master (
                    masked_stor_cd TEXT NOT NULL,
                    maked_stor_nm TEXT,
                    sido TEXT
                )
                """
            )
        )
    return OrderingRepository(engine=engine)


def test_get_weather_forecast_reads_store_weather_mart_by_store_and_date() -> None:
    repository = _build_repository()
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO mart_store_weather_daily(
                    store_id, store_name, sido, weather_dt, weather_type,
                    avg_temp_c, max_temp_c, min_temp_c, precipitation_mm, precipitation_probability_max
                ) VALUES (
                    'POC_010', 'POC Store', 'Seoul', '20260306', 'Rainy',
                    11, 14, 8, 3, 42
                )
                """
            )
        )

    payload = asyncio.run(
        repository.get_weather_forecast(store_id="POC_010", reference_date="2026-03-06")
    )

    assert payload is not None
    assert payload["region"] == "Seoul"
    assert payload["forecast_date"] == "2026-03-06"
    assert payload["weather_type"] == "Rainy"
    assert payload["max_temperature_c"] == 14
    assert payload["min_temperature_c"] == 8
    assert payload["precipitation_probability"] == 42


def test_get_weather_forecast_falls_back_to_raw_weather_when_mart_row_is_missing() -> None:
    repository = _build_repository()
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO mart_store_weather_daily(
                    store_id, store_name, sido, weather_dt, weather_type,
                    avg_temp_c, max_temp_c, min_temp_c, precipitation_mm, precipitation_probability_max
                ) VALUES (
                    'POC_010', 'POC Store', 'Seoul', '20260310', 'Cloudy',
                    9, 12, 6, 0, 5
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO raw_store_master(masked_stor_cd, maked_stor_nm, sido)
                VALUES ('POC_010', 'POC Store', 'Seoul')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO raw_weather_daily(weather_dt, sido, avg_temp_c, precipitation_mm)
                VALUES ('20260325', 'Seoul', 13, 1)
                """
            )
        )

    payload = asyncio.run(
        repository.get_weather_forecast(store_id="POC_010", reference_date="2026-03-25")
    )

    assert payload is not None
    assert payload["forecast_date"] == "2026-03-25"
    assert payload["region"] == "Seoul"
