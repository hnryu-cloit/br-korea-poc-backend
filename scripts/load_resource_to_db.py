from __future__ import annotations

print("SCRIPT STARTING")
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from openpyxl import load_workbook
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url

# Migration은 raw/운영 테이블을 만들고, 이 스크립트는 manifest 정의대로
# resource 파일을 해당 테이블에 적재한다.

BATCH_SIZE = 1000


def normalize_cell(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat(sep=" ", timespec="seconds")
        except TypeError:
            return value.isoformat()
    return str(value).strip()


def open_csv_reader(path: Path) -> tuple[Any, csv.reader[Any]]:
    for encoding in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            file = path.open("r", encoding=encoding, newline="", errors="replace")
            reader = csv.reader(file)
            next(iter(reader), None)
            file.seek(0)
            return file, csv.reader(file)
        except UnicodeDecodeError:
            file.close()
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, f"Unable to decode {path}")


def flush_tabular_batch(
    connection: Any,
    *,
    table: str,
    columns: list[str],
    payload_rows: list[dict[str, str | None]],
) -> int:
    if not payload_rows:
        return 0

    all_columns = columns + ["source_file", "source_sheet", "loaded_at"]
    sql = text(
        f'INSERT INTO "{table}" ({", ".join(all_columns)}) '
        f'VALUES ({", ".join(f":{column}" for column in all_columns)})'
    )
    connection.execute(sql, payload_rows)
    return len(payload_rows)


def insert_tabular_rows(
    connection: Any,
    *,
    table: str,
    columns: list[str],
    rows: Any,
    source_file: str,
    source_sheet: str | None,
    loaded_at: str,
) -> int:
    row_count = 0
    payload_rows: list[dict[str, str | None]] = []
    for row in rows:
        if not any(value not in (None, "") for value in row):
            continue
        values = list(row[: len(columns)])
        if len(values) < len(columns):
            values.extend([None] * (len(columns) - len(values)))
        payload = dict(zip(columns, values))
        payload["source_file"] = source_file
        payload["source_sheet"] = source_sheet
        payload["loaded_at"] = loaded_at
        payload_rows.append(payload)

        if len(payload_rows) >= BATCH_SIZE:
            row_count += flush_tabular_batch(
                connection, table=table, columns=columns, payload_rows=payload_rows
            )
            payload_rows = []

    row_count += flush_tabular_batch(connection, table=table, columns=columns, payload_rows=payload_rows)
    return row_count


def flush_workbook_rows_batch(connection: Any, payload_rows: list[dict[str, Any]]) -> int:
    if not payload_rows:
        return 0

    connection.execute(
        text(
            """
            INSERT INTO raw_workbook_rows(
                workbook_name, sheet_name, row_index, row_values_json, source_file, loaded_at
            ) VALUES (
                :workbook_name, :sheet_name, :row_index, :row_values_json, :source_file, :loaded_at
            )
            """
        ),
        payload_rows,
    )
    return len(payload_rows)


def load_dataset(connection: Any, dataset: dict[str, Any], run_id: int) -> None:
    backend_root = settings.backend_root
    print(f"Loading dataset {dataset['name']} into {dataset['table']}...")

    # dataset은 db/manifests/resource_load_manifest.json의 단일 항목이다.
    for relative_path in dataset["paths"]:
        loaded_at = datetime.now().isoformat(timespec="seconds")
        source_path = (backend_root / relative_path).resolve()
        source_file = str(source_path.relative_to(settings.project_root))
        table = dataset["table"]
        print(f"  -> ingesting {source_file}")
        connection.execute(
            text(f'DELETE FROM "{table}" WHERE source_file = :source_file'),
            {"source_file": source_file},
        )

        row_count = 0
        message = "loaded"
        status = "success"
        source_sheet = None

        try:
            if dataset["loader"] == "csv":
                source_sheet = "csv"
                file, reader = open_csv_reader(source_path)
                try:
                    next(reader, None)
                    row_count = insert_tabular_rows(
                        connection,
                        table=table,
                        columns=dataset["columns"],
                        rows=([normalize_cell(cell) for cell in row] for row in reader),
                        source_file=source_file,
                        source_sheet=source_sheet,
                        loaded_at=loaded_at,
                    )
                finally:
                    file.close()
            elif dataset["loader"] == "xlsx":
                workbook = load_workbook(source_path, read_only=True, data_only=True)
                try:
                    source_sheet = dataset.get("sheet") or dataset.get("sheet_name") or workbook.sheetnames[0]
                    worksheet = workbook[source_sheet]
                    row_iter = worksheet.iter_rows(values_only=True)
                    next(row_iter, None)
                    row_count = insert_tabular_rows(
                        connection,
                        table=table,
                        columns=dataset["columns"],
                        rows=([normalize_cell(cell) for cell in row] for row in row_iter),
                        source_file=source_file,
                        source_sheet=source_sheet,
                        loaded_at=loaded_at,
                    )
                finally:
                    workbook.close()
            elif dataset["loader"] == "workbook_rows":
                workbook = load_workbook(source_path, read_only=True, data_only=True)
                try:
                    connection.execute(
                        text("DELETE FROM raw_workbook_rows WHERE source_file = :source_file"),
                        {"source_file": source_file},
                    )
                    for sheet in workbook.sheetnames:
                        worksheet = workbook[sheet]
                        payload_rows: list[dict[str, Any]] = []
                        for index, row in enumerate(worksheet.iter_rows(values_only=True), start=1):
                            normalized = [normalize_cell(cell) for cell in row]
                            if not any(value not in (None, "") for value in normalized):
                                continue
                            payload_rows.append(
                                {
                                    "workbook_name": source_path.name,
                                    "sheet_name": sheet,
                                    "row_index": index,
                                    "row_values_json": json.dumps(normalized, ensure_ascii=False),
                                    "source_file": source_file,
                                    "loaded_at": loaded_at,
                                }
                            )

                            if len(payload_rows) >= BATCH_SIZE:
                                row_count += flush_workbook_rows_batch(connection, payload_rows)
                                payload_rows = []

                        row_count += flush_workbook_rows_batch(connection, payload_rows)
                finally:
                    workbook.close()
                source_sheet = "*"
            else:
                raise ValueError(f"Unsupported loader: {dataset['loader']}")
        except Exception as exc:
            status = "failed"
            message = str(exc)

        print(f"  <- {status} {source_file} ({row_count} rows)")
        connection.execute(
            text(
                """
                INSERT INTO ingestion_files(
                    run_id, table_name, source_file, source_sheet, row_count, loaded_at, status, message
                ) VALUES (
                    :run_id, :table_name, :source_file, :source_sheet, :row_count, :loaded_at, :status, :message
                )
                """
            ),
            {
                "run_id": run_id,
                "table_name": table,
                "source_file": source_file,
                "source_sheet": source_sheet,
                "row_count": row_count,
                "loaded_at": loaded_at,
                "status": status,
                "message": message,
            },
        )


def main() -> None:
    print("Starting main...")
    manifest = json.loads(settings.manifest_path.read_text(encoding="utf-8"))
    print(f"Manifest loaded from {settings.manifest_path}")
    engine = get_database_engine()
    print(f"Engine created: {engine}")
    if engine is None:
        raise RuntimeError(
            "PostgreSQL driver is not installed. Install psycopg before loading resource data."
        )

    print("Creating ingestion run...")
    with engine.begin() as connection:
        run_id = connection.execute(
            text(
                """
                INSERT INTO ingestion_runs(started_at, status, message)
                VALUES (:started_at, :status, :message)
                RETURNING run_id
                """
            ),
            {
                "started_at": datetime.now(),
                "status": "running",
                "message": "resource ingestion started",
            },
        ).scalar_one()

    try:
        for dataset in manifest["datasets"]:
            print(f"Starting transaction for dataset {dataset['name']}...")
            with engine.begin() as connection:
                load_dataset(connection, dataset, run_id)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE ingestion_runs
                    SET completed_at = :completed_at, status = :status, message = :message
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "completed_at": datetime.now(),
                    "status": "success",
                    "message": "resource ingestion completed",
                    "run_id": run_id,
                },
            )
    except Exception as exc:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE ingestion_runs
                    SET completed_at = :completed_at, status = :status, message = :message
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "completed_at": datetime.now(),
                    "status": "failed",
                    "message": str(exc),
                    "run_id": run_id,
                },
            )
        raise

    print(f"Loaded resource data into {get_safe_database_url()}")


if __name__ == "__main__":
    main()
