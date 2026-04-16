from __future__ import annotations

print("SCRIPT STARTING")
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from openpyxl import load_workbook
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url

# Migration은 raw/운영 테이블을 만들고, 이 스크립트는 manifest 정의대로
# resource 파일을 해당 테이블에 적재한다.

def normalize_cell(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat(sep=" ", timespec="seconds")
        except TypeError:
            return value.isoformat()
    return str(value).strip()


def read_csv_rows(path: Path) -> list[list[str | None]]:
    for encoding in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            with path.open("r", encoding=encoding, newline="", errors="replace") as file:
                reader = csv.reader(file)
                return [[normalize_cell(cell) for cell in row] for row in reader]
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, f"Unable to decode {path}")


def iter_xlsx_rows(path: Path, sheet_name: str | None = None) -> tuple[str, list[list[str | None]]]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        target_sheet = sheet_name or workbook.sheetnames[0]
        worksheet = workbook[target_sheet]
        rows = [[normalize_cell(cell) for cell in row] for row in worksheet.iter_rows(values_only=True)]
        return target_sheet, rows
    finally:
        workbook.close()


def insert_tabular_rows(
    connection: Any,
    *,
    table: str,
    columns: list[str],
    rows: Iterable[list[str | None]],
    source_file: str,
    source_sheet: str | None,
    loaded_at: str,
) -> int:
    all_columns = columns + ["source_file", "source_sheet", "loaded_at"]
    sql = text(
        f'INSERT INTO "{table}" ({", ".join(all_columns)}) '
        f'VALUES ({", ".join(f":{column}" for column in all_columns)})'
    )

    payload_rows = []
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

    if payload_rows:
        connection.execute(sql, payload_rows)
    return len(payload_rows)


def load_dataset(connection: Any, dataset: dict[str, Any], run_id: int) -> None:
    backend_root = settings.backend_root
    loaded_at = datetime.now().isoformat(timespec="seconds")

    # dataset은 db/manifests/resource_load_manifest.json의 단일 항목이다.
    for relative_path in dataset["paths"]:
        source_path = (backend_root / relative_path).resolve()
        source_file = str(source_path.relative_to(settings.project_root))
        table = dataset["table"]
        connection.execute(text(f'DELETE FROM "{table}" WHERE source_file = :source_file'), {"source_file": source_file})

        row_count = 0
        message = "loaded"
        status = "success"
        source_sheet = None

        try:
            if dataset["loader"] == "csv":
                rows = read_csv_rows(source_path)
                source_sheet = "csv"
                row_count = insert_tabular_rows(
                    connection,
                    table=table,
                    columns=dataset["columns"],
                    rows=rows[1:],
                    source_file=source_file,
                    source_sheet=source_sheet,
                    loaded_at=loaded_at,
                )
            elif dataset["loader"] == "xlsx":
                source_sheet, rows = iter_xlsx_rows(source_path, sheet_name=dataset.get("sheet") or dataset.get("sheet_name"))
                row_count = insert_tabular_rows(
                    connection,
                    table=table,
                    columns=dataset["columns"],
                    rows=rows[1:],
                    source_file=source_file,
                    source_sheet=source_sheet,
                    loaded_at=loaded_at,
                )
            elif dataset["loader"] == "workbook_rows":
                workbook = load_workbook(source_path, read_only=True, data_only=True)
                try:
                    for sheet in workbook.sheetnames:
                        connection.execute(
                            text("DELETE FROM raw_workbook_rows WHERE source_file = :source_file AND sheet_name = :sheet_name"),
                            {"source_file": source_file, "sheet_name": sheet},
                        )
                        worksheet = workbook[sheet]
                        payload_rows = []
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
                            row_count += 1
                        if payload_rows:
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
                finally:
                    workbook.close()
                source_sheet = "*"
            else:
                raise ValueError(f"Unsupported loader: {dataset['loader']}")
        except Exception as exc:
            status = "failed"
            message = str(exc)

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
        raise RuntimeError("PostgreSQL driver is not installed. Install psycopg before loading resource data.")

    print("Beginning transaction...")
    with engine.begin() as connection:
        print("Transaction started. Inserting ingestion_run...")
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
                load_dataset(connection, dataset, run_id)
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
