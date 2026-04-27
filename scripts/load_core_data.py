import csv
import os
import sys
from io import StringIO
from _runner import run_main

import pandas as pd

# 프로젝트 루트 경로 추가 (app 모듈을 찾기 위함)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.infrastructure.db.connection import get_database_engine


def psql_insert_copy(table, conn, keys, data_iter):
    """
    PostgreSQL의 COPY 명령어를 사용하여 데이터를 초고속으로 벌크 인서트하는 함수.
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(f'"{k}"' for k in keys)
        table_name = f'"{table.name}"'
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"

        # psycopg2 호환
        if hasattr(cur, "copy_expert"):
            cur.copy_expert(sql=sql, file=s_buf)
        # psycopg3 호환
        elif hasattr(cur, "copy"):
            with cur.copy(sql) as copy:
                copy.write(s_buf.getvalue())
        else:
            raise Exception("PostgreSQL COPY method is not supported by the current driver.")
    dbapi_conn.commit()


def load_table_data(engine, table_name, data_dir):
    """
    특정 테이블에 해당하는 데이터를 파일 또는 디렉토리에서 읽어 DB에 적재합니다.
    1. data_dir/table_name.xlsx 또는 .csv 파일 확인
    2. data_dir/table_name/ 디렉토리 내의 모든 .xlsx, .csv 파일 확인
    """
    print(f"⏳ {table_name} 데이터 적재 중...")

    files_to_read = []

    # 1. 파일 직접 확인
    direct_file_xlsx = os.path.join(data_dir, f"{table_name}.xlsx")
    direct_file_csv = os.path.join(data_dir, f"{table_name}.csv")

    if os.path.exists(direct_file_xlsx):
        files_to_read.append(direct_file_xlsx)
    elif os.path.exists(direct_file_csv):
        files_to_read.append(direct_file_csv)

    # 2. 디렉토리 확인
    table_dir = os.path.join(data_dir, table_name)
    if os.path.isdir(table_dir):
        for f in os.listdir(table_dir):
            if f.endswith((".xlsx", ".csv")) and not f.startswith("~$"):
                files_to_read.append(os.path.join(table_dir, f))

    if not files_to_read:
        print(f"⚠️ {table_name}에 해당하는 파일을 찾을 수 없습니다. 건너뜁니다.")
        return

    total_rows = 0
    first_file = True

    for file_path in sorted(files_to_read):
        print(f"  - {os.path.basename(file_path)} 읽는 중...")
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path)
            else:
                try:
                    df = pd.read_csv(file_path, encoding="utf-8")
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding="euc-kr")

            row_count = len(df)
            total_rows += row_count

            # 테이블 스키마만 먼저 생성/초기화 (데이터 없이)
            if first_file:
                df.head(0).to_sql(table_name.upper(), con=engine, if_exists="replace", index=False)

            # COPY 명령어로 데이터 고속 벌크 인서트
            df.to_sql(
                table_name.upper(),
                con=engine,
                if_exists="append",
                index=False,
                method=psql_insert_copy,
                chunksize=500000,  # 메모리 과부하 방지
            )
            first_file = False

            print(f"    -> {row_count} 행 적재 완료")
        except Exception as e:
            print(f"  ❌ 에러 발생 ({os.path.basename(file_path)}): {e}")

    if total_rows > 0:
        print(f"✅ {table_name} 총 적재 완료: {total_rows} 행")
    else:
        print(f"⚠️ {table_name}에 적재할 데이터가 없습니다.")


def load_data():
    engine = get_database_engine()
    data_dir = "../resources/data"

    # 적재할 테이블 리스트 (파일명/폴더명 기준)
    tables = [
        "SPL_DAY_STOCK_DTL",
        "PROD_DTL",
        "ORD_DTL",
        "DAILY_STOR_ITEM_TMZON",
        "DAILY_STOR_CHL_TMZON",
        "DAILY_STOR_PAY_WAY",
        "DAILY_STOR_ITEM",
        "DAILY_STOR_CPI",
        "STOR_MST",
        "CPI_MST",
        "CPI_ITEM_MNG",
        "CPI_ITEM_GRP_MNG",
        "MST_PAY_DC_INFO",
        "MST_TERM_COOP_CMP_DC_ITEM",
        "MST_TERM_COOP_CMP_PAY_DC",
        "PAY_CD",
    ]

    for table in tables:
        load_table_data(engine, table, data_dir)

    print("\n🎉 모든 데이터 적재 프로세스가 완료되었습니다!")


if __name__ == "__main__":
    run_main(load_data)