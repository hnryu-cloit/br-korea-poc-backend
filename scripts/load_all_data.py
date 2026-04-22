import os
import sys
import time

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, os.path.abspath("br-korea-poc-backend"))
from app.infrastructure.db.connection import get_database_engine


def load_all_data():
    engine = get_database_engine()
    data_dir = os.path.abspath("resources/data")
    log_file = "data_load_status.txt"

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"🚀 데이터 적재 시작: {time.strftime('%H:%M:%S')}\n")

    table_mappings = [
        ("STOR_MST", "raw_store_master"),
        ("DAILY_STOR_PAY_WAY", "raw_daily_store_pay_way"),
        ("DAILY_STOR_CPI_TMZON", "raw_daily_store_cpi_tmzon"),
        ("DAILY_STOR_ITEM.xlsx", "raw_daily_store_item"),
        ("DAILY_STOR_ITEM_TMZON", "raw_daily_store_item_tmzon"),
        ("DAILY_STOR_CHL_TMZON", "raw_daily_store_channel"),
        ("PAY_CD", "raw_pay_cd"),
        ("PROD_DTL", "raw_production_extract"),
        ("ORD_DTL", "raw_order_extract"),
        ("SPL_DAY_STOCK_DTL", "raw_inventory_extract"),
        ("MST_PAY_DC_INFO", "raw_settlement_master"),
        ("MST_TERM_COOP_CMP_PAY_DC", "raw_telecom_discount_policy"),
        ("CPI_MST", "raw_campaign_master"),
        ("점별+생산+품목+테이블", "STOR_PROD_ITEM"),
    ]

    def log_print(msg):
        print(msg)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    for root, dirs, files in os.walk(data_dir):
        for file in sorted(files):  # Sort for consistent order
            if file.startswith("~") or not (file.endswith(".xlsx") or file.endswith(".csv")):
                continue

            file_path = os.path.join(root, file)
            target_table = None

            for key, table_name in table_mappings:
                if key in file:
                    target_table = table_name
                    break

            if not target_table:
                log_print(f"⏭️ 매핑 대상 아님 (Skip): {file}")
                continue

            log_print(f"⏳ 적재 중... [{target_table}] <- '{file}'")
            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(file_path, encoding="utf-8")
                else:
                    df = pd.read_excel(file_path)

                df.columns = [str(c).strip().upper() for c in df.columns]

                df.to_sql(
                    target_table,
                    con=engine,
                    if_exists="append",
                    index=False,
                    chunksize=5000,
                    method="multi",
                )
                log_print(f"   ✅ 성공 (Append): {len(df)}행 적재 완료")
            except Exception as e:
                try:
                    log_print(
                        f"   ⚠️ Append 실패 ({e.__class__.__name__}), 기존 데이터 TRUNCATE 후 재시도..."
                    )
                    # 뷰(View) 종속성 에러 방지를 위해 강제 삭제(DROP)가 아닌 초기화(TRUNCATE CASCADE) 사용
                    with engine.connect() as conn:
                        conn.execute(text(f'TRUNCATE TABLE "{target_table}" CASCADE'))
                        conn.commit()

                    df.to_sql(
                        target_table,
                        con=engine,
                        if_exists="append",
                        index=False,
                        chunksize=5000,
                        method="multi",
                    )
                    log_print(f"   ✅ 성공 (Truncate 후 적재): {len(df)}행 적재 완료")
                except Exception as inner_e:
                    log_print(f"   ❌ 최종 적재 실패: {file}\n   - {inner_e}")

    log_print(f"\n🎉 모든 데이터 적재 완료! (종료 시간: {time.strftime('%H:%M:%S')})")


if __name__ == "__main__":
    load_all_data()
