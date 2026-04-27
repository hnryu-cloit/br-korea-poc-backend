import os
import sys
from _runner import run_main

import pandas as pd

sys.path.insert(0, os.path.abspath("br-korea-poc-backend"))
from app.infrastructure.db.connection import get_database_engine


def load_store_prod_item():
    engine = get_database_engine()
    file_path = os.path.abspath("resources/data/점별+생산+품목+테이블.xlsx")
    target_table = "STOR_PROD_ITEM"

    print(f"⏳ 적재 중... [{target_table}] <- '{file_path}'")
    try:
        df = pd.read_excel(file_path)
        df.columns = [str(c).strip().upper() for c in df.columns]

        df.to_sql(target_table, con=engine, if_exists="replace", index=False)
        print(f"✅ 성공: {len(df)}행 적재 완료")
    except Exception as e:
        print(f"❌ 적재 실패: {e}")


if __name__ == "__main__":
    run_main(load_store_prod_item)