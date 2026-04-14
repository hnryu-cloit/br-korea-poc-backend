import sys
import os
import pandas as pd
sys.path.insert(0, os.path.abspath('br-korea-poc-backend'))
from app.infrastructure.db.connection import get_database_engine

engine = get_database_engine()
df_stor = pd.read_sql('SELECT * FROM "STOR_MST" LIMIT 1', engine)
df_item = pd.read_sql('SELECT * FROM "DAILY_STOR_ITEM" LIMIT 1', engine)

print("STOR_MST columns:", df_stor.columns.tolist())
print("DAILY_STOR_ITEM columns:", df_item.columns.tolist())
