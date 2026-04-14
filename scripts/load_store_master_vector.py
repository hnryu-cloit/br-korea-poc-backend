import os
import pandas as pd
import psycopg
from pathlib import Path
from datetime import datetime

# DB Connection Settings
DB_URL = "postgresql://postgres:postgres@localhost:5435/br_korea_poc"

def load_store_master():
    # File Path (Updated to new location)
    excel_path = Path("resources/data/STOR_MST.xlsx")
    if not excel_path.exists():
        # Fallback to absolute path or check if we are in scripts directory
        if Path("../resources/data/STOR_MST.xlsx").exists():
            excel_path = Path("../resources/data/STOR_MST.xlsx")
        else:
            print(f"Error: File not found at {excel_path}")
            return

    # Load Excel
    df = pd.read_excel(excel_path)
    print(f"Loaded {len(df)} rows from {excel_path}")

    # Column mapping (Excel to DB)
    # The Excel file 'STOR_MST.xlsx' should match these columns. 
    # If the column names differ, they should be mapped here.
    
    # Existing columns in raw_store_master: 
    # row_no, masked_stor_cd, maked_stor_nm, actual_sales_amt, 
    # campaign_sales_ratio, store_type, business_type, sido, region, shipment_center, 
    # store_area_pyeong, source_file, source_sheet, loaded_at, embedding
    
    # Let's check the actual columns in the DataFrame first
    print(f"Actual columns in Excel: {df.columns.tolist()}")
    
    # If the number of columns matches our expectation (11 columns for data)
    if len(df.columns) >= 11:
        cols = [
            "row_no", "masked_stor_cd", "maked_stor_nm", "actual_sales_amt",
            "campaign_sales_ratio", "store_type", "business_type", "sido",
            "region", "shipment_center", "store_area_pyeong"
        ]
        # Assign names to the first 11 columns
        df.columns = cols + df.columns.tolist()[11:]
    
    # Metadata
    source_file = "resources/data/STOR_MST.xlsx"
    source_sheet = "Sheet1"
    loaded_at = datetime.now().isoformat()

    # Prepare for insertion
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # Clear existing data for this table (instead of filtering by source_file to be safe)
            cur.execute("TRUNCATE TABLE raw_store_master")
            
            # Insert rows
            for _, row in df.iterrows():
                # Prepare data
                data = {
                    "row_no": str(row.get("row_no", "")),
                    "masked_stor_cd": str(row.get("masked_stor_cd", "")),
                    "maked_stor_nm": str(row.get("maked_stor_nm", "")),
                    "actual_sales_amt": str(row.get("actual_sales_amt", "")),
                    "campaign_sales_ratio": str(row.get("campaign_sales_ratio", "")),
                    "store_type": str(row.get("store_type", "")),
                    "business_type": str(row.get("business_type", "")),
                    "sido": str(row.get("sido", "")),
                    "region": str(row.get("region", "")),
                    "shipment_center": str(row.get("shipment_center", "")),
                    "store_area_pyeong": str(row.get("store_area_pyeong", "")),
                    "source_file": source_file,
                    "source_sheet": source_sheet,
                    "loaded_at": loaded_at,
                    "embedding": None
                }
                
                # Construct query
                fields = data.keys()
                placeholders = [f"%({f})s" for f in fields]
                query = f"INSERT INTO raw_store_master ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                
                cur.execute(query, data)
            
            conn.commit()
    
    print(f"Successfully uploaded {len(df)} rows to raw_store_master.")

if __name__ == "__main__":
    load_store_master()
