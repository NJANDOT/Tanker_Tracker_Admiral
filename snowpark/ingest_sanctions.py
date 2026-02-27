import snowflake.snowpark as snowpark
import pandas as pd
import requests
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
# Path Hack to find 'include' if running directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from include.eczachly.snowflake_queries import get_snowpark_session


def ingest_sanctions(session: snowpark.Session, local_path: str = "include/sanctions.csv", table_name: str = "BRONZE_SANCTIONS", schema_name: str = "NICOLASSTEEL", date_str: str = None):
    """
    Ingests OpenSanctions Maritime data into Snowflake.
    Idempotent: If table exists and has data, skips upload.
    """
    
    # 1. Check if table exists
    try:
        # Simple check: Try to count rows
        count = session.table(f"{schema_name}.{table_name}").count()
        if count > 0:
            print(f"✅ Table {table_name} already exists with {count} rows. Skipping upload.")
            return "Skipped (Already Exists)"
    except Exception:
        print(f"Table {table_name} not found or empty. Proceeding to upload.")

    # 2. Get Data (Local Priority, then URL)
    # Using argument 'local_path'
    
    # Dynamic URL based on today's date (OpenSanctions pattern)
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
        
    url = f"https://data.opensanctions.org/datasets/{date_str}/maritime/maritime.csv"
    
    df = None
    
    if os.path.exists(local_path):
        print(f"📂 Found local file: {local_path}")
        df = pd.read_csv(local_path)
    else:
        print(f"🌐 Local file not found at {local_path}. Downloading from {url}...")
        try:
            df = pd.read_csv(url)
            # Save for future use?
            # df.to_csv(local_path, index=False) 
        except Exception as e:
            raise Exception(f"Failed to download sanctions data: {e}")

    if df is not None:
        # Normalize headers
        df.columns = [c.upper().replace(':', '_').replace('.', '_') for c in df.columns]
        
        # 3. Write to Snowflake
        print(f"Uploading {len(df)} rows to {table_name}...")
        
        sp_df = session.create_dataframe(df)
        sp_df.write.mode("overwrite").save_as_table(f"{schema_name}.{table_name}")
        
        print(f"Upload Complete.")
        return "Success"
    
    return "Failed"


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="include/sanctions.csv", help="Path to local CSV file")
    parser.add_argument("--table", default="BRONZE_SANCTIONS", help="Target Snowflake table")
    parser.add_argument("--schema", default="NICOLASSTEEL", help="Target Snowflake schema")
    parser.add_argument("--date", required=False, help="Date string YYYYMMDD for URL construction")
    args = parser.parse_args()
    
    # Create Session
    session = get_snowpark_session()
    
    ingest_sanctions(session, local_path=args.file, table_name=args.table, schema_name=args.schema, date_str=args.date)
