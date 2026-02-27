import os
import sys
import argparse
from pathlib import Path
import pandas as pd

# Path Hack to find 'include' if running directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Safe Import
try:
    from include.eczachly.snowflake_queries import get_snowpark_session
except ImportError:
    # Try alternate path if layout varies
    sys.path.append(str(PROJECT_ROOT / "include"))
    try:
        from eczachly.snowflake_queries import get_snowpark_session
    except:
        print("CRITICAL: Could not find 'include.eczachly.snowflake_queries'")
        raise

def upload_to_snowflake(local_csv_path, table_name, date_str, logger=None):
    """
    Reads a local CSV, adds 'DS' column, and uploads to Snowflake.
    """
    log = logger.info if logger else print
    error = logger.error if logger else print
    
    log(f"Starting Snowpark Upload for {local_csv_path.name} -> {table_name}")
    
    try:

        df = pd.read_csv(
            local_csv_path, 
            encoding='utf-8', 
            on_bad_lines='skip',
            low_memory=False
        )
        
        df['DS'] = date_str
        

        df.columns = [
            c.strip().replace(' ', '_').replace('#', '').replace('.', '').upper() 
            for c in df.columns
        ]
        
        log(f"Prepared DataFrame: {len(df)} rows. Columns: {list(df.columns)}")
        
        session = get_snowpark_session(schema='NICOLASSTEEL')
        
        snowpark_df = session.create_dataframe(df)
        
        # --- Merge Pattern (Idempotency) ---
        
        # 0. Check if Schema & Table Exists
        
        # Ensure schema exists (should be handled by setup but good for resilience)
        try:
             session.sql(f"CREATE SCHEMA IF NOT EXISTS NICOLASSTEEL").collect()
             session.use_schema("NICOLASSTEEL")
        except Exception as e:
             log(f"Warning: Could not create/use schema: {e}")

        table_exists = False
        try:
            # Simple check: Try to describe the table
            session.sql(f"DESCRIBE TABLE {table_name}").collect()
            table_exists = True
        except:
            table_exists = False
            
        if not table_exists:
            log(f"Table {table_name} does not exist. Creating it now...")
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            log(f"Created {table_name} with {len(df)} rows.")
            session.close()
            return True
            
        # 1. Write to Temporary Table
        temp_table = f"{table_name}_TEMP"
        snowpark_df.write.mode("overwrite").save_as_table(temp_table)
        
        log(f"Staged data in {temp_table}. Performing MERGE into {table_name}...")
        
        # 2. Construct MERGE SQL
        # Key: MMSI + _TIMESTAMP (Assuming standard DMA fields)
        # Note: Columns are already cleaned (uppercased, stripped).
        # We need check if _TIMESTAMP exists, fallback to TIMESTAMP if not.
        merge_keys = []
        if "_TIMESTAMP" in df.columns:
            merge_keys = ["MMSI", "_TIMESTAMP"]
        elif "TIMESTAMP" in df.columns:
            merge_keys = ["MMSI", "TIMESTAMP"]
        else:
            # Fallback for generic tables? 
            # If no timestamp, maybe just MMSI? But duplicates rely on time.
            # Let's assume _TIMESTAMP is statistically likely for AIS.
            merge_keys = ["MMSI"]
            log("WARNING: No Timestamp column found for Merge Key. Using MMSI only.")

        on_clause = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        # Dynamic Column List
        cols = [f'"{c}"' for c in df.columns]
        cols_str = ", ".join(cols)
        src_cols_str = ", ".join([f"source.{c}" for c in cols])
        
        merge_sql = f"""
            MERGE INTO {table_name} target
            USING {temp_table} source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT ({cols_str})
                VALUES ({src_cols_str})
        """
        
        session.sql(merge_sql).collect()
        log(f"MERGE Complete. {len(df)} rows processed.")
        
        # 3. Cleanup
        session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
        
        session.close()
        return True
        
    except Exception as e:
        import traceback
        error(f"Snowflake Upload Failed: {e}")
        error(traceback.format_exc())
        raise e

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--table", default="DK_AIS_BRONZE")
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    
    upload_to_snowflake(Path(args.file), args.table, args.date)
