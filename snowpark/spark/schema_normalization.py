from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

# Configuration
APP_NAME = "TankerBrew_Schema_Normalization"
# TODO: Update paths to absolute or relative to execution context
RAW_US_PATH = r"c:\Users\warcrime\git\airflow-dbt-project\data\raw\ais_us"
RAW_DK_PATH = r"c:\Users\warcrime\git\airflow-dbt-project\capstone_tanker_brew_admiral\tmp"
TARGET_TABLE = "nicolassteel.dk_injestion_silver"
CATALOG_NAME = 'eczachly-academy-warehouse'

def create_spark_session():
    # Attempt to use the Tabular/Iceberg configuration from backfill_job_example.py
    # Requires Jars and Credentials in environment to work locally
    builder = SparkSession.builder \
        .appName(APP_NAME) \
        .config('spark.sql.defaultCatalog', CATALOG_NAME) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f'spark.sql.catalog.{CATALOG_NAME}.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog') \
        .config(f'spark.sql.catalog.{CATALOG_NAME}.warehouse', CATALOG_NAME) \
        .config(f'spark.sql.catalog.{CATALOG_NAME}.uri', 'https://api.tabular.io/ws/')
        
    # Optional: If running locally without pre-loaded jars, one might need to add .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3") or similar
    
    return builder.getOrCreate()

def normalize_us_data(spark, date_partition):
    """
    Reads US MarineCadastre data, renames columns to unified schema.
    Original: BaseDateTime, MMSI, LAT, LON, VesselType, Draft
    Target: timestamp, mmsi, lat, lon, vessel_type, draft, source
    """
    # Example pattern, might need adjustment based on actual filenames
    path = f"{RAW_US_PATH}/*{date_partition}*.csv"
    
    try:
        df = spark.read.option("header", "true").csv(path)
        
        normalized = df.select(
            to_timestamp(col("BaseDateTime")).alias("timestamp"),
            col("MMSI").cast("string").alias("mmsi"),
            col("LAT").cast("double").alias("lat"),
            col("LON").cast("double").alias("lon"),
            col("VesselType").cast("string").alias("vessel_type"),
            col("Draft").cast("double").alias("draft"),
            lit("US_NOAA").alias("source")
        )
        return normalized
    except Exception as e:
        print(f"Error reading US data for {date_partition}: {e}")
        return None

def normalize_dk_data(spark, date_partition):
    """
    Reads Danish DMA data, renames columns to unified schema.
    Original: Timestamp, MMSI, Lat, Lon, ShipType, Draught
    Target: timestamp, mmsi, lat, lon, vessel_type, draft, source, partition_date
    """
    path = f"{RAW_DK_PATH}/*{date_partition}*.csv"
    
    try:
        # Check if file exists roughly (spark will fail usually if empty glob)
        # We rely on Spark's recursive file lookup
        df = spark.read.option("header", "true").csv(path)
        
        normalized = df.select(
            to_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
            col("MMSI").cast("string").alias("mmsi"),
            col("Lat").cast("double").alias("lat"),
            col("Lon").cast("double").alias("lon"),
            col("Ship type").cast("string").alias("vessel_type"),
            col("Draught").cast("double").alias("draft"),
            lit("DK_DMA").alias("source"),
            lit(date_partition).cast("date").alias("partition_date")
        )
        return normalized
    except Exception as e:
        print(f"Error reading DK data for {date_partition}: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    # Example usage: python schema_normalization.py --date 2024-06-01
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to process (YYYY-MM-DD)", required=True)
    args = parser.parse_args()
    
    # Check if executed in environment with necessary Dependencies (or just dry-run logic)
    spark = create_spark_session()
    
    # Process US (Deferred/Dead Code path effectively if checked)
    # us_df = normalize_us_data(spark, args.date)
    
    # Process DK
    dk_df = normalize_dk_data(spark, args.date)
    
    if dk_df:
        # Create Table if Not Exists
        # NOTE: Using Iceberg syntax
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                TIMESTAMP TIMESTAMP,
                MMSI STRING,
                LAT DOUBLE,
                LON DOUBLE,
                VESSEL_TYPE STRING,
                DRAFT DOUBLE,
                SOURCE STRING,
                PARTITION_DATE DATE
            )
            USING iceberg
            PARTITIONED BY (PARTITION_DATE)
        """)
        
        # Write Append/Overlay
        print(f"Writing to {TARGET_TABLE}...")
        try:
            # Using overwritePartitions to support idempotency for that day
            dk_df.writeTo(TARGET_TABLE).using("iceberg").partitionedBy("PARTITION_DATE").append() 
            # Note: overwritePartitions() is cleaner for idempotency, but append() is safer if US data comes in separate job?
            # User said "junction... spark schema normalization... inserting it into the database".
            # If we run daily, overwritePartitions for that day is best.
            # But currently `dk_df` has only DK data. If US data comes later for same day?
            # Append is safer for concurrent independent sources, but risks dupes if re-run.
            # Strategy: For now, append.
            
            print(f"Successfully wrote data to {TARGET_TABLE}")
        except Exception as e:
            print(f"Failed to write to table: {e}")
            # Identify if it's a jar/config issue
            print("Ensure Spark environment has Iceberg jars and valid Catalog credentials (TABULAR_CREDENTIAL/AWS_VARS).")
            # Re-raise to signal failure to caller
            raise e
    else:
        print("No data found for this date.")

    spark.stop()

if __name__ == "__main__":
    main()
