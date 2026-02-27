import os
import argparse
import csv
import sys
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import requests
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INCLUDE_PATH = os.path.join(PROJECT_ROOT, 'include', 'eczachly')
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

try:
    from common import setup_logger, download_and_extract_zip
except ImportError:
    from capstone_tanker_brew_admiral.common import setup_logger, download_and_extract_zip


CSV_PATH = Path(os.path.dirname(os.path.abspath(__file__))) / "dk_data_urls.csv"
LOG_FILE = "dk_ingestion.log"


def filter_open_source_tankers(input_path: Path, output_path: Path, logger):
    """
    Filters CSV for Tankers (Plan B Logic).
    """
    tanker_count = 0
    total_count = 0
    
    logger.info(f"Filtering {input_path.name} for Tankers...")
    
    with open(input_path, mode='r', encoding='utf-8', errors='replace') as infile, \
         open(output_path, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        if not reader.fieldnames:
            logger.warning("Empty CSV or no headers.")
            return False

        ship_type_col = next((c for c in reader.fieldnames if 'Ship type' in c), None)
        logger.info(f"Selected Ship Type column: {ship_type_col}")
        
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for i, row in enumerate(reader):
            total_count += 1
            is_tanker = False
            
            if ship_type_col:
                val = row.get(ship_type_col, '')

                if val and val == 'Tanker':
                    is_tanker = True
            
            if is_tanker:
                writer.writerow(row)
                tanker_count += 1

    logger.info(f"Filtering Complete. Kept {tanker_count}/{total_count} rows.")
    return True

def get_url_for_date(target_date: str) -> str:
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    target_month = target_dt.strftime("%Y-%m")
    
    monthly_url = None
    
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"URL Registry not found at {CSV_PATH}")

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['dateday'] == target_date:
                return row['url']
            if row['datemonth'] == target_month and not row['dateday']:
                monthly_url = row['url']
    return monthly_url

def ingest_local_smart(url, execution_date, logger):
    
    logger.info(f"Downloading and Filtering locally from {url}")
    
    DEST_DIR = Path(__file__).resolve().parent / "tmp"
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    
    success = download_and_extract_zip(url, DEST_DIR, logger)
    if not success:
        raise Exception("Download failed")
        
    search_pattern = f"*{execution_date}*.csv"
    candidates = list(DEST_DIR.glob(search_pattern))
    if not candidates:
        logger.error(f"No CSV found for pattern {search_pattern}")
        return None
    
    raw_csv = candidates[0]
    expected_filename = f"tankers_aisdk-{execution_date}.csv"
    filtered_csv_path = DEST_DIR / expected_filename
    
    filter_success = filter_open_source_tankers(raw_csv, filtered_csv_path, logger)
    
    try:
        if raw_csv.exists():
            os.remove(raw_csv)
    except:
        pass
    
    if filter_success:
        return str(filtered_csv_path) 
    else:
        return None


def run_dk_ingestion(execution_date: str):
    logger = setup_logger("DK_AIS_Ingestion", LOG_FILE)
    
    logger.info(f"Starting Ingestion for {execution_date}.")

    
    url = get_url_for_date(execution_date)
    if not url:
        logger.warning(f"No URL found for {execution_date}.")
        return None

    return ingest_local_smart(url, execution_date, logger)

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv

        env_path = Path(__file__).resolve().parent.parent / '.env'
        
        if env_path.exists():
            print(f"Loading environment from {env_path}")
            load_dotenv(env_path)
        else:
            print(f"Warning: .env file not found at {env_path}")
    except ImportError:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    # Mode argument is deprecated but kept for CLI compatibility (ignored)
    parser.add_argument("--mode", help="Ingestion Mode (Deprecated)", default="LOCAL_SMART")
    args = parser.parse_args()
    
    run_dk_ingestion(args.date)
