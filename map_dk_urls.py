import requests
import csv
import sys
from datetime import datetime, timedelta
import concurrent.futures

# Configuration
BASE_URL = "http://aisdata.ais.dk"
OUTPUT_FILE = "dk_data_urls.csv"
START_YEAR = 2006
END_YEAR = datetime.now().year + 1

import requests
import csv
import sys
import threading
from datetime import datetime, timedelta
import concurrent.futures

# Configuration
BASE_URL = "http://aisdata.ais.dk"
OUTPUT_FILE = "dk_data_urls.csv"
START_YEAR = 2006
END_YEAR = datetime.now().year + 1

CSV_LOCK = threading.Lock()

def check_url(url_info):
    """
    Checks if a URL exists via HEAD request.
    Returns the url_info dict if exists, else None.
    """
    url = url_info['url']
    try:
        response = requests.head(url, timeout=5)
        if response.status_code == 200:
            return url_info
    except:
        pass
    return None

def process_month(year, month):
    """
    Checks monthly file, if exists, check days too.
    """
    valid_urls = []
    
    # Check Monthly
    monthly_url = f"{BASE_URL}/{year}/aisdk-{year}-{month:02d}.zip"
    monthly_info = {
        'dateday': None,
        'datemonth': f"{year}-{month:02d}",
        'url': monthly_url
    }
    
    if check_url(monthly_info):
        valid_urls.append(monthly_info)
        # If monthly exists, likely days exist (or maybe distinct?)
        # We will check days regardless if monthly exists OR if we want to be exhaustive
        # User prompt was "map all files".
        # Optimization: We assume if MONTHLY exists, we check days.
        # If MONTHLY does NOT exist, is it possible DAYS exist?
        # Maybe. But usually these dumps are consistent.
        # To be safe but fast: we will check days anyway? No, that defeats optimization.
        # Let's assume strict hierarchy: Only check days if we found the month bucket or the month file?
        # Actually, the 404 on directory suggests no bucket browsing.
        # Let's try checking days ONLY if monthly file existed? 
        # Or better: Check days in parallel.
        pass
    
    # We'll just generate all candidates for this month and let the thread pool handle it.
    # But to speed up, we can yield them.
    
    candidates = [monthly_info]
    
    try:
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        days_in_month = end_date.day
        
        for day in range(1, days_in_month + 1):
            day_str = f"{year}-{month:02d}-{day:02d}"
            daily_url = f"{BASE_URL}/{year}/aisdk-{day_str}.zip"
            candidates.append({
                'dateday': day_str,
                'datemonth': f"{year}-{month:02d}",
                'url': daily_url
            })
    except ValueError:
        pass
        
    return candidates

def main():
    print(f"Generating candidate URLs from {START_YEAR} to {END_YEAR}...")
    
    # Initialize CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['dateday', 'datemonth', 'url'])
        writer.writeheader()

    all_candidates = []
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            all_candidates.extend(process_month(year, month))
            
    print(f"Generated {len(all_candidates)} candidates. Checking with high concurrency...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        future_to_url = {executor.submit(check_url, c): c for c in all_candidates}
        
        completed = 0
        found_count = 0
        total = len(all_candidates)
        
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            if result:
                found_count += 1
                with CSV_LOCK:
                    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=['dateday', 'datemonth', 'url'])
                        writer.writerow(result)
            
            completed += 1
            if completed % 500 == 0:
                print(f"Progress: {completed}/{total} checked ({found_count} found)...")

    print(f"Done! Found {found_count} valid files. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
