import json
import csv
import sys
import os
from pathlib import Path
import re

# Increase CSV field limit (Safe limit for Windows)
csv.field_size_limit(2147483647)

def normalize_name(name):
    if not name: return ""
    return re.sub(r'[^A-Z0-9]', '', name.upper())

def load_sanctions(json_path):
    print(f"Loading sanctions from: {json_path}")
    sanctioned_vessels = {
        'imo': {},
        'mmsi': {},
        'name': {}
    }
    
    count = 0
    vessel_count = 0
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entity = json.loads(line)
                    schema = entity.get('schema')
                    
                    # OpenSanctions/FTM schemas for vessels
                    if schema in ['Vessel', 'Ship', 'TransportationDevice']:
                        props = entity.get('properties', {})
                        name_list = props.get('name', [])
                        imo_list = props.get('imoNumber', [])
                        mmsi_list = props.get('mmsi', [])
                        
                        # Store main info
                        info = {
                            'id': entity.get('id'),
                            'caption': entity.get('caption'),
                            'countries': props.get('country', []),
                            'flags': props.get('flag', [])
                        }
                        
                        # Index by IMO
                        for imo in imo_list:
                            sanctioned_vessels['imo'][imo] = info
                            
                        # Index by MMSI & Suffix (User Hypothesis: Lazy reflagging?)
                        for mmsi in mmsi_list:
                            sanctioned_vessels['mmsi'][mmsi] = info
                            if len(mmsi) == 9:
                                suffix = mmsi[3:]
                                if suffix not in sanctioned_vessels.get('mmsi_suffix', {}):
                                    if 'mmsi_suffix' not in sanctioned_vessels:
                                        sanctioned_vessels['mmsi_suffix'] = {}
                                    # Store list because suffixes might not be unique globally (though rare)
                                    if suffix not in sanctioned_vessels['mmsi_suffix']:
                                        sanctioned_vessels['mmsi_suffix'][suffix] = []
                                    sanctioned_vessels['mmsi_suffix'][suffix].append(info)
                            
                        # Index by Name (Normalized)
                        for name in name_list:
                            norm_name = normalize_name(name)
                            if len(norm_name) > 3: # Avoid short noise
                                if norm_name not in sanctioned_vessels['name']:
                                    sanctioned_vessels['name'][norm_name] = []
                                sanctioned_vessels['name'][norm_name].append(info)
                                
                        vessel_count += 1
                    
                    count += 1
                    if count % 100000 == 0:
                        print(f"Processed {count} columns...")
                        
                except json.JSONDecodeError:
                    continue
                    
    except FileNotFoundError:
        print(f"Error: File not found {json_path}")
        return None

    print(f"Sanctions loaded. Found {vessel_count} vessels.")
    return sanctioned_vessels

def analyze_csv(csv_path, sanctions):
    print(f"Analyzing Tankers from: {csv_path}")
    
    matches_found = []
    unique_hits = {}
    
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        # Identify columns
        # Expected: MMSI, IMO, Name, etc.
        headers = reader.fieldnames
        print(f"CSV Headers: {headers}")
        
        col_imo = next((c for c in headers if 'IMO' in c), None)
        col_mmsi = next((c for c in headers if 'MMSI' in c), None)
        col_name = next((c for c in headers if 'Name' in c), None)
        
        if not (col_imo or col_mmsi or col_name):
            print("Error: Could not find IMO, MMSI, or Name columns.")
            return

        row_count = 0
        for row in reader:
            row_count += 1
            
            # Extract Vessel Info
            v_imo = row.get(col_imo, '').strip() if col_imo else ''
            v_mmsi = row.get(col_mmsi, '').strip() if col_mmsi else ''
            v_name = row.get(col_name, '').strip() if col_name else ''
            
            matched = False
            match_details = []
            
            # Check IMO
            if v_imo and v_imo in sanctions['imo']:
                matched = True
                match_details.append(f"IMO Match ({v_imo}) -> {sanctions['imo'][v_imo]['caption']}")

            # Check MMSI (Exact)
            if not matched and v_mmsi and v_mmsi in sanctions['mmsi']:
                matched = True
                match_details.append(f"MMSI Exact Match ({v_mmsi}) -> {sanctions['mmsi'][v_mmsi]['caption']}")
            
            # Check MMSI Suffix (Experimental)
            if not matched and v_mmsi and len(v_mmsi) == 9:
                suffix = v_mmsi[3:]
                if suffix in sanctions.get('mmsi_suffix', {}):
                    matched = True
                    # Could be multiple
                    targets = sanctions['mmsi_suffix'][suffix]
                    # Format: TargetName (SuffixMatch)
                    t_names = ",".join([t['caption'] for t in targets[:2]]) 
                    match_details.append(f"MMSI Suffix Match (xx{suffix}) -> {t_names}")

            # Check Name (Fuzzy/Normalized)
            if not matched and v_name:
                norm_name = normalize_name(v_name)
                if norm_name in sanctions['name']:
                    matched = True
                    targets = sanctions['name'][norm_name]
                    # Create detail string for first match (keep it brief)
                    match_details.append(f"Name Match ({v_name}) -> {targets[0]['caption']}")

            if matched:
                match_key = v_imo if v_imo else (v_mmsi if v_mmsi else v_name)
                
                vals = {
                    'Source_Row': row_count,
                    'Vessel_Name': v_name,
                    'IMO': v_imo,
                    'MMSI': v_mmsi,
                    'Sanction_Info': "; ".join(match_details)
                }
                
                matches_found.append(vals)
                
                # Track unique hits
                if match_key not in unique_hits:
                    unique_hits[match_key] = {
                        'name': v_name, 
                        'count': 0,
                        'info': vals['Sanction_Info']
                    }
                unique_hits[match_key]['count'] += 1

                # Optional: Print every 1000th match to avoid spamming
                if len(matches_found) % 1000 == 0:
                    print(f"Running Total: {len(matches_found)} hits...")

    print(f"\nAnalysis Complete. Scanned {row_count} rows.")
    print(f"Total AIS Rows Matched: {len(matches_found)}")
    print(f"Unique Sanctioned Vessels Found: {len(unique_hits)}")
    
    print("\n--- Unique Vessels List ---")
    print(f"{'Vessel Name':<30} | {'Hits':<10} | {'Info'}")
    print("-" * 100)
    for k, v in unique_hits.items():
        print(f"{v['name']:<30} | {v['count']:<10} | {v['info'][:60]}...")

if __name__ == "__main__":
    # Hardcoded paths for quick user request
    TANKER_CSV = r"c:\Users\warcrime\git\airflow-dbt-project\capstone_tanker_brew_admiral\tmp\tankers_aisdk-2024-09-10.csv"
    ENTITIES_JSON = r"C:\Users\warcrime\Downloads\entities.ftm.json"
    
    if not os.path.exists(TANKER_CSV):
        print(f"CSV not found: {TANKER_CSV}")
    elif not os.path.exists(ENTITIES_JSON):
        print(f"JSON not found: {ENTITIES_JSON}")
    else:
        sanctions_db = load_sanctions(ENTITIES_JSON)
        if sanctions_db:
            analyze_csv(TANKER_CSV, sanctions_db)
