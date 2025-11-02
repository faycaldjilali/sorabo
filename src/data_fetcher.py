import requests
import pandas as pd
from datetime import datetime
import json

def get_all_records_for_date(target_date, max_records=5000):
    """Get all records for a specific date with all available fields"""
    url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    all_records = []
    offset = 0
    limit = 100

    while len(all_records) < max_records:
        params = {
            'order_by': 'dateparution DESC',
            'limit': limit,
            'offset': offset
        }

        print(f"Requesting offset {offset}...")
        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        records = data.get('results', [])

        if not records:
            break  # No more records

        # Filter records for our target date
        target_records = [record for record in records if record.get('dateparution') == target_date]

        # If we found target records, add them
        if target_records:
            all_records.extend(target_records)
            print(f"Retrieved {len(target_records)} records for {target_date}... Total so far: {len(all_records)}")

        # Check if we've moved past our target date (since we're sorting DESC)
        if records and records[-1].get('dateparution', '') < target_date:
            print(f"Reached dates earlier than {target_date}. Stopping.")
            break

        offset += limit

        if offset > 10000:
            print("Safety limit reached. Stopping.")
            break

    return all_records

def create_excel_simple(records, target_date):
    """Simple and robust Excel creation"""

    # Clean the records
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            # Handle different data types
            if isinstance(value, (list, dict)):
                cleaned_record[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    # Create DataFrame
    df = pd.DataFrame(cleaned_records)

    # Create filename
    filename = f"BOAMP_{target_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    # Export to Excel - SIMPLE VERSION, just the data
    df.to_excel(filename, index=False, engine='openpyxl')

    return filename, df