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

# Main execution
if __name__ == "__main__":
    target_date = '2025-10-29'

    print(f"Searching for BOAMP records with publication date: {target_date}")
    print("=" * 60)

    # Get all records
    all_records = get_all_records_for_date(target_date)

    print(f"\n{'='*60}")
    print(f"EXTRACTION COMPLETE")
    print(f"Found {len(all_records)} records for date {target_date}")

    if all_records:
        # Create Excel file
        excel_filename, df = create_excel_simple(all_records, target_date)

        print(f"\n✅ Excel file created: {excel_filename}")
        print(f"📊 Total records exported: {len(all_records)}")
        print(f"📋 Total columns: {len(df.columns)}")

        # Show column names
        print(f"\n📋 Columns in Excel file:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")

        # Display sample data
        print(f"\n📄 SAMPLE DATA (first 3 records):")
        print(f"{'='*60}")
        for i, record in enumerate(all_records[:3], 1):
            print(f"Record {i}:")
            print(f"  Title: {record.get('objet', 'N/A')[:80]}...")
            print(f"  Buyer: {record.get('nomacheteur', 'N/A')}")
            print(f"  Procedure: {record.get('procedure_libelle', 'N/A')}")
            print(f"  Date: {record.get('dateparution', 'N/A')}")
            print("-" * 60)
    else:
        print("❌ No records found for the specified date.")