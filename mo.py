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
    target_date = '2025-10-31'

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

def get_keword_excel(file_path, output_path, keywords):
   
    print("📂 Reang Excel file...")
    df = pd.read_excel(file_path)
    df_str = df.astype(str).apply(lambda x: x.str.lower())  # for case-insensitive search

    # --- 4. Create one big dataframe for all results ---
    all_matches = pd.DataFrame()

    # --- 5. Loop through each keyword ---
    for keyword in keywords:
        mask = df_str.apply(lambda x: x.str.contains(keyword.lower(), na=False))
        filtered_df = df[mask.any(axis=1)]
        
        if not filtered_df.empty:
            print(f"✅ Found {len(filtered_df)} rows for '{keyword}'")
            filtered_df = filtered_df.copy()
            filtered_df["keyword"] = keyword
            all_matches = pd.concat([all_matches, filtered_df], ignore_index=True)
        else:
            print(f"⚠️ No matches found for '{keyword}'.")

    # --- 6. Save everything to one file ---
    if not all_matches.empty:
        all_matches.to_excel(output_path, index=False)
        print(f"\n📊 All matching data saved successfully to:\n{output_path}")
    else:
        print("\n❌ No matches found for any keyword.")
        return output_path
keywords = [
        # Secteurs d’activité
        "miroiterie",
        "métallerie",
        "menuiserie extérieure",

        # CPV simples
        
    
        
    

        # CPV détaillés avec description
        "45420000", "Travaux de menuiserie et de charpenterie",
        "45421100", "Pose de portes et de fenêtres et d'éléments accessoires",
        "45421110", "Pose d'encadrements de portes et de fenêtres",
        "45421111", "Pose d'encadrements de portes",
        "45421112", "Pose d'encadrements de fenêtres",
        "45421120", "Pose de seuils",
        "45421130", "Poses de portes et de fenêtres",
        "45421131", "Pose de portes",
        "45421132", "Pose de fenêtres",
        "45421140", "Pose de menuiseries métalliques, excepté portes et fenêtres",
        "45421141", "Travaux de cloisonnement",
        "45421142", "Installation de volets",
        "45421143", "Travaux d'installation de stores",
        "45421144", "Travaux d'installation de vélums",
        "45421145", "Travaux d'installation de volets roulants",
        "44316500", "Serrurerie",
        "98395000", "Services de serrurerie",
        "44220000", "Menuiserie pour la construction",
        "45421000", "Travaux de menuiserie",
        "45421140", "Pose de menuiseries métalliques, excepté portes et fenêtres",
        "45421150", "Travaux d'installation de menuiseries non métalliques",
        "34928200", "Clôtures",
        "34928310", "Clôtures de protection",
        "45340000", "Travaux d'installation de clôtures, de garde-corps et de dispositifs de sécurité",
        "45342000", "Pose de clôtures",
        "42416000", "Ascenseurs, skips, monte-charges, escaliers mécaniques et trottoirs roulants",
        "42416400", "Escaliers mécaniques",
        "42419500", "Pièces pour ascenseurs, skips ou escaliers mécaniques",
        "42419530", "Pièces pour escaliers mécaniques",
        "44233000", "Escaliers",
        "44423220", "Escaliers pliants",
        "45313000", "Travaux d'installation d'ascenseurs et d'escaliers mécaniques",
        "45313200", "Travaux d'installation d'escaliers mécaniques",
        "50740000", "Services de réparation et d'entretien d'escaliers mécaniques",
        "51511000", "Services d'installation de matériel de levage et de manutention, excepté ascenseurs et escaliers mécaniques",]

file_path = excel_filename

output_path = f"BOAMP_{target_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_secteur.xlsx"


get_keword_excel(file_path, output_path, keywords)
def delete_duplicated_rows(file_path,id_column):
# === 1️⃣ Path to your Excel file ===

    # === 2️⃣ Read the Excel file ===
    df = pd.read_excel(file_path)

    # === 3️⃣ Column name containing the ID ===
    # Change this if your ID column has a different name

    # === 4️⃣ Remove duplicate rows — keep only the first occurrence per ID ===
    df_clean = df.drop_duplicates(subset=[id_column], keep='first')

    # === 5️⃣ Save to a new Excel file ===
    df_clean.to_excel(output_path, index=False)

    print(f"✅ Done! Kept {len(df_clean)} unique rows out of {len(df)} total.")
    print(f"💾 Clean file saved to: {output_path}")


id_column = 'id'
file_path = output_path
delete_duplicated_rows( file_path,id_column)