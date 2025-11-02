# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import json
import io

# ------------------------------------------
# Functions
# ------------------------------------------

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

        response = requests.get(url, params=params)

        if response.status_code != 200:
            st.error(f"Erreur {response.status_code}: {response.text}")
            break

        data = response.json()
        records = data.get('results', [])

        if not records:
            break  # No more records

        # Filter records for our target date
        target_records = [record for record in records if record.get('dateparution') == target_date]

        if target_records:
            all_records.extend(target_records)

        # Stop if we've moved past our target date
        if records and records[-1].get('dateparution', '') < target_date:
            break

        offset += limit

        if offset > 10000:
            st.warning("Limite de sécurité atteinte (10 000 enregistrements).")
            break

    return all_records


def create_excel_simple(records, target_date):
    """Create Excel file and return buffer + DataFrame"""
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if isinstance(value, (list, dict)):
                cleaned_record[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    df = pd.DataFrame(cleaned_records)

    output = io.BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    filename = f"BOAMP_{target_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return output, filename, df

# ------------------------------------------
# Streamlit UI
# ------------------------------------------

st.set_page_config(page_title="BOAMP Data Extractor", layout="wide")

st.title("📜 BOAMP Data Extractor")
st.write("Entrez une **date de parution** pour extraire toutes les annonces du BOAMP publiées ce jour-là.")

target_date = st.date_input("📅 Sélectionnez une date", datetime.today()).strftime("%Y-%m-%d")

if st.button("🔍 Extraire les données"):
    with st.spinner("Extraction en cours..."):
        records = get_all_records_for_date(target_date)

    if records:
        st.success(f"{len(records)} enregistrements trouvés pour le {target_date}.")
        output, filename, df = create_excel_simple(records, target_date)

        st.download_button(
            label="💾 Télécharger le fichier Excel",
            data=output,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.subheader("📊 Aperçu des données")
        st.dataframe(df.head(20))

        st.write(f"**Nombre total de colonnes :** {len(df.columns)}")
        st.write(f"**Colonnes disponibles :** {', '.join(df.columns[:10])} ...")

    else:
        st.warning("Aucun enregistrement trouvé pour cette date.")
