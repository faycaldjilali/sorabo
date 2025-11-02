import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import json
import io

st.set_page_config(page_title="BOAMP Data Extractor", page_icon="📊", layout="wide")

def get_all_records_for_date(target_date, max_records=5000):
    """Get all records for a specific date with all available fields"""
    url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    all_records = []
    offset = 0
    limit = 100

    progress_bar = st.progress(0)
    status_text = st.empty()

    while len(all_records) < max_records:
        params = {
            'order_by': 'dateparution DESC',
            'limit': limit,
            'offset': offset
        }

        status_text.text(f"Requesting offset {offset}... ({len(all_records)} records found so far)")
        progress_bar.progress(min(offset / 10000, 1.0))

        response = requests.get(url, params=params)

        if response.status_code != 200:
            st.error(f"Error {response.status_code}: {response.text}")
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
            status_text.text(f"Retrieved {len(target_records)} records for {target_date}... Total so far: {len(all_records)}")

        # Check if we've moved past our target date (since we're sorting DESC)
        if records and records[-1].get('dateparution', '') < target_date:
            status_text.text(f"Reached dates earlier than {target_date}. Stopping.")
            break

        offset += limit

        if offset > 10000:
            status_text.text("Safety limit reached. Stopping.")
            break

    progress_bar.empty()
    status_text.empty()
    return all_records

def create_excel_simple(records, target_date):
    """Simple and robust Excel creation"""
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
    return df

def filter_by_keywords(df, keywords):
    """Filter DataFrame by keywords"""
    df_str = df.astype(str).apply(lambda x: x.str.lower())
    all_matches = pd.DataFrame()

    for keyword in keywords:
        mask = df_str.apply(lambda x: x.str.contains(keyword.lower(), na=False))
        filtered_df = df[mask.any(axis=1)]
        
        if not filtered_df.empty:
            filtered_df = filtered_df.copy()
            filtered_df["keyword"] = keyword
            all_matches = pd.concat([all_matches, filtered_df], ignore_index=True)

    return all_matches

def main():
    st.title("📊 BOAMP Data Extractor")
    st.markdown("Extract public procurement data from BOAMP API")

    # Sidebar for configuration
    st.sidebar.header("Configuration")

    # Date input
    target_date = st.sidebar.date_input("Select target date", value=datetime(2025, 10, 31))
    target_date_str = target_date.strftime('%Y-%m-%d')

    # Max records
    max_records = st.sidebar.number_input("Maximum records", min_value=100, max_value=10000, value=5000)

    # Main content area
    tab1, tab2, tab3 = st.tabs(["Data Extraction", "Keyword Filtering", "Results"])

    with tab1:
        st.header("Data Extraction")
        
        if st.button("🚀 Extract Data", type="primary"):
            with st.spinner(f"Extracting data for {target_date_str}..."):
                all_records = get_all_records_for_date(target_date_str, max_records)

            if all_records:
                st.success(f"✅ Found {len(all_records)} records for date {target_date_str}")
                
                # Create DataFrame
                df = create_excel_simple(all_records, target_date_str)
                
                # Store in session state
                st.session_state.df = df
                st.session_state.records = all_records
                st.session_state.target_date = target_date_str
                
                # Show preview
                st.subheader("Data Preview")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Download button for raw data
                excel_buffer = io.BytesIO()
                df.to_excel(excel_buffer, index=False, engine='openpyxl')
                excel_buffer.seek(0)
                
                st.download_button(
                    label="📥 Download Raw Data Excel",
                    data=excel_buffer,
                    file_name=f"BOAMP_{target_date_str}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.error("❌ No records found for the specified date.")

    with tab2:
        st.header("Keyword Filtering")
        
        # Predefined keywords
        predefined_keywords = [
            "miroiterie", "métallerie", "menuiserie extérieure",
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
            "51511000", "Services d'installation de matériel de levage et de manutention, excepté ascenseurs et escaliers mécaniques"
        ]

        # Keyword selection
        st.subheader("Select Keywords")
        selected_keywords = st.multiselect(
            "Choose keywords to filter by:",
            options=predefined_keywords,
            default=predefined_keywords[:-1],  # Default to first 10
            help="Select keywords to filter the extracted data"
        )

        # Custom keywords
        custom_keywords_text = st.text_area(
            "Additional custom keywords (one per line):",
            placeholder="Enter additional keywords, one per line...",
            help="Add any custom keywords not in the predefined list"
        )

        # Combine selected and custom keywords
        all_keywords = selected_keywords.copy()
        if custom_keywords_text:
            custom_keywords = [k.strip() for k in custom_keywords_text.split('\n') if k.strip()]
            all_keywords.extend(custom_keywords)

        if st.button("🔍 Apply Keyword Filter", type="primary") and 'df' in st.session_state:
            if all_keywords:
                with st.spinner("Filtering data by keywords..."):
                    filtered_df = filter_by_keywords(st.session_state.df, all_keywords)
                
                if not filtered_df.empty:
                    st.session_state.filtered_df = filtered_df
                    st.success(f"✅ Found {len(filtered_df)} matching records")
                    
                    # Show preview
                    st.subheader("Filtered Data Preview")
                    st.dataframe(filtered_df.head(10), use_container_width=True)
                    
                    # Download button for filtered data
                    excel_buffer = io.BytesIO()
                    filtered_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                    excel_buffer.seek(0)
                    
                    st.download_button(
                        label="📥 Download Filtered Data Excel",
                        data=excel_buffer,
                        file_name=f"BOAMP_{st.session_state.target_date}_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.warning("⚠️ No matches found for the selected keywords.")
            else:
                st.warning("Please select at least one keyword.")

        # Deduplication
        if 'filtered_df' in st.session_state:
            st.subheader("Remove Duplicates")
            id_column = st.selectbox(
                "Select ID column for deduplication:",
                options=st.session_state.filtered_df.columns.tolist(),
                index=0
            )
            
            if st.button("🧹 Remove Duplicates"):
                df_clean = st.session_state.filtered_df.drop_duplicates(subset=[id_column], keep='first')
                st.session_state.cleaned_df = df_clean
                
                st.success(f"✅ Removed duplicates! Kept {len(df_clean)} unique rows out of {len(st.session_state.filtered_df)} total.")
                
                # Download button for cleaned data
                excel_buffer = io.BytesIO()
                df_clean.to_excel(excel_buffer, index=False, engine='openpyxl')
                excel_buffer.seek(0)
                
                st.download_button(
                    label="📥 Download Cleaned Data Excel",
                    data=excel_buffer,
                    file_name=f"BOAMP_{st.session_state.target_date}_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    with tab3:
        st.header("Results Summary")
        
        if 'df' in st.session_state:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Records", len(st.session_state.df))
                st.metric("Total Columns", len(st.session_state.df.columns))
            
            with col2:
                if 'filtered_df' in st.session_state:
                    st.metric("Filtered Records", len(st.session_state.filtered_df))
                else:
                    st.metric("Filtered Records", "Not filtered")
            
            with col3:
                if 'cleaned_df' in st.session_state:
                    st.metric("Unique Records", len(st.session_state.cleaned_df))
                else:
                    st.metric("Unique Records", "Not cleaned")
            
            # Column information
            st.subheader("Data Columns")
            columns_df = pd.DataFrame({
                'Column Name': st.session_state.df.columns.tolist(),
                'Data Type': st.session_state.df.dtypes.astype(str).tolist(),
                'Non-Null Count': st.session_state.df.notna().sum().tolist()
            })
            st.dataframe(columns_df, use_container_width=True)
            
            # Sample data
            st.subheader("Sample Data (First 3 Records)")
            for i, record in enumerate(st.session_state.records[:-1], 1):
                with st.expander(f"Record {i}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Title:** {record.get('objet', 'N/A')[:80]}...")
                        st.write(f"**Buyer:** {record.get('nomacheteur', 'N/A')}")
                    with col2:
                        st.write(f"**Procedure:** {record.get('procedure_libelle', 'N/A')}")
                        st.write(f"**Date:** {record.get('dateparution', 'N/A')}")

if __name__ == "__main__":
    main()