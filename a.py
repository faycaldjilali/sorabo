import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import json
import io
import openai
import json
from openai import OpenAI

st.set_page_config(page_title="BOAMP Data Extractor", page_icon="📊", layout="wide")

def get_all_records_for_date(target_date, max_records=10000):
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
    target_date = st.sidebar.date_input("Select target date", value=datetime.today())
    target_date_str = target_date.strftime('%Y-%m-%d')

    # Max records
    max_records = st.sidebar.number_input("Maximum records", min_value=100, max_value=10000, value=10000)

    # Main content area
    tab1, tab2, tab3,tab4 = st.tabs(["Data Extraction", "Keyword Filtering", "Results", "AI Processing"])

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
             "Travaux de menuiserie et de charpenterie",
             "Pose de portes et de fenêtres et d'éléments accessoires",
             "Pose d'encadrements de portes et de fenêtres",
             "Pose d'encadrements de portes",
             "Pose d'encadrements de fenêtres",
             "Pose de seuils",
             "Poses de portes et de fenêtres",
             "Pose de portes",
             "Pose de fenêtres",
             "Pose de menuiseries métalliques, excepté portes et fenêtres",
             "Travaux de cloisonnement",
             "Installation de volets",
             "Travaux d'installation de stores",
             "Travaux d'installation de vélums",
             "Travaux d'installation de volets roulants",
             "Serrurerie",
             "Services de serrurerie",
             "Menuiserie pour la construction",
             "Travaux de menuiserie",
             "Clôtures",
             "Clôtures de protection",
             "Travaux d'installation de clôtures, de garde-corps et de dispositifs de sécurité",
             "Pose de clôtures",
             "Ascenseurs, skips, monte-charges, escaliers mécaniques et trottoirs roulants",
             "Escaliers mécaniques",
             "Pièces pour ascenseurs, skips ou escaliers mécaniques",
             "Pièces pour escaliers mécaniques",
             "Escaliers",
             "Escaliers pliants",
             "Travaux d'installation d'ascenseurs et d'escaliers mécaniques",
             "Travaux d'installation d'escaliers mécaniques",
             "Services de réparation et d'entretien d'escaliers mécaniques",
             "Services d'installation de matériel de levage et de manutention, excepté ascenseurs et escaliers mécaniques",
             "45420000","45421100","45421110","45421111","45421112","45421120","45421130","45421131","45421132","45421140",
             "45421141","45421142","45421143","45421144","45421145","44316500","98395000","44220000","45421000","34928200",
             "34928310","45340000","45342000","42416000","42416400","42419500","42419530","44233000","44423220","45313000",
             "45313200","50740000","51511000",
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
            
            # Sample data - UPDATED TO SHOW FINAL TABLE RECORDS
            st.subheader("Final Data Sample ")
            
            # Determine which dataset to show (priority: cleaned > filtered > raw)
            if 'cleaned_df' in st.session_state:
                display_df = st.session_state.cleaned_df
                data_source = "Cleaned Data (After Deduplication)"
            elif 'filtered_df' in st.session_state:
                display_df = st.session_state.filtered_df
                data_source = "Filtered Data"
            else:
                display_df = st.session_state.df
                data_source = "Raw Data"
            
            st.info(f"Showing sample from: **{data_source}**")
            
            # Display sample records
            if len(display_df) > 0:
                for i, (index, row) in enumerate(display_df.head(-1).iterrows(), 1):
                    with st.expander(f"Record {i} - ID: {row.get('id', 'N/A')}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Title:** {str(row.get('objet', 'N/A'))[:80]}...")
                            st.write(f"**Buyer:** {row.get('nomacheteur', 'N/A')}")
                            if 'keyword' in row:
                                st.write(f"**Matched Keyword:** {row.get('keyword', 'N/A')}")
                        with col2:
                            st.write(f"**Procedure:** {row.get('procedure_libelle', 'N/A')}")
                            st.write(f"**date limite reponse:** {row.get('datelimitereponse', 'N/A')}")
                            st.write(f"**code de departement:** {row.get('code_departement', 'N/A')}")
            else:
                st.warning("No records available to display.")
    # Add this with your other imports at the top


# Add this in your main() function where tabs are defined - add "AI Processing" to your tabs

# Add this entire section for the new tab
    with tab4:
        st.header("🤖 AI Data Processing")
        st.markdown("Transform extracted data into readable JSON using OpenAI")
        
        # API key input
        openai_api_key = st.text_input("OpenAI API Key", type="password", 
                                    help="Enter your OpenAI API key to use this feature")
        
        if st.button("🪄 Process with AI", type="primary") and openai_api_key:
            # Determine which dataset to use (priority: cleaned > filtered > raw)
            if 'cleaned_df' in st.session_state:
                processed_df = st.session_state.cleaned_df
                data_source = "Cleaned Data"
            elif 'filtered_df' in st.session_state:
                processed_df = st.session_state.filtered_df
                data_source = "Filtered Data"
            elif 'df' in st.session_state:
                processed_df = st.session_state.df
                data_source = "Raw Data"
            else:
                st.error("❌ No data available. Please extract data first.")
                processed_df = None
            
            if processed_df is not None:
                st.info(f"Using: **{data_source}** ({len(processed_df)} records)")
                
                # Extract required columns
                required_columns = ["gestion", "donnees"]
                missing_columns = [col for col in required_columns if col not in processed_df.columns]
                
                if missing_columns:
                    st.error(f"Missing required columns: {missing_columns}")
                else:
                    with st.spinner("AI is processing your data..."):
                        try:
                            # Prepare data for AI - process ALL rows
                            extracted_data = processed_df[required_columns]
                            
                            # Initialize OpenAI client
                            client = openai.OpenAI(api_key=openai_api_key)
                            
                            all_json_results = []
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            # Process each row individually
                            for index, row in extracted_data.iterrows():
                                status_text.text(f"Processing row {index + 1} of {len(extracted_data)}")
                                progress_bar.progress((index + 1) / len(extracted_data))
                                
                                # Create row data for display
                                row_data = {
                                    "row_index": index,
                                    "donnees": str(row['donnees'])
                                }
                                
                                # Display which row is being processed
                                with st.expander(f"📋 Row {index} sent to AI", expanded=False):
                                    st.json(row_data)
                                
                                # Create prompt for OpenAI for this specific row
                                prompt = f"""
                                Convert this single data row into a well-structured, readable JSON format.
                                Extract and organize information from these two fields:
                                
                                DONNEES: {row['donnees']}
                                
                                Create a JSON object that represents this single row with clear, organized structure.
                                Make the data easily readable and well-formatted.
                                If any field is empty or null, represent it appropriately in JSON.
                                """
                                
                                # Call OpenAI API for this row
                                response = client.chat.completions.create(
                                    model="gpt-3.5-turbo",  # Fixed model name
                                    messages=[
                                        {"role": "system", "content": "You are a data processing expert that converts tabular data into clean JSON format."},
                                        {"role": "user", "content": prompt}
                                    ],
                                    temperature=0.1
                                )
                                
                                # Get and parse AI response
                                ai_output = response.choices[0].message.content
                                
                                # Clean the response to extract JSON
                                try:
                                    if "```json" in ai_output:
                                        json_str = ai_output.split("```json")[1].split("```")[0].strip()
                                    elif "```" in ai_output:
                                        json_str = ai_output.split("```")[1].split("```")[0].strip()
                                    else:
                                        json_str = ai_output.strip()
                                    
                                    row_json = json.loads(json_str)
                                    # Add original row index to the JSON
                                    row_json["original_row_index"] = index
                                    all_json_results.append(row_json)
                                    
                                except json.JSONDecodeError:
                                    st.error(f"❌ Row {index}: AI response contained invalid JSON")
                                    st.text(f"AI Response for row {index}:")
                                    st.text(ai_output)
                                    # Add error info to results
                                    all_json_results.append({
                                        "original_row_index": index,
                                        "error": "Failed to parse AI response",
                                        "raw_ai_output": ai_output
                                    })
                            
                            # Clear progress indicators
                            progress_bar.empty()
                            status_text.empty()
                            
                            # Display final results
                            if all_json_results:
                                st.success(f"✅ AI Processing Complete! Processed {len(all_json_results)} rows")
                                
                                st.subheader("Final Processed JSON Data")
                                st.json(all_json_results)
                                
                                # Download button for all results
                                json_string = json.dumps(all_json_results, indent=2, ensure_ascii=False)
                                st.download_button(
                                    label="📥 Download Complete JSON File",
                                    data=json_string,
                                    file_name=f"BOAMP_AI_processed_all_rows_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                    mime="application/json"
                                )
                                
                                # Show summary
                                st.subheader("Processing Summary")
                                successful_rows = len([r for r in all_json_results if "error" not in r])
                                failed_rows = len([r for r in all_json_results if "error" in r])
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Successfully Processed", successful_rows)
                                with col2:
                                    st.metric("Failed Rows", failed_rows)
                                
                        except Exception as e:
                            st.error(f"❌ OpenAI API Error: {str(e)}")
        
        elif not openai_api_key:
            st.warning("⚠️ Please enter your OpenAI API key to use this feature")

if __name__ == "__main__":
    main()