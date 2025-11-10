import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import json
import io
import tempfile
import os
import PyPDF2
import time
import re

# Initialize session state variables
def initialize_session_state():
    """Initialize all session state variables"""
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = None
    if 'cleaned_df' not in st.session_state:
        st.session_state.cleaned_df = None
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None
    if 'uploaded_df' not in st.session_state:
        st.session_state.uploaded_df = None
    if 'records' not in st.session_state:
        st.session_state.records = None
    if 'target_date' not in st.session_state:
        st.session_state.target_date = None

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

def search_keywords_and_find_lot(text, keywords):
    """
    Search for keywords in PDF text and find ALL lot numbers that appear before them
    """
    try:
        results = []
        
        # Search for each keyword
        for keyword in keywords:
            # Find all occurrences of the keyword
            keyword_matches = list(re.finditer(re.escape(keyword), text, re.IGNORECASE))
            
            for match in keyword_matches:
                keyword_position = match.start()
                
                # Extract more text before the keyword (look back up to 500 characters)
                text_before = text[max(0, keyword_position - 1000):keyword_position]
                
                # Improved lot pattern to catch more formats
                lot_patterns = [
                    r'(lot|LOT)\s*[:\-\s]*\s*(\d+[-\w]*)',  # lot: 123, LOT-456, lot 789
                    r'(Lot\s*\d+)',  # Lot 123
                    r'(lot\s*\d+)',  # lot 123
                    r'\b(\d+)\s*-\s*Lot',  # 123 - Lot
                    r'\b(LOT\s*[A-Z]*\d+)',  # LOT A123, LOT 456
                ]
                
                all_lot_matches = []
                
                for pattern in lot_patterns:
                    matches = re.findall(pattern, text_before, re.IGNORECASE)
                    for match_tuple in matches:
                        if isinstance(match_tuple, tuple):
                            # For patterns that capture groups
                            lot_number = match_tuple[1] if len(match_tuple) > 1 else match_tuple[0]
                        else:
                            # For patterns that capture directly
                            lot_number = match_tuple
                        
                        # Clean up the lot number
                        lot_number = re.sub(r'^(lot|LOT)\s*', '', lot_number, flags=re.IGNORECASE)
                        lot_number = lot_number.strip(' :-\t')
                        
                        if lot_number and lot_number not in [lm[0] for lm in all_lot_matches]:
                            all_lot_matches.append((lot_number, pattern))
                
                # Remove duplicates while preserving order
                unique_lots = []
                seen = set()
                for lot_num, pattern in all_lot_matches:
                    if lot_num not in seen:
                        seen.add(lot_num)
                        unique_lots.append(lot_num)
                
                if unique_lots:
                    for lot_number in unique_lots:
                        results.append({
                            'keyword': keyword,
                            'lot_number': lot_number
                        })
        
        return results
            
    except Exception as e:
        return []

def check_visite_obligatoire(text, keywords):
    """
    Search for keywords in PDF text and check if 'visite' appears before them
    """
    try:
        # Search for each keyword
        for keyword in keywords:
            # Find all occurrences of the keyword
            keyword_matches = list(re.finditer(re.escape(keyword), text, re.IGNORECASE))
            
            for match in keyword_matches:
                keyword_position = match.start()
                
                # Extract text before the keyword (look back up to 500 characters)
                text_before = text[max(0, keyword_position - 500):keyword_position]
                
                # Check if "visite" appears before the keyword
                visite_patterns = [r"visites", r"visite"]
                
                for pattern in visite_patterns:
                    if re.search(pattern, text_before, re.IGNORECASE):
                        return "yes"
        
        return "no"
            
    except Exception as e:
        return "no"

def extract_pdf_content(df):
    """Extract PDF content and analyze for lots and visite information"""
    df_with_pdf = df.copy()
    df_with_pdf['generated_link'] = ""
    df_with_pdf['pdf_content'] = ""
    df_with_pdf['pdf_status'] = ""
    df_with_pdf['pages_extracted'] = 0
    df_with_pdf['lot_numbers'] = ""
    df_with_pdf['visite_obligatoire'] = ""
    df_with_pdf['keywords_used'] = ""
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    results_placeholder = st.empty()
    
    total_records = len(df_with_pdf)
    successful = 0
    errors = 0
    
    results_data = []
    
    for index, row in df_with_pdf.iterrows():
        dateparution_str = row.get('dateparution')
        idweb = row.get('idweb', 'N/A')
        keywords_from_row = row.get('keyword', '')
        
        # Update progress
        progress = (index + 1) / total_records
        progress_bar.progress(progress)
        status_text.text(f"Processing {index + 1}/{total_records}: {idweb}")
        
        if idweb == 'N/A':
            df_with_pdf.at[index, 'pdf_status'] = "Skipped - No ID"
            errors += 1
            continue
            
        try:
            # Parse date
            if isinstance(dateparution_str, str):
                date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']
                dateparution = None
                for fmt in date_formats:
                    try:
                        dateparution = datetime.strptime(dateparution_str, fmt)
                        break
                    except ValueError:
                        continue
                if dateparution is None:
                    df_with_pdf.at[index, 'pdf_status'] = "Error - Date parsing failed"
                    errors += 1
                    continue
            else:
                dateparution = dateparution_str
            
            # Generate link
            link = f"https://www.boamp.fr/telechargements/FILES/PDF/{dateparution.year}/{dateparution.month:02d}/{idweb}.pdf"
            
            # Add link to DataFrame
            df_with_pdf.at[index, 'generated_link'] = link
            
            # Store keywords used for this row
            df_with_pdf.at[index, 'keywords_used'] = str(keywords_from_row)
            
            # Download and extract PDF content
            try:
                # Download the PDF
                response = requests.get(link, timeout=30)
                response.raise_for_status()
                
                # Save to temporary file
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                    temp_file.write(response.content)
                    temp_path = temp_file.name
                
                # Extract text using PyPDF2
                with open(temp_path, 'rb') as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    
                    # Extract text from each page
                    full_text = ""
                    for page_num, page in enumerate(pdf_reader.pages):
                        page_text = page.extract_text()
                        full_text += f"Page {page_num + 1}:\n{page_text}\n\n"
                    
                    # Add PDF content to DataFrame
                    df_with_pdf.at[index, 'pdf_content'] = full_text
                    df_with_pdf.at[index, 'pages_extracted'] = len(pdf_reader.pages)
                    df_with_pdf.at[index, 'pdf_status'] = "Success"
                    
                    # Extract keywords from the row (could be string or list)
                    if isinstance(keywords_from_row, str):
                        # Split by semicolon if it's a combined string from deduplication
                        search_keywords = [k.strip() for k in keywords_from_row.split(';') if k.strip()]
                    else:
                        search_keywords = [str(keywords_from_row)]
                    
                    # Search for lot numbers
                    lot_results = search_keywords_and_find_lot(full_text, search_keywords)
                    if lot_results:
                        unique_lots = set()
                        for result in lot_results:
                            unique_lots.add(f"lot-{result['lot_number']}")
                        df_with_pdf.at[index, 'lot_numbers'] = ', '.join(sorted(unique_lots))
                    
                    # Check for visite obligatoire
                    visite_keywords = ["obligatoires", "obligatoire"]
                    visite_result = check_visite_obligatoire(full_text, visite_keywords)
                    df_with_pdf.at[index, 'visite_obligatoire'] = visite_result
                    
                    successful += 1
                
                # Clean up
                os.unlink(temp_path)
                
                # Update results in real-time
                results_data.append({
                    'ID': idweb,
                    'Status': '✅ Success',
                    'Pages': len(pdf_reader.pages),
                    'Lots Found': df_with_pdf.at[index, 'lot_numbers'] or 'None',
                    'Visite': df_with_pdf.at[index, 'visite_obligatoire'],
                    'Link': link
                })
                
            except Exception as e:
                error_msg = f"Error processing PDF: {str(e)}"
                df_with_pdf.at[index, 'pdf_content'] = error_msg
                df_with_pdf.at[index, 'pdf_status'] = f"Error: {str(e)}"
                errors += 1
                
                results_data.append({
                    'ID': idweb,
                    'Status': '❌ Error',
                    'Pages': 0,
                    'Lots Found': 'N/A',
                    'Visite': 'N/A',
                    'Link': link
                })
            
            # Add a small delay to be respectful to the server
            time.sleep(0.5)
            
        except Exception as e:
            error_msg = f"Error processing row: {str(e)}"
            df_with_pdf.at[index, 'pdf_content'] = error_msg
            df_with_pdf.at[index, 'pdf_status'] = f"Error: {str(e)}"
            errors += 1
            continue
        
        # Update results table every 5 records or at the end
        if len(results_data) % 5 == 0 or index == total_records - 1:
            results_df = pd.DataFrame(results_data)
            with results_placeholder.container():
                st.subheader("Processing Results")
                st.dataframe(results_df, use_container_width=True)
                
                # Show summary
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Processed", f"{index + 1}/{total_records}")
                with col2:
                    st.metric("Successful", successful)
                with col3:
                    st.metric("Errors", errors)
                with col4:
                    lots_found = len([r for r in results_data if r['Lots Found'] != 'None' and r['Lots Found'] != 'N/A'])
                    st.metric("Lots Found", lots_found)
    
    progress_bar.empty()
    status_text.empty()
    
    return df_with_pdf

def validate_dataframe(df):
    """Validate that the uploaded DataFrame has required columns"""
    required_columns = ['dateparution', 'idweb']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"
    
    return True, "DataFrame is valid"

def remove_duplicates(df, id_column, keyword_column):
    """Remove duplicates from DataFrame by combining keywords"""
    # Group by ID and combine keywords
    def combine_keywords(group):
        if len(group) > 1:
            # Combine keywords from all rows with the same ID
            combined_keywords = '; '.join(str(keyword) for keyword in group[keyword_column] if pd.notna(keyword) and str(keyword).strip())
            # Keep the first row but update the keyword column with combined values
            first_row = group.iloc[0].copy()
            first_row[keyword_column] = combined_keywords
            return first_row
        else:
            return group.iloc[0]
    
    # Apply the combination logic
    df_clean = df.groupby(id_column).apply(combine_keywords).reset_index(drop=True)
    return df_clean

def get_predefined_keywords():
    """Return predefined keywords for filtering"""
    return [
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
        "45420000", "45421100", "45421110", "45421111", "45421112", "45421120", 
        "45421130", "45421131", "45421132", "45421140", "45421141", "45421142", 
        "45421143", "45421144", "45421145", "44316500", "98395000", "44220000", 
        "45421000", "34928200", "34928310", "45340000", "45342000", "42416000", 
        "42416400", "42419500", "42419530", "44233000", "44423220", "45313000", 
        "45313200", "50740000", "51511000",
    ]

def render_data_extraction_tab():
    """Render the Data Extraction tab"""
    st.header("Data Extraction")
    
    if st.button("🚀 Extract Data", type="primary"):
        target_date_str = st.session_state.target_date
        max_records = 10000
        
        with st.spinner(f"Extracting data for {target_date_str}..."):
            all_records = get_all_records_for_date(target_date_str, max_records)

        if all_records:
            st.success(f"✅ Found {len(all_records)} records for date {target_date_str}")
            
            # Create DataFrame
            df = create_excel_simple(all_records, target_date_str)
            
            # Store in session state
            st.session_state.df = df
            st.session_state.records = all_records
            

            # Download button for raw data
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            
            st.download_button(
                label=f"📥 Download all boamp data {target_date_str}",
                data=excel_buffer,
                file_name=f"BOAMP data {target_date_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        else:
            st.error("❌ No records found for the specified date.")

    
    # Predefined keywords
    predefined_keywords = get_predefined_keywords()

    # Keyword selection
    st.subheader("Select Keywords")
    selected_keywords = st.multiselect(
        "Choose keywords to filter by:",
        options=predefined_keywords,
        default=predefined_keywords[:-1],
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

    if st.button("🔍 Apply Keyword Filter", type="primary") and st.session_state.df is not None:
        if all_keywords:
            with st.spinner("Filtering data by keywords..."):
                filtered_df = filter_by_keywords(st.session_state.df, all_keywords)
            
            if not filtered_df.empty:
                st.session_state.filtered_df = filtered_df
                st.success(f"✅ Found {len(filtered_df)} matching records")
                

                
                # Download button for filtered data
                excel_buffer = io.BytesIO()
                filtered_df.to_excel(excel_buffer, index=False, engine='openpyxl')
                excel_buffer.seek(0)
                
                
            else:
                st.session_state.filtered_df = None
                st.warning("⚠️ No matches found for the selected keywords.")
        else:
            st.warning("Please select at least one keyword.")

        
    # Only show duplicate removal if we have a valid filtered_df
    if st.session_state.filtered_df is not None and not st.session_state.filtered_df.empty:
        st.subheader("Remove Duplicates")
        
        # Safely get column options
        available_columns = st.session_state.filtered_df.columns.tolist()
        
        id_column = available_columns[0]
        
        
        # Let user select the keyword column (assuming it's the last column by default)
        keyword_column = available_columns[-1]
        
        
        # Create a copy to avoid modifying the original during processing
        df = st.session_state.filtered_df.copy()
        
        # Remove duplicates
        df_clean = remove_duplicates(df, id_column, keyword_column)
        
        st.session_state.cleaned_df = df_clean
        
        st.success(f"✅ Removed duplicates! Kept {len(df_clean)} unique rows out of {len(st.session_state.filtered_df)} total.")
        
        # Show some combined keywords as examples
        duplicate_ids = df[df.duplicated(subset=[id_column], keep=False)][id_column].unique()
        if len(duplicate_ids) > 0:
            st.info(f"Found {len(duplicate_ids)} IDs with duplicates. Keywords have been combined.")
            
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


def render_results_tab():
    """Render the Results tab"""
    st.header("Tableau des Appels d'Offres en Cours")
    
    if st.session_state.df is not None:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Records from boamp", len(st.session_state.df))
        
        with col3:
            if st.session_state.cleaned_df is not None:
                st.metric("Records Filtred", len(st.session_state.cleaned_df))
            else:
                st.metric("Records Filtred", "Not cleaned")
        
        # Determine which dataset to show
        if st.session_state.cleaned_df is not None:
            display_df = st.session_state.cleaned_df
            data_source = "Cleaned Data (After Deduplication)"
        elif st.session_state.filtered_df is not None:
            display_df = st.session_state.filtered_df
            data_source = "Filtered Data"
        else:
            display_df = st.session_state.df
            data_source = "Raw Data"
        
        st.info(f"Showing data from: **{data_source}**")
        
        # Create the table with the desired columns
        if len(display_df) > 0:
            # Create a copy for display with proper column mapping
            table_df = display_df.copy()
            
            # Map your existing columns to the desired table structure
            # You may need to adjust these mappings based on your actual data structure
            
            # Create the display table with the specific columns you want
            result_table = pd.DataFrame({
                "keywods": table_df.get('keyword', 'N/A'),
                'IDENTIFICATION ACHETEUR': table_df.get('nomacheteur', 'N/A'),
                'OBJET CONSULTATION': table_df.get('objet', 'N/A'),
                'LOTS D\'INTÉRÊT': table_df.get('objet', 'N/A'),  # You may need to calculate this
                'VISITE DE SITE': table_df.get('objet', 'N/A'),  # You may need to extract this from your data
                'DATE LIMITE': table_df.get('datelimitereponse', 'Pas Mentionné'),
            })
            
            # Display the table
            st.subheader("Appels d'Offres Actifs")
            st.dataframe(result_table, use_container_width=True, hide_index=True)
            
            # Add download button for the table
            csv = result_table.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Télécharger le tableau en CSV",
                data=csv,
                file_name="appels_offres_en_cours.csv",
                mime="text/csv"
            )
            
        else:
            st.warning("Aucun enregistrement disponible à afficher.")
    else:
        st.info("Aucune donnée extraite pour le moment. Veuillez utiliser l'onglet Extraction de données d'abord.")

def render_upload_tab():

    
    if st.session_state.cleaned_df is not None:
        df = st.session_state.cleaned_df
        st.session_state.uploaded_df = df
        
        st.success(f"✅ Using cleaned data from extraction tab! Found {len(df)} records.")
        
        # Show preview
        st.subheader("Data Preview")
        st.dataframe(df.head(), use_container_width=True)
        
        # Show basic statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            st.metric("Columns", len(df.columns))
        with col3:
            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Show column information
        with st.expander("📋 Column Details"):
            columns_info = pd.DataFrame({
                'Column Name': df.columns.tolist(),
                'Data Type': df.dtypes.astype(str).tolist(),
                'Non-Null Count': df.notna().sum().tolist(),
                'Null Count': df.isna().sum().tolist()
            })
            st.dataframe(columns_info, use_container_width=True)
            
        # Show which data source is being used
        if st.session_state.filtered_df is not None and len(df) == len(st.session_state.filtered_df):
            st.info("📝 Currently using filtered data (before deduplication)")
        else:
            st.info("🧹 Currently using cleaned data (after deduplication)")
            
    else:
        st.warning("""
        ⚠️ No processed data available from the extraction tab. 
        
        Please go to the **Data Extraction** tab first and:
        1. Extract data for a specific date
        2. Apply keyword filtering (optional)
        3. Remove duplicates (optional)
        
        Then return here to use the processed data for PDF extraction.
        """)

def render_process_pdfs_tab():
    """Render the Process PDFs tab"""
    st.header("Process PDF Content")
    
    if st.session_state.uploaded_df is not None:
        df = st.session_state.uploaded_df
        
        st.info(f"Ready to process **{len(df)}** records. This may take several minutes.")
        
        # Configuration options
        st.subheader("Processing Options")
        col1, col2 = st.columns(2)
        
        with col1:
            delay = st.slider(
                "Delay between requests (seconds)",
                min_value=0.1,
                max_value=2.0,
                value=0.5,
                step=0.1,
                help="Slower delays are more respectful to the server"
            )
        
        with col2:
            timeout = st.slider(
                "Request timeout (seconds)",
                min_value=10,
                max_value=60,
                value=30,
                step=5,
                help="Timeout for PDF download requests"
            )
        
        if st.button("🚀 Start PDF Processing", type="primary", use_container_width=True):
            with st.spinner("Starting PDF content extraction..."):
                processed_df = extract_pdf_content(df)
                st.session_state.processed_df = processed_df
            
            st.success("✅ PDF processing completed!")
            
            # Show final summary
            successful = processed_df[processed_df['pdf_status'] == 'Success'].shape[0]
            errors = processed_df[processed_df['pdf_status'].str.contains('Error', na=False)].shape[0]
            skipped = processed_df[processed_df['pdf_status'].str.contains('Skipped', na=False)].shape[0]
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Processed", len(processed_df))
            with col2:
                st.metric("Successful", successful)
            with col3:
                st.metric("Errors", errors)
            with col4:
                st.metric("Skipped", skipped)
    
    else:
        st.warning("⚠️ Please select a data source first in the 'Upload File' tab.")

def render_results_download_tab():
    """Render the Results & Download tab"""
    st.header("Results & Download")
    
    if st.session_state.processed_df is not None:
        processed_df = st.session_state.processed_df
        
        st.success(f"Processing completed for {len(processed_df)} records!")
        
        # Summary statistics
        st.subheader("Processing Summary")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            successful = processed_df[processed_df['pdf_status'] == 'Success'].shape[0]
            st.metric("Successful Extractions", successful)
        with col2:
            errors = processed_df[processed_df['pdf_status'].str.contains('Error', na=False)].shape[0]
            st.metric("Errors", errors)
        with col3:
            skipped = processed_df[processed_df['pdf_status'].str.contains('Skipped', na=False)].shape[0]
            st.metric("Skipped", skipped)
        with col4:
            total_pages = processed_df['pages_extracted'].sum()
            st.metric("Total Pages", total_pages)
        with col5:
            lots_found = processed_df[processed_df['lot_numbers'] != ''].shape[0]
            st.metric("Lots Found", lots_found)
        
        # Show sample of processed data
        st.subheader("Processed Data Preview")
        
        # Select columns to display
        display_columns = [ 'objet', 'keyword', 'lot_numbers', 'visite_obligatoire','code_departement','datelimitereponse','nomacheteur']
        available_columns = [col for col in display_columns if col in processed_df.columns]
        
        st.dataframe(processed_df[available_columns].head(-1), use_container_width=True)
        
        # Lots and Visite Analysis
        st.subheader("Lots and Visite Analysis")
        
        if 'lot_numbers' in processed_df.columns and 'visite_obligatoire' in processed_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Lots analysis
                lots_data = processed_df[processed_df['lot_numbers'] != '']
                st.metric("Records with Lots Found", len(lots_data))
                if len(lots_data) > 0:
                    with st.expander("View Records with Lots"):
                        st.dataframe(lots_data[[ 'objet', 'keyword', 'lot_numbers']], use_container_width=True)
            
            with col2:
                # Visite analysis
                visite_data = processed_df[processed_df['visite_obligatoire'] == 'yes']
                st.metric("Records with Visite Obligatoire", len(visite_data))
                if len(visite_data) > 0:
                    with st.expander("View Records with Visite Obligatoire"):
                        st.dataframe(visite_data[[ 'objet', 'keyword', 'visite_obligatoire']], use_container_width=True)
        
        # PDF Content Samples
        st.subheader("PDF Content Samples")
        
        successful_records = processed_df[processed_df['pdf_status'] == 'Success']
        
        if len(successful_records) > 0:
            for i, (index, row) in enumerate(successful_records.head(-1).iterrows(), 1):
                with st.expander(f"Sample {i} - objet: {row.get('objet', 'N/A')} "):
                    st.write(f"**pdf link ** [{row.get('generated_link', 'N/A')}]({row.get('generated_link', '')})")
                    st.write(f"**Achteur:** {row.get('nomacheteur', 'N/A')}")
                    st.write(f"**Keywords Used:** {row.get('keywords_used', 'N/A')}")
                    st.write(f"**Lots Found:** {row.get('lot_numbers', 'None')}")
                    st.write(f"**Visite Obligatoire:** {row.get('visite_obligatoire', 'no')}")
                    st.write("**PDF Content Preview:**")
                    pdf_content = row.get('pdf_content', '')
                    if pdf_content:
                        # Show first 1000 characters
                        preview = pdf_content[:5000] + "..." if len(pdf_content) > 5000 else pdf_content
                        st.text_area(
                            f"Content Preview {i}",
                            preview,
                            height=300,
                            key=f"preview_{i}"
                        )
        else:
            st.warning("No successful PDF extractions to display.")
        
        # Error samples
        error_records = processed_df[processed_df['pdf_status'].str.contains('Error', na=False)]
        if len(error_records) > 0:
            with st.expander("View Errors"):
                st.dataframe(error_records[['idweb', 'generated_link', 'pdf_status']].head(10), use_container_width=True)
        
        # Download Section
        st.subheader("📥 Download Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Download full data with PDF content
            excel_buffer = io.BytesIO()
            processed_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            
            st.download_button(
                label="💾 Download Full Data (Excel)",
                data=excel_buffer,
                file_name=f"BOAMP_PDF_Extraction_Full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        with col2:
            # Download only successful extractions
            successful_df = processed_df[processed_df['pdf_status'] == 'Success']
            if len(successful_df) > 0:
                excel_buffer_success = io.BytesIO()
                successful_df.to_excel(excel_buffer_success, index=False, engine='openpyxl')
                excel_buffer_success.seek(0)
                

            else:
                st.warning("No successful extractions to download")
        
        # Download as CSV option
        
        csv_col1, csv_col2 = st.columns(2)
        
        with csv_col1:
            csv_buffer = io.BytesIO()
            # For CSV, we might want to exclude the large pdf_content column or handle it differently
            csv_columns = [col for col in processed_df.columns if col != 'pdf_content']
            processed_df[csv_columns].to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_buffer.seek(0)
            

    
    else:
        st.info("👆 Process your PDFs in the 'Process PDFs' tab to see results and download options.")

def main():
    st.set_page_config(page_title="BOAMP Data Extractor", page_icon="📊", layout="wide")
    
    st.title("📊 BOAMP Data Extractor")
    st.markdown("Extract public procurement data from BOAMP API")

    # Initialize session state
    initialize_session_state()

    # Sidebar for configuration
    st.sidebar.header("Configuration")

    # Date input
    target_date = st.sidebar.date_input("Select target date", value=datetime.today())
    target_date_str = target_date.strftime('%Y-%m-%d')
    st.session_state.target_date = target_date_str

    # Max records
    max_records = st.sidebar.number_input("Maximum records", min_value=100, max_value=10000, value=10000)

    # Main content area with tabs
    tab1, tab3,  tab5, tab6 = st.tabs(["Data Extraction", "  ",  "🔗 Process PDFs", "📊 Results & Download"])

    with tab1:
        render_data_extraction_tab()

    with tab3:
        render_results_tab()
        render_upload_tab()

        

    with tab5:
        render_process_pdfs_tab()

    with tab6:
        render_results_download_tab()

if __name__ == "__main__":
    main()