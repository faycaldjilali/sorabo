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

st.set_page_config(page_title="BOAMP PDF Extractor", page_icon="📄", layout="wide")

def extract_pdf_content(df):
    """Extract PDF content for each record in the DataFrame"""
    df_with_pdf = df.copy()
    df_with_pdf['generated_link'] = ""
    df_with_pdf['pdf_content'] = ""
    df_with_pdf['pdf_status'] = ""
    df_with_pdf['pages_extracted'] = 0
    
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
                    successful += 1
                
                # Clean up
                os.unlink(temp_path)
                
                # Update results in real-time
                results_data.append({
                    'ID': idweb,
                    'Status': '✅ Success',
                    'Pages': len(pdf_reader.pages),
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
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Processed", f"{index + 1}/{total_records}")
                with col2:
                    st.metric("Successful", successful)
                with col3:
                    st.metric("Errors", errors)
    
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

def main():
    st.title("📄 BOAMP PDF Content Extractor")
    st.markdown("Upload an Excel file with BOAMP data to extract PDF content from generated links")
    
    # Initialize session state
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None
    if 'uploaded_df' not in st.session_state:
        st.session_state.uploaded_df = None
    
    # Main tabs
    tab1, tab2, tab3 = st.tabs(["📤 Upload File", "🔗 Process PDFs", "📊 Results & Download"])
    
    with tab1:
        st.header("Upload Excel File")
        st.info("""
        **Required columns in your Excel file:**
        - `dateparution`: Publication date (YYYY-MM-DD format)
        - `idweb`: Unique identifier for each record
        """)
        
        uploaded_file = st.file_uploader(
            "Choose an Excel file", 
            type=['xlsx', 'xls'],
            help="Upload an Excel file containing BOAMP data with required columns"
        )
        
        if uploaded_file is not None:
            try:
                # Read the Excel file
                df = pd.read_excel(uploaded_file)
                st.session_state.uploaded_df = df
                
                # Validate the DataFrame
                is_valid, message = validate_dataframe(df)
                
                if is_valid:
                    st.success(f"✅ File uploaded successfully! Found {len(df)} records.")
                    
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
                        
                else:
                    st.error(f"❌ {message}")
                    st.stop()
                    
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
                st.stop()
    
    with tab2:
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
                    # Update the extract_pdf_content function with user settings
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
            st.warning("⚠️ Please upload an Excel file first in the 'Upload File' tab.")
    
    with tab3:
        st.header("Results & Download")
        
        if st.session_state.processed_df is not None:
            processed_df = st.session_state.processed_df
            
            st.success(f"Processing completed for {len(processed_df)} records!")
            
            # Summary statistics
            st.subheader("Processing Summary")
            
            col1, col2, col3, col4 = st.columns(4)
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
            
            # Show sample of processed data
            st.subheader("Processed Data Preview")
            
            # Select columns to display
            display_columns = ['idweb', 'objet', 'generated_link', 'pdf_status', 'pages_extracted']
            available_columns = [col for col in display_columns if col in processed_df.columns]
            
            st.dataframe(processed_df[available_columns].head(10), use_container_width=True)
            
            # PDF Content Samples
            st.subheader("PDF Content Samples")
            
            successful_records = processed_df[processed_df['pdf_status'] == 'Success']
            
            if len(successful_records) > 0:
                for i, (index, row) in enumerate(successful_records.head(3).iterrows(), 1):
                    with st.expander(f"Sample {i} - ID: {row.get('idweb', 'N/A')} ({row.get('pages_extracted', 0)} pages)"):
                        st.write(f"**Generated Link:** [{row.get('generated_link', 'N/A')}]({row.get('generated_link', '')})")
                        st.write(f"**Object:** {row.get('objet', 'N/A')}")
                        st.write("**PDF Content Preview:**")
                        pdf_content = row.get('pdf_content', '')
                        if pdf_content:
                            # Show first 1000 characters
                            preview = pdf_content[:1000] + "..." if len(pdf_content) > 1000 else pdf_content
                            st.text_area(
                                f"Content Preview {i}",
                                preview,
                                height=200,
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
                    
                    st.download_button(
                        label="✅ Download Successful Only (Excel)",
                        data=excel_buffer_success,
                        file_name=f"BOAMP_PDF_Extraction_Successful_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.warning("No successful extractions to download")
            
            # Download as CSV option
            st.subheader("CSV Export Options")
            
            csv_col1, csv_col2 = st.columns(2)
            
            with csv_col1:
                csv_buffer = io.BytesIO()
                # For CSV, we might want to exclude the large pdf_content column or handle it differently
                csv_columns = [col for col in processed_df.columns if col != 'pdf_content']
                processed_df[csv_columns].to_csv(csv_buffer, index=False, encoding='utf-8')
                csv_buffer.seek(0)
                
                st.download_button(
                    label="📊 Download as CSV (No PDF Content)",
                    data=csv_buffer,
                    file_name=f"BOAMP_PDF_Extraction_Summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        else:
            st.info("👆 Process your PDFs in the 'Process PDFs' tab to see results and download options.")

if __name__ == "__main__":
    main()