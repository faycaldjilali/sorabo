import streamlit as st
import pandas as pd
from datetime import datetime
import sys
import os
from src.data_fetcher import get_all_records_for_date, create_excel_simple
from src.data_processor import filter_by_keywords, remove_duplicates, get_keywords
from src.utils import load_excel_files
# Add src to path



def main():
    st.set_page_config(page_title="BOAMP Data Processor", layout="wide")
    
    st.title("📊 BOAMP Data Processor")
    st.markdown("---")
    
    # Sidebar navigation
    page = st.sidebar.selectbox(
        "Navigation",
        ["Data Fetching", "Keyword Filtering", "Duplicate Removal", "File Info"]
    )
    
    if page == "Data Fetching":
        data_fetching_page()
    elif page == "Keyword Filtering":
        keyword_filtering_page()
    elif page == "Duplicate Removal":
        duplicate_removal_page()
    elif page == "File Info":
        file_info_page()

def data_fetching_page():
    st.header("📥 Data Fetching from BOAMP API")
    
    col1, col2 = st.columns(2)
    
    with col1:
        target_date = st.date_input("Target Date", value=datetime(2025, 10, 29))
        max_records = st.number_input("Max Records", min_value=100, max_value=10000, value=5000)
    
    with col2:
        st.info("""
        This will fetch data from the BOAMP API for the specified date.
        The data will be saved as an Excel file.
        """)
    
    if st.button("Fetch Data", type="primary"):
        with st.spinner("Fetching data from BOAMP API..."):
            try:
                target_date_str = target_date.strftime('%Y-%m-%d')
                all_records = get_all_records_for_date(target_date_str, max_records)
                
                if all_records:
                    excel_filename, df = create_excel_simple(all_records, target_date_str)
                    
                    st.success(f"✅ Data fetched successfully!")
                    st.info(f"""
                    - **Records Found**: {len(all_records)}
                    - **Columns**: {len(df.columns)}
                    - **File Saved**: {excel_filename}
                    """)
                    
                    # Show sample data
                    st.subheader("Sample Data (First 3 Records)")
                    sample_df = pd.DataFrame(all_records[:3])
                    st.dataframe(sample_df[['objet', 'nomacheteur', 'procedure_libelle', 'dateparution']])
                    
                else:
                    st.warning("❌ No records found for the specified date.")
                    
            except Exception as e:
                st.error(f"Error fetching data: {str(e)}")

def keyword_filtering_page():
    st.header("🔍 Keyword Filtering")
    
    uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx'])
    
    if uploaded_file is not None:
        # Save uploaded file temporarily
        file_path = f"temp_{uploaded_file.name}"
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        # Get keywords
        keywords = get_keywords()
        
        st.subheader("Keywords to Search")
        st.write(", ".join([k for k in keywords if not k.isdigit()][:10]) + "...")
        
        if st.button("Filter by Keywords", type="primary"):
            with st.spinner("Filtering data..."):
                try:
                    all_matches = filter_by_keywords(file_path, keywords)
                    
                    if not all_matches.empty:
                        output_filename = f"BOAMP_FILTERED_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                        all_matches.to_excel(output_filename, index=False)
                        
                        st.success(f"✅ Filtering complete!")
                        st.info(f"**Matches Found**: {len(all_matches)}")
                        
                        # Download button
                        with open(output_filename, "rb") as file:
                            st.download_button(
                                label="Download Filtered Data",
                                data=file,
                                file_name=output_filename,
                                mime="application/vnd.ms-excel"
                            )
                        
                        # Show preview
                        st.subheader("Preview of Filtered Data")
                        st.dataframe(all_matches.head())
                    else:
                        st.warning("❌ No matches found for any keyword.")
                        
                except Exception as e:
                    st.error(f"Error during filtering: {str(e)}")
        
        # Clean up temp file
        if os.path.exists(file_path):
            os.remove(file_path)

def duplicate_removal_page():
    st.header("🧹 Duplicate Removal")
    
    uploaded_file = st.file_uploader("Upload Excel File for Duplicate Removal", type=['xlsx'])
    
    if uploaded_file is not None:
        col1, col2 = st.columns(2)
        
        with col1:
            id_column = st.text_input("ID Column Name", value="id")
        
        with col2:
            st.info("Specify the column name that contains unique IDs")
        
        if st.button("Remove Duplicates", type="primary"):
            with st.spinner("Removing duplicates..."):
                try:
                    # Save uploaded file temporarily
                    file_path = f"temp_dup_{uploaded_file.name}"
                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    df_clean = remove_duplicates(file_path, id_column)
                    
                    output_filename = f"CLEANED_{uploaded_file.name}"
                    df_clean.to_excel(output_filename, index=False)
                    
                    st.success(f"✅ Duplicates removed!")
                    st.info(f"""
                    - **Original Rows**: {pd.read_excel(file_path).shape[0]}
                    - **After Cleaning**: {len(df_clean)}
                    - **Duplicates Removed**: {pd.read_excel(file_path).shape[0] - len(df_clean)}
                    """)
                    
                    # Download button
                    with open(output_filename, "rb") as file:
                        st.download_button(
                            label="Download Cleaned Data",
                            data=file,
                            file_name=output_filename,
                            mime="application/vnd.ms-excel"
                        )
                    
                    # Clean up temp file
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        
                except Exception as e:
                    st.error(f"Error removing duplicates: {str(e)}")

def file_info_page():
    st.header("📁 File Information")
    
    st.info("This section would display information about your existing Excel files")
    
    # You can modify this to allow file uploads or specify paths
    file_paths = st.text_area(
        "Enter file paths (one per line)",
        placeholder="D:\\sorabo\\sorabo\\BOAMP_2025-10-31_ALL_KEYWORDS2.xlsx\nD:\\sorabo\\sorabo\\data\\octobre.xlsx",
        height=100
    )
    
    if st.button("Load File Info"):
        if file_paths:
            paths = [path.strip() for path in file_paths.split('\n') if path.strip()]
            results = []
            
            for file_path in paths:
                try:
                    df = pd.read_excel(file_path)
                    results.append({
                        "File": file_path,
                        "Rows": len(df),
                        "Columns": len(df.columns),
                        "Status": "✅ Loaded"
                    })
                except Exception as e:
                    results.append({
                        "File": file_path,
                        "Rows": 0,
                        "Columns": 0,
                        "Status": f"❌ Error: {str(e)}"
                    })
            
            st.table(pd.DataFrame(results))

if __name__ == "__main__":
    main()