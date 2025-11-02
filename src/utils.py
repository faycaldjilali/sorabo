import streamlit as st
import pandas as pd

def display_records_info(records, df):
    """Display information about the extracted records"""
    if records:
        st.success(f"✅ Found {len(records)} records")
        st.info(f"📊 Total columns: {len(df.columns)}")
        
        # Show column names
        with st.expander("📋 Columns in Excel file"):
            for i, col in enumerate(df.columns, 1):
                st.write(f"{i:2d}. {col}")

        # Display sample data
        with st.expander("📄 SAMPLE DATA (first 3 records)"):
            for i, record in enumerate(records[:3], 1):
                st.write(f"**Record {i}:**")
                st.write(f"  **Title:** {record.get('objet', 'N/A')[:80]}...")
                st.write(f"  **Buyer:** {record.get('nomacheteur', 'N/A')}")
                st.write(f"  **Procedure:** {record.get('procedure_libelle', 'N/A')}")
                st.write(f"  **Date:** {record.get('dateparution', 'N/A')}")
                st.write("-" * 40)



import pandas as pd

def load_excel_files(file_paths):
    """Load multiple Excel files and display their info"""
    for file in file_paths:
        try:
            df = pd.read_excel(file)
            print(f"📘 {file} → {len(df)} rows")
        except FileNotFoundError:
            print(f"❌ File not found: {file}")
        except Exception as e:
            print(f"⚠️ Error reading {file}: {e}")