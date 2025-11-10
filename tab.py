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

    # Initialize session state
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None
    if 'uploaded_df' not in st.session_state:
        st.session_state.uploaded_df = None
    
    # Main tabs
    