import streamlit as st
import os
import tempfile
from typing import List, Dict, Any, Optional, Callable


def render_header():
    """Render the application header."""
    st.title("🛒 Shopping Scraper Data Import")
    st.write("Upload Excel files from shopping scraper and import them into the database.")


def render_campaign_selection(campaigns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Render the campaign selection component.
    
    Args:
        campaigns: List of campaign dictionaries
        
    Returns:
        Selected campaign or None
    """
    st.subheader("Select Campaign")
    
    if not campaigns:
        st.warning("No campaigns found in the database.")
        return None
    
    # Create display names for each campaign
    campaign_options = [f"{c['domain_name']} (ID: {c['campaign_id']})" for c in campaigns]
    
    # Create selectbox
    selected_idx = st.selectbox(
        "Select campaign:",
        range(len(campaign_options)),
        format_func=lambda i: campaign_options[i]
    )
    
    # Get the selected campaign
    selected_campaign = campaigns[selected_idx]
    
    # Display campaign info
    st.write(f"Selected Campaign: **{selected_campaign['domain_name']}**")
    
    # Extract and display client info
    client_info = None
    if 'clients' in selected_campaign:
        client_info = selected_campaign['clients']
        if isinstance(client_info, dict) and 'name' in client_info:
            client_name = client_info['name']
            if 'surname' in client_info and client_info['surname']:
                client_name += f" {client_info['surname']}"
            st.write(f"Client: **{client_name}**")
    
    return selected_campaign


def render_scrape_type_selection(scrape_types: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Render the scrape type selection component.
    
    Args:
        scrape_types: List of scrape type dictionaries
        
    Returns:
        Selected scrape type or None
    """
    st.subheader("Select Scrape Type")
    
    if not scrape_types:
        st.warning("No scrape types found in the database.")
        return None
    
    # Create display names
    scrape_type_options = [t['name'] for t in scrape_types]
    
    # Create selectbox
    selected_idx = st.selectbox(
        "Select scrape type:",
        range(len(scrape_type_options)),
        format_func=lambda i: scrape_type_options[i]
    )
    
    # Get the selected scrape type
    selected_scrape_type = scrape_types[selected_idx]
    
    return selected_scrape_type


def render_file_upload(process_callback: Callable[[str, int, int], Dict[str, Any]], 
                        campaign_id: int, scrape_type_id: int):
    """
    Render the file upload component.
    
    Args:
        process_callback: Function to call to process the file
        campaign_id: ID of the selected campaign
        scrape_type_id: ID of the selected scrape type
    """
    st.subheader("Upload Excel File")
    
    uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])
    
    if uploaded_file is not None:
        # Show file info
        st.write(f"File: {uploaded_file.name}")
        
        file_details = {
            "Filename": uploaded_file.name,
            "FileType": uploaded_file.type,
            "FileSize": f"{uploaded_file.size / 1024:.2f} KB"
        }
        st.write(file_details)
        
        # Process button
        if st.button("Process File"):
            with st.spinner('Processing file...'):
                # Save uploaded file to a temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(uploaded_file.getbuffer())
                    temp_path = tmp_file.name
                
                try:
                    # Process the file
                    stats = process_callback(temp_path, campaign_id, scrape_type_id)
                    
                    # Clean up temp file
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        st.warning(f"Note: Could not remove temporary file ({str(e)}). This won't affect your data import.")
                    
                    # Display results
                    st.success("File processed successfully!")
                    st.write(f"Keywords processed: {stats['keywords_processed']}")
                    st.write(f"Rows processed: {stats['rows_processed']}")
                    
                    if stats['errors']:
                        st.error("Some errors occurred during processing:")
                        for error in stats['errors'][:10]:  # Show first 10 errors
                            st.write(f"- {error}")
                        
                        if len(stats['errors']) > 10:
                            st.write(f"... and {len(stats['errors']) - 10} more errors.")
                
                except Exception as e:
                    st.error(f"Error processing file: {e}")
                    # Clean up temp file
                    try:
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)
                    except Exception:
                        pass


def render_footer():
    """Render the application footer."""
    st.markdown("---")
    st.markdown("© 2025 Calibre Nine | [GitHub Repository](https://github.com/chrisprideC9)")