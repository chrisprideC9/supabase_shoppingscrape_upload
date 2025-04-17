import streamlit as st
import pandas as pd
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables

#variablshit
load_dotenv()

# Configure logging
logging_level = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, logging_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import components
from database.connection import init_connection
from database.models import get_campaigns, get_scrape_types, batch_insert_scrape_data
from utils.data_processing import detect_and_transform_data
from ui.components import (
    render_header,
    render_campaign_selection,
    render_scrape_type_selection,
    render_file_upload,
    render_footer
)


def process_excel_file(file_path: str, campaign_id: int, scrape_type_id: int, force_upload: bool = False) -> dict:
    """
    Process an Excel file and insert the data into the database using batch processing.
    
    Args:
        file_path: Path to the Excel file
        campaign_id: ID of the campaign
        scrape_type_id: ID of the scrape type
        force_upload: Whether to force upload without checking for duplicates
        
    Returns:
        Statistics about the processing
    """
    logger.info(f"Processing Excel file for campaign ID: {campaign_id}, scrape type ID: {scrape_type_id}, force upload: {force_upload}")
    
    # Get database connection
    conn = init_connection()
    
    # Stats to return
    stats = {
        "keywords_processed": 0,
        "rows_processed": 0,
        "errors": []
    }
    
    # Batch size for database inserts
    BATCH_SIZE = 100
    
    try:
        # Load Excel file
        xl = pd.ExcelFile(file_path)
        
        # Get all sheet names
        all_sheets = xl.sheet_names
        
        # Determine which sheets to process based on scrape type
        sheets_to_process = []
        
        if scrape_type_id == 1:  # Products Scrape
            logger.info("Processing as Products Scrape - skipping non-data sheets")
            # Skip common non-data sheets
            skip_sheets = ["Keywords", "Aggregated Results", "Error Logs"]
            sheets_to_process = [sheet for sheet in all_sheets if sheet not in skip_sheets]
        elif scrape_type_id == 2:  # Shopping Tab Scrape
            logger.info("Processing as Shopping Tab Scrape - skipping non-data sheets")
            # Skip common non-data sheets
            skip_sheets = ["Keywords", "Aggregated Results", "Error Logs", "Output"]
            sheets_to_process = [sheet for sheet in all_sheets if sheet not in skip_sheets]
        else:
            # For other scrape types, use all sheets except common ones to skip
            skip_sheets = ["Keywords", "Aggregated Results", "Error Logs"]
            sheets_to_process = [sheet for sheet in all_sheets if sheet not in skip_sheets]
        
        logger.info(f"Found {len(sheets_to_process)} sheets to process: {', '.join(sheets_to_process)}")
        
        # Process each sheet (keyword)
        for sheet_name in sheets_to_process:
            try:
                logger.info(f"Processing sheet: {sheet_name}")
                
                # Read sheet data
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                if df.empty:
                    logger.warning(f"No data in sheet: {sheet_name}")
                    stats["errors"].append(f"No data in sheet: {sheet_name}")
                    continue
                
                # Determine the date for this sheet
                sheet_date = None
                if 'Date' in df.columns:
                    for _, row in df.iterrows():
                        if not pd.isna(row['Date']):
                            sheet_date = row['Date']
                            break
                
                if not sheet_date:
                    sheet_date = datetime.now()
                    logger.info(f"No valid date found in sheet, using current date")
                
                # Transform data based on its format
                transformed_records = detect_and_transform_data(
                    df,
                    sheet_name,
                    sheet_date,
                    campaign_id,
                    scrape_type_id
                )
                
                if not transformed_records:
                    logger.warning(f"No valid records found in sheet: {sheet_name}")
                    stats["errors"].append(f"No valid records found in sheet: {sheet_name}")
                    continue
                
                # Insert records in batches
                total_inserted = 0
                for i in range(0, len(transformed_records), BATCH_SIZE):
                    batch = transformed_records[i:i+BATCH_SIZE]
                    try:
                        # Pass the force_upload parameter to the batch insert function
                        batch_insert_scrape_data(conn, batch, force_upload)
                        total_inserted += len(batch)
                        logger.debug(f"Inserted batch of {len(batch)} records for sheet: {sheet_name}")
                    except Exception as e:
                        error_msg = f"Error inserting batch for sheet {sheet_name}: {str(e)}"
                        logger.error(error_msg, exc_info=True)
                        stats["errors"].append(error_msg)
                
                logger.info(f"Processed {total_inserted} rows from sheet: {sheet_name}")
                stats["rows_processed"] += total_inserted
                stats["keywords_processed"] += 1
                
            except Exception as e:
                error_msg = f"Error processing sheet {sheet_name}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                stats["errors"].append(error_msg)
        
        return stats
        
    except Exception as e:
        error_msg = f"Error processing file: {str(e)}"
        logger.error(error_msg, exc_info=True)
        stats["errors"].append(error_msg)
        return stats


def main():
    """Main application entry point."""
    st.set_page_config(
        page_title="Shopping Scraper Data Import",
        page_icon="🛒",
        layout="centered"
    )
    
    # Render header
    render_header()
    
    # Initialize PostgreSQL connection
    try:
        conn = init_connection()
        st.success("Connected to PostgreSQL database!")
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.stop()
    
    try:
        # Get campaigns
        campaigns = get_campaigns(conn)
        
        # Get scrape types
        scrape_types = get_scrape_types(conn)
        
        # Campaign selection
        selected_campaign = render_campaign_selection(campaigns)
        
        if selected_campaign:
            # Scrape type selection
            selected_scrape_type = render_scrape_type_selection(scrape_types)
            
            if selected_scrape_type:
                # File upload
                render_file_upload(
                    process_excel_file,
                    selected_campaign['campaign_id'],
                    selected_scrape_type['id']
                )
    
    except Exception as e:
        st.error(f"An error occurred: {e}")
        logger.error(f"Application error: {e}", exc_info=True)
    
    # Render footer
    render_footer()


if __name__ == "__main__":
    main()