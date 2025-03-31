import pandas as pd
import numpy as np
import uuid
import logging
import re
from datetime import datetime
from typing import Optional, Any, Dict, List, Union

logger = logging.getLogger(__name__)

def parse_date(date_value: Any) -> Optional[datetime]:
    """
    Parse a date value from various formats into a datetime object.
    
    Args:
        date_value: The date value to parse (string, datetime, etc.)
        
    Returns:
        datetime object or None if parsing fails
    """
    if pd.isna(date_value) or date_value is None:
        return None
        
    try:
        # If already a datetime object
        if isinstance(date_value, datetime):
            return date_value
            
        # If string, try various formats
        if isinstance(date_value, str):
            # Remove the Z and milliseconds if present
            date_value = re.sub(r'\.\d+Z$', 'Z', date_value)
            # Parse ISO format
            try:
                return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            except ValueError:
                # Try other common formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y']:
                    try:
                        return datetime.strptime(date_value, fmt)
                    except ValueError:
                        continue
    except Exception as e:
        logger.warning(f"Failed to parse date {date_value}: {e}")
        
    return None


def clean_price(price_value: Any) -> Optional[float]:
    """
    Convert price value to a proper float.
    
    Args:
        price_value: The price value to clean (string, float, etc.)
        
    Returns:
        Cleaned float value or None if conversion fails
    """
    if pd.isna(price_value) or price_value is None:
        return None
        
    try:
        # If already a number
        if isinstance(price_value, (int, float)):
            return float(price_value)
            
        # If string, extract numeric part
        if isinstance(price_value, str):
            # Remove currency symbols and commas
            price_str = price_value.replace('$', '').replace('£', '').replace('€', '').replace(',', '')
            # Extract numeric part
            matches = re.findall(r'\d+\.?\d*', price_str)
            if matches:
                return float(matches[0])
    except Exception as e:
        logger.warning(f"Failed to parse price {price_value}: {e}")
        
    return None


def clean_to_bool(value: Any) -> Optional[bool]:
    """
    Convert various value types to boolean.
    
    Args:
        value: The value to convert to boolean
        
    Returns:
        Boolean value or None if conversion fails
    """
    if pd.isna(value) or value is None:
        return None
        
    # If already boolean
    if isinstance(value, bool):
        return value
        
    # If number
    if isinstance(value, (int, float)):
        return bool(value)
        
    # If string
    if isinstance(value, str):
        value = value.lower().strip()
        if value in ('true', 't', 'yes', 'y', '1', '1.0'):
            return True
        if value in ('false', 'f', 'no', 'n', '0', '0.0'):
            return False
            
    return None


def transform_shopping_grid_data(df, keyword, scrape_date, campaign_id, scrape_type_id):
    """
    Transform shopping grid data (Format 1 - with Product1_Title, etc).
    
    Args:
        df: DataFrame with shopping grid data
        keyword: The keyword (sheet name)
        scrape_date: Default date of the scrape
        campaign_id: ID of the campaign
        scrape_type_id: ID of the scrape type
        
    Returns:
        List of dictionaries ready for database insertion
    """
    logger.info(f"Transforming shopping grid data for keyword: {keyword}")
    
    # First, check if this is a shopping grid format
    if not any(col.startswith('Product') and ('_Title' in col or '_Link' in col) for col in df.columns):
        logger.warning(f"Sheet doesn't appear to be in shopping grid format. Columns: {df.columns.tolist()}")
        return []  # Return empty list
    
    # Initialize list to hold transformed records
    transformed_records = []
    
    # Process each row (each row is a different query/date)
    for idx, row in df.iterrows():
        # Get the date and query for this row
        row_date = row.get('Date', scrape_date)
        if isinstance(row_date, str):
            try:
                row_date = datetime.strptime(row_date, '%Y-%m-%d')
            except ValueError:
                try:
                    row_date = datetime.strptime(row_date, '%d/%m/%Y')
                except ValueError:
                    row_date = scrape_date
        elif not isinstance(row_date, datetime):
            row_date = scrape_date
        
        query = row.get('Query', keyword)
        
        # Process each product in the row
        for i in range(1, 15):  # Up to 15 products to be safe
            prefix = f"Product{i}_"
            title_col = f"{prefix}Title"
            link_col = f"{prefix}Link"
            price_col = f"{prefix}Price"
            merchant_col = f"{prefix}Merchant"
            delivery_col = f"{prefix}Delivery"
            
            # Check if this product has basic required data
            if (title_col in df.columns and link_col in df.columns and 
                pd.notna(row.get(title_col)) and pd.notna(row.get(link_col))):
                
                # Generate a unique ID for this product
                product_id = str(uuid.uuid4())
                
                # Position is based on product number
                position = i
                
                # Create record
                record = {
                    "campaign_id": campaign_id,
                    "scrape_type_id": scrape_type_id,
                    "scrape_date": row_date.isoformat(),
                    "keyword": query if query else keyword,
                    "product_id": product_id,
                    "title": str(row.get(title_col, "")),
                    "link": str(row.get(link_col, "")),
                    "position": position
                }
                
                # Add optional fields if they exist
                if price_col in df.columns and pd.notna(row.get(price_col)):
                    # Process price - remove currency symbols, etc.
                    price_str = str(row.get(price_col, ""))
                    price_str = price_str.replace('$', '').replace('£', '').replace('€', '').replace(',', '')
                    try:
                        # Extract first number from string
                        price_match = re.search(r'\d+\.?\d*', price_str)
                        if price_match:
                            record["price"] = float(price_match.group())
                        record["price_raw"] = price_str
                    except (ValueError, TypeError):
                        record["price_raw"] = price_str
                
                if merchant_col in df.columns and pd.notna(row.get(merchant_col)):
                    record["merchant"] = str(row.get(merchant_col, ""))
                
                transformed_records.append(record)
    
    logger.info(f"Transformed {len(transformed_records)} products for keyword: {keyword}")
    return transformed_records


def transform_product_scraper_data(df, keyword, scrape_date, campaign_id, scrape_type_id):
    """
    Transform product scraper data (Format 2 - with id, title, link columns).
    
    Args:
        df: DataFrame with product scraper data
        keyword: The keyword (sheet name)
        scrape_date: Default date of the scrape
        campaign_id: ID of the campaign
        scrape_type_id: ID of the scrape type
        
    Returns:
        List of dictionaries ready for database insertion
    """
    logger.info(f"Transforming product scraper data for keyword: {keyword}")
    
    # More flexible check - just need to have some key columns
    key_columns = ['id', 'title']
    if not all(col in df.columns for col in key_columns):
        logger.warning(f"Sheet doesn't have minimum required columns. Columns: {df.columns.tolist()}")
        return []  # Return empty list
    
    # Log how many rows we're processing
    logger.info(f"Processing {len(df)} rows for keyword: {keyword}")
    
    # Initialize list to hold transformed records
    transformed_records = []
    
    # Process each row
    for idx, row in df.iterrows():
        # Skip rows without basic required data
        if pd.isna(row.get('id')) or pd.isna(row.get('title')):
            continue
        
        # Get the date for this row
        row_date = row.get('Date', scrape_date)
        if isinstance(row_date, str):
            try:
                row_date = datetime.strptime(row_date, '%Y-%m-%d')
            except ValueError:
                try:
                    row_date = datetime.strptime(row_date, '%d/%m/%Y')
                except ValueError:
                    row_date = scrape_date
        elif not isinstance(row_date, datetime):
            row_date = scrape_date
        
        # Create record with required fields
        record = {
            "campaign_id": campaign_id,
            "scrape_type_id": scrape_type_id,
            "scrape_date": row_date.isoformat(),
            "keyword": keyword,
            "product_id": str(row['id']),
            "title": str(row['title']),
        }
        
        # Add link if available, otherwise use a placeholder
        if 'link' in df.columns and not pd.isna(row.get('link')):
            record["link"] = str(row['link'])
        else:
            record["link"] = f"https://example.com/product/{record['product_id']}"
        
        # Add other optional fields if they exist
        optional_fields = {
            'position': 'position',
            'rating': 'rating',
            'reviews': 'reviews',
            'price': 'price',
            'price_raw': 'price_raw',
            'merchant': 'merchant',
            'is_carousel': 'is_carousel',
            'carousel_position': 'carousel_position',
            'has_product_page': 'has_product_page'
        }
        
        for db_field, excel_field in optional_fields.items():
            if excel_field in df.columns and not pd.isna(row.get(excel_field)):
                # Special handling for certain fields
                if db_field == 'position' and isinstance(row[excel_field], (int, float, str)):
                    try:
                        record[db_field] = int(float(row[excel_field]))
                    except (ValueError, TypeError):
                        pass
                elif db_field in ['is_carousel', 'has_product_page']:
                    value = row[excel_field]
                    if isinstance(value, bool):
                        record[db_field] = value
                    elif isinstance(value, str):
                        record[db_field] = value.lower() in ('true', 't', 'yes', 'y', '1')
                    elif isinstance(value, (int, float)):
                        record[db_field] = bool(value)
                else:
                    record[db_field] = row[excel_field]
        
        transformed_records.append(record)
    
    logger.info(f"Transformed {len(transformed_records)} products for keyword: {keyword}")
    return transformed_records

def detect_and_transform_data(df, keyword, scrape_date, campaign_id, scrape_type_id):
    """
    Detect the type of data format and transform accordingly.
    
    Args:
        df: DataFrame with scraper data
        keyword: The keyword (sheet name)
        scrape_date: Default date of the scrape
        campaign_id: ID of the campaign
        scrape_type_id: ID of the scrape type
        
    Returns:
        List of dictionaries ready for database insertion
    """
    logger.info(f"Detecting data format for sheet {keyword}. Columns: {df.columns.tolist()}")
    
    # Detect Shopping Grid format (Product1_Title, etc.)
    if any(col.startswith('Product') and ('_Title' in col or '_Link' in col) for col in df.columns):
        return transform_shopping_grid_data(df, keyword, scrape_date, campaign_id, scrape_type_id)
    
    # Case 1: Standard Product Scraper format
    if all(col in df.columns for col in ['id', 'title', 'link']):
        return transform_product_scraper_data(df, keyword, scrape_date, campaign_id, scrape_type_id)
    
    # Case 2: Product Scraper format with position as first column
    if all(col in df.columns for col in ['position', 'title', 'id']) and 'link' not in df.columns:
        # This is still the product format, but we need to be careful
        # It's missing the 'link' column which is required in our database
        logger.warning(f"Sheet {keyword} has position but is missing link column. Generating placeholder links.")
        
        # Create a copy with a generated link column
        df_with_link = df.copy()
        df_with_link['link'] = df_with_link.apply(
            lambda row: f"https://example.com/product/{row['id']}" if 'id' in row else "", 
            axis=1
        )
        
        return transform_product_scraper_data(df_with_link, keyword, scrape_date, campaign_id, scrape_type_id)
    
    # Could not determine format
    logger.warning(f"Could not determine data format for sheet {keyword}. Columns: {df.columns.tolist()}")
    return []