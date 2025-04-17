from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import json

logger = logging.getLogger(__name__)


def get_campaigns(conn) -> List[Dict[str, Any]]:
    """
    Get all campaigns with client information using direct SQL query.
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of campaign dictionaries, each containing client info
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # First test if we can access the campaigns table
            cur.execute("SELECT count(*) FROM campaigns")
            count = cur.fetchone()['count']
            logger.info(f"Found {count} campaigns in database")
            
            # Now get campaigns with client info
            query = """
            SELECT 
                c.campaign_id, 
                c.client_id, 
                c.domain_name, 
                c.brand_name,
                json_build_object(
                    'client_id', cl.client_id,
                    'name', cl.name,
                    'surname', cl.surname,
                    'email', cl.email
                ) as clients
            FROM campaigns c
            JOIN clients cl ON c.client_id = cl.client_id
            ORDER BY c.domain_name
            """
            cur.execute(query)
            campaigns = cur.fetchall()
            
            # Convert RealDictRow objects to regular dictionaries
            result = [dict(campaign) for campaign in campaigns]
            logger.info(f"Retrieved {len(result)} campaigns with client info")
            return result
    except Exception as e:
        logger.error(f"Error getting campaigns: {e}")
        return []


def get_scrape_types(conn) -> List[Dict[str, Any]]:
    """
    Get all scrape types from the database.
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of scrape type dictionaries
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM scrape_types ORDER BY name")
            scrape_types = cur.fetchall()
            return [dict(st) for st in scrape_types]
    except Exception as e:
        logger.error(f"Error getting scrape types: {e}")
        return []


def batch_insert_scrape_data(conn, scrape_data_batch: List[Dict[str, Any]], force_upload: bool = False) -> List[Dict[str, Any]]:
    """
    Insert multiple scrape data records into the database as a batch,
    with efficient duplicate checking.
    """
    if not scrape_data_batch:
        return []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Simple check to see if the table is empty for this campaign
            if not force_upload:
                cur.execute("SELECT COUNT(*) FROM scrape_data WHERE campaign_id = %s", (scrape_data_batch[0]['campaign_id'],))
                count = cur.fetchone()['count']
                
                # If the table is empty for this campaign, we don't need duplicate checking
                if count == 0:
                    logger.info(f"No existing records for campaign {scrape_data_batch[0]['campaign_id']} - skipping duplicate check")
                    force_upload = True
            
            # If force upload is enabled, skip duplicate checking
            if force_upload:
                # Proceed directly to insertion
                records_to_insert = scrape_data_batch
                skipped_count = 0
            else:
                # Get unique campaign IDs, scrape type IDs, and keywords from the batch
                campaign_ids = set(r['campaign_id'] for r in scrape_data_batch)
                scrape_type_ids = set(r['scrape_type_id'] for r in scrape_data_batch)
                keywords = set(r['keyword'] for r in scrape_data_batch)
                
                # Extract unique dates (just the date part) and convert to proper date format
                # This must be fixed to avoid the type cast error
                dates_list = []
                for record in scrape_data_batch:
                    date_str = record.get('scrape_date', '')
                    if isinstance(date_str, str):
                        import re
                        date_match = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
                        if date_match:
                            dates_list.append(date_match.group(1))
                
                # Build a query to get all potentially matching records in one go
                # Modified to use a date-safe comparison
                fetch_sql = """
                SELECT 
                    id, 
                    campaign_id, 
                    scrape_type_id, 
                    DATE(scrape_date) as scrape_date, 
                    keyword, 
                    product_id, 
                    title, 
                    link
                FROM scrape_data 
                WHERE campaign_id = ANY(%s)
                AND scrape_type_id = ANY(%s)
                AND keyword = ANY(%s)
                """
                
                query_params = [
                    list(campaign_ids), 
                    list(scrape_type_ids),
                    list(keywords)
                ]
                
                # Only add date filtering if we have dates to filter by
                if dates_list:
                    date_placeholders = []
                    for i, date_str in enumerate(dates_list):
                        date_placeholders.append(f"%s::date")
                        query_params.append(date_str)
                    
                    fetch_sql += f" AND DATE(scrape_date) IN ({', '.join(date_placeholders)})"
                
                # Execute the query
                cur.execute(fetch_sql, query_params)
                
                existing_records = cur.fetchall()
                
                # Create lookup sets for quick duplicate checking
                product_id_set = set()
                title_link_set = set()
                
                for rec in existing_records:
                    # Create a composite key for each existing record
                    campaign_date_kw_prod = (
                        rec['campaign_id'], 
                        rec['scrape_type_id'],
                        str(rec['scrape_date']), 
                        rec['keyword'], 
                        rec['product_id']
                    )
                    product_id_set.add(campaign_date_kw_prod)
                    
                    # Also check title+link combination
                    campaign_date_kw_title_link = (
                        rec['campaign_id'], 
                        rec['scrape_type_id'],
                        str(rec['scrape_date']), 
                        rec['keyword'], 
                        rec['title'], 
                        rec.get('link', '')
                    )
                    title_link_set.add(campaign_date_kw_title_link)
                
                # Filter out duplicates
                records_to_insert = []
                skipped_count = 0
                
                for record in scrape_data_batch:
                    # Extract date part for comparison
                    date_str = record.get('scrape_date', '')
                    if isinstance(date_str, str):
                        import re
                        date_match = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
                        date_part = date_match.group(1) if date_match else date_str
                    else:
                        date_part = str(date_str)
                    
                    # Check if it's a duplicate by product_id
                    product_key = (
                        record['campaign_id'], 
                        record['scrape_type_id'],
                        date_part, 
                        record['keyword'], 
                        record['product_id']
                    )
                    
                    # Check if it's a duplicate by title+link
                    title_link_key = (
                        record['campaign_id'], 
                        record['scrape_type_id'],
                        date_part, 
                        record['keyword'], 
                        record['title'], 
                        record.get('link', '')
                    )
                    
                    # If neither key exists, it's not a duplicate
                    if product_key not in product_id_set and title_link_key not in title_link_set:
                        records_to_insert.append(record)
                    else:
                        skipped_count += 1
            
            # Now insert all non-duplicate records
            inserted_ids = []
            for record in records_to_insert:
                columns = list(record.keys())
                placeholders = [f"%({col})s" for col in columns]
                
                sql = f"""
                INSERT INTO scrape_data ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                RETURNING id
                """
                
                # Execute for a single record
                cur.execute(sql, record)
                result = cur.fetchone()
                if result and 'id' in result:
                    inserted_ids.append({"id": result["id"]})
            
            conn.commit()
            
            if force_upload:
                logger.info(f"Force uploaded {len(inserted_ids)} records (bypassed duplicate check)")
            else:
                logger.info(f"Inserted {len(inserted_ids)} new records, skipped {skipped_count} existing records")
                
            return inserted_ids
    except Exception as e:
        conn.rollback()
        logger.error(f"Error batch inserting scrape data: {e}")
        raise