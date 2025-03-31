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


def batch_insert_scrape_data(conn, scrape_data_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Insert multiple scrape data records into the database as a batch,
    skipping records that already exist.
    
    Args:
        conn: PostgreSQL connection
        scrape_data_batch: List of dictionaries containing the scrape data to insert
    
    Returns:
        List of inserted data records (with IDs)
    """
    if not scrape_data_batch:
        return []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            inserted_ids = []
            skipped_count = 0
            
            # Insert records one by one, checking for duplicates
            for record in scrape_data_batch:
                # Check if a similar record already exists
                # IMPORTANT: We explicitly check all key identifying factors:
                # - scrape_type_id (to distinguish different scrape types)
                # - scrape_date (to distinguish different scrape runs on different dates)
                # - campaign_id, keyword, and product identification (product_id, title, link)
                
                # Format the scrape_date for comparison
                scrape_date = record.get('scrape_date')
                if isinstance(scrape_date, str):
                    # If it's a date string, keep it as is for the DATE() function
                    date_param = scrape_date
                else:
                    # Default to current date if missing
                    from datetime import datetime
                    date_param = datetime.now().isoformat()
                
                check_sql = """
                SELECT id FROM scrape_data 
                WHERE campaign_id = %(campaign_id)s
                AND scrape_type_id = %(scrape_type_id)s
                AND DATE(scrape_date) = DATE(%(scrape_date)s)  -- Compare just the date part
                AND keyword = %(keyword)s
                AND (
                    (product_id = %(product_id)s)
                    OR (title = %(title)s AND link = %(link)s)
                )
                LIMIT 1
                """
                
                check_params = {
                    'campaign_id': record['campaign_id'],
                    'scrape_type_id': record['scrape_type_id'],
                    'scrape_date': date_param,
                    'keyword': record['keyword'],
                    'product_id': record['product_id'],
                    'title': record['title'],
                    'link': record.get('link', '')
                }
                
                # Execute check
                cur.execute(check_sql, check_params)
                existing = cur.fetchone()
                
                if existing:
                    # Skip this record as it already exists
                    skipped_count += 1
                    continue
                
                # Record doesn't exist, so insert it
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
            logger.info(f"Inserted {len(inserted_ids)} new records, skipped {skipped_count} existing records")
            return inserted_ids
    except Exception as e:
        conn.rollback()
        logger.error(f"Error batch inserting scrape data: {e}")
        raise