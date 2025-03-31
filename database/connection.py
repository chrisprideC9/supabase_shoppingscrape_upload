import os
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)

@st.cache_resource
def init_connection():
    """Initialize a direct PostgreSQL connection."""
    # Get connection parameters from environment variables
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    
    # If env vars aren't set, try Streamlit secrets
    if not db_host or not db_port or not db_name or not db_user or not db_password:
        db_host = st.secrets.get("DB_HOST", "")
        db_port = st.secrets.get("DB_PORT", "")
        db_name = st.secrets.get("DB_NAME", "")
        db_user = st.secrets.get("DB_USER", "")
        db_password = st.secrets.get("DB_PASSWORD", "")
    
    if not db_host or not db_port or not db_name or not db_user:
        st.error("Database credentials not found! Please set DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD in .env file or Streamlit secrets.")
        st.stop()
    
    try:
        logger.info(f"Connecting to PostgreSQL database at {db_host}:{db_port}/{db_name}")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise