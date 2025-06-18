#!/usr/bin/env python3
"""
Script to retry only the failed recordings with the passcode fix.
This is safer than running the full main process.
"""

import os
import psycopg2
import yaml
from datetime import datetime
from dotenv import load_dotenv
from logging_config import setup_logging
from zoom_api.auth import get_access_token
from zoom_api.download import download_recordings_from_inventory
from database.inventory import get_status_counts

load_dotenv()

def load_config():
    """Load configuration from config.yaml"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def retry_failed_recordings():
    """Retry downloading only the failed recordings."""
    
    # Load configuration
    config = load_config()
    logger = setup_logging(config)
    
    version = config["version"]
    postgres_url = config["database"]["url"]
    
    logger.info("Starting retry of failed recordings...")
    logger.info(f"Database version: {version}")
    
    # Database connection
    conn = psycopg2.connect(postgres_url)
    cursor = conn.cursor()
    
    try:
        # Get access token
        token = get_access_token(config)
        logger.info("✅ Access token obtained")
        
        # Check initial status
        status_counts_before = get_status_counts(cursor, version)
        failed_before = next((count for status, count in status_counts_before if status == 'failed'), 0)
        
        logger.info(f"Failed recordings before retry: {failed_before}")
        
        if failed_before == 0:
            logger.info("No failed recordings to retry!")
            return
        
        # Reset failed recordings back to 'found' status so they get picked up
        logger.info("Resetting failed recordings to 'found' status...")
        cursor.execute(f"""
            UPDATE zoom_recording_inventory_{version} 
            SET status = 'found', error_message = NULL, downloaded_at = NULL
            WHERE status = 'failed'
        """)
        conn.commit()
        
        reset_count = cursor.rowcount
        logger.info(f"Reset {reset_count} failed recordings to 'found' status")
        
        # Now run the download process - it will pick up the 'found' recordings
        logger.info("Starting download process for previously failed recordings...")
        updated_token = download_recordings_from_inventory(token, config, cursor, conn, version)
        
        if updated_token != token:
            logger.info("Token was refreshed during download process")
        
        # Check final status
        status_counts_after = get_status_counts(cursor, version)
        failed_after = next((count for status, count in status_counts_after if status == 'failed'), 0)
        downloaded_after = next((count for status, count in status_counts_after if status == 'downloaded'), 0)
        
        logger.info("=" * 60)
        logger.info("RETRY RESULTS:")
        logger.info(f"  Failed recordings before: {failed_before}")
        logger.info(f"  Failed recordings after: {failed_after}")
        logger.info(f"  Successfully fixed: {failed_before - failed_after}")
        logger.info(f"  Total downloaded now: {downloaded_after}")
        
        logger.info("\nFinal status distribution:")
        for status, count in status_counts_after:
            logger.info(f"  {status}: {count}")
            
        if failed_after < failed_before:
            logger.info("🎉 SUCCESS! Some failed recordings were fixed!")
        else:
            logger.warning("⚠️  No improvement in failed recordings. Check logs for errors.")
        
    except Exception as e:
        logger.error(f"Error during retry process: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    retry_failed_recordings() 