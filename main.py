import os
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from logging_config import setup_logging
from database.setup import setup_database
from database.inventory import get_status_counts, get_year_distribution, get_download_counts
from zoom_api.auth import get_access_token
from zoom_api.user import get_zoom_users
from zoom_api.discovery import discover_all_recordings
from zoom_api.download import download_recordings_from_inventory

load_dotenv()

# Load configuration
from utils.misc import load_config, validate_config
CONFIG = load_config()
validate_config(CONFIG)


# Initialize logger
logger = setup_logging(CONFIG)

# === Server-to-Server OAuth Credentials ===
ZOOM_ACCOUNT_ID = os.getenv("ZOOM_ACCOUNT_ID")
ZOOM_CLIENT_ID = os.getenv("ZOOM_CLIENT_ID")
ZOOM_CLIENT_SECRET = os.getenv("ZOOM_CLIENT_SECRET")

# === Configuration Values ===
VERSION = CONFIG['version']
POSTGRES_URL = CONFIG['database']['url']
BASE_DIR = CONFIG['directories']['base_dir'] + "_" + VERSION
START_DATE = CONFIG['dates']['start_date']
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# === Database Connection ===
conn = psycopg2.connect(POSTGRES_URL)
cursor = conn.cursor()

# === Main Process ===
def main():
    logger.info("Starting Zoom backup process...")
    setup_database(cursor, conn, VERSION)

    token = get_access_token(CONFIG)
    user_emails = get_zoom_users(token, CONFIG)

    logger.info(f"Found {len(user_emails)} users to process...")

    # Phase 1: Discovery
    discover_all_recordings(user_emails, token, CONFIG, cursor, conn, VERSION)

    # Phase 2: Download
    download_recordings_from_inventory(token, CONFIG, cursor, conn, VERSION)

    logger.info("Backup process completed!")

    # Print summary
    meeting_count, phone_count = get_download_counts(cursor, VERSION)
    status_counts = get_status_counts(cursor, VERSION)

    logger.info("Summary:")
    logger.info(f"  Meeting recordings downloaded: {meeting_count}")
    logger.info(f"  Phone recordings downloaded: {phone_count}")
    logger.info("  Inventory status:")
    for status, count in status_counts:
        logger.info(f"    {status}: {count}")

    # Show year distribution from inventory
    results = get_year_distribution(cursor, VERSION)
    logger.info("Inventory by year and type:")
    for year, count, rec_type in results:
        logger.info(f"  {year or 'NULL'} ({rec_type}): {count}")


if __name__ == "__main__":
    main()
