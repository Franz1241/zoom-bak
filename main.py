import json
import time
import os
import requests
import psycopg2
import yaml
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from urllib.parse import quote
from logging_config import setup_logging
from database.setup import setup_database
from database.metadata import save_meeting_metadata, save_phone_metadata
from database.inventory import (
    insert_meeting_inventory, insert_phone_inventory, get_undownloaded_recordings,
    update_recording_status, get_discovery_summary, get_2020_recordings,
    get_status_counts, get_year_distribution, get_download_counts
)

load_dotenv()

# Load configuration from YAML file
def load_config():
    """Load configuration from config.yaml"""
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Replace version placeholder in base_dir
    if 'directories' in config and 'base_dir' in config['directories']:
        config['directories']['base_dir'] = config['directories']['base_dir'].format(
            version=config['version']
        )
    
    return config

# Load configuration
CONFIG = load_config()


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

# Global token variable for automatic refresh
access_token = None
token_expires_at = None





# === Get Access Token with Auto-Refresh ===
def get_access_token(force_refresh=False):
    global access_token, token_expires_at

    # Check if we need to refresh the token
    if not force_refresh and access_token and token_expires_at:
        if datetime.now() < token_expires_at:
            return access_token

    logger.info("Refreshing access token...")
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={ZOOM_ACCOUNT_ID}"
    assert ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET, "Missing Zoom client credentials"

    try:
        response = requests.post(url, auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET))
        response.raise_for_status()
        token_data = response.json()

        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
        token_expires_at = datetime.now() + relativedelta(
            seconds=expires_in - CONFIG['api']['token_refresh_buffer']
        )  # Refresh early based on config

        logger.info("Access token refreshed successfully")
        return access_token

    except Exception as e:
        logger.error(f"Failed to get access token: {e}")
        raise


# === File Utilities ===
def create_dirs(user_email, data_type="meetings"):
    user_dir = os.path.join(BASE_DIR, data_type, user_email)
    os.makedirs(user_dir, exist_ok=True)
    logger.debug(f"Created directory: {user_dir}")
    return user_dir


def download_file(url, token, dest_path, file_description="file", retries=None):
    """Download file with proper error handling and logging"""
    if retries is None:
        retries = CONFIG['api']['retries']
    
    logger.debug(f"Downloading {file_description} to {dest_path}")

    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, stream=True, timeout=CONFIG['api']['request_timeout'])
            r.raise_for_status()
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(1024):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(dest_path)
            logger.info(
                f"Downloaded {file_description}: {os.path.basename(dest_path)} ({file_size} bytes)"
            )
            return True

        except Exception as e:
            logger.warning(
                f"Download failed ({attempt + 1}/{retries}) for {file_description} — {e}"
            )
            if attempt < retries - 1:
                time.sleep(CONFIG['api']['sleep_durations']['download_retry'])

    logger.error(f"Failed to download {file_description} after {retries} attempts")
    return False


def get_file_extension(file_info):
    """Get appropriate file extension from file info"""
    file_ext = file_info.get("file_extension")
    if file_ext:
        return file_ext.lower()

    file_type = file_info.get("file_type", "").lower()

    # Get mapping from config
    type_mapping = CONFIG['file_extensions']

    return type_mapping.get(file_type, file_type or "unknown")


# === API Utilities ===
def make_api_request(url, token, params=None, retries=None):
    """Make API request with rate limiting and error handling"""
    if retries is None:
        retries = CONFIG['api']['retries']
    
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(retries):
        time.sleep(CONFIG['api']['rate_limit_delay'])
        response = None

        try:
            logger.debug(f"API Request: {url} with params: {params}")

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            logger.debug(
                f"API Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}"
            )
            return data

        except requests.exceptions.HTTPError as e:
            if response is None:
                logger.error(f"API Error: {e}")
                if attempt < retries - 1:
                    time.sleep(CONFIG['api']['sleep_durations']['retry'])
                continue

            if response.status_code == 401:  # Unauthorized
                logger.warning(f"401 Unauthorized for {url} - attempting token refresh")
                if attempt < retries - 1:
                    token = get_access_token(force_refresh=True)
                    headers["Authorization"] = f"Bearer {token}"
                    time.sleep(CONFIG['api']['sleep_durations']['token_refresh'])
                    continue
                else:
                    logger.error(
                        f"Final 401 error for {url} - user may not have permissions"
                    )
                    return None

            elif response.status_code == 429:  # Rate limited
                logger.warning(f"Rate limited, waiting {CONFIG['api']['sleep_durations']['rate_limit']} seconds...")
                time.sleep(CONFIG['api']['sleep_durations']['rate_limit'])
                continue
            elif response.status_code in [400, 404] and "phone" in url:
                # Many users don't have phone licenses - this is expected
                logger.debug(
                    f"Phone API {response.status_code} for {url} - user likely has no phone license"
                )
                return None
            else:
                logger.error(f"API Error: {e}")
                logger.error(
                    f"Response: {response.text if response else 'No response'}"
                )
                if attempt < retries - 1:
                    time.sleep(CONFIG['api']['sleep_durations']['retry'])
                continue
                return None

        except Exception as e:
            logger.error(f"Request Error: {e}")
            if attempt < retries - 1:
                time.sleep(CONFIG['api']['sleep_durations']['retry'])
                continue
            return None

    return None


def generate_date_ranges(start_date, end_date, months_per_range=None):
    """Generate smaller date ranges to avoid API limits"""
    if months_per_range is None:
        months_per_range = CONFIG['processing']['months_per_range']
    
    ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current < end:
        range_end = min(current + relativedelta(months=months_per_range), end)
        ranges.append((current.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
        current = range_end

    logger.debug(f"Generated {len(ranges)} date ranges")
    return ranges


# === Recording Discovery Phase ===
def discover_all_recordings(user_emails, token):
    """Phase 1: Discover and catalog all recordings before downloading"""
    logger.info("Starting recording discovery phase...")

    for i, email in enumerate(user_emails, 1):
        logger.info(f"[{i}/{len(user_emails)}] Discovering recordings for: {email}")

        try:
            discover_meeting_recordings(email, token)
            discover_phone_recordings(email, token)
            # discover_webinar_recordings(email, token)  # Add if needed

        except Exception as e:
            logger.error(f"Error discovering recordings for {email}: {e}")
            continue

    # Report discovery results
    results = get_discovery_summary(cursor, VERSION)
    logger.info("Discovery Results:")
    for rec_type, count, earliest, latest in results:
        logger.info(f"  {rec_type}: {count} recordings ({earliest} to {latest})")

    # Check for 2020 recordings specifically
    results_2020 = get_2020_recordings(cursor, VERSION)
    if results_2020:
        logger.info("Found recordings from Nov-Dec 2020:")
        for rec_type, email, count in results_2020:
            logger.info(f"  {email} ({rec_type}): {count} recordings")
    else:
        logger.warning("NO RECORDINGS FOUND FROM NOV-DEC 2020!")


def discover_meeting_recordings(user_email, token):
    """Discover meeting recordings and store in inventory"""
    logger.debug(f"Discovering meeting recordings for: {user_email}")

    date_ranges = generate_date_ranges(START_DATE, END_DATE)
    total_found = 0

    for range_start, range_end in date_ranges:
        next_page_token = None
        range_found = 0

        while True:
            params = {"from": range_start, "to": range_end, "page_size": CONFIG['api']['page_sizes']['recordings']}
            if next_page_token:
                params["next_page_token"] = next_page_token

            url = f"https://api.zoom.us/v2/users/{quote(user_email)}/recordings"
            data = make_api_request(url, token, params)

            if not data or "meetings" not in data:
                break

            for meeting in data["meetings"]:
                for file_info in meeting.get("recording_files", []):
                    if (
                        file_info.get("download_url")
                        and file_info.get("status") == "completed"
                    ):
                        if insert_meeting_inventory(
                            cursor, conn, "meeting", file_info.get("id"), 
                            meeting.get("uuid"), user_email, meeting.get("topic"),
                            meeting.get("start_time"), meeting.get("duration"),
                            file_info.get("file_type"), file_info.get("file_size"),
                            file_info.get("download_url"),
                            {"meeting": meeting, "file_info": file_info}, VERSION
                        ):
                            range_found += 1

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

        total_found += range_found
        if range_found > 0:
            logger.debug(
                f"Found {range_found} meeting recordings for {user_email} in {range_start} to {range_end}"
            )

    conn.commit()
    if total_found > 0:
        logger.info(f"Discovered {total_found} meeting recordings for {user_email}")


def discover_phone_recordings(user_email, token):
    """Discover phone recordings and store in inventory"""
    logger.debug(f"Discovering phone recordings for: {user_email}")

    next_page_token = None
    total_found = 0

    while True:
        params = {"from": START_DATE, "to": END_DATE, "page_size": CONFIG['api']['page_sizes']['phone_recordings']}
        if next_page_token:
            params["next_page_token"] = next_page_token

        url = f"https://api.zoom.us/v2/phone/users/{quote(user_email)}/recordings"
        data = make_api_request(url, token, params)

        if not data or "recordings" not in data:
            break

        for recording in data["recordings"]:
            if recording.get("download_url"):
                if insert_phone_inventory(
                    cursor, conn, recording.get("id"), user_email,
                    recording.get("start_time"), recording.get("duration"),
                    "mp3", recording.get("file_size"), recording.get("download_url"),
                    {"recording": recording}, VERSION
                ):
                    total_found += 1

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    conn.commit()
    if total_found > 0:
        logger.info(f"Discovered {total_found} phone recordings for {user_email}")


# === Download Phase ===
def download_recordings_from_inventory():
    """Phase 2: Download recordings from inventory"""
    logger.info("Starting download phase...")

    # Get all recordings that haven't been downloaded yet
    recordings = get_undownloaded_recordings(cursor, VERSION)
    logger.info(f"Found {len(recordings)} recordings to download")

    token = get_access_token()

    for i, recording_data in enumerate(recordings, 1):
        # Properly unpack the tuple
        (
            inv_id,
            rec_type,
            rec_id,
            user_email,
            file_type,
            download_url,
            raw_data,
            start_time,
            topic,
        ) = recording_data

        logger.info(
            f"[{i}/{len(recordings)}] Downloading {rec_type} recording: {rec_id} ({file_type})"
        )

        try:
            if rec_type == "meeting":
                success = download_meeting_from_inventory(
                    inv_id, user_email, raw_data, token
                )
            elif rec_type == "phone":
                success = download_phone_from_inventory(
                    inv_id, user_email, raw_data, token
                )
            else:
                success = False
                logger.warning(f"Unknown recording type: {rec_type}")

            # Update status
            status = "downloaded" if success else "failed"
            update_recording_status(
                cursor, conn, inv_id, status, 
                datetime.now() if success else None, None, VERSION
            )

        except Exception as e:
            logger.error(f"Error downloading recording {rec_id}: {e}")
            update_recording_status(
                cursor, conn, inv_id, "failed", None, str(e), VERSION
            )


def download_meeting_from_inventory(inv_id, user_email, raw_data, token):
    """Download meeting recording from inventory data"""
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        meeting = data["meeting"]
        file_info = data["file_info"]

        user_dir = create_dirs(user_email, "meetings")

        file_ext = get_file_extension(file_info)
        filename = f"{meeting.get('id', 'unknown')}_{file_info.get('id', 'unknown')}.{file_ext}"
        file_path = os.path.join(user_dir, filename)

        # Download the main file
        if not os.path.exists(file_path):
            success = download_file(
                file_info["download_url"],
                token,
                file_path,
                f"meeting recording ({file_info.get('file_type', 'unknown')})",
            )
            if not success:
                return False
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata
        return save_meeting_metadata(cursor, conn, meeting, user_email, file_info, file_path, None, VERSION)

    except Exception as e:
        logger.error(f"Error in download_meeting_from_inventory: {e}")
        return False


def download_phone_from_inventory(inv_id, user_email, raw_data, token):
    """Download phone recording from inventory data"""
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        recording = data["recording"]

        user_dir = create_dirs(user_email, "phone")

        start_time_clean = (
            recording.get("start_time", "")
            .replace(":", "-")
            .replace("T", "_")
            .replace("Z", "")
        )
        filename = f"call_{recording.get('id', 'unknown')}_{start_time_clean}.mp3"
        file_path = os.path.join(user_dir, filename)

        # Download file
        if not os.path.exists(file_path):
            success = download_file(
                recording["download_url"], token, file_path, "phone recording"
            )
            if not success:
                return False
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata
        return save_phone_metadata(cursor, conn, recording, user_email, file_path, VERSION)

    except Exception as e:
        logger.error(f"Error in download_phone_from_inventory: {e}")
        return False





# === User Management ===
def get_zoom_users(token):
    """Get all users with pagination"""
    users = []
    next_page_token = None

    while True:
        params = {"page_size": CONFIG['api']['page_sizes']['users'], "status": "active"}
        if next_page_token:
            params["next_page_token"] = next_page_token

        url = "https://api.zoom.us/v2/users"
        data = make_api_request(url, token, params)

        if not data:
            break

        for user in data.get("users", []):
            users.append(user["email"])

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    logger.info(f"Found {len(users)} users")
    return users


# === Main Process ===
def main():
    logger.info("Starting Zoom backup process...")
    setup_database(cursor, conn, VERSION)

    token = get_access_token()
    user_emails = get_zoom_users(token)

    logger.info(f"Found {len(user_emails)} users to process...")

    # Phase 1: Discovery
    discover_all_recordings(user_emails, token)

    # Phase 2: Download
    download_recordings_from_inventory()

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
