import json
import time
import os
import requests
import psycopg2
import logging
import yaml
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from urllib.parse import quote

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


# === Setup Logging ===
def setup_logging():
    """Setup logging with separate files for different levels"""
    log_config = CONFIG['logging']
    log_dir = CONFIG['directories']['log_dir']
    os.makedirs(log_dir, exist_ok=True)

    # Create logger
    logger = logging.getLogger("zoom_backup")
    logger.setLevel(getattr(logging, log_config['levels']['file_debug']))

    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    simple_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Debug file handler (all messages)
    debug_handler = logging.FileHandler(os.path.join(log_dir, log_config['files']['debug']))
    debug_handler.setLevel(getattr(logging, log_config['levels']['file_debug']))
    debug_handler.setFormatter(detailed_formatter)
    logger.addHandler(debug_handler)

    # Info file handler (info and above)
    info_handler = logging.FileHandler(os.path.join(log_dir, log_config['files']['info']))
    info_handler.setLevel(getattr(logging, log_config['levels']['file_info']))
    info_handler.setFormatter(simple_formatter)
    logger.addHandler(info_handler)

    # Warning file handler (warnings and errors only)
    warning_handler = logging.FileHandler(os.path.join(log_dir, log_config['files']['warnings']))
    warning_handler.setLevel(getattr(logging, log_config['levels']['file_warning']))
    warning_handler.setFormatter(simple_formatter)
    logger.addHandler(warning_handler)

    # Console handler for important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_config['levels']['console']))
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    return logger


# Initialize logger
logger = setup_logging()

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


# === Database Setup ===
def setup_database():
    """Create tables if they don't exist"""
    logger.info("Setting up database tables...")

    # Recording inventory table - tracks all recordings found before download
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_recording_inventory_{VERSION} (
            id SERIAL PRIMARY KEY,
            recording_type VARCHAR(20) NOT NULL, -- 'meeting', 'phone', 'webinar'
            recording_id VARCHAR(128) NOT NULL,
            meeting_id VARCHAR(128),
            user_email VARCHAR(320),
            topic TEXT,
            start_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            download_url TEXT,
            status VARCHAR(20) DEFAULT 'found', -- 'found', 'downloaded', 'failed', 'skipped'
            found_at TIMESTAMPTZ DEFAULT NOW(),
            downloaded_at TIMESTAMPTZ,
            error_message TEXT,
            raw_data JSON,
            UNIQUE(recording_type, recording_id, file_type)
        );
    """)

    # Meeting recordings table (updated with version and longer varchar fields)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_recordings_{VERSION} (
            id SERIAL PRIMARY KEY,
            meeting_id VARCHAR(128),
            recording_id VARCHAR(128),
            topic TEXT,
            host_id VARCHAR(128),
            host_email VARCHAR(320),
            start_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            recording_type VARCHAR(64),
            download_url TEXT,
            transcript_url TEXT,
            path TEXT,
            transcript_path TEXT,
            downloaded_at TIMESTAMPTZ DEFAULT NOW(),
            data_type VARCHAR(20) DEFAULT 'meeting',
            unprocessed JSON DEFAULT '{{}}'
        );
    """)

    # Phone recordings table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_phone_recordings_{VERSION} (
            id SERIAL PRIMARY KEY,
            recording_id VARCHAR(128) UNIQUE,
            call_id VARCHAR(128),
            caller_number VARCHAR(32),
            callee_number VARCHAR(32),
            caller_name VARCHAR(255),
            callee_name VARCHAR(255),
            direction VARCHAR(16),
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            download_url TEXT,
            path TEXT,
            owner_id VARCHAR(128),
            owner_email VARCHAR(320),
            downloaded_at TIMESTAMPTZ DEFAULT NOW(),
            unprocessed JSON DEFAULT '{{}}'
        );
    """)

    # Webinars table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_webinar_recordings_{VERSION} (
            id SERIAL PRIMARY KEY,
            webinar_id VARCHAR(128),
            recording_id VARCHAR(128),
            topic TEXT,
            host_id VARCHAR(128),
            host_email VARCHAR(320),
            start_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            recording_type VARCHAR(64),
            download_url TEXT,
            transcript_url TEXT,
            path TEXT,
            transcript_path TEXT,
            downloaded_at TIMESTAMPTZ DEFAULT NOW(),
            unprocessed JSON DEFAULT '{{}}'
        );
    """)

    # Create indexes for better performance
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_inventory_user_email ON zoom_recording_inventory_{VERSION}(user_email);"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_inventory_start_time ON zoom_recording_inventory_{VERSION}(start_time);"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_inventory_status ON zoom_recording_inventory_{VERSION}(status);"
    )

    conn.commit()
    logger.info("Database tables created successfully")


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
    cursor.execute(f"""
        SELECT recording_type, COUNT(*), 
               MIN(start_time) as earliest, 
               MAX(start_time) as latest
        FROM zoom_recording_inventory_{VERSION} 
        GROUP BY recording_type
    """)
    results = cursor.fetchall()

    logger.info("Discovery Results:")
    for rec_type, count, earliest, latest in results:
        logger.info(f"  {rec_type}: {count} recordings ({earliest} to {latest})")

    # Check for 2020 recordings specifically
    cursor.execute(f"""
        SELECT recording_type, user_email, COUNT(*) 
        FROM zoom_recording_inventory_{VERSION} 
        WHERE start_time >= '2020-11-01' AND start_time < '2021-01-01'
        GROUP BY recording_type, user_email
        ORDER BY user_email
    """)
    results_2020 = cursor.fetchall()

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
                        try:
                            cursor.execute(
                                f"""
                                INSERT INTO zoom_recording_inventory_{VERSION} 
                                (recording_type, recording_id, meeting_id, user_email, topic, 
                                 start_time, duration, file_type, file_size, download_url, raw_data)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (recording_type, recording_id, file_type) DO NOTHING
                            """,
                                (
                                    "meeting",
                                    file_info.get("id"),
                                    meeting.get("uuid"),
                                    user_email,
                                    meeting.get("topic"),
                                    meeting.get("start_time"),
                                    meeting.get("duration"),
                                    file_info.get("file_type"),
                                    file_info.get("file_size"),
                                    file_info.get("download_url"),
                                    json.dumps(
                                        {"meeting": meeting, "file_info": file_info}
                                    ),
                                ),
                            )
                            range_found += 1
                        except Exception as e:
                            logger.error(
                                f"Error inserting meeting recording inventory: {e}"
                            )

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
                try:
                    cursor.execute(
                        f"""
                        INSERT INTO zoom_recording_inventory_{VERSION} 
                        (recording_type, recording_id, user_email, start_time, 
                         duration, file_type, file_size, download_url, raw_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (recording_type, recording_id, file_type) DO NOTHING
                    """,
                        (
                            "phone",
                            recording.get("id"),
                            user_email,
                            recording.get("start_time"),
                            recording.get("duration"),
                            "mp3",
                            recording.get("file_size"),
                            recording.get("download_url"),
                            json.dumps({"recording": recording}),
                        ),
                    )
                    total_found += 1
                except Exception as e:
                    logger.error(f"Error inserting phone recording inventory: {e}")

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
    cursor.execute(f"""
        SELECT id, recording_type, recording_id, user_email, file_type, 
               download_url, raw_data, start_time, topic
        FROM zoom_recording_inventory_{VERSION} 
        WHERE status = 'found'
        ORDER BY start_time DESC
    """)

    recordings = cursor.fetchall()
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
            cursor.execute(
                f"""
                UPDATE zoom_recording_inventory_{VERSION} 
                SET status = %s, downloaded_at = %s
                WHERE id = %s
            """,
                (status, datetime.now() if success else None, inv_id),
            )
            conn.commit()

        except Exception as e:
            logger.error(f"Error downloading recording {rec_id}: {e}")
            cursor.execute(
                f"""
                UPDATE zoom_recording_inventory_{VERSION} 
                SET status = 'failed', error_message = %s
                WHERE id = %s
            """,
                (str(e), inv_id),
            )
            conn.commit()


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
        return save_meeting_metadata(meeting, user_email, file_info, file_path)

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
        return save_phone_metadata(recording, user_email, file_path)

    except Exception as e:
        logger.error(f"Error in download_phone_from_inventory: {e}")
        return False


# === Metadata Saving Functions ===
def save_meeting_metadata(
    meeting, user_email, file_info, local_path, transcript_path=None
):
    """Save meeting recording metadata with fallback strategy"""
    logger.debug(f"Saving metadata for meeting: {meeting.get('topic', 'No topic')}")

    fallback_data = {
        "meeting": meeting,
        "file_info": file_info,
        "user_email": user_email,
        "path": local_path,
        "transcript_path": transcript_path,
        "data_type": "meeting",
    }

    try:
        cursor.execute(
            f"""
            INSERT INTO zoom_recordings_{VERSION} (
                meeting_id, recording_id, topic, host_id, host_email, start_time,
                duration, file_type, file_size, recording_type,
                download_url, transcript_url, path, transcript_path, data_type, unprocessed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """,
            (
                meeting.get("uuid"),
                file_info.get("id"),
                meeting.get("topic"),
                meeting.get("host_id"),
                user_email,
                meeting.get("start_time"),
                meeting.get("duration"),
                file_info.get("file_type"),
                file_info.get("file_size"),
                file_info.get("recording_type"),
                file_info.get("download_url"),
                None,
                local_path,
                transcript_path,
                "meeting",
                json.dumps(fallback_data),
            ),
        )
        conn.commit()
        return True

    except Exception as e:
        logger.warning(f"Meeting metadata save failed: {e}")
        conn.rollback()
        return False


def save_phone_metadata(recording, user_email, local_path):
    """Save phone recording metadata with fallback strategy"""
    fallback_data = {
        "recording": recording,
        "user_email": user_email,
        "path": local_path,
        "data_type": "phone",
    }

    try:
        cursor.execute(
            f"""
            INSERT INTO zoom_phone_recordings_{VERSION} (
                recording_id, call_id, caller_number, callee_number,
                caller_name, callee_name, direction, start_time, end_time,
                duration, file_type, file_size, download_url, path,
                owner_id, owner_email, unprocessed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (recording_id) DO NOTHING
        """,
            (
                recording.get("id"),
                recording.get("call_id"),
                recording.get("caller_number"),
                recording.get("callee_number"),
                recording.get("caller_name"),
                recording.get("callee_name"),
                recording.get("direction"),
                recording.get("start_time"),
                recording.get("end_time"),
                recording.get("duration"),
                recording.get("file_type", "mp3"),
                recording.get("file_size"),
                recording.get("download_url"),
                local_path,
                recording.get("owner_id"),
                user_email,
                json.dumps(fallback_data),
            ),
        )
        conn.commit()
        return True

    except Exception as e:
        logger.warning(f"Phone metadata save failed: {e}")
        conn.rollback()
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
    setup_database()

    token = get_access_token()
    user_emails = get_zoom_users(token)

    logger.info(f"Found {len(user_emails)} users to process...")

    # Phase 1: Discovery
    discover_all_recordings(user_emails, token)

    # Phase 2: Download
    download_recordings_from_inventory()

    logger.info("Backup process completed!")

    # Print summary
    cursor.execute(f"SELECT COUNT(*) FROM zoom_recordings_{VERSION}")
    result = cursor.fetchone()
    meeting_count = result[0] if result else 0

    cursor.execute(f"SELECT COUNT(*) FROM zoom_phone_recordings_{VERSION}")
    result = cursor.fetchone()
    phone_count = result[0] if result else 0

    cursor.execute(
        f"SELECT status, COUNT(*) FROM zoom_recording_inventory_{VERSION} GROUP BY status"
    )
    status_counts = cursor.fetchall()

    logger.info("Summary:")
    logger.info(f"  Meeting recordings downloaded: {meeting_count}")
    logger.info(f"  Phone recordings downloaded: {phone_count}")
    logger.info("  Inventory status:")
    for status, count in status_counts:
        logger.info(f"    {status}: {count}")

    # Show year distribution from inventory
    cursor.execute(f"""
        SELECT EXTRACT(YEAR FROM start_time) as year, COUNT(*), recording_type
        FROM zoom_recording_inventory_{VERSION} 
        GROUP BY EXTRACT(YEAR FROM start_time), recording_type
        ORDER BY year, recording_type
    """)
    results = cursor.fetchall()

    logger.info("Inventory by year and type:")
    for year, count, rec_type in results:
        logger.info(f"  {year or 'NULL'} ({rec_type}): {count}")


if __name__ == "__main__":
    main()
