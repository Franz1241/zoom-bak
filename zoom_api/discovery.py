"""
Zoom API recording discovery module.
Handles discovery and cataloging of recordings before download.
"""
import json
from urllib.parse import quote
from utils.api import make_api_request, generate_date_ranges
from database.inventory import (
    insert_meeting_inventory, insert_phone_inventory, get_discovery_summary, get_2020_recordings
)
from logging_config import get_logger

logger = get_logger()


def discover_all_recordings(user_emails, token, config, cursor, conn, version):
    """
    Phase 1: Discover and catalog all recordings before downloading.
    
    Args:
        user_emails: List of user email addresses
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string
    """
    logger.info("Starting recording discovery phase...")

    for i, email in enumerate(user_emails, 1):
        logger.info(f"[{i}/{len(user_emails)}] Discovering recordings for: {email}")

        try:
            discover_meeting_recordings(email, token, config, cursor, conn, version)
            discover_phone_recordings(email, token, config, cursor, conn, version)
            # discover_webinar_recordings(email, token, config, cursor, conn, version)  # Add if needed

        except Exception as e:
            logger.error(f"Error discovering recordings for {email}: {e}")
            continue

    # Report discovery results
    results = get_discovery_summary(cursor, version)
    logger.info("Discovery Results:")
    for rec_type, count, earliest, latest in results:
        logger.info(f"  {rec_type}: {count} recordings ({earliest} to {latest})")

    # Check for 2020 recordings specifically
    results_2020 = get_2020_recordings(cursor, version)
    if results_2020:
        logger.info("Found recordings from Nov-Dec 2020:")
        for rec_type, email, count in results_2020:
            logger.info(f"  {email} ({rec_type}): {count} recordings")
    else:
        logger.warning("NO RECORDINGS FOUND FROM NOV-DEC 2020!")


def discover_meeting_recordings(user_email, token, config, cursor, conn, version):
    """
    Discover meeting recordings and store in inventory.
    
    Args:
        user_email: User email address
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string
    """
    logger.debug(f"Discovering meeting recordings for: {user_email}")

    start_date = config['dates']['start_date']
    from datetime import datetime, timezone
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    date_ranges = generate_date_ranges(start_date, end_date, config)
    total_found = 0

    for range_start, range_end in date_ranges:
        next_page_token = None
        range_found = 0

        while True:
            params = {"from": range_start, "to": range_end, "page_size": config['api']['page_sizes']['recordings']}
            if next_page_token:
                params["next_page_token"] = next_page_token

            url = f"https://api.zoom.us/v2/users/{quote(user_email)}/recordings"
            data = make_api_request(url, token, config, params)

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
                            {"meeting": meeting, "file_info": file_info}, version
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


def discover_phone_recordings(user_email, token, config, cursor, conn, version):
    """
    Discover phone recordings and store in inventory.
    
    Args:
        user_email: User email address
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string
    """
    logger.debug(f"Discovering phone recordings for: {user_email}")

    start_date = config['dates']['start_date']
    from datetime import datetime, timezone
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    next_page_token = None
    total_found = 0

    while True:
        params = {"from": start_date, "to": end_date, "page_size": config['api']['page_sizes']['phone_recordings']}
        if next_page_token:
            params["next_page_token"] = next_page_token

        url = f"https://api.zoom.us/v2/phone/users/{quote(user_email)}/recordings"
        data = make_api_request(url, token, config, params)

        if not data or "recordings" not in data:
            break

        for recording in data["recordings"]:
            if recording.get("download_url"):
                if insert_phone_inventory(
                    cursor, conn, recording.get("id"), user_email,
                    recording.get("start_time"), recording.get("duration"),
                    "mp3", recording.get("file_size"), recording.get("download_url"),
                    {"recording": recording}, version
                ):
                    total_found += 1

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    conn.commit()
    if total_found > 0:
        logger.info(f"Discovered {total_found} phone recordings for {user_email}")


def discover_webinar_recordings(user_email, token, config, cursor, conn, version):
    """
    Discover webinar recordings and store in inventory.
    
    Args:
        user_email: User email address
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string
    """
    logger.debug(f"Discovering webinar recordings for: {user_email}")

    start_date = config['dates']['start_date']
    from datetime import datetime, timezone
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    date_ranges = generate_date_ranges(start_date, end_date, config)
    total_found = 0

    for range_start, range_end in date_ranges:
        next_page_token = None
        range_found = 0

        while True:
            params = {"from": range_start, "to": range_end, "page_size": config['api']['page_sizes']['recordings']}
            if next_page_token:
                params["next_page_token"] = next_page_token

            url = f"https://api.zoom.us/v2/users/{quote(user_email)}/webinars/recordings"
            data = make_api_request(url, token, config, params)

            if not data or "webinars" not in data:
                break

            for webinar in data["webinars"]:
                for file_info in webinar.get("recording_files", []):
                    if (
                        file_info.get("download_url")
                        and file_info.get("status") == "completed"
                    ):
                        # Use similar structure to meeting recordings but for webinars
                        if insert_meeting_inventory(  # Reuse the same function with webinar type
                            cursor, conn, "webinar", file_info.get("id"), 
                            webinar.get("uuid"), user_email, webinar.get("topic"),
                            webinar.get("start_time"), webinar.get("duration"),
                            file_info.get("file_type"), file_info.get("file_size"),
                            file_info.get("download_url"),
                            {"webinar": webinar, "file_info": file_info}, version
                        ):
                            range_found += 1

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

        total_found += range_found
        if range_found > 0:
            logger.debug(
                f"Found {range_found} webinar recordings for {user_email} in {range_start} to {range_end}"
            )

    conn.commit()
    if total_found > 0:
        logger.info(f"Discovered {total_found} webinar recordings for {user_email}") 