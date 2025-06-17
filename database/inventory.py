"""
Database inventory operations for Zoom Backup application.
Handles recording inventory management and queries.
"""

import json
from logging_config import get_logger
from utils.misc import db_retry

logger = get_logger()


@db_retry(tries=3, delay=1, backoff=2, logger=logger)
def insert_meeting_inventory(
    cursor,
    conn,
    recording_type,
    recording_id,
    meeting_id,
    user_email,
    topic,
    start_time,
    duration,
    file_type,
    file_size,
    download_url,
    raw_data,
    version="v4",
):
    """
    Insert meeting recording into inventory.

    Args:
        cursor: Database cursor
        conn: Database connection
        recording_type: Type of recording ('meeting', 'phone', 'webinar')
        recording_id: Unique recording ID
        meeting_id: Meeting UUID
        user_email: User email address
        topic: Meeting topic
        start_time: Meeting start time
        duration: Meeting duration
        file_type: File type
        file_size: File size in bytes
        download_url: Download URL
        raw_data: Raw API response data
        version: Database version for table naming

    Returns:
        bool: True if successful, False otherwise
    """
    cursor.execute(
        f"""
        INSERT INTO zoom_recording_inventory_{version} 
        (recording_type, recording_id, meeting_id, user_email, topic, 
         start_time, duration, file_type, file_size, download_url, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (recording_type, recording_id, file_type) DO NOTHING
    """,
        (
            recording_type,
            recording_id,
            meeting_id,
            user_email,
            topic,
            start_time,
            duration,
            file_type,
            file_size,
            download_url,
            json.dumps(raw_data),
        ),
    )
    return True


@db_retry(tries=3, delay=1, backoff=2, logger=logger)
def insert_phone_inventory(
    cursor,
    conn,
    recording_id,
    user_email,
    start_time,
    duration,
    file_type,
    file_size,
    download_url,
    raw_data,
    version="v4",
):
    """
    Insert phone recording into inventory.

    Args:
        cursor: Database cursor
        conn: Database connection
        recording_id: Unique recording ID
        user_email: User email address
        start_time: Recording start time
        duration: Recording duration
        file_type: File type
        file_size: File size in bytes
        download_url: Download URL
        raw_data: Raw API response data
        version: Database version for table naming

    Returns:
        bool: True if successful, False otherwise
    """
    cursor.execute(
        f"""
        INSERT INTO zoom_recording_inventory_{version} 
        (recording_type, recording_id, user_email, start_time, 
         duration, file_type, file_size, download_url, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (recording_type, recording_id, file_type) DO NOTHING
    """,
        (
            "phone",
            recording_id,
            user_email,
            start_time,
            duration,
            file_type,
            file_size,
            download_url,
            json.dumps(raw_data),
        ),
    )
    return True


def get_undownloaded_recordings(cursor, version="v4"):
    """
    Get all recordings that haven't been downloaded yet.

    Args:
        cursor: Database cursor
        version: Database version for table naming

    Returns:
        list: List of recording tuples
    """
    cursor.execute(f"""
        SELECT id, recording_type, recording_id, user_email, file_type, 
               download_url, raw_data, start_time, topic
        FROM zoom_recording_inventory_{version} 
        WHERE status = 'found'
        ORDER BY start_time DESC
    """)
    return cursor.fetchall()


@db_retry(tries=3, delay=1, backoff=2, logger=logger)
def update_recording_status(
    cursor,
    conn,
    inventory_id,
    status,
    downloaded_at=None,
    error_message=None,
    version="v4",
):
    """
    Update the status of a recording in inventory.

    Args:
        cursor: Database cursor
        conn: Database connection
        inventory_id: Inventory record ID
        status: New status ('downloaded', 'failed', 'skipped')
        downloaded_at: Optional download timestamp
        error_message: Optional error message
        version: Database version for table naming

    Returns:
        bool: True if successful, False otherwise
    """
    cursor.execute(
        f"""
        UPDATE zoom_recording_inventory_{version} 
        SET status = %s, downloaded_at = %s, error_message = %s
        WHERE id = %s
    """,
        (status, downloaded_at, error_message, inventory_id),
    )
    conn.commit()
    return True


def get_discovery_summary(cursor, version="v4"):
    """
    Get summary of discovered recordings by type.

    Args:
        cursor: Database cursor
        version: Database version for table naming

    Returns:
        list: List of (recording_type, count, earliest, latest) tuples
    """
    cursor.execute(f"""
        SELECT recording_type, COUNT(*), 
               MIN(start_time) as earliest, 
               MAX(start_time) as latest
        FROM zoom_recording_inventory_{version} 
        GROUP BY recording_type
    """)
    return cursor.fetchall()


def get_2020_recordings(cursor, version="v4"):
    """
    Get recordings from November-December 2020.

    Args:
        cursor: Database cursor
        version: Database version for table naming

    Returns:
        list: List of (recording_type, user_email, count) tuples
    """
    cursor.execute(f"""
        SELECT recording_type, user_email, COUNT(*) 
        FROM zoom_recording_inventory_{version} 
        WHERE start_time >= '2020-11-01' AND start_time < '2021-01-01'
        GROUP BY recording_type, user_email
        ORDER BY user_email
    """)
    return cursor.fetchall()


def get_status_counts(cursor, version="v4"):
    """
    Get count of recordings by status.

    Args:
        cursor: Database cursor
        version: Database version for table naming

    Returns:
        list: List of (status, count) tuples
    """
    cursor.execute(
        f"SELECT status, COUNT(*) FROM zoom_recording_inventory_{version} GROUP BY status"
    )
    return cursor.fetchall()


def get_year_distribution(cursor, version="v4"):
    """
    Get distribution of recordings by year and type.

    Args:
        cursor: Database cursor
        version: Database version for table naming

    Returns:
        list: List of (year, count, recording_type) tuples
    """
    cursor.execute(f"""
        SELECT EXTRACT(YEAR FROM start_time) as year, COUNT(*), recording_type
        FROM zoom_recording_inventory_{version} 
        GROUP BY EXTRACT(YEAR FROM start_time), recording_type
        ORDER BY year, recording_type
    """)
    return cursor.fetchall()


def get_download_counts(cursor, version="v4"):
    """
    Get counts of downloaded recordings by type.

    Args:
        cursor: Database cursor
        version: Database version for table naming

    Returns:
        tuple: (meeting_count, phone_count)
    """
    # Get meeting recordings count
    cursor.execute(f"SELECT COUNT(*) FROM zoom_recordings_{version}")
    result = cursor.fetchone()
    meeting_count = result[0] if result else 0

    # Get phone recordings count
    cursor.execute(f"SELECT COUNT(*) FROM zoom_phone_recordings_{version}")
    result = cursor.fetchone()
    phone_count = result[0] if result else 0

    return meeting_count, phone_count

