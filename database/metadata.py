"""
Database metadata operations for Zoom Backup application.
Handles saving of meeting and phone recording metadata.
"""
import json
from logging_config import get_logger

logger = get_logger()


def save_meeting_metadata(cursor, conn, meeting, user_email, file_info, local_path, 
                         transcript_path=None, version="v4"):
    """
    Save meeting recording metadata with fallback strategy.
    
    Args:
        cursor: Database cursor
        conn: Database connection
        meeting: Meeting data from API
        user_email: User email address
        file_info: File information from API
        local_path: Local file path
        transcript_path: Optional transcript file path
        version: Database version for table naming
        
    Returns:
        bool: True if successful, False otherwise
    """
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
            INSERT INTO zoom_recordings_{version} (
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
        logger.debug(f"Successfully saved meeting metadata for: {meeting.get('topic', 'No topic')}")
        return True

    except Exception as e:
        logger.warning(f"Meeting metadata save failed: {e}")
        conn.rollback()
        return False


def save_phone_metadata(cursor, conn, recording, user_email, local_path, version="v4"):
    """
    Save phone recording metadata with fallback strategy.
    
    Args:
        cursor: Database cursor
        conn: Database connection
        recording: Phone recording data from API
        user_email: User email address
        local_path: Local file path
        version: Database version for table naming
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger.debug(f"Saving metadata for phone recording: {recording.get('id', 'No ID')}")
    
    fallback_data = {
        "recording": recording,
        "user_email": user_email,
        "path": local_path,
        "data_type": "phone",
    }

    try:
        cursor.execute(
            f"""
            INSERT INTO zoom_phone_recordings_{version} (
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
        logger.debug(f"Successfully saved phone metadata for: {recording.get('id', 'No ID')}")
        return True

    except Exception as e:
        logger.warning(f"Phone metadata save failed: {e}")
        conn.rollback()
        return False


def save_webinar_metadata(cursor, conn, webinar, user_email, file_info, local_path, 
                         transcript_path=None, version="v4"):
    """
    Save webinar recording metadata.
    
    Args:
        cursor: Database cursor
        conn: Database connection
        webinar: Webinar data from API
        user_email: User email address
        file_info: File information from API
        local_path: Local file path
        transcript_path: Optional transcript file path
        version: Database version for table naming
        
    Returns:
        bool: True if successful, False otherwise
    """
    logger.debug(f"Saving metadata for webinar: {webinar.get('topic', 'No topic')}")

    fallback_data = {
        "webinar": webinar,
        "file_info": file_info,
        "user_email": user_email,
        "path": local_path,
        "transcript_path": transcript_path,
        "data_type": "webinar",
    }

    try:
        cursor.execute(
            f"""
            INSERT INTO zoom_webinar_recordings_{version} (
                webinar_id, recording_id, topic, host_id, host_email, start_time,
                duration, file_type, file_size, recording_type,
                download_url, transcript_url, path, transcript_path, unprocessed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """,
            (
                webinar.get("uuid"),
                file_info.get("id"),
                webinar.get("topic"),
                webinar.get("host_id"),
                user_email,
                webinar.get("start_time"),
                webinar.get("duration"),
                file_info.get("file_type"),
                file_info.get("file_size"),
                file_info.get("recording_type"),
                file_info.get("download_url"),
                None,
                local_path,
                transcript_path,
                json.dumps(fallback_data),
            ),
        )
        conn.commit()
        logger.debug(f"Successfully saved webinar metadata for: {webinar.get('topic', 'No topic')}")
        return True

    except Exception as e:
        logger.warning(f"Webinar metadata save failed: {e}")
        conn.rollback()
        return False 