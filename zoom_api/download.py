"""
Zoom API recording download module.
Handles downloading of recordings from inventory.
"""

import json
import os
from datetime import datetime
from utils.file import create_dirs, download_file, get_file_extension
from database.inventory import get_undownloaded_recordings, update_recording_status
from database.metadata import save_meeting_metadata, save_phone_metadata
from logging_config import get_logger

logger = get_logger()


def download_recordings_from_inventory(token, config, cursor, conn, version):
    """
    Phase 2: Download recordings from inventory.

    Args:
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string
    """
    logger.info("Starting download phase...")

    # Get all recordings that haven't been downloaded yet
    recordings = get_undownloaded_recordings(cursor, version)
    logger.info(f"Found {len(recordings)} recordings to download")

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
                    inv_id, user_email, raw_data, token, config, cursor, conn, version
                )
            elif rec_type == "phone":
                success = download_phone_from_inventory(
                    inv_id, user_email, raw_data, token, config, cursor, conn, version
                )
            else:
                success = False
                logger.warning(f"Unknown recording type: {rec_type}")

            # Update status
            status = "downloaded" if success else "failed"
            update_recording_status(
                cursor,
                conn,
                inv_id,
                status,
                datetime.now() if success else None,
                None,
                version,
            )

        except Exception as e:
            logger.error(f"Error downloading recording {rec_id}: {e}")
            update_recording_status(
                cursor, conn, inv_id, "failed", None, str(e), version
            )


def download_meeting_from_inventory(
    inv_id, user_email, raw_data, token, config, cursor, conn, version
):
    """
    Download meeting recording from inventory data.

    Args:
        inv_id: Inventory ID
        user_email: User email address
        raw_data: Raw recording data from inventory
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        meeting = data["meeting"]
        file_info = data["file_info"]

        base_dir = config["directories"]["base_dir"]
        user_dir = create_dirs(base_dir, user_email, "meetings")

        file_ext = get_file_extension(file_info, config)
        filename = f"{meeting.get('id', 'unknown')}_{file_info.get('id', 'unknown')}.{file_ext}"
        file_path = os.path.join(user_dir, filename)

        # Download the main file
        if not os.path.exists(file_path):
            success = download_file(
                file_info["download_url"],
                token,
                file_path,
                config,
                f"meeting recording ({file_info.get('file_type', 'unknown')})",
            )
            if not success:
                return False
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata
        return save_meeting_metadata(
            cursor, conn, meeting, user_email, file_info, file_path, None, version
        )

    except Exception as e:
        logger.error(f"Error in download_meeting_from_inventory: {e}")
        return False


def download_phone_from_inventory(
    inv_id, user_email, raw_data, token, config, cursor, conn, version
):
    """
    Download phone recording from inventory data.

    Args:
        inv_id: Inventory ID
        user_email: User email address
        raw_data: Raw recording data from inventory
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        recording = data["recording"]

        base_dir = config["directories"]["base_dir"]
        user_dir = create_dirs(base_dir, user_email, "phone")

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
                recording["download_url"], token, file_path, config, "phone recording"
            )
            if not success:
                return False
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata
        return save_phone_metadata(
            cursor, conn, recording, user_email, file_path, version
        )

    except Exception as e:
        logger.error(f"Error in download_phone_from_inventory: {e}")
        return False


def download_webinar_from_inventory(
    inv_id, user_email, raw_data, token, config, cursor, conn, version
):
    """
    Download webinar recording from inventory data.

    Args:
        inv_id: Inventory ID
        user_email: User email address
        raw_data: Raw recording data from inventory
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        webinar = data["webinar"]
        file_info = data["file_info"]

        base_dir = config["directories"]["base_dir"]
        user_dir = create_dirs(base_dir, user_email, "webinars")

        file_ext = get_file_extension(file_info, config)
        filename = f"{webinar.get('id', 'unknown')}_{file_info.get('id', 'unknown')}.{file_ext}"
        file_path = os.path.join(user_dir, filename)

        # Download the main file
        if not os.path.exists(file_path):
            success = download_file(
                file_info["download_url"],
                token,
                file_path,
                config,
                f"webinar recording ({file_info.get('file_type', 'unknown')})",
            )
            if not success:
                return False
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata (reuse webinar metadata function from database.metadata)
        from database.metadata import save_webinar_metadata

        return save_webinar_metadata(
            cursor, conn, webinar, user_email, file_info, file_path, None, version
        )

    except Exception as e:
        logger.error(f"Error in download_webinar_from_inventory: {e}")
        return False


def get_download_progress(cursor, version):
    """
    Get download progress statistics.

    Args:
        cursor: Database cursor
        version: Database version string

    Returns:
        dict: Progress statistics
    """
    from database.inventory import get_status_counts

    status_counts = get_status_counts(cursor, version)
    total = sum(count for _, count in status_counts)

    progress = {
        "total": total,
        "by_status": {status: count for status, count in status_counts},
    }

    if total > 0:
        downloaded = progress["by_status"].get("downloaded", 0)
        progress["percentage"] = (downloaded / total) * 100
    else:
        progress["percentage"] = 0

    return progress

