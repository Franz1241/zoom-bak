"""
Zoom API recording download module.
Handles downloading of recordings from inventory.
"""

import json
import os
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from utils.file import create_dirs, download_file, get_file_extension
from database.inventory import get_undownloaded_recordings, update_recording_status
from database.metadata import save_meeting_metadata, save_phone_metadata
from logging_config import get_logger

logger = get_logger()

# Global token refresh tracking
_token_refresh_counter = {
    'consecutive_401s': 0,
    'total_401s': 0,
    'last_refresh': None
}


def add_passcode_to_url(download_url, meeting_data):
    """
    Add passcode to download URL if available in meeting data.
    
    Args:
        download_url: Original download URL
        meeting_data: Meeting data dictionary containing passcode info
        
    Returns:
        str: URL with passcode parameter added if available
    """
    # Check if passcode is available
    passcode = meeting_data.get('recording_play_passcode')
    if not passcode:
        logger.debug("No passcode found in meeting data")
        return download_url
    
    # Parse the URL
    parsed = urlparse(download_url)
    query_params = parse_qs(parsed.query)
    
    # Add passcode parameter
    query_params['pwd'] = [passcode]
    
    # Reconstruct URL
    new_query = urlencode(query_params, doseq=True)
    new_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))
    
    logger.debug(f"Added passcode to URL: {passcode[:8]}...")
    return new_url


def should_refresh_token():
    """
    Check if token should be refreshed based on 401 error count.
    
    Returns:
        bool: True if token should be refreshed
    """
    return _token_refresh_counter['consecutive_401s'] >= 1


def reset_401_counter():
    """Reset the 401 error counter after successful operation."""
    _token_refresh_counter['consecutive_401s'] = 0


def increment_401_counter():
    """Increment 401 error counters."""
    _token_refresh_counter['consecutive_401s'] += 1
    _token_refresh_counter['total_401s'] += 1
    logger.warning(f"401 error count: {_token_refresh_counter['consecutive_401s']} consecutive, {_token_refresh_counter['total_401s']} total")


def refresh_token_if_needed(config):
    """
    Refresh token if needed based on 401 error count.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        str: New token or None if refresh failed
    """
    if should_refresh_token():
        logger.warning(f"Refreshing token due to {_token_refresh_counter['consecutive_401s']} consecutive 401 errors...")
        try:
            from zoom_api.auth import get_access_token
            new_token = get_access_token(config, force_refresh=True)
            _token_refresh_counter['last_refresh'] = datetime.now()
            reset_401_counter()
            logger.info("✅ Token refreshed successfully")
            return new_token
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            return None
    return None


def download_file_with_token_refresh(url, token, dest_path, config, file_description="file"):
    """
    Download file with automatic token refresh on 401 errors.
    
    Args:
        url: Download URL
        token: Authorization token (will be updated if refreshed)
        dest_path: Destination file path
        config: Configuration dictionary
        file_description: Description for logging
        
    Returns:
        tuple: (success, new_token) - new_token is updated if refreshed
    """
    import requests
    
    current_token = token
    
    try:
        # First attempt with current token
        success = download_file(url, current_token, dest_path, config, file_description)
        
        if success:
            reset_401_counter()
            return True, current_token
        else:
            # Check if we should refresh token and try again
            increment_401_counter()
            new_token = refresh_token_if_needed(config)
            
            if new_token:
                logger.info(f"Retrying download with refreshed token: {file_description}")
                success = download_file(url, new_token, dest_path, config, file_description)
                if success:
                    reset_401_counter()
                    return True, new_token
                else:
                    return False, new_token
            else:
                return False, current_token
                
    except Exception as e:
        if "401" in str(e) or "Unauthorized" in str(e):
            increment_401_counter()
            new_token = refresh_token_if_needed(config)
            if new_token:
                try:
                    logger.info(f"Retrying download with refreshed token after 401: {file_description}")
                    success = download_file(url, new_token, dest_path, config, file_description)
                    if success:
                        reset_401_counter()
                        return True, new_token
                    else:
                        return False, new_token
                except Exception as retry_e:
                    logger.error(f"Retry after token refresh also failed: {retry_e}")
                    return False, new_token
            else:
                return False, current_token
        else:
            logger.error(f"Non-401 error during download: {e}")
            return False, current_token


def download_recordings_from_inventory(token, config, cursor, conn, version):
    """
    Phase 2: Download recordings from inventory.

    Args:
        token: Access token
        config: Configuration dictionary
        cursor: Database cursor
        conn: Database connection
        version: Database version string
        
    Returns:
        str: Updated token (may be refreshed during process)
    """
    logger.info("Starting download phase...")

    # Get all recordings that haven't been downloaded yet
    recordings = get_undownloaded_recordings(cursor, version)
    logger.info(f"Found {len(recordings)} recordings to download")

    current_token = token
    successful_downloads = 0
    failed_downloads = 0

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
                success, current_token = download_meeting_from_inventory(
                    inv_id, user_email, raw_data, current_token, config, cursor, conn, version
                )
            elif rec_type == "phone":
                success, current_token = download_phone_from_inventory(
                    inv_id, user_email, raw_data, current_token, config, cursor, conn, version
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
            
            if success:
                successful_downloads += 1
            else:
                failed_downloads += 1

        except Exception as e:
            logger.error(f"Error downloading recording {rec_id}: {e}")
            update_recording_status(
                cursor, conn, inv_id, "failed", None, str(e), version
            )
            failed_downloads += 1

    logger.info(f"Download phase completed: {successful_downloads} successful, {failed_downloads} failed")
    logger.info(f"Total 401 errors encountered: {_token_refresh_counter['total_401s']}")
    
    return current_token


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
        tuple: (success, updated_token)
    """
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        meeting = data["meeting"]
        file_info = data["file_info"]

        base_dir = config["directories"]["base_dir"] + "_" + version
        user_dir = create_dirs(base_dir, user_email, "meetings")

        file_ext = get_file_extension(file_info, config)
        filename = f"{meeting.get('id', 'unknown')}_{file_info.get('id', 'unknown')}.{file_ext}"
        file_path = os.path.join(user_dir, filename)

        # Download the main file
        if not os.path.exists(file_path):
            # Add passcode to download URL if available
            download_url = add_passcode_to_url(file_info["download_url"], meeting)
            
            success, token = download_file_with_token_refresh(
                download_url,
                token,
                file_path,
                config,
                f"meeting recording ({file_info.get('file_type', 'unknown')})",
            )
            if not success:
                return False, token
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata
        metadata_success = save_meeting_metadata(
            cursor, conn, meeting, user_email, file_info, file_path, None, version
        )
        return metadata_success, token

    except Exception as e:
        logger.error(f"Error in download_meeting_from_inventory: {e}")
        return False, token


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
        tuple: (success, updated_token)
    """
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
        recording = data["recording"]

        base_dir = config["directories"]["base_dir"] + "_" + version
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
            success, token = download_file_with_token_refresh(
                recording["download_url"], token, file_path, config, "phone recording"
            )
            if not success:
                return False, token
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata
        metadata_success = save_phone_metadata(
            cursor, conn, recording, user_email, file_path, version
        )
        return metadata_success, token

    except Exception as e:
        logger.error(f"Error in download_phone_from_inventory: {e}")
        return False, token


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

        base_dir = config["directories"]["base_dir"] + "_" + version
        user_dir = create_dirs(base_dir, user_email, "webinars")

        file_ext = get_file_extension(file_info, config)
        filename = f"{webinar.get('id', 'unknown')}_{file_info.get('id', 'unknown')}.{file_ext}"
        file_path = os.path.join(user_dir, filename)

        # Download the main file
        if not os.path.exists(file_path):
            # Add passcode to download URL if available
            download_url = add_passcode_to_url(file_info["download_url"], webinar)
            
            success, token = download_file_with_token_refresh(
                download_url,
                token,
                file_path,
                config,
                f"webinar recording ({file_info.get('file_type', 'unknown')})",
            )
            if not success:
                return False, token
        else:
            logger.debug(f"File already exists: {file_path}")

        # Save metadata (reuse webinar metadata function from database.metadata)
        from database.metadata import save_webinar_metadata

        metadata_success = save_webinar_metadata(
            cursor, conn, webinar, user_email, file_info, file_path, None, version
        )
        return metadata_success, token

    except Exception as e:
        logger.error(f"Error in download_webinar_from_inventory: {e}")
        return False, token


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

