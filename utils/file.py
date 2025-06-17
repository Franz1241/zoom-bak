"""
File utilities module.
Contains file handling and directory operations.
"""

import os
import requests
from logging_config import get_logger
from utils.misc import file_retry

logger = get_logger()


def create_dirs(base_dir, user_email, data_type="meetings"):
    """
    Create directory structure for user recordings.

    Args:
        base_dir: Base directory path
        user_email: User email address
        data_type: Type of data ("meetings", "phone", "webinar")

    Returns:
        str: Created directory path
    """
    user_dir = os.path.join(base_dir, data_type, user_email)
    os.makedirs(user_dir, exist_ok=True)
    logger.debug(f"Created directory: {user_dir}")
    return user_dir


def _download_file_with_retry(url, token, dest_path, config, file_description="file"):
    """
    Internal function to download file with retry decorator.

    Args:
        url: Download URL
        token: Authorization token
        dest_path: Destination file path
        config: Configuration dictionary
        file_description: Description for logging

    Returns:
        bool: True if successful, False otherwise
    """
    logger.debug(f"Downloading {file_description} to {dest_path}")

    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(
        url, headers=headers, stream=True, timeout=config["api"]["request_timeout"]
    )
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


def download_file(url, token, dest_path, config, file_description="file", retries=None):
    """
    Download file with proper error handling and logging.

    Args:
        url: Download URL
        token: Authorization token
        dest_path: Destination file path
        config: Configuration dictionary
        file_description: Description for logging
        retries: Number of retries (defaults to config value)

    Returns:
        bool: True if successful, False otherwise
    """
    if retries is None:
        retries = config["api"]["retries"]

    # Create a new decorated function with the correct parameters
    decorated_download = file_retry(
        tries=retries,
        delay=config["api"]["sleep_durations"]["download_retry"],
        backoff=1,  # No exponential backoff
        logger=logger,
    )(_download_file_with_retry)

    try:
        return decorated_download(url, token, dest_path, config, file_description)
    except Exception as e:
        logger.error(
            f"Failed to download {file_description} after {retries} attempts: {e}"
        )
        return False


def get_file_extension(file_info, config):
    """
    Get appropriate file extension from file info.

    Args:
        file_info: File information dictionary
        config: Configuration dictionary

    Returns:
        str: File extension
    """
    file_ext = file_info.get("file_extension")
    if file_ext:
        return file_ext.lower()

    file_type = file_info.get("file_type", "").lower()

    # Get mapping from config
    type_mapping = config["file_extensions"]

    return type_mapping.get(file_type, file_type or "unknown")


def get_safe_filename(filename):
    """
    Convert filename to safe format for filesystem.

    Args:
        filename: Original filename

    Returns:
        str: Safe filename
    """
    # Replace unsafe characters
    unsafe_chars = ["<", ">", ":", '"', "/", "\\", "|", "?", "*"]
    safe_filename = filename
    for char in unsafe_chars:
        safe_filename = safe_filename.replace(char, "_")

    return safe_filename.strip()


def ensure_directory_exists(directory_path):
    """
    Ensure directory exists, create if it doesn't.

    Args:
        directory_path: Directory path to ensure

    Returns:
        bool: True if directory exists or was created successfully
    """
    try:
        os.makedirs(directory_path, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Failed to create directory {directory_path}: {e}")
        return False

