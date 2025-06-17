"""
Zoom API authentication module.
Handles OAuth token management and refresh.
"""
import os
import time
import functools
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from logging_config import get_logger
from utils.misc import api_retry

logger = get_logger()

# Global token variables for automatic refresh
access_token = None
token_expires_at = None


@api_retry(tries=3, delay=5, backoff=2, logger=logger)
def _get_token_from_api(config):
    """
    Internal function to get token from API with retry logic.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        dict: Token data from API
    """
    # Get credentials from environment
    zoom_account_id = os.getenv("ZOOM_ACCOUNT_ID")
    zoom_client_id = os.getenv("ZOOM_CLIENT_ID")
    zoom_client_secret = os.getenv("ZOOM_CLIENT_SECRET")
    
    if not all([zoom_account_id, zoom_client_id, zoom_client_secret]):
        raise ValueError("Missing Zoom client credentials in environment variables")
    
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={zoom_account_id}"

    response = requests.post(url, auth=(zoom_client_id, zoom_client_secret))
    response.raise_for_status()
    return response.json()


def get_access_token(config, force_refresh=False):
    """
    Get access token with auto-refresh capability.
    
    Args:
        config: Configuration dictionary
        force_refresh: Force token refresh even if current token is valid
        
    Returns:
        str: Access token
        
    Raises:
        Exception: If token acquisition fails
    """
    global access_token, token_expires_at

    # Check if we need to refresh the token
    if not force_refresh and access_token and token_expires_at:
        if datetime.now() < token_expires_at:
            return access_token

    logger.info("Refreshing access token...")
    
    try:
        token_data = _get_token_from_api(config)

        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
        token_expires_at = datetime.now() + relativedelta(
            seconds=expires_in - config['api']['token_refresh_buffer']
        )  # Refresh early based on config

        logger.info("Access token refreshed successfully")
        return access_token

    except Exception as e:
        logger.error(f"Failed to get access token: {e}")
        raise


def get_current_token():
    """
    Get the current access token without refresh.
    
    Returns:
        str: Current access token or None if not available
    """
    return access_token


def is_token_valid():
    """
    Check if the current token is valid.
    
    Returns:
        bool: True if token is valid, False otherwise
    """
    if not access_token or not token_expires_at:
        return False
    return datetime.now() < token_expires_at 