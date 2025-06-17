"""
Zoom API user management module.
Handles user-related API operations.
"""
from urllib.parse import quote
from utils.api import make_api_request
from logging_config import get_logger

logger = get_logger()


def get_zoom_users(token, config):
    """
    Get all Zoom users with pagination.
    
    Args:
        token: Access token
        config: Configuration dictionary
        
    Returns:
        list: List of user email addresses
    """
    users = []
    next_page_token = None

    while True:
        params = {"page_size": config['api']['page_sizes']['users'], "status": "active"}
        if next_page_token:
            params["next_page_token"] = next_page_token

        url = "https://api.zoom.us/v2/users"
        data = make_api_request(url, token, config, params)

        if not data:
            break

        for user in data.get("users", []):
            users.append(user["email"])

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    logger.info(f"Found {len(users)} users")
    return users


def get_user_info(user_email, token, config):
    """
    Get detailed information for a specific user.
    
    Args:
        user_email: User email address
        token: Access token
        config: Configuration dictionary
        
    Returns:
        dict: User information or None if failed
    """
    url = f"https://api.zoom.us/v2/users/{quote(user_email)}"
    return make_api_request(url, token, config)


def get_user_settings(user_email, token, config):
    """
    Get user settings.
    
    Args:
        user_email: User email address
        token: Access token
        config: Configuration dictionary
        
    Returns:
        dict: User settings or None if failed
    """
    url = f"https://api.zoom.us/v2/users/{quote(user_email)}/settings"
    return make_api_request(url, token, config)


def is_user_active(user_email, token, config):
    """
    Check if a user is active.
    
    Args:
        user_email: User email address
        token: Access token
        config: Configuration dictionary
        
    Returns:
        bool: True if user is active, False otherwise
    """
    user_info = get_user_info(user_email, token, config)
    if not user_info:
        return False
    
    return user_info.get("status") == "active"


def validate_user_email(user_email):
    """
    Basic validation for user email format.
    
    Args:
        user_email: Email address to validate
        
    Returns:
        bool: True if email format is valid
    """
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, user_email)) 