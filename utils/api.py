"""
API utilities module.
Contains generic API request handling and utilities.
"""
import time
import functools
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from logging_config import get_logger

logger = get_logger()


def make_api_request(url, token, config, params=None, retries=None):
    """
    Make API request with rate limiting and error handling.
    
    Args:
        url: API endpoint URL
        token: Authorization token
        config: Configuration dictionary
        params: Optional query parameters
        retries: Number of retries (defaults to config value)
        
    Returns:
        dict: API response data or None if failed
    """
    if retries is None:
        retries = config['api']['retries']
    
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(retries):
        time.sleep(config['api']['rate_limit_delay'])
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
                    time.sleep(config['api']['sleep_durations']['retry'])
                continue

            if response.status_code == 401:  # Unauthorized
                logger.warning(f"401 Unauthorized for {url} - attempting token refresh")
                if attempt < retries - 1:
                    # Import here to avoid circular imports
                    from zoom_api.auth import get_access_token
                    token = get_access_token(config, force_refresh=True)
                    headers["Authorization"] = f"Bearer {token}"
                    time.sleep(config['api']['sleep_durations']['token_refresh'])
                    continue
                else:
                    logger.error(
                        f"Final 401 error for {url} - user may not have permissions"
                    )
                    return None

            elif response.status_code == 429:  # Rate limited
                logger.warning(f"Rate limited, waiting {config['api']['sleep_durations']['rate_limit']} seconds...")
                time.sleep(config['api']['sleep_durations']['rate_limit'])
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
                    time.sleep(config['api']['sleep_durations']['retry'])
                continue
                return None

        except Exception as e:
            logger.error(f"Request Error: {e}")
            if attempt < retries - 1:
                time.sleep(config['api']['sleep_durations']['retry'])
                continue
            return None

    return None


def generate_date_ranges(start_date, end_date, config, months_per_range=None):
    """
    Generate smaller date ranges to avoid API limits.
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        config: Configuration dictionary
        months_per_range: Months per range (defaults to config value)
        
    Returns:
        list: List of (start_date, end_date) tuples
    """
    if months_per_range is None:
        months_per_range = config['processing']['months_per_range']
    
    ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current < end:
        range_end = min(current + relativedelta(months=months_per_range), end)
        ranges.append((current.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
        current = range_end

    logger.debug(f"Generated {len(ranges)} date ranges")
    return ranges 