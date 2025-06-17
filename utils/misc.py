"""
Configuration utilities module.
Handles loading and validation of application configuration.
"""
import os
import yaml
from logging_config import get_logger

logger = get_logger()


def load_config(config_path="config.yaml"):
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file (default: "config.yaml")
        
    Returns:
        dict: Configuration dictionary
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        yaml.YAMLError: If YAML file is malformed
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found")
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Replace version placeholder in base_dir
        if 'directories' in config and 'base_dir' in config['directories']:
            config['directories']['base_dir'] = config['directories']['base_dir'].format(
                version=config['version']
            )
        
        logger.debug(f"Configuration loaded successfully from {config_path}")
        return config
        
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration file {config_path}: {e}")
        raise


def validate_config(config):
    """
    Validate required configuration sections and keys.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        bool: True if configuration is valid
        
    Raises:
        ValueError: If required configuration is missing
    """
    required_sections = ['version', 'database', 'directories', 'dates', 'api', 'logging']
    
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required configuration section: {section}")
    
    # Validate specific required keys
    required_keys = {
        'database': ['url'],
        'directories': ['base_dir', 'log_dir'],
        'dates': ['start_date'],
        'api': ['rate_limit_delay', 'retries', 'page_sizes', 'sleep_durations'],
        'logging': ['levels', 'files']
    }
    
    for section, keys in required_keys.items():
        for key in keys:
            if key not in config[section]:
                raise ValueError(f"Missing required configuration key: {section}.{key}")
    
    logger.debug("Configuration validation passed")
    return True


def get_config_value(config, key_path, default=None):
    """
    Get a configuration value using dot notation.
    
    Args:
        config: Configuration dictionary
        key_path: Dot-separated path to the configuration key (e.g., "api.retries")
        default: Default value if key is not found
        
    Returns:
        Configuration value or default
        
    Example:
        retries = get_config_value(config, "api.retries", 3)
        log_level = get_config_value(config, "logging.levels.console", "INFO")
    """
    keys = key_path.split('.')
    value = config
    
    try:
        for key in keys:
            value = value[key]
        return value
    except (KeyError, TypeError):
        return default


def update_config_value(config, key_path, new_value):
    """
    Update a configuration value using dot notation.
    
    Args:
        config: Configuration dictionary
        key_path: Dot-separated path to the configuration key
        new_value: New value to set
        
    Returns:
        bool: True if value was updated successfully
        
    Example:
        update_config_value(config, "api.retries", 5)
        update_config_value(config, "logging.levels.console", "DEBUG")
    """
    keys = key_path.split('.')
    current = config
    
    try:
        # Navigate to the parent of the target key
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        # Set the final value
        current[keys[-1]] = new_value
        logger.debug(f"Updated configuration: {key_path} = {new_value}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating configuration {key_path}: {e}")
        return False


def save_config(config, config_path="config.yaml"):
    """
    Save configuration to YAML file.
    
    Args:
        config: Configuration dictionary to save
        config_path: Path to save the configuration file
        
    Returns:
        bool: True if saved successfully
    """
    try:
        with open(config_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False, indent=2)
        
        logger.debug(f"Configuration saved to {config_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving configuration to {config_path}: {e}")
        return False 