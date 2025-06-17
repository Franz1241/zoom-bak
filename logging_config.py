"""
Logging configuration module for Zoom Backup application.
Provides centralized logging setup with file and console handlers.
"""
import logging
import os
from typing import Dict, Any


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Setup logging with separate files for different levels based on configuration.
    
    Args:
        config: Configuration dictionary containing logging settings
        
    Returns:
        Configured logger instance
    """
    log_config = config['logging']
    log_dir = config['directories']['log_dir']
    os.makedirs(log_dir, exist_ok=True)

    # Create logger
    logger = logging.getLogger("zoom_backup")
    logger.setLevel(getattr(logging, log_config['levels']['file_debug']))

    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    simple_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Debug file handler (all messages)
    debug_handler = logging.FileHandler(
        os.path.join(log_dir, log_config['files']['debug'])
    )
    debug_handler.setLevel(getattr(logging, log_config['levels']['file_debug']))
    debug_handler.setFormatter(detailed_formatter)
    logger.addHandler(debug_handler)

    # Info file handler (info and above)
    info_handler = logging.FileHandler(
        os.path.join(log_dir, log_config['files']['info'])
    )
    info_handler.setLevel(getattr(logging, log_config['levels']['file_info']))
    info_handler.setFormatter(simple_formatter)
    logger.addHandler(info_handler)

    # Warning file handler (warnings and errors only)
    warning_handler = logging.FileHandler(
        os.path.join(log_dir, log_config['files']['warnings'])
    )
    warning_handler.setLevel(getattr(logging, log_config['levels']['file_warning']))
    warning_handler.setFormatter(simple_formatter)
    logger.addHandler(warning_handler)

    # Console handler for important messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_config['levels']['console']))
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    logger.info("Logging setup completed successfully")
    return logger


def get_logger(name: str = "zoom_backup") -> logging.Logger:
    """
    Get an existing logger instance.
    
    Args:
        name: Logger name (default: "zoom_backup")
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name) 