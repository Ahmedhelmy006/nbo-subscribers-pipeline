"""
Logging configuration for the NBO Pipeline.

This module configures logging for the pipeline, setting up log formats,
handlers, and log levels.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler
from pathlib import Path

from . import settings

def configure_logging():
    """
    Configure logging for the pipeline.
    
    Sets up console logging with rich formatting and file logging with rotation.
    """
    # Create logs directory if it doesn't exist
    log_file = Path(settings.LOG_FILE)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Get log level from settings
    log_level = getattr(logging, settings.LOG_LEVEL, logging.INFO)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatters
    file_formatter = logging.Formatter(settings.LOG_FORMAT)
    
    # Create handlers
    console_handler = RichHandler(rich_tracebacks=True, markup=True)
    console_handler.setLevel(log_level)
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    
    # Add handlers to root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Set specific log levels for noisy modules
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('playwright').setLevel(logging.WARNING)
    
    logging.info(f"Logging configured at level {settings.LOG_LEVEL}")
    return root_logger

# Initialize logging when imported
logger = configure_logging()