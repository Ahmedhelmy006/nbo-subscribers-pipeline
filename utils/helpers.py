"""
Helper utilities for the NBO Pipeline.

This module provides general utility functions used throughout the pipeline.
"""
import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime


logger = logging.getLogger(__name__)

def ensure_directory(directory):
    """
    Ensure a directory exists, create it if it doesn't.
    
    Args:
        directory: Directory path as string or Path object
    """
    path = Path(directory)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {path}")

def save_json(data, filepath):
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save
        filepath: Path to save the file
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        # Ensure directory exists
        ensure_directory(Path(filepath).parent)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved data to {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to {filepath}: {e}")
        return False

def load_json(filepath):
    """
    Load data from a JSON file.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        Data from the JSON file or None if error
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Loaded data from {filepath}")
        return data
    except Exception as e:
        logger.error(f"Error loading data from {filepath}: {e}")
        return None

def format_time(seconds):
    """
    Format time in seconds to a human-readable string.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted time string
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

def generate_filename(prefix, chunk_num=None, timestamp=True):
    """
    Generate a filename with optional timestamp and chunk number.
    
    Args:
        prefix: Filename prefix
        chunk_num: Chunk number
        timestamp: Whether to include a timestamp
        
    Returns:
        Generated filename
    """
    parts = [prefix]
    
    if timestamp:
        parts.append(datetime.now().strftime("%Y%m%d_%H%M%S"))
    
    if chunk_num is not None:
        parts.append(f"chunk_{chunk_num}")
    
    return "_".join(parts) + ".json"

def log_progress(total_processed, total_expected, start_time):
    """
    Log progress information.
    
    Args:
        total_processed: Number of items processed
        total_expected: Expected total number of items
        start_time: Start time in seconds since epoch
    """
    elapsed_time = time.time() - start_time
    
    if total_processed > 0:
        items_per_second = total_processed / elapsed_time
        estimated_total_time = total_expected / items_per_second if items_per_second > 0 else 0
        remaining_time = estimated_total_time - elapsed_time if estimated_total_time > 0 else 0
        
        percent_complete = (total_processed / total_expected) * 100 if total_expected > 0 else 0
        
        logger.info(
            f"Progress: {total_processed}/{total_expected} ({percent_complete:.1f}%) - "
            f"Elapsed: {format_time(elapsed_time)} - "
            f"Remaining: {format_time(remaining_time)} - "
            f"Speed: {items_per_second:.1f} items/sec"
        )
    else:
        logger.info(f"Starting process - Target: {total_expected} items")

def mask_email(email):
    """
    Mask an email address for privacy in logs.
    
    Args:
        email: Email address to mask
        
    Returns:
        Masked email address
    """
    if not email or '@' not in email:
        return email
        
    username, domain = email.split('@')
    
    # Special case for very short usernames (1-2 chars)
    if len(username) <= 2:
        masked_username = '*' * len(username)
    # Special case for 3-character usernames
    elif len(username) == 3:
        # For 3-character usernames like "joe", return "j*e"
        masked_username = username[0] + '*' + username[-1]
    else:
        # For longer usernames, use 4 asterisks
        masked_username = username[0] + '****' + username[-1]
    
    return f"{masked_username}@{domain}"

def safe_extract(data, *keys, default=None):
    """
    Safely extract a value from nested dictionaries.
    
    Args:
        data: Dictionary to extract from
        *keys: Keys to traverse
        default: Default value if not found
        
    Returns:
        Extracted value or default
    """
    current = data
    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError, IndexError):
        return default
    
def parse_iso_datetime(date_string: str) -> datetime:
    """
    Parse an ISO format date string into a datetime object.
    
    Args:
        date_string: ISO format date string (e.g., '2025-03-27T19:25:45Z')
        
    Returns:
        datetime: Parsed datetime object
    """
    if not date_string:
        return None
    
    # Skip parsing for timezone strings
    if date_string.startswith('UTC'):
        return None
    
    # Handle different ISO formats
    if date_string.endswith('Z'):
        # Remove the 'Z' and parse
        date_string = date_string[:-1]
        
    # Parse ISO format string
    try:
        # Try standard format first
        return datetime.fromisoformat(date_string)
    except ValueError:
        try:
            # Try with microseconds
            return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            try:
                # Try without microseconds
                return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # Return None for any other format errors
                return None
            
def _format_region_name(self, region_name: str) -> Optional[str]:
    """
    Format region name with proper capitalization and spaces.
    
    Args:
        region_name: Original region name
        
    Returns:
        Formatted region name or None if 'Unknown'
    """
    if not region_name or region_name.lower() == 'unknown':
        return None
        
    # Replace underscores with spaces and use title case
    formatted_name = region_name.replace('_', ' ').title()
    
    # Special case for specific regions if needed
    if formatted_name.lower() == "latin america":
        formatted_name = "South America"
        
    return formatted_name