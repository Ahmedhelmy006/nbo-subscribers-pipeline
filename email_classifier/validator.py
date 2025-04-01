"""
Email validation for the NBO Pipeline.

This module provides functionality for validating email addresses.
"""
import re
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

# Simple regex for email validation
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

def is_valid_email(email: str) -> bool:
    """
    Check if an email address is valid.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not email:
        return False
    
    # Check if email matches regex
    return bool(EMAIL_REGEX.match(email))

def extract_email_parts(email: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract username and domain parts from an email address.
    
    Args:
        email: Email address to extract parts from
        
    Returns:
        Tuple of (username, domain) or (None, None) if invalid
    """
    if not is_valid_email(email):
        return None, None
    
    parts = email.split('@')
    if len(parts) != 2:
        return None, None
    
    username, domain = parts
    return username.lower(), domain.lower()

def normalize_email(email: str) -> str:
    """
    Normalize an email address by lowercasing it.
    
    Args:
        email: Email address to normalize
        
    Returns:
        Normalized email address or empty string if invalid
    """
    if not is_valid_email(email):
        return ""
    
    return email.lower()

def get_domain(email: str) -> str:
    """
    Get the domain from an email address.
    
    Args:
        email: Email address
        
    Returns:
        Domain or empty string if invalid
    """
    _, domain = extract_email_parts(email)
    return domain or ""

def get_username(email: str) -> str:
    """
    Get the username from an email address.
    
    Args:
        email: Email address
        
    Returns:
        Username or empty string if invalid
    """
    username, _ = extract_email_parts(email)
    return username or ""