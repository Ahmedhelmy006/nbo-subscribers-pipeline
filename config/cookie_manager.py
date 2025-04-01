"""
Cookie management system for the NBO Pipeline.

This module provides a centralized way to manage cookies for various services,
making it easy to update them when they expire without modifying code.
"""
import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path

from . import settings

logger = logging.getLogger(__name__)

class CookieManager:
    """
    Manages cookies for different services, allowing for easy updates
    when cookies expire.
    """
    
    def __init__(self, cookie_file=None):
        """
        Initialize the cookie manager.
        
        Args:
            cookie_file (str, optional): Path to the cookie file.
                Defaults to the value in settings.COOKIE_FILE.
        """
        self.cookie_file = cookie_file or settings.COOKIE_FILE
        self.cookies = {}
        self.load_cookies()
    
    def load_cookies(self):
        """Load cookies from the central file."""
        try:
            cookie_path = Path(self.cookie_file)
            if cookie_path.exists():
                with open(cookie_path, 'r') as f:
                    self.cookies = json.load(f)
                logger.info(f"Loaded cookies from {self.cookie_file}")
            else:
                logger.warning(f"Cookie file {self.cookie_file} not found. Using empty cookies.")
                self.cookies = {}
                
                # Try to create the directory structure if it doesn't exist
                cookie_dir = cookie_path.parent
                try:
                    cookie_dir.mkdir(parents=True, exist_ok=True)
                    # Create an empty cookie file
                    with open(cookie_path, 'w') as f:
                        json.dump({}, f, indent=2)
                    logger.info(f"Created empty cookie file at {self.cookie_file}")
                except Exception as e:
                    logger.warning(f"Could not create cookie file directory: {e}")
                    # Don't treat this as a fatal error
                    
        except Exception as e:
            logger.warning(f"Error loading cookies: {e}")
            self.cookies = {}
    
    def get_cookie(self, service_name):
        """
        Get cookie for a specific service.
        
        Args:
            service_name (str): Name of the service (e.g., 'convertkit', 'google')
            
        Returns:
            str: Cookie string or empty string if not found
        """
        if service_name in self.cookies:
            return self.cookies[service_name].get('value', '')
        logger.debug(f"No cookies found for service: {service_name}")
        return ""
    
    def update_cookie(self, service_name, cookie_value):
        """
        Update cookie for a specific service.
        
        Args:
            service_name (str): Name of the service
            cookie_value (str): Cookie string
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        self.cookies[service_name] = {
            'value': cookie_value,
            'updated_at': datetime.now().isoformat()
        }
        
        try:
            # Ensure directory exists
            cookie_path = Path(self.cookie_file)
            cookie_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write cookies to file
            with open(cookie_path, 'w') as f:
                json.dump(self.cookies, f, indent=2)
            
            logger.info(f"Updated cookies for {service_name}")
            return True
        except Exception as e:
            logger.warning(f"Error updating cookies: {e}")
            return False
    
    def is_cookie_expired(self, service_name, max_age_days=7):
        """
        Check if a cookie is likely expired based on when it was last updated.
        
        Args:
            service_name (str): Name of the service
            max_age_days (int): Maximum age in days before considering expired
            
        Returns:
            bool: True if cookie likely expired, False otherwise
        """
        if service_name not in self.cookies:
            return True
            
        try:
            updated_at = self.cookies[service_name].get('updated_at')
            if not updated_at:
                return True
                
            updated_timestamp = datetime.fromisoformat(updated_at)
            now = datetime.now()
            
            # Check if cookie is older than max_age_days
            age = (now - updated_timestamp).days
            return age >= max_age_days
            
        except Exception as e:
            logger.warning(f"Error checking cookie expiration: {e}")
            return True
    
    def get_all_services(self):
        """
        Get a list of all services that have cookies.
        
        Returns:
            list: List of service names
        """
        return list(self.cookies.keys())
    
    def get_cookie_info(self, service_name):
        """
        Get detailed information about a cookie.
        
        Args:
            service_name (str): Name of the service
            
        Returns:
            dict: Cookie information including value and update timestamp
        """
        if service_name in self.cookies:
            return self.cookies[service_name]
        return None

# Create a singleton instance of the cookie manager
cookie_manager = CookieManager()