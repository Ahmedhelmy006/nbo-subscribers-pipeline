"""
LinkedIn stack for the NBO Pipeline.

This module provides a stack for LinkedIn URLs to be processed
by an external scraper.
"""
import logging
from typing import Dict, List, Any, Tuple

from config import settings
from .base_stack import BaseStack

logger = logging.getLogger(__name__)

class LinkedInStack(BaseStack):
    """
    Stack for LinkedIn URLs.
    
    Stores LinkedIn URLs for processing by an external scraper.
    """
    
    def __init__(self, stack_dir=None, max_size=None):
        """
        Initialize the LinkedIn stack.
        
        Args:
            stack_dir (str, optional): Directory to store stack files
            max_size (int, optional): Maximum number of items in a batch
        """
        super().__init__(
            stack_dir=stack_dir or settings.STACKS_DIR,
            prefix=settings.LINKEDIN_STACK_PREFIX,
            max_size=max_size or settings.LINKEDIN_STACK_MAX_SIZE,
            file_extension='.json'
        )
    
    def add_linkedin_url(self, subscriber_id: str, email_address: str, linkedin_url: str) -> bool:
        """
        Add a LinkedIn URL to the stack.
        
        Args:
            subscriber_id (str): Subscriber ID
            email_address (str): Email address
            linkedin_url (str): LinkedIn profile URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Create item
        item = {
            "subscriber_id": subscriber_id,
            "email_address": email_address,
            "linkedin_url": linkedin_url
        }
        
        # Add to stack
        return self.add_item(item)
    
    def add_linkedin_urls(self, linkedin_data: List[Tuple[str, str, str]]) -> int:
        """
        Add multiple LinkedIn URLs to the stack.
        
        Args:
            linkedin_data (List[Tuple]): List of (subscriber_id, email_address, linkedin_url) tuples
            
        Returns:
            int: Number of items successfully added
        """
        items = []
        
        for subscriber_id, email_address, linkedin_url in linkedin_data:
            items.append({
                "subscriber_id": subscriber_id,
                "email_address": email_address,
                "linkedin_url": linkedin_url
            })
        
        return self.add_items(items)