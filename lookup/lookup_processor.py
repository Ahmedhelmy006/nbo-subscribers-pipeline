"""
LinkedIn lookup processor for the NBO Pipeline.

This module provides functionality for orchestrating the LinkedIn
profile lookup process.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple

from .name_extractor import NameExtractor
from .google_search import GoogleSearch
from utils.helpers import mask_email

logger = logging.getLogger(__name__)

class LinkedInLookupProcessor:
    """
    Orchestrates the LinkedIn profile lookup process.
    """
    
    def __init__(self):
        """Initialize the lookup processor."""
        self.name_extractor = NameExtractor()
        self.google_search = GoogleSearch()
    
    async def find_linkedin_profile(
        self,
        subscriber_id: str,
        email: str,
        first_name: Optional[str] = None,
        location_city: Optional[str] = None,
        location_state: Optional[str] = None,
        location_country: Optional[str] = None
    ) -> Optional[str]:
        """
        Find a LinkedIn profile for an email address.
        
        Args:
            subscriber_id: Subscriber ID
            email: Email address
            first_name: Provided first name (if available)
            location_city: City
            location_state: State/province
            location_country: Country
            
        Returns:
            LinkedIn profile URL or None if not found
        """
        # Extract the domain and check if it's a work email
        domain = email.split('@')[1] if '@' in email else None
        
        if not domain:
            logger.warning(f"Invalid email format: {mask_email(email)}")
            return None
        
        # Extract name from email
        extracted_name, extraction_method = self.name_extractor.extract_name_from_email(email, first_name)
        
        if not extracted_name:
            logger.warning(f"Could not extract name from {mask_email(email)}: {extraction_method}")
            return None
        
        logger.info(f"Extracted name '{extracted_name}' from {mask_email(email)} using {extraction_method}")
        
        # Parse first and last name
        name_parts = extracted_name.split()
        if len(name_parts) < 2:
            logger.warning(f"Insufficient name parts extracted for {mask_email(email)}: {extracted_name}")
            return None
            
        # Extract first and last name
        extracted_first_name = name_parts[0]
        extracted_last_name = name_parts[-1]
        
        # Build the search query
        search_components = []
        
        # Add name components
        search_components.append(f"{extracted_first_name} {extracted_last_name}")
        
        # Add company domain
        search_components.append(domain)
        
        # Add location information if available
        if location_state and location_state not in ["None", "null", "N/A", None, ""]:
            search_components.append(str(location_state))
        
        if location_country and location_country not in ["None", "null", "N/A", None, ""]:
            search_components.append(str(location_country))
        
        # Add LinkedIn
        search_components.append("LinkedIn")
        
        # Join with " + " format
        search_query = " + ".join(search_components)
        
        logger.info(f"Searching for: {search_query}")
        
        # Perform the search
        search_results = await self.google_search.google_search(search_query)
        
        if not search_results:
            logger.warning(f"No search results found for {mask_email(email)}")
            return None
        
        # Prepare member info for OpenAI
        member_info = {
            "email": email,
            "first_name": extracted_first_name,
            "last_name": extracted_last_name,
            "state": location_state,
            "country": location_country,
            "company_domain": domain
        }
        
        # Use OpenAI to analyze the search results
        linkedin_url = await self.google_search.query_openai(member_info, search_results)
        
        if linkedin_url:
            logger.info(f"Found LinkedIn profile for {mask_email(email)}: {linkedin_url}")
        else:
            logger.info(f"No LinkedIn profile found for {mask_email(email)}")
        
        return linkedin_url