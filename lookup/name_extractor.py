"""
Name extractor for the NBO Pipeline using OpenAI API.

This module provides functionality for extracting names from email addresses
for LinkedIn lookup by leveraging OpenAI's language models.
"""
import re
import logging
import os
import json
import requests
from typing import Tuple, Optional, List, Dict
from config import settings
from config.api_keys import OPENAI_API_KEY
from config.headers import get_openai_headers
from utils.helpers import mask_email

logger = logging.getLogger(__name__)

class NameExtractor:
    """
    Extracts names from email addresses using OpenAI API.
    """
    
    def __init__(self, openai_api_key=None):
        """Initialize the name extractor."""
        self.api_key = openai_api_key or OPENAI_API_KEY
        
        if not self.api_key:
            logger.warning("OpenAI API key not set. Name extraction may not work correctly.")
        
        # For cases when API might be unavailable or rate limited
        self.non_personal = {
            'admin', 'info', 'contact', 'hello', 'sales', 'support',
            'marketing', 'help', 'webmaster', 'noreply', 'no-reply',
            'team', 'office', 'billing', 'mail', 'postmaster', 'jobs',
            'career', 'hr', 'service', 'services'
        }
    
    def extract_name_from_email(self, email: str, given_name: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Extract a name from an email address using OpenAI API.
        
        Args:
            email: Email address
            given_name: Provided first name (if available)
            
        Returns:
            Tuple of (extracted name, method)
        """
        if not isinstance(email, str) or '@' not in email:
            return None, "Invalid email"
        
        # Get username part
        username = email.split('@')[0].lower()
        
        # Quick filter for non-personal emails
        if username in self.non_personal:
            return None, "Non-personal email"
        
        # For obviously invalid or random usernames, don't bother calling the API
        if len(username) < 2 or re.match(r'^[0-9]+$', username):
            return None, "Invalid username"
        
        # Use OpenAI to extract the name
        try:
            extracted_name = self._call_openai_api(email, given_name)
            
            if extracted_name:
                return extracted_name, "OpenAI extraction"
            else:
                return None, "No name detected by OpenAI"
        
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            
            # Fallback to basic extraction in case of API failure
            return self._basic_fallback_extraction(email, given_name)
    
    def _call_openai_api(self, email: str, given_name: Optional[str] = None) -> Optional[str]:
        """
        Call OpenAI API to extract a name from an email address.
        
        Args:
            email: Email address
            given_name: Provided first name (if available)
            
        Returns:
            Extracted name or None if not detected
        """
        # Use the API key from configuration/environment
        api_key = self.api_key or OPENAI_API_KEY
        
        if not api_key:
            logger.error("OpenAI API key not available. Set it as an environment variable: OPENAI_API_KEY")
            return None
        
        prompt = (
""" You will be given two inputs: an email and and a user entered name. 
                        These emails and names are user given. So they are messy. That's why You will be given these two inputs and you job is to return the first name and the last name from these inputs. 
                        There are a couple patterns that I expect you to break down. First pattern is: mlindholm@hlcsweden.com, Marko This is the input. I expect you to provide the name and the name only which is Marko Lindholm.
                        Why? Because the first letter in the email is referencing the user first name. Another example: ngeorges@pinnacleclimate.com, Nick ===> Nick Georges 
                        There are some straight forward cases like: steve.desalvo@kzf.com, Steve 
                        I expect the output to be Steve Desalvo 
                        And some Random cases like: igspam@wevalueprivacy.com, MindYourBusiness So I expect this to be None qzmlnhgzwwuzgdhgv@poplk.com, ko This one too is None
                        Note, your output will be used in a python code, so the output should be strict. If none then it's None, not none. 
                        same for returned names, first letter should be capital and there is a space between the first and last name."""
        )
        
        # If given_name is None, set it to empty string for the API call
        if given_name is None:
            given_name = ""
        
        try:
            # Use the get_openai_headers utility function which handles the API key correctly
            headers = get_openai_headers(api_key)
            
            data = {
                "model": settings.OPENAI_MODEL,  # Use model from settings
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": f"{email}, {given_name}"}
                ],
                "temperature": 0.2,  # Low temperature for consistent results
                "max_tokens": 50     # We just need the name, so limit tokens
            }
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=10  # Set a reasonable timeout
            )
            
            if response.status_code == 200:
                response_data = response.json()
                extracted_text = response_data["choices"][0]["message"]["content"].strip()
                
                # Check if result is None
                if extracted_text.lower() == "none":
                    return None
                
                # Return the extracted name with proper capitalization
                return self._format_name(extracted_text)
            else:
                logger.error(f"OpenAI API error: {response.status_code} - {response.text}")
                return None
        
        except Exception as e:
            logger.error(f"Error in OpenAI API call: {e}")
            return None
    
    def _format_name(self, name: str) -> str:
        """
        Format a name with proper capitalization.
        
        Args:
            name: Name to format
            
        Returns:
            Formatted name
        """
        # Split the name into parts
        parts = name.split()
        
        # Format each part with proper capitalization
        formatted_parts = []
        for part in parts:
            if not part:
                continue
            formatted_part = part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper()
            formatted_parts.append(formatted_part)
        
        # Join the parts back together
        return " ".join(formatted_parts)
    
    def _basic_fallback_extraction(self, email: str, given_name: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Fallback method for basic name extraction when API is unavailable.
        
        Args:
            email: Email address
            given_name: Provided first name (if available)
            
        Returns:
            Tuple of (extracted name, method)
        """
        # Get username part
        username = email.split('@')[0].lower()
        
        # If given_name is provided and not empty, use it
        if given_name:
            # If it's a full name, use it directly
            if ' ' in given_name:
                return given_name, "Used provided full name"
            else:
                # If just a first name is provided
                return given_name, "Used provided full name"
        
        # Check for structured email with separators
        if '.' in username or '_' in username or '-' in username:
            # Replace separators with spaces
            clean_username = re.sub(r'[._-]', ' ', username)
            # Strip out numbers
            clean_username = re.sub(r'[0-9]', '', clean_username).strip()
            
            # Format the parts
            parts = clean_username.split()
            if parts:
                formatted_parts = []
                for part in parts:
                    if len(part) == 1:
                        formatted_parts.append(part.upper())
                    else:
                        formatted_parts.append(part.capitalize())
                
                return " ".join(formatted_parts), "Structured email fallback"
        
        # Fallback for single-word usernames
        if len(username) >= 3:
            return username.capitalize(), "Single word username fallback"
        
        return None, "No name detected in fallback"