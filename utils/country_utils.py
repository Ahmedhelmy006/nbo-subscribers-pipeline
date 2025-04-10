"""
Country utilities for the NBO Pipeline.

This module provides utilities for working with country data,
particularly for checking purchase power and regional information.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Optional

from config import settings

logger = logging.getLogger(__name__)

class CountryPurchasePowerChecker:
    """Checks the purchase power and regional information of countries based on metadata."""
    
    def __init__(self, metadata_file=None):
        """
        Initialize the country purchase power checker.
        
        Args:
            metadata_file: Path to the countries metadata JSON file
        """
        self.metadata_file = metadata_file or settings.COUNTRIES_METADATA_PATH
        self.country_data = {}
        self._load_country_data()
    
    def _load_country_data(self):
        """Load country metadata from JSON file."""
        try:
            metadata_path = Path(self.metadata_file)
            if metadata_path.exists():
                logger.info(f"Loading country metadata from {self.metadata_file}")
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    countries = json.load(f)
                
                # Create a lookup dictionary for faster access
                for country in countries:
                    name = country.get('name', '')
                    if name:
                        # Store the purchase power info and new fields
                        country_entry = {
                            'purchasing_power': country.get('purchasing_power', 'Unknown'),
                            'purchase_score': country.get('purchase_score', 0),
                            # Format region name - replace underscores with spaces and use title case
                            'subscriber_region': self._format_region_name(country.get('region', 'Unknown')),
                            'timezone': country.get('timezones', [{}])[0].get('gmtOffsetName', 'Unknown')
                        }
                        
                        # Store for the country name
                        self.country_data[name.lower()] = country_entry
                        
                        # Also store with common variants
                        if name == 'United States':
                            self.country_data['usa'] = country_entry
                            self.country_data['us'] = country_entry
                        elif name == 'United Kingdom':
                            self.country_data['uk'] = country_entry
                            self.country_data['britain'] = country_entry
                
                logger.info(f"Loaded purchase power data for {len(self.country_data)} countries")
            else:
                logger.warning(f"Countries metadata file not found: {self.metadata_file}")
        except Exception as e:
            logger.error(f"Error loading country metadata: {e}")
    
    def has_low_purchase_power(self, country: str) -> bool:
        """
        Check if a country has low purchase power.
        
        Args:
            country: Country name
            
        Returns:
            True if the country has low purchase power, False otherwise
        """
        if not country:
            return False
            
        country_data = self.country_data.get(country.lower())
        if not country_data:
            logger.warning(f"No purchase power data for country: {country}")
            return False
            
        return country_data.get('purchasing_power', '').lower() == 'low'
    
    def get_purchase_power(self, country: str) -> str:
        """
        Get the purchase power category for a country.
        
        Args:
            country: Country name
            
        Returns:
            Purchase power category ('High', 'Medium', 'Low', or 'Unknown')
        """
        if not country:
            return 'Unknown'
            
        country_data = self.country_data.get(country.lower())
        if not country_data:
            return 'Unknown'
            
        return country_data.get('purchasing_power', None)
    
    def get_purchase_score(self, country: str) -> float:
        """
        Get the purchase score for a country.
        
        Args:
            country: Country name
            
        Returns:
            Purchase score (float) or 0 if unknown
        """
        if not country:
            return 0.0
            
        country_data = self.country_data.get(country.lower())
        if not country_data:
            return 0.0
            
        try:
            return country_data.get('purchase_score', 'NULL')
        except (ValueError, TypeError):
            return 0.0
    
    def get_subscriber_region(self, country: str) -> str:
        """
        Get the region for a country.
        
        Args:
            country: Country name
            
        Returns:
            Region name or 'Unknown'
        """
        if not country:
            return None
            
        country_data = self.country_data.get(country.lower())
        if not country_data:
            return  None
            
        return country_data.get('subscriber_region')

    def get_timezone(self, country: str) -> str:
        """
        Get the timezone for a country.
        
        Args:
            country: Country name
            
        Returns:
            Timezone offset name or 'Unknown'
        """
        if not country:
            return 'Unknown'
            
        country_data = self.country_data.get(country.lower())
        if not country_data:
            return 'Unknown'
            
        return country_data.get('timezone', None)
    
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
        if formatted_name.lower() == "asia":
            formatted_name = "Asia"
        if formatted_name.lower() == "africa":
            formatted_name = "Africa"
        if formatted_name.lower() == "north_america":
            formatted_name = "North America"
        if formatted_name.lower() == "Unknown":
            formatted_name = None
            
        return formatted_name

# Create a singleton instance
_checker = None

def get_purchase_power_checker() -> CountryPurchasePowerChecker:
    """
    Get the singleton instance of the purchase power checker.
    
    Returns:
        CountryPurchasePowerChecker instance
    """
    global _checker
    if _checker is None:
        _checker = CountryPurchasePowerChecker()
    return _checker