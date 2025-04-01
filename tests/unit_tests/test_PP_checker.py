"""
Country utilities for the NBO Pipeline.

This module provides utilities for working with country data,
particularly for checking purchase power.
"""
import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any, Union
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import settings
logger = logging.getLogger(__name__)

class CountryPurchasePowerChecker:
    """Checks the purchase power of countries based on metadata."""
    
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
                        # Store the purchase power info
                        self.country_data[name.lower()] = {
                            'purchasing_power': country.get('purchasing_power', 'Unknown'),
                            'purchase_score': country.get('purchase_score', 0)
                        }
                        
                        # Also store with common variants
                        if name == 'United States':
                            self.country_data['usa'] = self.country_data[name.lower()]
                            self.country_data['us'] = self.country_data[name.lower()]
                        elif name == 'United Kingdom':
                            self.country_data['uk'] = self.country_data[name.lower()]
                            self.country_data['britain'] = self.country_data[name.lower()]
                
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
            
        return country_data.get('purchasing_power', 'Unknown')
    
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
            return float(country_data.get('purchase_score', 0))
        except (ValueError, TypeError):
            return 0.0
    
    def get_country_data(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all country data.
        
        Returns:
            Dictionary of country data
        """
        return self.country_data


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


if __name__ == "__main__":
    """
    Debug entry point when running this module directly in VS Code.
    """
    import sys
    import tempfile
    from pprint import pprint
    
    # Configure basic logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # If a metadata file is provided as an argument, use it
    if len(sys.argv) > 1:
        metadata_file = sys.argv[1]
        print(f"Using metadata file: {metadata_file}")
        checker = CountryPurchasePowerChecker(metadata_file=metadata_file)
    else:
        # Otherwise, use a sample dataset for testing
        print("No metadata file provided. Using sample data for testing.")
        sample_countries = [
            {"name": "United States", "purchasing_power": "High", "purchase_score": "0.876"},
            {"name": "Germany", "purchasing_power": "High", "purchase_score": "0.821"},
            {"name": "Brazil", "purchasing_power": "Medium", "purchase_score": "0.456"},
            {"name": "India", "purchasing_power": "Low", "purchase_score": "0.231"},
            {"name": "Nigeria", "purchasing_power": "Low", "purchase_score": "0.112"}
        ]
        
        # Create a temporary file with the sample data
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
            json.dump(sample_countries, f)
            temp_filepath = f.name
        
        try:
            # Initialize checker with the temporary file
            checker = CountryPurchasePowerChecker(metadata_file=temp_filepath)
            
            # Test the checker functionality
            print("\n=== Country Purchase Power Checker Test ===")
            
            # Test basic functionality
            test_countries = ["United States", "Germany", "Brazil", "India", "Nigeria", "Unknown"]
            
            print("\nPurchase Power Categories:")
            for country in test_countries:
                power = checker.get_purchase_power(country)
                score = checker.get_purchase_score(country)
                is_low = checker.has_low_purchase_power(country)
                print(f"{country}: {power} (Score: {score:.3f}, Low: {is_low})")
            
            # Print country variants
            print("\nCountry Variants:")
            variants = ["USA", "US", "america", "uk", "Britain"]
            for variant in variants:
                power = checker.get_purchase_power(variant)
                print(f"{variant}: {power}")
            
            # Print some raw data for debugging
            print("\nRaw Country Data Sample:")
            sample_keys = list(checker.country_data.keys())[:5]
            for key in sample_keys:
                print(f"{key}: {checker.country_data[key]}")
                
        finally:
            # Clean up the temporary file
            os.unlink(temp_filepath)
    
    print("\nDebug run completed successfully.")