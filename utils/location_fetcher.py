import json
import logging
from pathlib import Path
import sys
import os
# Import the settings to get the file path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import settings

logger = logging.getLogger(__name__)

class LocationIdentifier:
    # Use the path from settings, with a robust Path conversion
    JSON_PATH = Path(settings.COUNTRIES_JSON_PATH)

    def __init__(self, city=None, state=None):
        self.city = city.lower().strip() if city else None
        self.state = state.lower().strip() if state else None
        
        try:
            # Use the Path object to open the file
            with open(self.JSON_PATH, 'r', encoding='utf-8') as f:
                self.countries = json.load(f)
        except FileNotFoundError:
            # Improved error message with actual file path
            logger.error(f"JSON file not found. Checked path: {self.JSON_PATH}")
            logger.error(f"Absolute path: {self.JSON_PATH.resolve()}")
            
            # Optional: Log contents of the data directory
            data_dir = self.JSON_PATH.parent
            logger.error("Files in data directory:")
            for file in data_dir.glob('*'):
                logger.error(f"  - {file.name}")
            
            raise RuntimeError(f"JSON file not found at: {self.JSON_PATH}")
        except Exception as e:
            raise RuntimeError(f"Failed to load JSON data: {str(e)}")

    def search(self):
        if not self.city and not self.state:
            return "#N/A"

        candidate_countries = set()
        exact_match_country = None

        # Phase 1: Check for capital city match
        for country in self.countries:
            if self.city and self.city == country.get('capital', '').lower().strip():
                return country['name']  # Capital city match is definitive

        # Phase 2: Preprocess and match city and state together
        for country in self.countries:
            for state in country.get('states', []):
                state_name = state['name'].lower().strip()
                
                for city in state.get('cities', []):
                    city_name = city['name'].lower().strip()

                    # Exact match on both city and state
                    if self.city == city_name and self.state == state_name:
                        exact_match_country = country['name']
                        break  # No need to check further
                    # Match only on city
                    if self.city == city_name:
                        candidate_countries.add(country['name'])
            
            if exact_match_country:
                break  # Stop searching if an exact match is found

        if exact_match_country:
            return exact_match_country

        # Phase 3: Narrow down candidates using state
        if self.state and len(candidate_countries) > 1:
            narrowed_candidates = {
                country['name'] for country in self.countries
                for state in country.get('states', [])
                if self.state == state['name'].lower().strip() and country['name'] in candidate_countries
            }
            if len(narrowed_candidates) == 1:
                return narrowed_candidates.pop()

        # Phase 4: Fallback to state-based lookup
        if self.state:
            for country in self.countries:
                for state in country.get('states', []):
                    if self.state == state['name'].lower().strip():
                        return country['name']

        return "#N/A"

    def search_with_purchase_data(self):
        """
        Search for a country based on city and state and return purchase power data.
        
        Returns:
            Tuple of (country_name, purchase_power, purchase_score)
        """
        country = self.search()
        
        # If fallback search is needed
        if country == "#N/A":
            handler = Handler(self.JSON_PATH)
            country = handler.handle(self.city, self.state)
        
        # Default values if country not found or no purchase data available
        purchase_power = "Unknown"
        purchase_score = "0"
        
        # If country found, try to get purchase power data
        if country and country != "#N/A":
            try:
                # Import the purchase power checker here to avoid circular imports
                from utils.country_utils import get_purchase_power_checker
                
                # Get purchase power data
                purchase_power_checker = get_purchase_power_checker()
                purchase_power = purchase_power_checker.get_purchase_power(country)
                purchase_score = str(purchase_power_checker.get_purchase_score(country))
                
                logger.debug(f"Found purchase data for {country}: Power={purchase_power}, Score={purchase_score}")
            except ImportError as e:
                logger.warning(f"Could not import purchase power checker: {e}")
            except Exception as e:
                logger.warning(f"Error getting purchase data for country {country}: {e}")
        
        return country, purchase_power, purchase_score

    def search_with_handler(self):
        result = self.search()
        if result == "#N/A":
            handler = Handler(self.JSON_PATH)
            result = handler.handle(self.city, self.state)
        return result


class Handler:
    def __init__(self, json_path):
        self.json_path = json_path
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.countries = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(f"JSON file not found at: {self.json_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load JSON data: {str(e)}")

    def handle(self, city, state=None):
        city = city.lower().strip() if city else None
        state = state.lower().strip() if state else None

        # Search for the city globally, including capitals
        for country in self.countries:
            # Check the capital
            if city and city == country.get('capital', '').lower().strip():
                return country['name']
            
            # Check in states and cities
            for state_entry in country.get('states', []):
                for city_entry in state_entry.get('cities', []):
                    if city and city == city_entry['name'].lower().strip():
                        return country['name']
        return "#N/A"