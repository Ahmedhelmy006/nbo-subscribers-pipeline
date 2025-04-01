"""
Location pipeline for the NBO Pipeline.

This module provides functionality for fetching location data and 
purchase power information for subscribers.
"""
import asyncio
import logging
import aiohttp
import time
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional

from config import settings
from config.headers import get_convertkit_cookie_headers
from pipelines.base import FetchPipeline
from utils.location_fetcher import LocationIdentifier
from utils.country_utils import get_purchase_power_checker

logger = logging.getLogger(__name__)

class LocationPipeline(FetchPipeline):
    """
    Pipeline for fetching location data and purchase power information.
    """
    
    def __init__(self, max_concurrent=20):
        """
        Initialize the location pipeline.
        
        Args:
            max_concurrent (int): Maximum concurrent requests
        """
        super().__init__(max_concurrent)
        self.headers = get_convertkit_cookie_headers()
        self.total_processed = 0
        self.start_time = time.time()
    
    def clean_response(self, html):
        """
        Extract city and state from HTML response.
        
        Args:
            html (str): HTML content
            
        Returns:
            tuple: (city, state)
        """
        soup = BeautifulSoup(html, 'html.parser')
        locations = soup.find(attrs={"data-city": True, "data-state": True})
        if locations:
            city = locations['data-city']
            state = locations['data-state']
            return city, state
        return None, None
    
    async def process_item(self, subscriber):
        """
        Process a single subscriber to get location data and purchase power.
        
        Args:
            subscriber (dict): Subscriber data
            
        Returns:
            dict: Updated subscriber data with location info
        """
        subscriber_id = subscriber.get("id")
        if not subscriber_id:
            logger.warning("Subscriber ID is missing, skipping location processing")
            return subscriber
            
        url = f"https://app.kit.com/subscribers/{subscriber_id}"
        
        try:
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=self.headers, timeout=30) as response:
                        if response.status == 200:
                            html = await response.text()
                            city, state = self.clean_response(html)
                            country = "N/A"
                            purchase_power = "Unknown"
                            purchase_score = "0"
                            subscriber_region = "Unknown"
                            timezone = "Unknown"
                            
                            if city and state:
                                try:
                                    identifier = LocationIdentifier(city=city, state=state)
                                    # Enhanced method that returns country plus purchasing data
                                    country, purchase_power, purchase_score = identifier.search_with_purchase_data()
                                    
                                    # Get purchase power checker to retrieve additional info
                                    purchase_power_checker = get_purchase_power_checker()
                                    subscriber_region = purchase_power_checker.get_subscriber_region(country)
                                    timezone = purchase_power_checker.get_timezone(country)
                                    
                                    self.console.print(f"[green]Location found for {subscriber_id}: {city}, {state}, {country}")
                                    self.console.print(f"[blue]Purchase data: Power={purchase_power}, Score={purchase_score}")
                                    self.console.print(f"[blue]Region: {subscriber_region}, Timezone: {timezone}")
                                except Exception as e:
                                    logger.error(f"Error identifying country for {subscriber_id}: {e}")
                            
                            # Add location data including purchase metrics and new fields
                            subscriber["location_city"] = city
                            subscriber["location_state"] = state 
                            subscriber["location_country"] = country
                            subscriber["purchase_power"] = purchase_power
                            subscriber["purchase_score"] = purchase_score
                            subscriber["subscriber_region"] = subscriber_region
                            subscriber["timezone"] = timezone
                        else:
                            # Handle rate limiting
                            if response.status == 429:
                                self.console.print(f"[yellow]Rate limited for {subscriber_id}, waiting 5 seconds...")
                                await asyncio.sleep(5)
                                return await self.process_item(subscriber)
                            
                            logger.warning(f"Failed to fetch location for {subscriber_id}: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching location for {subscriber_id}: {e}")
        
        # Update tracking metrics
        self.total_processed += 1
        
        # Log progress occasionally
        if self.total_processed % 100 == 0:
            elapsed = time.time() - self.start_time
            rate = self.total_processed / elapsed if elapsed > 0 else 0
            self.console.print(f"[cyan]Processed {self.total_processed} locations at {rate:.2f} records/second")
        
        return subscriber
    
    async def process_batch(self, subscribers):
        """
        Process a batch of subscribers to fetch location data.
        
        Args:
            subscribers (list): List of subscriber dictionaries
            
        Returns:
            list: Updated subscribers with location data
        """
        self.console.print(f"[bold yellow]Fetching locations for {len(subscribers)} subscribers...")
        
        tasks = []
        for subscriber in subscribers:
            tasks.append(self.process_item(subscriber))
        
        return await asyncio.gather(*tasks)