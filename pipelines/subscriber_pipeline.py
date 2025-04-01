"""
Subscriber pipeline for the NBO Pipeline.

This module provides functionality for fetching subscribers from the ConvertKit API.
"""
import asyncio
import time
import logging
import aiohttp
from typing import Dict, List, Any, Optional

from config import settings
from config.headers import get_convertkit_api_headers
from pipelines.base import FetchPipeline
from cache import cache_manager

logger = logging.getLogger(__name__)

class SubscriberPipeline(FetchPipeline):
    """
    Pipeline for fetching subscriber data from the ConvertKit API.
    """
    
    def __init__(self, api_key, base_url="https://api.kit.com/v4", max_concurrent=20):
        """
        Initialize the subscriber pipeline.
        
        Args:
            api_key (str): API key for access
            base_url (str): API base URL
            max_concurrent (int): Maximum concurrent requests
        """
        super().__init__(max_concurrent)
        self.base_url = base_url
        self.api_headers = get_convertkit_api_headers(api_key)
        
        # Tracking variables
        self.processed_ids = set()
        self.total_fetched = 0
        self.duplicate_count = 0
        self.start_time = time.time()
        self.last_print_time = self.start_time
    
    def print_progress(self, current_batch_size, max_records):
        """
        Print progress information including time estimates.
        
        Args:
            current_batch_size (int): Size of the current batch
            max_records (int): Maximum records to fetch
        """
        elapsed_time = time.time() - self.start_time
        total_fetched = self.total_fetched + current_batch_size
        
        # Calculate estimates
        if total_fetched > 0:
            time_per_record = elapsed_time / total_fetched
            estimated_total_time = time_per_record * max_records
            time_left = estimated_total_time - elapsed_time
        else:
            time_left = 0
        
        self.console.print(f"[cyan]Total fetched so far: {total_fetched}")
        self.console.print(f"[cyan]Duplicates skipped: {self.duplicate_count}")
        self.console.print(f"[cyan]Records left to go: {max_records - total_fetched}")
        self.console.print(f"[cyan]Elapsed time: {elapsed_time / 60:.2f} minutes")
        self.console.print(f"[cyan]Estimated time left: {time_left / 60:.2f} minutes")
        self.console.print("------")
    
    async def fetch_subscribers(self, after_cursor=None, limit=None):
        """
        Fetch a batch of subscribers from the API.
        
        Args:
            after_cursor (str, optional): Cursor for pagination
            limit (int, optional): Maximum number of subscribers to fetch
            
        Returns:
            dict: Dictionary with subscribers and pagination info
        """
        per_page = min(limit or settings.RECORDS_PER_PAGE, settings.RECORDS_PER_PAGE)
        
        # Construct the URL
        url = f'{self.base_url}/subscribers?per_page={per_page}'
        if settings.SUBSCRIBER_STATUS != "all":
            url += f'&state={settings.SUBSCRIBER_STATUS}'
        if after_cursor:
            url += f'&after={after_cursor}'
        
        self.console.print(f"[yellow]Fetching subscribers from: {url}")
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.api_headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        subscribers = data.get('subscribers', [])
                        
                        # Check for duplicate IDs that we've already processed
                        new_subscribers = []
                        for sub in subscribers:
                            sub_id = sub.get('id')
                            if sub_id and sub_id not in self.processed_ids:
                                new_subscribers.append(sub)
                                self.processed_ids.add(sub_id)
                            else:
                                self.duplicate_count += 1
                        
                        # If we found duplicates, log them
                        if len(subscribers) != len(new_subscribers):
                            self.console.print(f"[yellow]Found {len(subscribers) - len(new_subscribers)} duplicate subscribers")
                        
                        # Print progress periodically
                        current_time = time.time()
                        if current_time - self.last_print_time >= 30:
                            self.print_progress(len(new_subscribers), limit or 250000)
                            self.last_print_time = current_time
                        
                        # Update total fetched
                        self.total_fetched += len(new_subscribers)
                        
                        # Return the subscribers and pagination info
                        return {
                            'subscribers': new_subscribers,
                            'pagination': data.get('pagination', {})
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to fetch subscribers. Status code: {response.status}")
                        logger.error(f"Error response: {error_text}")
                        
                        # Retry logic on server errors (5xx)
                        if 500 <= response.status < 600:
                            self.console.print(f"[yellow]Server error, waiting 5 seconds before retrying...")
                            await asyncio.sleep(5)
                            return await self.fetch_subscribers(after_cursor, limit)
                        else:
                            return None
            except Exception as e:
                logger.error(f"Error fetching subscribers: {e}")
                self.console.print(f"[red]Error: {e}")
                self.console.print("[yellow]Waiting 5 seconds before retrying...")
                await asyncio.sleep(5)
                return await self.fetch_subscribers(after_cursor, limit)
    
    async def process_batch(self, items):
        """
        Process a batch of subscribers (not used in this pipeline).
        
        Args:
            items (list): List of subscribers
            
        Returns:
            list: Processed subscribers
        """
        # This is just a placeholder to satisfy the interface
        # The actual processing happens in fetch_subscribers
        return items
    
    async def process_item(self, item):
        """
        Process a single subscriber (not used in this pipeline).
        
        Args:
            item: Subscriber data
            
        Returns:
            Processed subscriber
        """
        # This is just a placeholder to satisfy the interface
        return item