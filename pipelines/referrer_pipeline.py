"""
Referrer pipeline for the NBO Pipeline.

This module provides functionality for fetching referrer information
for subscribers.
"""
import asyncio
import logging
import aiohttp
import time
from typing import Dict, List, Any, Optional

from config import settings
from config.headers import get_referrer_info_headers
from pipelines.base import FetchPipeline

logger = logging.getLogger(__name__)

class ReferrerPipeline(FetchPipeline):
    """
    Pipeline for fetching referrer information.
    """
    
    def __init__(self, max_concurrent=20):
        """
        Initialize the referrer pipeline.
        
        Args:
            max_concurrent (int): Maximum concurrent requests
        """
        super().__init__(max_concurrent)
        self.headers = get_referrer_info_headers()
        self.total_processed = 0
        self.start_time = time.time()
    
    async def process_item(self, subscriber):
        """
        Process a single subscriber to get referrer info.
        
        Args:
            subscriber (dict): Subscriber data
            
        Returns:
            dict: Updated subscriber data with referrer info
        """
        subscriber_id = subscriber.get("id")
        if not subscriber_id:
            logger.warning("Subscriber ID is missing, skipping referrer processing")
            return subscriber
            
        url = f"https://app.kit.com/subscribers/{subscriber_id}/referrer_info"
        
        try:
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=self.headers, timeout=30) as response:
                        if response.status == 200:
                            referrer_info = await response.json()
                            print(f"RAW REFERRER INFO FOR {subscriber_id}: {referrer_info}")
                            
                            # Extract and store relevant referrer info
                            if referrer_info:
                                # Store the full referrer info
                                subscriber["referrer_info"] = referrer_info
                                
                                # Extract specific fields for direct query access
                                if 'referrer_domain' in referrer_info:
                                    subscriber["referrer_domain"] = referrer_info['referrer_domain']
                                
                                # Extract UTM parameters - FIX: Changed from utm_params to referrer_utm
                                utm_params = referrer_info.get('referrer_utm', {})
                                if utm_params:
                                    subscriber["referrer_utm_source"] = utm_params.get('source', '')
                                    subscriber["referrer_utm_medium"] = utm_params.get('medium', '')
                                    subscriber["referrer_utm_campaign"] = utm_params.get('campaign', '')
                                    subscriber["referrer_utm_content"] = utm_params.get('content', '')
                                    subscriber["referrer_utm_term"] = utm_params.get('term', '')
                                
                                # ADD THIS: Extract form name from origin
                                origin = referrer_info.get('origin', {})
                                if origin and 'name' in origin:
                                    subscriber["form_name"] = origin.get('name', '')
                            
                            self.console.print(f"[green]Referrer info found for {subscriber_id}")
                        else:
                            # Handle rate limiting
                            if response.status == 429:
                                self.console.print(f"[yellow]Rate limited for referrer {subscriber_id}, waiting 5 seconds...")
                                await asyncio.sleep(5)
                                return await self.process_item(subscriber)
                            
                            logger.warning(f"Failed to fetch referrer for {subscriber_id}: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching referrer for {subscriber_id}: {e}")
        
        # Update tracking metrics
        self.total_processed += 1
        
        # Log progress occasionally
        if self.total_processed % 100 == 0:
            elapsed = time.time() - self.start_time
            rate = self.total_processed / elapsed if elapsed > 0 else 0
            self.console.print(f"[cyan]Processed {self.total_processed} referrers at {rate:.2f} records/second")
        
        return subscriber
    
    async def process_batch(self, subscribers):
        """
        Process a batch of subscribers to fetch referrer data.
        
        Args:
            subscribers (list): List of subscriber dictionaries
            
        Returns:
            list: Updated subscribers with referrer data
        """
        self.console.print(f"[bold yellow]Fetching referrer info for {len(subscribers)} subscribers...")
        
        tasks = []
        for subscriber in subscribers:
            tasks.append(self.process_item(subscriber))
        
        return await asyncio.gather(*tasks)