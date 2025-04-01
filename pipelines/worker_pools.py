"""
Worker pools for the NBO Pipeline.

This module provides worker pools for processing data in the pipeline.
"""
import asyncio
import logging
import time
from typing import Dict, List, Any, Optional

from utils.worker_pool import WorkerPool

logger = logging.getLogger(__name__)

class LocationWorkerPool(WorkerPool):
    """
    Worker pool for processing location data.
    """
    
    def __init__(self, location_pipeline, location_queue, write_queue, min_workers=5, max_workers=None):
        """
        Initialize the location worker pool.
        
        Args:
            location_pipeline: Pipeline for processing location data
            location_queue: Queue of subscribers to process
            write_queue: Queue for processed subscribers
            min_workers: Minimum number of workers
            max_workers: Maximum number of workers
        """
        self.location_pipeline = location_pipeline
        self.location_queue = location_queue
        self.write_queue = write_queue
        self.processed_count = 0
        self.last_count = 0
        
        # Create worker function
        async def worker_func():
            while not self.should_stop.is_set():
                try:
                    # Get a subscriber from the queue with timeout
                    try:
                        subscriber = await asyncio.wait_for(self.location_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # No items in queue, check if we should stop
                        if self.location_queue.empty() and self.should_stop.is_set():
                            break
                        continue
                    
                    # Check for sentinel value
                    if subscriber is None:
                        self.location_queue.task_done()
                        # Put the sentinel back for other workers
                        await self.location_queue.put(None)
                        break
                    
                    # Process the subscriber
                    try:
                        processed_subscriber = await self.location_pipeline.process_item(subscriber)
                        
                        # Create a shallow copy to avoid modifying the original while it's being processed
                        # by other pipelines
                        subscriber_copy = processed_subscriber.copy()
                        
                        # Add to write queue
                        await self.write_queue.put(subscriber_copy)
                        self.processed_count += 1
                    except Exception as e:
                        logger.error(f"Error processing location for subscriber {subscriber.get('id')}: {e}")
                    
                    # Mark task as done
                    self.location_queue.task_done()
                
                except Exception as e:
                    logger.error(f"Error in location worker: {e}")
                    # Small delay to prevent tight loop on errors
                    await asyncio.sleep(0.1)
        
        # Initialize the worker pool
        super().__init__(worker_func, "Location", min_workers, max_workers)
    
    def calculate_throughput(self):
        """
        Calculate throughput for monitoring.
        
        Returns:
            float: Records processed per second
        """
        current_count = self.processed_count
        throughput = current_count - self.last_count
        self.last_count = current_count
        return throughput

class ReferrerWorkerPool(WorkerPool):
    """
    Worker pool for processing referrer data.
    """
    
    def __init__(self, referrer_pipeline, referrer_queue, write_queue, min_workers=5, max_workers=None):
        """
        Initialize the referrer worker pool.
        
        Args:
            referrer_pipeline: Pipeline for processing referrer data
            referrer_queue: Queue of subscribers to process
            write_queue: Queue for processed subscribers
            min_workers: Minimum number of workers
            max_workers: Maximum number of workers
        """
        self.referrer_pipeline = referrer_pipeline
        self.referrer_queue = referrer_queue
        self.write_queue = write_queue
        self.processed_count = 0
        self.last_count = 0
        
        # Create worker function
        async def worker_func():
            while not self.should_stop.is_set():
                try:
                    # Get a subscriber from the queue with timeout
                    try:
                        subscriber = await asyncio.wait_for(self.referrer_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # No items in queue, check if we should stop
                        if self.referrer_queue.empty() and self.should_stop.is_set():
                            break
                        continue
                    
                    # Check for sentinel value
                    if subscriber is None:
                        self.referrer_queue.task_done()
                        # Put the sentinel back for other workers
                        await self.referrer_queue.put(None)
                        break
                    
                    # Process the subscriber
                    try:
                        processed_subscriber = await self.referrer_pipeline.process_item(subscriber)
                        
                        # Create a shallow copy to avoid modifying the original while it's being processed
                        # by other pipelines
                        subscriber_copy = processed_subscriber.copy()
                        
                        # Add to write queue
                        await self.write_queue.put(subscriber_copy)
                        self.processed_count += 1
                    except Exception as e:
                        logger.error(f"Error processing referrer for subscriber {subscriber.get('id')}: {e}")
                    
                    # Mark task as done
                    self.referrer_queue.task_done()
                
                except Exception as e:
                    logger.error(f"Error in referrer worker: {e}")
                    # Small delay to prevent tight loop on errors
                    await asyncio.sleep(0.1)
        
        # Initialize the worker pool
        super().__init__(worker_func, "Referrer", min_workers, max_workers)
    
    def calculate_throughput(self):
        """
        Calculate throughput for monitoring.
        
        Returns:
            float: Records processed per second
        """
        current_count = self.processed_count
        throughput = current_count - self.last_count
        self.last_count = current_count
        return throughput

class LinkedInWorkerPool(WorkerPool):
    """
    Worker pool for processing LinkedIn data.
    """
    
    def __init__(self, linkedin_pipeline, linkedin_queue, write_queue, min_workers=2, max_workers=None):
        """
        Initialize the LinkedIn worker pool.
        
        Args:
            linkedin_pipeline: Pipeline for processing LinkedIn data
            linkedin_queue: Queue of subscribers to process
            write_queue: Queue for processed subscribers
            min_workers: Minimum number of workers
            max_workers: Maximum number of workers
        """
        self.linkedin_pipeline = linkedin_pipeline
        self.linkedin_queue = linkedin_queue
        self.write_queue = write_queue
        self.processed_count = 0
        self.last_count = 0
        
        # Create worker function
        async def worker_func():
            while not self.should_stop.is_set():
                try:
                    # Get a subscriber from the queue with timeout
                    try:
                        subscriber = await asyncio.wait_for(self.linkedin_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # No items in queue, check if we should stop
                        if self.linkedin_queue.empty() and self.should_stop.is_set():
                            break
                        continue
                    
                    # Check for sentinel value
                    if subscriber is None:
                        self.linkedin_queue.task_done()
                        # Put the sentinel back for other workers
                        await self.linkedin_queue.put(None)
                        break
                    
                    # Process the subscriber
                    try:
                        processed_subscriber = await self.linkedin_pipeline.process_item(subscriber)
                        
                        # Create a shallow copy to avoid modifying the original while it's being processed
                        # by other pipelines
                        subscriber_copy = processed_subscriber.copy()
                        
                        # Add to write queue
                        await self.write_queue.put(subscriber_copy)
                        self.processed_count += 1
                    except Exception as e:
                        logger.error(f"Error processing LinkedIn for subscriber {subscriber.get('id')}: {e}")
                    
                    # Mark task as done
                    self.linkedin_queue.task_done()
                
                except Exception as e:
                    logger.error(f"Error in LinkedIn worker: {e}")
                    # Small delay to prevent tight loop on errors
                    await asyncio.sleep(0.1)
        
        # Initialize the worker pool
        super().__init__(worker_func, "LinkedIn", min_workers, max_workers)
    
    def calculate_throughput(self):
        """
        Calculate throughput for monitoring.
        
        Returns:
            float: Records processed per second
        """
        current_count = self.processed_count
        throughput = current_count - self.last_count
        self.last_count = current_count
        return throughput