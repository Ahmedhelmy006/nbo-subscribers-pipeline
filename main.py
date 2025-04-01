"""
Main module for the NBO Pipeline.

This module provides the main entry point for the pipeline and
orchestrates the different pipeline components.
"""
import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

from config import settings, logger
from db import initialize_database, cleanup_database
from db.state import get_pipeline_state_manager
from db.models import SubscriberModel, SubscriberMetadataModel
from utils.country_utils import get_purchase_power_checker
from utils.helpers import mask_email
from cache import cache_manager

# We'll import specific pipeline implementations later
# to avoid circular imports
from pipelines.base import FetchPipeline

async def run_pipeline(max_records: int = None) -> bool:
    """
    Run the pipeline.
    
    Args:
        max_records: Maximum number of records to process
        
    Returns:
        bool: True if the pipeline ran successfully, False otherwise
    """
    # Import pipelines here to avoid circular imports
    from pipelines.subscriber_pipeline import SubscriberPipeline
    from pipelines.location_pipeline import LocationPipeline
    from pipelines.referrer_pipeline import ReferrerPipeline
    from pipelines.linkedin_pipeline import LinkedInPipeline
    from pipelines.worker_pools import LocationWorkerPool, ReferrerWorkerPool, LinkedInWorkerPool
    
    # Get pipeline state manager
    state_manager = get_pipeline_state_manager("main")
    
    # Check if the pipeline is already running
    if await state_manager.is_pipeline_running():
        logger.warning("Pipeline is already running, skipping")
        return False
    
    # Get the last processed ID
    last_processed_id = await state_manager.get_last_processed_id()
    
    # Start a new run
    run_metadata = {
        "max_records": max_records or settings.MAX_RECORDS_PER_BATCH,
        "start_time": datetime.now().isoformat(),
        "last_processed_id": last_processed_id
    }
    
    run_id = await state_manager.start_run(metadata=run_metadata)
    logger.info(f"Started pipeline run {run_id}")
    
    try:
        # Initialize pipelines
        subscriber_pipeline = SubscriberPipeline(
            api_key=settings.CONVERTKIT_API_KEY,
            base_url=settings.API_BASE_URL
        )
        
        location_pipeline = LocationPipeline()
        referrer_pipeline = ReferrerPipeline()
        linkedin_pipeline = LinkedInPipeline()
        
        # Set up queues for pipeline processing
        location_queue = asyncio.Queue()
        referrer_queue = asyncio.Queue()
        linkedin_queue = asyncio.Queue()
        write_queue = asyncio.Queue()
        
        # Initialize purchase power checker
        purchase_power_checker = get_purchase_power_checker()
        
        # Create worker pools
        location_pool = LocationWorkerPool(
            location_pipeline, 
            location_queue, 
            write_queue,
            min_workers=5
        )
        
        referrer_pool = ReferrerWorkerPool(
            referrer_pipeline,
            referrer_queue,
            write_queue,
            min_workers=5
        )
        
        linkedin_pool = LinkedInWorkerPool(
            linkedin_pipeline,
            linkedin_queue,
            write_queue,
            min_workers=2
        )
        
        # Start worker pools
        await location_pool.start()
        await referrer_pool.start()
        await linkedin_pool.start()
        
        # Create writer task
        writer_task = asyncio.create_task(
            process_write_queue(write_queue)
        )
        
        # Fetch subscribers and distribute to processing queues
        max_limit = max_records or settings.MAX_RECORDS_PER_BATCH
        
        # Get existing subscribers from the cache
        existing_subscribers = set()
        
        processed_count = await process_subscribers(
            subscriber_pipeline,
            location_queue,
            referrer_queue,
            linkedin_queue,
            last_processed_id,
            max_limit,
            existing_subscribers,
            purchase_power_checker
        )
        
        # Set a signal that we're done processing subscribers
        # Add None sentinel values to queues to signal completion
        await location_queue.put(None)
        await referrer_queue.put(None)
        await linkedin_queue.put(None)
        
        # Wait for all worker pools to finish
        await location_pool.stop()
        await referrer_pool.stop()
        await linkedin_pool.stop()
        
        # Add a sentinel value to write queue
        await write_queue.put(None)
        
        # Wait for writer to finish
        await writer_task
        
        # Complete the run
        await state_manager.complete_run(
            records_processed=processed_count,
            metadata_updates={
                "end_time": datetime.now().isoformat(),
            }
        )
        
        logger.info(f"Pipeline run {run_id} completed successfully")
        return True
        
    except Exception as e:
        # If anything goes wrong, mark the run as failed
        logger.error(f"Pipeline run {run_id} failed: {e}", exc_info=True)
        
        await state_manager.fail_run(
            error_message=str(e),
            metadata_updates={
                "end_time": datetime.now().isoformat(),
                "error_traceback": str(e)
            }
        )
        
        return False

async def process_subscribers(
    subscriber_pipeline,
    location_queue,
    referrer_queue,
    linkedin_queue,
    last_processed_id,
    max_limit,
    existing_subscribers,
    purchase_power_checker
) -> int:
    """
    Process subscribers from the API.
    
    Args:
        subscriber_pipeline: Pipeline for fetching subscribers
        location_queue: Queue for location processing
        referrer_queue: Queue for referrer processing
        linkedin_queue: Queue for LinkedIn processing
        last_processed_id: Last processed subscriber ID
        max_limit: Maximum number of subscribers to process
        existing_subscribers: Set of already processed IDs
        purchase_power_checker: Utility for checking country purchase power
        
    Returns:
        int: Number of subscribers processed
    """
    from email_classifier.classifier import is_work_email
    
    processed_count = 0
    next_cursor = None
    
    # Tracking counters for LinkedIn filtering
    work_email_count = 0
    inactive_count = 0
    low_purchase_power_count = 0
    added_to_linkedin_count = 0
    
    while processed_count < max_limit:
        try:
            # Fetch a batch of subscribers
            result = await subscriber_pipeline.fetch_subscribers(
                after_cursor=next_cursor,
                limit=min(settings.RECORDS_PER_PAGE, max_limit - processed_count)
            )
            
            if not result or not result.get('subscribers'):
                # No more subscribers to process
                break
            
            subscribers = result['subscribers']
            next_cursor = result.get('pagination', {}).get('end_cursor')
            
            # Filter out subscribers we've already processed
            new_subscribers = []
            for subscriber in subscribers:
                subscriber_id = subscriber.get('id')
                
                # Skip if no ID
                if not subscriber_id:
                    continue
                    
                # Check if already processed
                if subscriber_id in existing_subscribers:
                    continue
                
                # Check if in cache
                if cache_manager.exists(subscriber_id):
                    continue
                
                # Add to new subscribers
                new_subscribers.append(subscriber)
                existing_subscribers.add(subscriber_id)
                
                # Add to cache
                cache_manager.set(subscriber_id, True)
            
            # Add new subscribers to processing queues
            for subscriber in new_subscribers:
                # Basic requirement checks
                if not subscriber.get('email_address'):
                    continue
                
                # Add to location queue
                await location_queue.put(subscriber)
                
                # Add to referrer queue
                await referrer_queue.put(subscriber)
                
                # Determine if this is a work email
                email = subscriber.get('email_address', '')
                
                # Check if it's a work email
                is_work = is_work_email(email)
                
                # Update email_domain_type field
                subscriber['email_domain_type'] = 'work' if is_work else 'personal'
                
                if is_work:
                    work_email_count += 1
                    
                    # Enhanced LinkedIn filtering
                    # Only process active work emails from non-low-purchase-power countries
                    subscriber_state = subscriber.get("state", "").lower()
                    country = subscriber.get("location_country", "")
                    
                    if subscriber_state != "active":
                        inactive_count += 1
                        logger.debug(f"Skipping LinkedIn for inactive subscriber: {mask_email(email)}")
                        continue
                        
                    if purchase_power_checker.has_low_purchase_power(country):
                        low_purchase_power_count += 1
                        logger.debug(f"Skipping LinkedIn for low purchase power country ({country}): {mask_email(email)}")
                        continue
                    
                    # If we get here, add to LinkedIn queue
                    await linkedin_queue.put(subscriber)
                    added_to_linkedin_count += 1
            
            # Update processed count
            processed_count += len(new_subscribers)
            
            logger.info(f"Fetched and queued {len(new_subscribers)} new subscribers (total: {processed_count})")
            logger.info(f"LinkedIn filtering: {work_email_count} work emails, {inactive_count} inactive, "
                        f"{low_purchase_power_count} low purchase power, {added_to_linkedin_count} added to queue")
            
            # Check if there are more subscribers to fetch
            if not next_cursor:
                logger.info("No more subscribers to fetch")
                break
                
        except Exception as e:
            logger.error(f"Error processing subscribers: {e}", exc_info=True)
            # Continue with next batch
            continue
    
    return processed_count

async def process_write_queue(write_queue: asyncio.Queue) -> int:
    """
    Process the write queue, saving processed subscribers to the database.
    
    Args:
        write_queue: Queue of processed subscribers to write
        
    Returns:
        int: Number of subscribers written
    """
    written_count = 0
    batch_size = 100
    current_batch = []
    
    while True:
        try:
            # Get an item from the queue
            subscriber = await write_queue.get()
            
            # Check for sentinel value
            if subscriber is None:
                write_queue.task_done()
                break
            
            # Add to current batch
            current_batch.append(subscriber)
            
            # If batch is full, write it
            if len(current_batch) >= batch_size:
                # Write batch
                await write_batch(current_batch)
                written_count += len(current_batch)
                current_batch = []
            
            # Mark task as done
            write_queue.task_done()
            
        except Exception as e:
            logger.error(f"Error processing write queue: {e}", exc_info=True)
            # Continue with next item
            continue
    
    # Write any remaining items
    if current_batch:
        await write_batch(current_batch)
        written_count += len(current_batch)
    
    logger.info(f"Finished writing {written_count} subscribers")
    return written_count

async def write_batch(subscribers: List[Dict[str, Any]]) -> bool:
    """
    Write a batch of subscribers to the database.
    
    Args:
        subscribers: List of subscriber dictionaries
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Bulk update or create subscribers
        await SubscriberModel.bulk_update_or_create(subscribers)
        
        # Update subscriber metadata
        for subscriber in subscribers:
            subscriber_id = subscriber.get('id')
            if not subscriber_id:
                continue
            
            # Determine which components have been processed
            has_location = (
                subscriber.get('location_city') is not None or
                subscriber.get('location_state') is not None or
                subscriber.get('location_country') is not None
            )
            
            has_referrer = subscriber.get('referrer_info') is not None
            
            # Update metadata
            await SubscriberMetadataModel.update_metadata(
                subscriber_id=subscriber_id,
                has_location=has_location,
                has_referrer=has_referrer,
                processed_complete=True
            )
        
        logger.info(f"Wrote {len(subscribers)} subscribers to database")
        return True
    
    except Exception as e:
        logger.error(f"Error writing subscribers to database: {e}", exc_info=True)
        return False

async def main():
    """
    Main entry point for running the pipeline directly.
    """
    parser = argparse.ArgumentParser(description="NBO Pipeline")
    parser.add_argument(
        "--max-records", 
        type=int, 
        help="Maximum number of records to process"
    )
    args = parser.parse_args()
    
    # Initialize database
    if not await initialize_database():
        logger.error("Failed to initialize database, exiting")
        return 1
    
    try:
        # Run the pipeline
        success = await run_pipeline(max_records=args.max_records)
        return 0 if success else 1
    
    finally:
        # Clean up
        await cleanup_database()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)