#!/usr/bin/env python3
"""
Script to fetch subscribers from a specific date range, enrich their data,
save to the database, and update ConvertKit.
Includes parallel processing for efficiency.
"""
import sys
import os

# Add project root to path - THIS MUST COME BEFORE ANY IMPORTS
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import the modules
import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Any

from config import settings, logger
from db.connection import db_manager

async def initialize_database():
    """Initialize the database connection."""
    try:
        await db_manager.initialize()
        return True
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return False

async def cleanup_database():
    """Clean up database resources."""
    try:
        await db_manager.close()
    except Exception as e:
        logger.error(f"Database cleanup error: {e}")


from db.models import SubscriberModel
from pipelines.location_pipeline import LocationPipeline
from pipelines.referrer_pipeline import ReferrerPipeline
from pipelines.linkedin_pipeline import LinkedInPipeline
from email_classifier.classifier import is_work_email
from utils.country_utils import get_purchase_power_checker
from convertkit.updater import convertkit_updater
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn, SpinnerColumn

# Configure console
console = Console()

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def fetch_subscribers_by_date(from_date: str, to_date: str):
    """
    Fetch subscribers created within a specific date range.
    
    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        
    Returns:
        List of subscribers
    """
    import aiohttp
    from config.headers import get_convertkit_api_headers
    
    # Convert dates to the format ConvertKit expects
    from_date_formatted = f"{from_date}T00:00:00Z"
    to_date_formatted = f"{to_date}T23:59:59Z"
    
    api_headers = get_convertkit_api_headers()
    base_url = settings.API_BASE_URL
    
    subscribers = []
    cursor = None  # Track the pagination cursor
    batch_count = 0
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
    ) as progress:
        fetch_task = progress.add_task("[yellow]Fetching subscribers...", total=None)
        
        while True:
            batch_count += 1
            
            # Build URL with cursor for pagination
            if cursor:
                # Use cursor-based pagination for subsequent requests
                url = f"{base_url}/subscribers?from={from_date_formatted}&to={to_date_formatted}&after={cursor}&per_page={settings.RECORDS_PER_PAGE}"
                progress.update(fetch_task, description=f"[yellow]Fetching batch {batch_count} (with cursor)...")
            else:
                # First request without cursor
                url = f"{base_url}/subscribers?from={from_date_formatted}&to={to_date_formatted}&per_page={settings.RECORDS_PER_PAGE}"
                progress.update(fetch_task, description=f"[yellow]Fetching batch {batch_count} (initial)...")
            
            progress.print(f"[cyan]Request URL: {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=api_headers, timeout=60) as response:
                    if response.status == 200:
                        data = await response.json()
                        batch_subscribers = data.get('subscribers', [])
                        
                        # No more subscribers in this batch, we're done
                        if not batch_subscribers:
                            progress.update(fetch_task, description="[green]No more subscribers to fetch")
                            break
                        
                        # Add subscribers to our list
                        current_batch_count = len(batch_subscribers)
                        subscribers.extend(batch_subscribers)
                        progress.print(f"[green]Fetched {current_batch_count} subscribers (total: {len(subscribers)})")
                        
                        # Get pagination info for the next request
                        pagination = data.get('pagination', {})
                        progress.print(f"[blue]Pagination info: {json.dumps(pagination)}")
                        
                        # Check if we have more pages
                        if not pagination.get('has_next_page'):
                            progress.update(fetch_task, description="[green]No more pages to fetch")
                            break
                        
                        # Update cursor for the next request
                        new_cursor = pagination.get('end_cursor')
                        if not new_cursor:
                            progress.print("[yellow]No end_cursor found, stopping pagination")
                            break
                            
                        # Check if we're getting stuck on the same cursor
                        if new_cursor == cursor:
                            progress.print("[yellow]Warning: Same cursor received, stopping to avoid infinite loop")
                            break
                            
                        # Update cursor for next request
                        cursor = new_cursor
                    else:
                        error_text = await response.text()
                        progress.print(f"[red]Error fetching subscribers: {response.status} - {error_text}")
                        break
            
            # Brief delay to avoid rate limiting
            await asyncio.sleep(1)
        
        progress.update(fetch_task, description=f"[green]Completed fetch - {len(subscribers)} subscribers", completed=True)
    
    console.print(f"[bold green]Total subscribers fetched: {len(subscribers)}")
    return subscribers

async def process_subscriber_parallel(
    subscriber_queue: asyncio.Queue,
    location_queue: asyncio.Queue,
    referrer_queue: asyncio.Queue,
    linkedin_queue: asyncio.Queue,
    write_queue: asyncio.Queue,
    update_queue: asyncio.Queue
):
    """
    Main pipeline coordinator that distributes subscribers to appropriate queues.
    
    Args:
        subscriber_queue: Queue of subscribers to process
        location_queue: Queue for location processing
        referrer_queue: Queue for referrer processing
        linkedin_queue: Queue for LinkedIn processing
        write_queue: Queue for database writes
        update_queue: Queue for ConvertKit updates
    """
    purchase_power_checker = get_purchase_power_checker()
    
    while True:
        subscriber = await subscriber_queue.get()
        
        if subscriber is None:
            # Pass the sentinel value to all queues
            await location_queue.put(None)
            await referrer_queue.put(None)
            await linkedin_queue.put(None)
            subscriber_queue.task_done()
            break
        
        try:
            # Queue for location processing
            await location_queue.put(subscriber)
            
            # Queue for referrer processing
            await referrer_queue.put(subscriber)
            
            # Determine if this is a work email for LinkedIn processing
            email = subscriber.get('email_address', '')
            
            if email:
                is_work = is_work_email(email)
                subscriber['email_domain_type'] = 'work' if is_work else 'personal'
                
                # Process through LinkedIn pipeline if it's a work email
                if is_work:
                    # Check if subscriber is active
                    subscriber_state = subscriber.get("state", "").lower()
                    if subscriber_state == "active":
                        # Check if country has good purchase power
                        country = subscriber.get("location_country", "")
                        
                        if not purchase_power_checker.has_low_purchase_power(country):
                            await linkedin_queue.put(subscriber)
            
            subscriber_queue.task_done()
            
        except Exception as e:
            logger.error(f"Error processing subscriber {subscriber.get('id')}: {e}")
            subscriber_queue.task_done()

async def process_location(location_queue: asyncio.Queue, write_queue: asyncio.Queue):
    """Process location data for subscribers."""
    location_pipeline = LocationPipeline()
    processed_count = 0
    
    while True:
        subscriber = await location_queue.get()
        
        if subscriber is None:
            location_queue.task_done()
            break
        
        try:
            enriched = await location_pipeline.process_item(subscriber)
            await write_queue.put(enriched)
            processed_count += 1
            
            if processed_count % 10 == 0:
                console.print(f"[cyan]Location processed: {processed_count}")
            
        except Exception as e:
            logger.error(f"Error in location processing for {subscriber.get('id')}: {e}")
        
        location_queue.task_done()
    
    console.print(f"[green]Location processing completed: {processed_count} subscribers")

async def process_referrer(referrer_queue: asyncio.Queue, write_queue: asyncio.Queue):
    """Process referrer data for subscribers."""
    referrer_pipeline = ReferrerPipeline()
    processed_count = 0
    
    while True:
        subscriber = await referrer_queue.get()
        
        if subscriber is None:
            referrer_queue.task_done()
            break
        
        try:
            enriched = await referrer_pipeline.process_item(subscriber)
            await write_queue.put(enriched)
            processed_count += 1
            
            if processed_count % 10 == 0:
                console.print(f"[cyan]Referrer processed: {processed_count}")
            
        except Exception as e:
            logger.error(f"Error in referrer processing for {subscriber.get('id')}: {e}")
        
        referrer_queue.task_done()
    
    console.print(f"[green]Referrer processing completed: {processed_count} subscribers")

async def process_linkedin(linkedin_queue: asyncio.Queue, write_queue: asyncio.Queue):
    """Process LinkedIn data for subscribers."""
    linkedin_pipeline = LinkedInPipeline()
    processed_count = 0
    found_count = 0
    
    while True:
        subscriber = await linkedin_queue.get()
        
        if subscriber is None:
            linkedin_queue.task_done()
            break
        
        try:
            enriched = await linkedin_pipeline.process_item(subscriber)
            
            # Count LinkedIn profiles found
            if enriched.get('linkedin_profile_url'):
                found_count += 1
            
            await write_queue.put(enriched)
            processed_count += 1
            
            if processed_count % 5 == 0:
                console.print(f"[cyan]LinkedIn processed: {processed_count} (found: {found_count})")
            
        except Exception as e:
            logger.error(f"Error in LinkedIn processing for {subscriber.get('id')}: {e}")
        
        linkedin_queue.task_done()
    
    console.print(f"[green]LinkedIn processing completed: {processed_count} subscribers, {found_count} profiles found")
    return found_count

async def save_to_database(write_queue: asyncio.Queue, update_queue: asyncio.Queue):
    """Save enriched subscribers to the database."""
    processed_count = 0
    success_count = 0
    error_count = 0
    
    # Create a set to track unique subscribers
    processed_ids = set()
    
    while True:
        subscriber = await write_queue.get()
        
        if subscriber is None:
            # Signal that we're done saving
            await update_queue.put(None)
            write_queue.task_done()
            break
        
        # Get subscriber ID
        subscriber_id = subscriber.get('id')
        
        # Skip if we've already processed this subscriber
        if subscriber_id in processed_ids:
            write_queue.task_done()
            continue
        
        try:
            # Save to database
            result = await SubscriberModel.save_subscriber_with_mapping(subscriber)
            
            if result:
                success_count += 1
                # Queue for ConvertKit update
                await update_queue.put(subscriber)
            else:
                error_count += 1
                logger.error(f"Failed to save subscriber: {subscriber_id}")
            
            processed_ids.add(subscriber_id)
            processed_count += 1
            
            if processed_count % 10 == 0:
                console.print(f"[cyan]Saved to database: {processed_count} (success: {success_count}, errors: {error_count})")
            
        except Exception as e:
            error_count += 1
            logger.error(f"Error saving subscriber {subscriber_id}: {e}")
        
        write_queue.task_done()
    
    console.print(f"[green]Database saves completed: {processed_count} subscribers (success: {success_count}, errors: {error_count})")

async def update_convertkit(update_queue: asyncio.Queue):
    """Update subscribers in ConvertKit."""
    processed_count = 0
    success_count = 0
    error_count = 0
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        update_task = progress.add_task("[yellow]Updating ConvertKit...", total=None)
        
        while True:
            subscriber = await update_queue.get()
            
            if subscriber is None:
                progress.update(update_task, completed=True, description="[green]ConvertKit updates completed")
                update_queue.task_done()
                break
            
            try:
                # Get subscriber ID
                subscriber_id = subscriber.get('id')
                
                # Update in ConvertKit
                success, message = await convertkit_updater.update_subscriber(subscriber_id)
                
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    progress.print(f"[red]Error updating subscriber {subscriber_id} in ConvertKit: {message}")
                
                processed_count += 1
                progress.update(update_task, description=f"[yellow]Updating ConvertKit ({processed_count} processed, {success_count} success)")
                
            except Exception as e:
                error_count += 1
                progress.print(f"[red]Exception updating subscriber in ConvertKit: {e}")
            
            update_queue.task_done()
    
    console.print(f"[green]ConvertKit updates completed: {processed_count} subscribers (success: {success_count}, errors: {error_count})")

async def main():
    """
    Main function to fetch and enrich subscribers from a specific date range.
    """
    # Date range parameters
    from_date = "2025-03-07"
    to_date = "2025-03-27"
    
    console.print(f"[bold yellow]Starting enrichment for subscribers created between {from_date} and {to_date}")
    
    # Initialize database
    if not await initialize_database():
        console.print("[bold red]Failed to initialize database, exiting")
        return 1
    
    try:
        # Fetch subscribers
        subscribers = await fetch_subscribers_by_date(from_date, to_date)
        
        if not subscribers:
            console.print("[bold yellow]No subscribers found in the specified date range")
            return 0
        
        total_subscribers = len(subscribers)
        
        # Create queues for parallel processing
        subscriber_queue = asyncio.Queue()
        location_queue = asyncio.Queue()
        referrer_queue = asyncio.Queue()
        linkedin_queue = asyncio.Queue()
        write_queue = asyncio.Queue()
        update_queue = asyncio.Queue()
        
        # Set concurrency limits
        location_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_LOCATION_REQUESTS)
        referrer_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_REFERRER_REQUESTS)
        linkedin_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_LINKEDIN_REQUESTS)
        
        # Fill the queue with subscribers
        for subscriber in subscribers:
            await subscriber_queue.put(subscriber)
            
        # Add sentinel value at the end
        await subscriber_queue.put(None)
        
        # Start processing tasks
        start_time = time.time()
        
        console.print("[bold blue]Starting parallel processing pipelines")
        
        # Create tasks
        processor_task = asyncio.create_task(
            process_subscriber_parallel(
                subscriber_queue, location_queue, referrer_queue, 
                linkedin_queue, write_queue, update_queue
            )
        )
        
        location_task = asyncio.create_task(process_location(location_queue, write_queue))
        referrer_task = asyncio.create_task(process_referrer(referrer_queue, write_queue))
        linkedin_task = asyncio.create_task(process_linkedin(linkedin_queue, write_queue))
        db_task = asyncio.create_task(save_to_database(write_queue, update_queue))
        convertkit_task = asyncio.create_task(update_convertkit(update_queue))
        
        # Wait for all tasks to complete
        await asyncio.gather(
            processor_task, location_task, referrer_task,
            linkedin_task, db_task, convertkit_task,
            return_exceptions=True
        )
        
        # Final summary
        elapsed = time.time() - start_time
        console.print("\n" + "="*50)
        console.print(f"[bold green]Enrichment completed for {total_subscribers} subscribers")
        console.print(f"[green]Total time: {elapsed:.1f} seconds")
        console.print(f"[green]Processing rate: {total_subscribers/elapsed:.1f} subscribers/second")
        console.print("="*50)
        
        return 0
    
    except Exception as e:
        console.print(f"[bold red]Error in enrichment process: {e}")
        import traceback
        console.print(traceback.format_exc())
        return 1
    
    finally:
        # Clean up
        await cleanup_database()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)