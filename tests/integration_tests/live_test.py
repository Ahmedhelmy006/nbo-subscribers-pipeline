#!/usr/bin/env python
"""
Continuous running test script that bypasses fcntl and other Unix-specific modules.
This script runs the pipeline indefinitely, processing subscribers in batches.
"""
import asyncio
import sys
import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from rich.console import Console
from rich.progress import Progress

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import components directly (avoiding problematic imports)
from config import settings
from db.connection import db_manager
from db.models import initialize_db_tables, SubscriberModel, SubscriberMetadataModel
from cache import cache_manager
from utils.helpers import mask_email
from monitoring.system_reporter import system_reporter

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# Define database initialization and cleanup functions
async def initialize_database():
    """Initialize the database connection."""
    try:
        await db_manager.initialize()
        return True
    except Exception as e:
        console.print(f"[bold red]Database initialization error: {e}[/bold red]")
        return False

async def cleanup_database():
    """Clean up database resources."""
    try:
        await db_manager.close()
    except Exception as e:
        console.print(f"[bold red]Database cleanup error: {e}[/bold red]")

console = Console()

# Create a mock for the BaseStack class to avoid fcntl dependency
class MockBaseStack:
    """Mock implementation of BaseStack for testing without fcntl."""
    
    def __init__(self, stack_dir="stacks/output", prefix="mock_stack", max_size=100):
        self.stack_dir = Path(stack_dir)
        self.prefix = prefix
        self.max_size = max_size
        self.stack_dir.mkdir(parents=True, exist_ok=True)
        self.items = []
    
    def add_item(self, item):
        """Add an item to the stack."""
        self.items.append(item)
        return True
        
    def get_stats(self):
        """Get stats about the stack."""
        return {
            "total_items": len(self.items),
            "current_batch_items": len(self.items)
        }

# Create a mock for LinkedInStack
class MockLinkedInStack(MockBaseStack):
    """Mock implementation of LinkedInStack for testing."""
    
    def __init__(self):
        super().__init__(prefix="linkedin_stack", max_size=settings.LINKEDIN_STACK_MAX_SIZE)
    
    def add_linkedin_url(self, subscriber_id, email_address, linkedin_url):
        """Add a LinkedIn URL to the stack."""
        item = {
            "subscriber_id": subscriber_id,
            "email_address": email_address,
            "linkedin_url": linkedin_url
        }
        return self.add_item(item)

# Mock the stack_manager module
class MockStackManager:
    """Mock implementation of stack_manager."""
    
    def get_linkedin_stack(self):
        """Get the LinkedIn stack."""
        return MockLinkedInStack()

# Patch sys.modules to use our mocks
sys.modules["stacks.base_stack"] = type("MockBaseStackModule", (), {"BaseStack": MockBaseStack})
sys.modules["stacks.linkedin_stack"] = type("MockLinkedStackModule", (), {"LinkedInStack": MockLinkedInStack})
sys.modules["stacks.stack_manager"] = MockStackManager()

# Global set to track processed subscriber IDs across batch runs
processed_ids = set()

async def save_subscriber_to_db(subscriber):
    """
    Save a subscriber to the database.
    
    Args:
        subscriber: Dictionary containing subscriber data
        
    Returns:
        str: Subscriber ID if successful, None otherwise
    """
    try:
        # Update or create the subscriber
        subscriber_id = await SubscriberModel.update_or_create(subscriber)
        
        if subscriber_id:
            # Determine which components have processed this subscriber
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
            
            return subscriber_id
    except Exception as e:
        console.print(f"[red]Error saving subscriber to database: {e}[/red]")
    
    return None

async def update_subscribers_in_convertkit(subscriber_ids, parallel=3):
    """
    Update processed subscribers in ConvertKit.
    
    Args:
        subscriber_ids: List of subscriber IDs to update
        parallel: Number of parallel updates to run
        
    Returns:
        Dict with update statistics
    """
    # Import ConvertKit updater
    from convertkit.updater import convertkit_updater
    
    console.print(f"[yellow]Updating {len(subscriber_ids)} subscribers in ConvertKit...[/yellow]")
    
    # Process in smaller batches for parallel execution
    total_subscribers = len(subscriber_ids)
    batch_size = min(10, max(1, total_subscribers // parallel))
    
    # Split into batches
    batches = []
    for i in range(0, len(subscriber_ids), batch_size):
        batch = subscriber_ids[i:i+batch_size]
        batches.append(batch)
    
    console.print(f"[yellow]Processing in {len(batches)} batches with up to {parallel} in parallel[/yellow]")
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(parallel)
    
    async def process_batch(batch):
        async with semaphore:
            return await convertkit_updater.update_batch(batch)
    
    # Process batches in parallel
    tasks = [process_batch(batch) for batch in batches]
    batch_results = await asyncio.gather(*tasks)
    
    # Combine results
    combined_stats = {
        "total": total_subscribers,
        "success": sum(result["success"] for result in batch_results),
        "failed": sum(result["failed"] for result in batch_results),
        "batches": len(batches)
    }
    
    console.print(f"[green]ConvertKit update completed:[/green]")
    console.print(f"Total: {combined_stats['total']}")
    console.print(f"Success: {combined_stats['success']}")
    console.print(f"Failed: {combined_stats['failed']}")
    
    return combined_stats

async def process_batch(num_subscribers=20, update_convertkit=True, update_parallel=3):
    """Process a batch of subscribers."""
    global processed_ids
    processed_subscriber_ids = []
    
    try:
        # Import pipelines directly to avoid __init__.py issues
        import importlib.util
        
        def import_module_from_file(module_name, file_path):
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        
        base_pipeline = import_module_from_file("base", os.path.join(project_root, "pipelines/base.py"))
        subscriber_pipeline_module = import_module_from_file("subscriber_pipeline", os.path.join(project_root, "pipelines/subscriber_pipeline.py")) 
        location_pipeline_module = import_module_from_file("location_pipeline", os.path.join(project_root, "pipelines/location_pipeline.py"))
        referrer_pipeline_module = import_module_from_file("referrer_pipeline", os.path.join(project_root, "pipelines/referrer_pipeline.py"))
        linkedin_pipeline_module = import_module_from_file("linkedin_pipeline", os.path.join(project_root, "pipelines/linkedin_pipeline.py"))
        
        # Get the pipeline classes
        SubscriberPipeline = subscriber_pipeline_module.SubscriberPipeline
        LocationPipeline = location_pipeline_module.LocationPipeline
        ReferrerPipeline = referrer_pipeline_module.ReferrerPipeline
        LinkedInPipeline = linkedin_pipeline_module.LinkedInPipeline
        
        # Set up pipelines
        subscriber_pipeline = SubscriberPipeline(
            api_key=settings.API_KEY,
            base_url=settings.API_BASE_URL
        )
        
        # Initialize the subscriber pipeline's processed_ids with our global set
        subscriber_pipeline.processed_ids = processed_ids.copy()
        
        # Fetch subscribers
        console.print(f"[yellow]Fetching {num_subscribers} subscribers from ConvertKit...[/yellow]")
        result = await subscriber_pipeline.fetch_subscribers(limit=num_subscribers)
        
        if not result or not result.get('subscribers'):
            console.print("[bold red]Failed to fetch subscribers[/bold red]")
            return 0
            
        subscribers = result['subscribers']
        
        # Filter out subscribers we've already processed
        new_subscribers = []
        for subscriber in subscribers:
            subscriber_id = subscriber.get("id")
            if subscriber_id and subscriber_id not in processed_ids:
                new_subscribers.append(subscriber)
                processed_ids.add(subscriber_id)
        
        if len(new_subscribers) < len(subscribers):
            console.print(f"[yellow]Filtered out {len(subscribers) - len(new_subscribers)} already processed subscribers[/yellow]")
        
        subscribers = new_subscribers
        console.print(f"[green]Successfully fetched {len(subscribers)} new subscribers[/green]")
        
        # Show sample of the data
        if subscribers:
            sample = subscribers[0]
            console.print("[cyan]Sample subscriber data:[/cyan]")
            for key, value in sample.items():
                if key == "email_address":
                    value = mask_email(value)
                console.print(f"  {key}: {value}")
        
        # Process them through each pipeline
        location_pipeline = LocationPipeline()
        referrer_pipeline = ReferrerPipeline()
        linkedin_pipeline = LinkedInPipeline()
        
        metrics = {
            "total_processed": 0,
            "with_location": 0,
            "with_referrer": 0,
            "work_emails": 0,
            "personal_emails": 0,
            "linkedin_found": 0,
            "saved_to_db": 0,
            "with_region": 0,
            "with_timezone": 0
        }
        
        # Import email classifier
        from email_classifier.classifier import classify_email
        
        # Process each subscriber through the pipelines
        with Progress() as progress:
            task = progress.add_task("[cyan]Processing subscribers...", total=len(subscribers))
            
            for subscriber in subscribers:
                try:
                    # First process location
                    subscriber = await location_pipeline.process_item(subscriber)
                    if subscriber.get("location_country"):
                        metrics["with_location"] += 1
                    
                    # Check for new region and timezone
                    if subscriber.get("subscriber_region"):
                        metrics["with_region"] += 1
                    if subscriber.get("timezone"):
                        metrics["with_timezone"] += 1
                    
                    # Process referrer
                    subscriber = await referrer_pipeline.process_item(subscriber)
                    if subscriber.get("referrer_info"):
                        metrics["with_referrer"] += 1
                    
                    # Classify email
                    email = subscriber.get("email_address")
                    if email:
                        domain_type, domain = classify_email(email)
                        subscriber["email_domain_type"] = domain_type
                        
                        if domain_type == "work":
                            metrics["work_emails"] += 1
                            # Process LinkedIn for work emails
                            subscriber = await linkedin_pipeline.process_item(subscriber)
                            if subscriber.get("linkedin_profile_url"):
                                metrics["linkedin_found"] += 1
                        else:
                            metrics["personal_emails"] += 1
                    
                    # Save to database
                    subscriber_id = await SubscriberModel.save_subscriber_with_mapping(subscriber)
                    if subscriber_id:
                        metrics["saved_to_db"] += 1
                        # Add to list for ConvertKit update
                        processed_subscriber_ids.append(subscriber_id)
                        
                    metrics["total_processed"] += 1
                    progress.update(task, advance=1)
                
                except Exception as e:
                    console.print(f"[red]Error processing subscriber {subscriber.get('id')}: {e}[/red]")
                    progress.update(task, advance=1)
                    continue
        
        # Print metrics
        console.print("\n[bold green]Batch Results:[/bold green]")
        console.print(f"Processed {metrics['total_processed']} subscribers")
        
        if len(subscribers) > 0:
            console.print(f"Found location for {metrics['with_location']} subscribers ({metrics['with_location']/len(subscribers)*100:.1f}%)")
            console.print(f"Found referrer for {metrics['with_referrer']} subscribers ({metrics['with_referrer']/len(subscribers)*100:.1f}%)")
            console.print(f"Work emails: {metrics['work_emails']} ({metrics['work_emails']/len(subscribers)*100:.1f}%)")
            console.print(f"Personal emails: {metrics['personal_emails']} ({metrics['personal_emails']/len(subscribers)*100:.1f}%)")
            
            if metrics["work_emails"] > 0:
                console.print(f"LinkedIn profiles found: {metrics['linkedin_found']} ({metrics['linkedin_found']/metrics['work_emails']*100:.1f}% of work emails)")
            
            console.print(f"Saved to database: {metrics['saved_to_db']} ({metrics['saved_to_db']/len(subscribers)*100:.1f}%)")
            
            # New metrics for region and timezone
            console.print(f"Found region for {metrics['with_region']} subscribers ({metrics['with_region']/len(subscribers)*100:.1f}%)")
            console.print(f"Found timezone for {metrics['with_timezone']} subscribers ({metrics['with_timezone']/len(subscribers)*100:.1f}%)")
        else:
            console.print("[yellow]No new subscribers to process in this batch[/yellow]")
        
        # Print database summary
        console.print("\n[bold blue]Database Summary:[/bold blue]")
        try:
            # Get a count of subscribers in the database
            count_query = f"SELECT COUNT(*) FROM {SubscriberModel.TABLE_NAME}"
            total_subscribers = await db_manager.fetchval(count_query)
            
            # Get count of subscribers with location data
            location_query = f"""
            SELECT COUNT(*) FROM {SubscriberMetadataModel.TABLE_NAME}
            WHERE has_location = TRUE
            """
            with_location = await db_manager.fetchval(location_query)
            
            # Get count of subscribers with referrer data
            referrer_query = f"""
            SELECT COUNT(*) FROM {SubscriberMetadataModel.TABLE_NAME}
            WHERE has_referrer = TRUE
            """
            with_referrer = await db_manager.fetchval(referrer_query)
            
            # Get count of fully processed subscribers
            processed_query = f"""
            SELECT COUNT(*) FROM {SubscriberMetadataModel.TABLE_NAME}
            WHERE processed_complete = TRUE
            """
            fully_processed = await db_manager.fetchval(processed_query)
            
            console.print(f"Total subscribers in database: {total_subscribers}")
            console.print(f"Subscribers with location data: {with_location}")
            console.print(f"Subscribers with referrer data: {with_referrer}")
            console.print(f"Fully processed subscribers: {fully_processed}")
            
        except Exception as e:
            console.print(f"[red]Error querying database: {e}[/red]")
        
        # Update ConvertKit if enabled
        if update_convertkit and processed_subscriber_ids:
            console.print("\n[bold blue]Updating ConvertKit:[/bold blue]")
            try:
                # Update processed subscribers in ConvertKit
                convertkit_stats = await update_subscribers_in_convertkit(
                    processed_subscriber_ids, 
                    parallel=update_parallel
                )
                
                # Add the stats to our metrics
                metrics["convertkit_updated"] = convertkit_stats["success"]
                metrics["convertkit_failed"] = convertkit_stats["failed"]
                
            except Exception as e:
                console.print(f"[red]Error updating ConvertKit: {e}[/red]")
                metrics["convertkit_updated"] = 0
                metrics["convertkit_failed"] = len(processed_subscriber_ids)

        # Return the number of processed subscribers for tracking
        return len(processed_subscriber_ids)
        
    except Exception as e:
        console.print(f"[bold red]Error during batch processing: {e}[/bold red]")
        import traceback
        console.print(traceback.format_exc())
        return 0

async def run_continuous_pipeline(batch_size=20, update_convertkit=True, update_parallel=3, 
                                 interval_seconds=120, cache_clear_hours=12):
    """
    Run the pipeline continuously, processing batches at regular intervals.
    
    Args:
        batch_size: Number of subscribers to process per batch
        update_convertkit: Whether to update ConvertKit
        update_parallel: Number of parallel ConvertKit updates
        interval_seconds: Seconds to wait between batches
        cache_clear_hours: Hours between cache clearing
    """
    global processed_ids
    
    console.print("[bold green]Starting continuous pipeline run[/bold green]")
    console.print(f"Processing batches of {batch_size} subscribers every {interval_seconds} seconds")
    console.print(f"Cache will be cleared every {cache_clear_hours} hours")
    
    # Send startup notification
    system_reporter.send_startup_notification()
    
    last_cache_clear = datetime.now()
    total_processed = 0
    batch_count = 0
    
    # Initialize database once
    await initialize_database()
    await initialize_db_tables()
    
    try:
        while True:
            batch_start = datetime.now()
            batch_count += 1
            
            console.print(f"\n[bold cyan]Starting batch #{batch_count} at {batch_start.strftime('%Y-%m-%d %H:%M:%S')}[/bold cyan]")
            
            # Process a batch
            subscribers_processed = await process_batch(
                num_subscribers=batch_size,
                update_convertkit=update_convertkit,
                update_parallel=update_parallel
            )
            
            # Update totals
            total_processed += subscribers_processed
            
            # Calculate batch time
            batch_end = datetime.now()
            batch_duration = (batch_end - batch_start).total_seconds()
            console.print(f"[bold cyan]Batch #{batch_count} completed in {batch_duration:.1f} seconds. Total processed: {total_processed}[/bold cyan]")
            
            # Prepare batch info for reporting
            batch_info = {
                'batch_number': batch_count,
                'total_processed': subscribers_processed,
                'duration': f"{batch_duration:.1f} seconds",
                'timestamp': batch_start.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Record and send batch summary
            system_reporter.record_batch_result(batch_info)
            system_reporter.send_batch_summary()
            
            # Check if we need to clear the cache
            time_since_cache_clear = (datetime.now() - last_cache_clear).total_seconds() / 3600  # in hours
            if time_since_cache_clear >= cache_clear_hours:
                console.print("[yellow]Clearing cache to prevent memory issues...[/yellow]")
                cache_manager.clear()
                # Also clear our global processed_ids set
                processed_ids.clear()
                last_cache_clear = datetime.now()
                console.print("[green]Cache cleared successfully[/green]")
            
            # Wait for the next interval, but subtract the processing time
            wait_time = max(1, interval_seconds - batch_duration)
            console.print(f"[blue]Waiting {wait_time:.1f} seconds until next batch...[/blue]")
            await asyncio.sleep(wait_time)
    
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Keyboard interrupt detected. Stopping gracefully...[/bold yellow]")
        system_reporter.send_error_alert("Pipeline stopped by keyboard interrupt")
    except Exception as e:
        console.print(f"\n[bold red]Error in continuous pipeline: {e}[/bold red]")
        import traceback
        error_traceback = traceback.format_exc()
        console.print(error_traceback)
        
        # Send detailed error to Slack
        system_reporter.send_error_alert(
            str(e), 
            context=f"Batch #{batch_count}, Total processed: {total_processed}"
        )
    finally:
        # Clean up
        await cleanup_database()
        console.print("[bold yellow]Pipeline stopped. Database connection closed.[/bold yellow]")

if __name__ == "__main__":
    # Parse command-line arguments if any
    import argparse
    parser = argparse.ArgumentParser(description="Run the NBO Pipeline continuously")
    parser.add_argument("--batch-size", type=int, default=40, help="Number of subscribers to process per batch")
    parser.add_argument("--no-convertkit", action="store_true", help="Skip updating ConvertKit")
    parser.add_argument("--parallel", type=int, default=3, help="Number of parallel ConvertKit updates")
    parser.add_argument("--interval", type=int, default=300, help="Seconds to wait between batches")
    parser.add_argument("--cache-clear", type=int, default=12, help="Hours between cache clearing")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_continuous_pipeline(
            batch_size=args.batch_size,
            update_convertkit=not args.no_convertkit,
            update_parallel=args.parallel,
            interval_seconds=args.interval,
            cache_clear_hours=args.cache_clear
        ))
    except KeyboardInterrupt:
        print("\nProcess stopped by user")