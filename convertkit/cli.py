"""
Command-line interface for ConvertKit updater.

This module provides a command-line interface for updating subscriber data
in ConvertKit from our database.
"""
import sys
import os
import asyncio
import argparse
import logging
from rich.console import Console
from rich.progress import Progress

from config import settings, logger
from db import initialize_database, cleanup_database
from .updater import convertkit_updater

console = Console()

async def update_single_subscriber(subscriber_id):
    """Update a single subscriber in ConvertKit."""
    console.print(f"[yellow]Updating subscriber {subscriber_id} in ConvertKit...[/yellow]")
    
    success, message = await convertkit_updater.update_subscriber(subscriber_id)
    
    if success:
        console.print(f"[green]Successfully updated subscriber {subscriber_id}[/green]")
    else:
        console.print(f"[red]Failed to update subscriber {subscriber_id}: {message}[/red]")
    
    return success

async def update_recent_subscribers(limit):
    """Update recently processed subscribers in ConvertKit."""
    console.print(f"[yellow]Updating {limit} recently processed subscribers in ConvertKit...[/yellow]")
    
    stats = await convertkit_updater.update_recent_subscribers(limit)
    
    console.print(f"[green]Update completed with the following results:[/green]")
    console.print(f"Total: {stats['total']}")
    console.print(f"Success: {stats['success']}")
    console.print(f"Failed: {stats['failed']}")
    
    return stats

async def update_all_subscribers(batch_size):
    """Update all fully processed subscribers in ConvertKit."""
    console.print(f"[yellow]Updating all fully processed subscribers in ConvertKit with batch size {batch_size}...[/yellow]")
    
    with Progress() as progress:
        # Get the total count first
        query = """
        SELECT COUNT(*) FROM subscriber_metadata
        WHERE processed_complete = TRUE
        """
        from db.connection import db_manager
        total = await db_manager.fetchval(query)
        
        task = progress.add_task("[cyan]Updating subscribers...", total=total)
        
        # Define a progress callback to update the progress bar
        async def progress_callback(batch_stats):
            nonlocal task
            # Update the progress based on batch size
            progress.update(task, advance=len(batch_stats["results"]))
        
        # Start the update process
        stats = await update_all_with_progress(batch_size, progress_callback)
    
    console.print(f"[green]Update completed with the following results:[/green]")
    console.print(f"Total processed: {stats['total_to_process']}")
    console.print(f"Batches processed: {stats['batches_processed']}")
    console.print(f"Success: {stats['success']}")
    console.print(f"Failed: {stats['failed']}")
    
    return stats

async def update_all_with_progress(batch_size, progress_callback=None):
    """
    Update all subscribers with progress reporting.
    
    Args:
        batch_size: Size of each batch
        progress_callback: Callback function for progress updates
        
    Returns:
        Update statistics
    """
    # Get total count
    query = """
    SELECT COUNT(*) FROM subscriber_metadata
    WHERE processed_complete = TRUE
    """
    from db.connection import db_manager
    total = await db_manager.fetchval(query)
    
    # Initialize stats
    stats = {
        "total_to_process": total,
        "batches_processed": 0,
        "success": 0,
        "failed": 0
    }
    
    # Process in batches
    offset = 0
    
    while offset < total:
        # Get a batch of subscriber IDs
        query = """
        SELECT subscriber_id FROM subscriber_metadata
        WHERE processed_complete = TRUE
        ORDER BY subscriber_id
        LIMIT $1 OFFSET $2
        """
        
        records = await db_manager.fetch(query, batch_size, offset)
        subscriber_ids = [record["subscriber_id"] for record in records]
        
        # Update the batch
        batch_stats = await convertkit_updater.update_batch(subscriber_ids)
        
        # Update overall stats
        stats["batches_processed"] += 1
        stats["success"] += batch_stats["success"]
        stats["failed"] += batch_stats["failed"]
        
        # Call progress callback if provided
        if progress_callback:
            await progress_callback(batch_stats)
        
        # Move to next batch
        offset += batch_size
        
        # Log progress
        logger.info(f"Processed batch {stats['batches_processed']}: "
                  f"Success: {batch_stats['success']}, Failed: {batch_stats['failed']}")
        
        # Add small delay between batches
        await asyncio.sleep(1)
    
    return stats

async def run_cli():
    """Run the command-line interface."""
    parser = argparse.ArgumentParser(description="Update ConvertKit with data from the database")
    
    # Create mutually exclusive group for update mode
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--subscriber-id", type=str, help="Update a single subscriber by ID")
    mode_group.add_argument("--recent", type=int, help="Update N recently processed subscribers")
    mode_group.add_argument("--all", action="store_true", help="Update all fully processed subscribers")
    
    # Additional options
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing all subscribers (default: 50)")
    
    args = parser.parse_args()
    
    # Initialize database
    console.print("[yellow]Initializing database connection...[/yellow]")
    if not await initialize_database():
        console.print("[bold red]Failed to initialize database, exiting[/bold red]")
        return 1
    
    try:
        if args.subscriber_id:
            await update_single_subscriber(args.subscriber_id)
        elif args.recent:
            await update_recent_subscribers(args.recent)
        elif args.all:
            await update_all_subscribers(args.batch_size)
        
        console.print("[bold green]Operations completed successfully![/bold green]")
        return 0
    
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        return 1
    
    finally:
        # Clean up
        await cleanup_database()

def main():
    """Entry point for the CLI script."""
    exit_code = asyncio.run(run_cli())
    sys.exit(exit_code)

if __name__ == "__main__":
    main()