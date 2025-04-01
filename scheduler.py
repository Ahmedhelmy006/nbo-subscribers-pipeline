"""
Scheduler for the NBO Pipeline.

This module provides a scheduler that runs the pipeline at regular intervals.
"""
import os
import sys
import time
import asyncio
import logging
import signal
import argparse
from datetime import datetime, timedelta

import config
from db import initialize_database, cleanup_database
from db.state import get_pipeline_state_manager

logger = logging.getLogger(__name__)

class PipelineScheduler:
    """
    Schedules pipeline runs at regular intervals.
    """
    
    def __init__(self, interval_seconds: int = None):
        """
        Initialize the scheduler.
        
        Args:
            interval_seconds: Interval between runs in seconds
        """
        self.interval_seconds = interval_seconds or config.settings.SCHEDULER_INTERVAL
        self.should_stop = asyncio.Event()
        self.current_run_task = None
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        logger.info(f"Initialized scheduler with interval of {self.interval_seconds} seconds")
    
    def _handle_signal(self, signum, frame):
        """
        Handle termination signals.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info(f"Received signal {signum}, shutting down...")
        self.should_stop.set()
        
        # If there's a run in progress, cancel it
        if self.current_run_task:
            self.current_run_task.cancel()
    
    async def should_run_batch(self) -> bool:
        """
        Determine if we should run a new batch.
        
        This is where we implement the watermark-based approach, checking:
        - If a batch is already running
        - If sufficient time has passed since the last batch
        - If system resources are available
        
        Returns:
            bool: True if we should run a new batch, False otherwise
        """
        # Get the pipeline state manager for the main pipeline
        state_manager = get_pipeline_state_manager("main")
        
        # Check if the pipeline is already running
        is_running = await state_manager.is_pipeline_running()
        if is_running:
            logger.info("Pipeline is already running, skipping batch")
            return False
        
        # Check when the last run finished
        state = await state_manager.get_current_state()
        last_run_timestamp = state.get("last_run_timestamp")
        
        if last_run_timestamp:
            # Convert to datetime if it's a string
            if isinstance(last_run_timestamp, str):
                try:
                    last_run_timestamp = datetime.fromisoformat(last_run_timestamp.replace('Z', '+00:00'))
                except ValueError:
                    # If we can't parse the timestamp, assume it's old enough
                    last_run_timestamp = datetime.now() - timedelta(hours=1)
            
            # Check if enough time has passed (minimum interval)
            min_interval = timedelta(seconds=self.interval_seconds)
            time_since_last_run = datetime.now() - last_run_timestamp
            
            if time_since_last_run < min_interval:
                logger.info(f"Not enough time since last run ({time_since_last_run.total_seconds():.1f} seconds), minimum is {min_interval.total_seconds()} seconds")
                return False
        
        # Check system resources (CPU, memory)
        # For now we'll use a simple approach; this could be enhanced with psutil
        # to make decisions based on actual system metrics
        
        # For now, always return True if other checks pass
        return True
    
    async def run_batch(self):
        """
        Run a single batch of the pipeline.
        """
        from main import run_pipeline
        
        try:
            logger.info("Starting pipeline batch")
            await run_pipeline()
            logger.info("Pipeline batch completed")
        except asyncio.CancelledError:
            logger.warning("Pipeline batch was cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in pipeline batch: {e}", exc_info=True)
    
    async def run(self):
        """
        Run the scheduler loop.
        """
        logger.info("Starting scheduler")
        
        # Initialize database
        if not await initialize_database():
            logger.error("Failed to initialize database, exiting")
            return
        
        try:
            # Main loop
            while not self.should_stop.is_set():
                # Check if we should run a batch
                should_run = await self.should_run_batch()
                
                if should_run:
                    # Run a batch
                    self.current_run_task = asyncio.create_task(self.run_batch())
                    
                    try:
                        await self.current_run_task
                    except asyncio.CancelledError:
                        logger.warning("Batch run was cancelled")
                    finally:
                        self.current_run_task = None
                
                # Wait for next check
                try:
                    # Use wait_for to be able to cancel it when stopping
                    await asyncio.wait_for(
                        self.should_stop.wait(),
                        timeout=min(60, self.interval_seconds / 4)  # Check more frequently than the interval
                    )
                except asyncio.TimeoutError:
                    # This is expected - timeout means we should continue the loop
                    pass
        finally:
            # Clean up
            logger.info("Cleaning up resources")
            await cleanup_database()
            logger.info("Scheduler stopped")

async def main():
    """
    Main entry point for the scheduler.
    """
    parser = argparse.ArgumentParser(description="NBO Pipeline Scheduler")
    parser.add_argument(
        "--interval", 
        type=int, 
        help=f"Interval between runs in seconds (default: {config.settings.SCHEDULER_INTERVAL})"
    )
    args = parser.parse_args()
    
    # Create scheduler
    scheduler = PipelineScheduler(interval_seconds=args.interval)
    
    # Run scheduler
    await scheduler.run()

if __name__ == "__main__":
    asyncio.run(main())