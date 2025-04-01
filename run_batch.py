"""
Batch runner for the NBO Pipeline.

This module provides a script to run a single batch of the pipeline.
"""
import asyncio
import logging
import argparse
from datetime import datetime

from config import logger
from db import initialize_database, cleanup_database

async def run_single_batch(max_records: int = None):
    """
    Run a single batch of the pipeline.
    
    Args:
        max_records: Maximum number of records to process
    """
    from main import run_pipeline
    
    # Initialize database
    if not await initialize_database():
        logger.error("Failed to initialize database, exiting")
        return False
    
    try:
        # Run the pipeline
        start_time = datetime.now()
        logger.info(f"Starting pipeline batch at {start_time.isoformat()}")
        
        success = await run_pipeline(max_records=max_records)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if success:
            logger.info(f"Pipeline batch completed successfully in {duration:.1f} seconds")
        else:
            logger.error(f"Pipeline batch failed after {duration:.1f} seconds")
        
        return success
    finally:
        # Clean up
        await cleanup_database()

async def main():
    """
    Main entry point for running a single batch.
    """
    parser = argparse.ArgumentParser(description="Run a single batch of the NBO Pipeline")
    parser.add_argument(
        "--max-records", 
        type=int, 
        help="Maximum number of records to process"
    )
    args = parser.parse_args()
    
    # Run the batch
    success = await run_single_batch(max_records=args.max_records)
    
    # Return appropriate exit code
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)