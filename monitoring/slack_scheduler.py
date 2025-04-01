#!/usr/bin/env python3
"""
Slack report scheduler script.

This script schedules daily Slack reports for the NBO Pipeline.
"""
import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime

# Add parent directory to path so we can import modules
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

from config import settings, logger
from db import initialize_database, cleanup_database
from monitoring.slack_reporter import schedule_daily_report

async def main():
    """
    Main entry point for the Slack scheduler.
    """
    parser = argparse.ArgumentParser(description="NBO Pipeline Slack Reporter")
    parser.add_argument(
        "--hour", 
        type=int, 
        default=settings.SLACK_REPORT_HOUR,
        help=f"Hour to send the report (24-hour format, default: {settings.SLACK_REPORT_HOUR})"
    )
    parser.add_argument(
        "--minute", 
        type=int, 
        default=settings.SLACK_REPORT_MINUTE,
        help=f"Minute to send the report (default: {settings.SLACK_REPORT_MINUTE})"
    )
    parser.add_argument(
        "--webhook", 
        type=str, 
        default=settings.SLACK_WEBHOOK_URL,
        help="Slack webhook URL"
    )
    args = parser.parse_args()
    
    # Check for required settings
    if not args.webhook:
        logger.error("No Slack webhook URL provided")
        return 1
    
    if not os.getenv('SLACK_BOT_TOKEN'):
        logger.warning("SLACK_BOT_TOKEN environment variable not set - charts will not be uploaded")
    
    if not os.getenv('SLACK_CHANNEL_ID'):
        logger.warning("SLACK_CHANNEL_ID environment variable not set - charts will not be uploaded")
    
    # Initialize database
    if not await initialize_database():
        logger.error("Failed to initialize database, exiting")
        return 1
    
    try:
        # Log startup information
        logger.info(f"Starting Slack reporter scheduler")
        logger.info(f"Reports will be sent at {args.hour:02d}:{args.minute:02d}")
        
        # Schedule daily reports
        await schedule_daily_report(
            webhook_url=args.webhook,
            hour=args.hour,
            minute=args.minute
        )
        
    except asyncio.CancelledError:
        logger.info("Slack scheduler cancelled")
    except Exception as e:
        logger.error(f"Error in Slack scheduler: {e}")
        return 1
    finally:
        # Clean up
        await cleanup_database()
    
    return 0

async def send_now():
    """
    Send a report immediately for testing.
    """
    from monitoring.slack_reporter import SlackReporter
    
    # Initialize database
    if not await initialize_database():
        logger.error("Failed to initialize database, exiting")
        return 1
    
    try:
        webhook_url = settings.SLACK_WEBHOOK_URL
        
        if not webhook_url:
            logger.error("No Slack webhook URL provided")
            return 1
        
        logger.info("Generating and sending Slack report...")
        reporter = SlackReporter(webhook_url)
        success = await reporter.send_daily_report(days=1)
        
        if success:
            logger.info("Report sent successfully")
            return 0
        else:
            logger.error("Failed to send report")
            return 1
            
    finally:
        # Clean up
        await cleanup_database()

if __name__ == "__main__":
    # Add a special flag for sending a report immediately
    if len(sys.argv) > 1 and sys.argv[1] == "--send-now":
        sys.exit(asyncio.run(send_now()))
    else:
        sys.exit(asyncio.run(main()))