"""
LinkedIn pipeline for the NBO Pipeline.

This module provides functionality for finding LinkedIn profiles
for subscribers with work emails.
"""
import asyncio
import logging
import time
import os
from typing import Dict, List, Any, Optional

from config import settings
from pipelines.base import FetchPipeline
from utils.country_utils import get_purchase_power_checker
from utils.helpers import mask_email
from stacks.stack_manager import get_linkedin_stack
from email_classifier.classifier import classify_email
from email_classifier.validator import is_valid_email
from lookup.lookup_processor import LinkedInLookupProcessor

logger = logging.getLogger(__name__)

class LinkedInPipeline(FetchPipeline):
    """
    Pipeline for finding LinkedIn profiles for subscribers.
    """
    
    def __init__(self, max_concurrent=5):
        """
        Initialize the LinkedIn pipeline.
        
        Args:
            max_concurrent (int): Maximum concurrent requests
        """
        # Use a lower concurrency for LinkedIn lookups as they are more resource-intensive
        super().__init__(max_concurrent=max_concurrent)
        
        # Initialize the lookup processor
        self.lookup_processor = LinkedInLookupProcessor()
        
        # Get LinkedIn stack
        self.linkedin_stack = get_linkedin_stack()
        
        # Get purchase power checker
        self.purchase_power_checker = get_purchase_power_checker()
        
        # Tracking metrics
        self.total_processed = 0
        self.found_count = 0
        self.skipped_inactive_count = 0
        self.skipped_low_purchase_power_count = 0
        self.start_time = time.time()
        
        # Cache for subscribers we've already processed
        self.processed_emails = set()
    
    async def process_item(self, subscriber):
        """
        Process a single subscriber to find their LinkedIn profile.
        
        Args:
            subscriber (dict): Subscriber data
            
        Returns:
            dict: Updated subscriber data with LinkedIn profile URL
        """
        # Extract necessary fields
        subscriber_id = subscriber.get("id")
        email = subscriber.get("email_address")
        
        if not subscriber_id or not email:
            logger.warning("Subscriber ID or email missing, skipping LinkedIn processing")
            return subscriber
        
        # Skip if we've already processed this email
        if email in self.processed_emails:
            logger.info(f"Already processed email {mask_email(email)}, skipping")
            return subscriber
        
        # Add to processed set
        self.processed_emails.add(email)
        
        # Check if subscriber is active
        subscriber_state = subscriber.get("state", "").lower()
        if subscriber_state != "active":
            logger.info(f"Skipping LinkedIn lookup for {mask_email(email)}: subscriber state is {subscriber_state}")
            self.skipped_inactive_count += 1
            return subscriber
        
        # Check country purchase power
        country = subscriber.get("location_country")
        if self.purchase_power_checker.has_low_purchase_power(country):
            logger.info(f"Skipping LinkedIn lookup for {mask_email(email)}: country {country} has low purchase power")
            self.skipped_low_purchase_power_count += 1
            return subscriber
        
        # Check if email is valid
        if not is_valid_email(email):
            logger.warning(f"Invalid email format: {mask_email(email)}, skipping LinkedIn processing")
            return subscriber
        
        # Classify email
        domain_type, domain = classify_email(email)
        
        # Update email domain type if needed
        if "email_domain_type" not in subscriber or not subscriber["email_domain_type"]:
            subscriber["email_domain_type"] = domain_type
        
        # Only process work emails
        if domain_type != "work":
            logger.info(f"Not a work email: {mask_email(email)} (type: {domain_type}), skipping LinkedIn processing")
            return subscriber
        
        # Limit concurrency
        async with self.semaphore:
            try:
                # Try to find LinkedIn profile
                linkedin_url = await self.lookup_processor.find_linkedin_profile(
                    subscriber_id=subscriber_id,
                    email=email,
                    first_name=subscriber.get("first_name", ""),
                    location_city=subscriber.get("location_city", ""),
                    location_state=subscriber.get("location_state", ""),
                    location_country=country
                )
                
                # Store LinkedIn URL if found
                if linkedin_url:
                    subscriber["linkedin_profile_url"] = linkedin_url
                    self.found_count += 1
                    
                    # Add to LinkedIn stack for external processing
                    await asyncio.to_thread(
                        self.linkedin_stack.add_linkedin_url,
                        subscriber_id,
                        email,
                        linkedin_url
                    )
                    
                    self.console.print(f"[green]LinkedIn profile found for {mask_email(email)}: {linkedin_url}")
                else:
                    self.console.print(f"[yellow]No LinkedIn profile found for {mask_email(email)}")
                    
            except Exception as e:
                logger.error(f"Error finding LinkedIn profile for {mask_email(email)}: {e}")
        
        # Update tracking metrics
        self.total_processed += 1
        
        # Log progress occasionally
        if self.total_processed % 10 == 0:
            elapsed = time.time() - self.start_time
            rate = self.total_processed / elapsed if elapsed > 0 else 0
            found_percent = (self.found_count / self.total_processed * 100) if self.total_processed > 0 else 0
            
            self.console.print(
                f"[cyan]Processed {self.total_processed} LinkedIn lookups at {rate:.2f} records/second "
                f"(found: {self.found_count}, {found_percent:.1f}%)"
            )
            
            # Log additional metrics
            total_evaluated = (self.total_processed + self.skipped_inactive_count + 
                               self.skipped_low_purchase_power_count)
            if total_evaluated > 0:
                self.console.print(
                    f"[cyan]Skipped: {self.skipped_inactive_count} inactive subscribers, "
                    f"{self.skipped_low_purchase_power_count} low purchase power countries "
                    f"({(self.skipped_inactive_count + self.skipped_low_purchase_power_count) / total_evaluated * 100:.1f}% of total)"
                )
        
        return subscriber
    
    async def process_batch(self, subscribers):
        """
        Process a batch of subscribers to find LinkedIn profiles.
        
        Args:
            subscribers (list): List of subscriber dictionaries
            
        Returns:
            list: Updated subscribers with LinkedIn profile URLs
        """
        # Count subscribers that meet initial criteria
        work_email_count = sum(1 for s in subscribers if s.get("email_domain_type") == "work")
        active_work_email_count = sum(1 for s in subscribers 
                                      if s.get("email_domain_type") == "work" 
                                      and s.get("state", "").lower() == "active")
        
        self.console.print(
            f"[bold yellow]Finding LinkedIn profiles for {active_work_email_count} "
            f"active subscribers out of {work_email_count} work emails...")
        
        # Process each subscriber in the batch
        processed = []
        for subscriber in subscribers:
            # Only process if it's a work email (detailed filtering inside process_item)
            if subscriber.get("email_domain_type") == "work":
                processed_subscriber = await self.process_item(subscriber)
                processed.append(processed_subscriber)
            else:
                # Skip non-work emails
                processed.append(subscriber)
        
        # Print final statistics
        total_evaluated = (self.total_processed + self.skipped_inactive_count + 
                           self.skipped_low_purchase_power_count)
        if total_evaluated > 0:
            self.console.print(
                f"[bold green]LinkedIn lookup completed: {self.found_count} profiles found "
                f"out of {self.total_processed} processed."
            )
            self.console.print(
                f"[bold yellow]Skipped: {self.skipped_inactive_count} inactive subscribers, "
                f"{self.skipped_low_purchase_power_count} low purchase power countries."
            )
        
        return processed