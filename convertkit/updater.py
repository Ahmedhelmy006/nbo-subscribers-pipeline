"""
ConvertKit updater module for the NBO Pipeline.

This module provides functionality for updating subscriber data in ConvertKit
with data processed and stored in our database.
"""
import os
import logging
import json
import asyncio
import aiohttp
import decimal
from typing import Dict, List, Any, Optional, Tuple

from config import settings, api_keys
from config.headers import get_convertkit_api_headers
from db.connection import db_manager
from db.models import SubscriberModel
from utils.helpers import mask_email

logger = logging.getLogger(__name__)

class ConvertKitUpdater:
    """
    Updates subscriber data in ConvertKit with data from the database.
    """
    
    def __init__(self, api_key=None, base_url=None):
        """
        Initialize the ConvertKit updater.
        
        Args:
            api_key: API key for ConvertKit
            base_url: Base URL for ConvertKit API
        """
        self.api_key = api_key or api_keys.CONVERTKIT_API_KEY
        self.base_url = base_url or settings.API_BASE_URL
        self.headers = get_convertkit_api_headers(self.api_key)
        
        # Fields that should NOT be updated in ConvertKit
        self.exclude_fields = [
            'email_address',
            'first_name',
            'created_at',
            'state',
            # Referrer fields
            'referrer_domain',
            'referrer_utm_source',
            'referrer_utm_medium',
            'referrer_utm_campaign',
            'referrer_utm_content',
            'referrer_utm_term',
            # Location fields except country_long
            'location_city',
            'location_state',
        ]
    
    async def update_subscriber(self, subscriber_id: str) -> Tuple[bool, str]:
        """
        Update a single subscriber in ConvertKit.
        
        Args:
            subscriber_id: ID of the subscriber to update
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Convert subscriber_id to integer for all operations
            # Both the database and API expect integer IDs
            try:
                int_subscriber_id = int(subscriber_id)
            except (ValueError, TypeError):
                return False, f"Invalid subscriber ID format: {subscriber_id} (must be convertible to integer)"
            
            # Get subscriber data from database using integer ID
            subscriber_data = await SubscriberModel.get_by_id(int_subscriber_id)
            
            if not subscriber_data:
                return False, f"Subscriber {int_subscriber_id} not found in database"
            
            # Prepare the update payload
            update_payload = self._prepare_update_payload(subscriber_data)
            
            # If no fields to update, skip
            if not update_payload.get('fields'):
                return False, f"No fields to update for subscriber {int_subscriber_id}"
            
            # Update in ConvertKit using integer ID
            api_url = f"{self.base_url}/subscribers/{int_subscriber_id}"
            
            # Custom JSON encoder to handle Decimal types
            class DecimalEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, decimal.Decimal):
                        return float(obj)
                    return super(DecimalEncoder, self).default(obj)
            
            async with aiohttp.ClientSession() as session:
                # First serialize the JSON with the custom encoder
                serialized_payload = json.dumps(update_payload, cls=DecimalEncoder)
                combined_headers = {"Content-Type": "application/json", **self.headers}

                async with session.put(
                    api_url,
                    headers=combined_headers,  # Use combined headers here
                    data=serialized_payload,   # Use pre-serialized payload
                    timeout=30
                ) as response:
                    if response.status == 200 or response.status == 202:
                        response_data = await response.json()
                        logger.info(f"Successfully updated subscriber {int_subscriber_id} in ConvertKit")
                        return True, f"Successfully updated subscriber {int_subscriber_id}"
                    else:
                        error_text = await response.text()
                        logger.error(f"Error updating subscriber {int_subscriber_id}: {response.status} - {error_text}")
                        return False, f"Error: {response.status} - {error_text}"
        
        except Exception as e:
            logger.error(f"Exception updating subscriber {subscriber_id}: {e}")
            return False, f"Exception: {str(e)}"
    
    def _prepare_update_payload(self, subscriber_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare the update payload for ConvertKit.
        
        Args:
            subscriber_data: Subscriber data from database
            
        Returns:
            Update payload for ConvertKit API
        """
        # Start with an empty fields dictionary
        fields = {}
        
        # Handle special case for country - map location_country to country_long
        if 'location_country' in subscriber_data and subscriber_data['location_country']:
            fields['country_long'] = subscriber_data['location_country']
        
        # Process other fields
        for key, value in subscriber_data.items():
            # Skip excluded fields
            if key in self.exclude_fields:
                continue
                
            # Skip empty values
            if value is None or value == "":
                continue
                
            # Convert Decimal objects to float for JSON serialization
            if isinstance(value, decimal.Decimal):
                value = float(value)
                
            # Special handling for some fields
            if key == 'linkedin_profile_url' and value:
                # Use proper custom field name for LinkedIn URL
                fields['linkedin_profile_url'] = value
            elif key == 'purchase_power' and value:
                fields['purchase_power'] = value
            elif key == 'purchase_score' and value:
                # Ensure purchase_score is a string or number, not Decimal
                if isinstance(value, decimal.Decimal):
                    fields['purchase_score'] = str(float(value))
                else:
                    fields['purchase_score'] = str(value)
            elif key == 'subscriber_region' and value:
                fields['subscriber_region'] = value
            elif key == 'timezone' and value:
                fields['timezone'] = value
            elif key == 'email_domain_type' and value:
                fields['email_domain_type'] = value
            else:
                # Add other fields directly
                fields[key] = value
        
        # Build the final payload
        payload = {"fields": fields}
        
        return payload

    async def update_batch(self, subscriber_ids: List[str]) -> Dict[str, Any]:
        """
        Update a batch of subscribers in ConvertKit.
        
        Args:
            subscriber_ids: List of subscriber IDs to update
            
        Returns:
            Dictionary with update statistics
        """
        stats = {
            "total": len(subscriber_ids),
            "success": 0,
            "failed": 0,
            "results": []
        }
        
        for subscriber_id in subscriber_ids:
            # Try to convert to integer - our database queries expect integers
            try:
                int_subscriber_id = int(subscriber_id)
                # Use the integer ID for the update
                success, message = await self.update_subscriber(int_subscriber_id)
            except (ValueError, TypeError):
                success = False
                message = f"Invalid subscriber ID format: {subscriber_id} (must be convertible to integer)"
            
            if success:
                stats["success"] += 1
            else:
                stats["failed"] += 1
            
            # Add detailed result
            stats["results"].append({
                "subscriber_id": subscriber_id,
                "success": success,
                "message": message
            })
            
            # Add small delay to avoid rate limiting
            await asyncio.sleep(0.1)
        
        return stats
    
    async def update_recent_subscribers(self, limit: int = 100) -> Dict[str, Any]:
        """
        Update recently processed subscribers in ConvertKit.
        
        Args:
            limit: Maximum number of subscribers to update
            
        Returns:
            Dictionary with update statistics
        """
        # Query for recently processed subscribers
        query = """
        SELECT subscriber_id FROM subscriber_metadata
        WHERE processed_complete = TRUE
        ORDER BY subscriber_id DESC
        LIMIT $1
        """
        
        records = await db_manager.fetch(query, limit)
        subscriber_ids = [record["subscriber_id"] for record in records]
        
        # Update the batch
        stats = await self.update_batch(subscriber_ids)
        
        return stats
    
    async def update_all_processed_subscribers(self, batch_size: int = 50) -> Dict[str, Any]:
        """
        Update all fully processed subscribers in ConvertKit.
        
        Args:
            batch_size: Size of batches to process
            
        Returns:
            Dictionary with update statistics
        """
        # Get total count
        query = """
        SELECT COUNT(*) FROM subscriber_metadata
        WHERE processed_complete = TRUE
        """
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
            batch_stats = await self.update_batch(subscriber_ids)
            
            # Update overall stats
            stats["batches_processed"] += 1
            stats["success"] += batch_stats["success"]
            stats["failed"] += batch_stats["failed"]
            
            # Move to next batch
            offset += batch_size
            
            # Log progress
            logger.info(f"Processed batch {stats['batches_processed']}: "
                       f"Success: {batch_stats['success']}, Failed: {batch_stats['failed']}")
            
            # Add small delay between batches
            await asyncio.sleep(1)
        
        return stats

# Create a global instance
convertkit_updater = ConvertKitUpdater()