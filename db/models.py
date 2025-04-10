"""
Database models for the NBO Pipeline.

This module defines the table structures and provides methods
for interacting with database tables.
"""
import uuid
import logging
import json
from datetime import datetime
from utils.helpers import parse_iso_datetime
from typing import List, Dict, Any, Optional, Union

import asyncpg

from .connection import db_manager

logger = logging.getLogger(__name__)

class SubscriberModel:
    """
    Model for interacting with the subscribers table.
    """
    
    TABLE_NAME = "subscribers"
    
    @staticmethod
    async def get_by_id(subscriber_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a subscriber by ID.
        
        Args:
            subscriber_id: The ID of the subscriber
            
        Returns:
            Dict containing subscriber data or None if not found
        """
        query = f"""
        SELECT * FROM {SubscriberModel.TABLE_NAME}
        WHERE id = $1
        """
        
        record = await db_manager.fetchrow(query, subscriber_id)
        return dict(record) if record else None
    
    @staticmethod
    async def update_or_create(subscriber_data: Dict[str, Any]) -> str:
        """
        Update an existing subscriber or create a new one.
        
        Args:
            subscriber_data: Dictionary of subscriber data
            
        Returns:
            The ID of the updated or created subscriber
        """
        subscriber_id = subscriber_data.get('id')
        if not subscriber_id:
            logger.error("Cannot update or create subscriber without ID")
            raise ValueError("Subscriber ID is required")
        
        # Check if subscriber exists
        existing = await SubscriberModel.get_by_id(subscriber_id)
        
        if existing:
            # Update existing subscriber
            return await SubscriberModel.update(subscriber_data)
        else:
            # Create new subscriber
            return await SubscriberModel.create(subscriber_data)
    
    @staticmethod
    async def create(subscriber_data: Dict[str, Any]) -> str:
        """
        Create a new subscriber.
        
        Args:
            subscriber_data: Dictionary of subscriber data
            
        Returns:
            The ID of the created subscriber
        """
        # Extract the required fields
        subscriber_id = subscriber_data.get('id')
        email_address = subscriber_data.get('email_address')
        
        if not subscriber_id or not email_address:
            logger.error("Cannot create subscriber without ID and email address")
            raise ValueError("Subscriber ID and email address are required")
        
        # Get all column names from the data
        columns = list(subscriber_data.keys())
        
        # Create placeholders for the values
        placeholders = [f"${i+1}" for i in range(len(columns))]
        
        # Create the query
        query = f"""
        INSERT INTO {SubscriberModel.TABLE_NAME} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        RETURNING id
        """
        
        # Get the values in the same order as the columns
        values = [subscriber_data.get(column) for column in columns]
        
        try:
            # Execute the query
            result = await db_manager.fetchval(query, *values)
            logger.info(f"Created subscriber {subscriber_id}")
            return result
        except Exception as e:
            logger.error(f"Error creating subscriber {subscriber_id}: {e}")
            raise
    
    @staticmethod
    async def update(subscriber_data: Dict[str, Any]) -> str:
        """
        Update an existing subscriber.
        
        Args:
            subscriber_data: Dictionary of subscriber data
            
        Returns:
            The ID of the updated subscriber
        """
        subscriber_id = subscriber_data.get('id')
        if not subscriber_id:
            logger.error("Cannot update subscriber without ID")
            raise ValueError("Subscriber ID is required")
        
        # Remove ID from the data to avoid updating it
        update_data = {k: v for k, v in subscriber_data.items() if k != 'id'}
        
        # Create SET clause
        set_clause = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
        
        # Create the query
        query = f"""
        UPDATE {SubscriberModel.TABLE_NAME}
        SET {set_clause}
        WHERE id = $1
        RETURNING id
        """
        
        # Get the values in the same order as the keys
        values = [subscriber_id] + [update_data.get(key) for key in update_data.keys()]
        
        try:
            # Execute the query
            result = await db_manager.fetchval(query, *values)
            logger.info(f"Updated subscriber {subscriber_id}")
            return result
        except Exception as e:
            logger.error(f"Error updating subscriber {subscriber_id}: {e}")
            raise
    
    @staticmethod
    async def bulk_update_or_create(subscribers: List[Dict[str, Any]]) -> List[str]:
        """
        Update or create multiple subscribers in bulk.
        
        Args:
            subscribers: List of subscriber data dictionaries
            
        Returns:
            List of IDs of updated or created subscribers
        """
        # Use a transaction to ensure all updates are atomic
        async def _bulk_process(connection, subscribers):
            results = []
            for subscriber in subscribers:
                subscriber_id = subscriber.get('id')
                
                # Check if subscriber exists
                query = f"""
                SELECT id FROM {SubscriberModel.TABLE_NAME}
                WHERE id = $1
                """
                existing = await connection.fetchrow(query, subscriber_id)
                
                if existing:
                    # Update existing subscriber
                    update_data = {k: v for k, v in subscriber.items() if k != 'id'}
                    set_clause = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
                    
                    query = f"""
                    UPDATE {SubscriberModel.TABLE_NAME}
                    SET {set_clause}
                    WHERE id = $1
                    RETURNING id
                    """
                    
                    values = [subscriber_id] + [update_data.get(key) for key in update_data.keys()]
                    result = await connection.fetchval(query, *values)
                else:
                    # Create new subscriber
                    columns = list(subscriber.keys())
                    placeholders = [f"${i+1}" for i in range(len(columns))]
                    
                    query = f"""
                    INSERT INTO {SubscriberModel.TABLE_NAME} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    RETURNING id
                    """
                    
                    values = [subscriber.get(column) for column in columns]
                    result = await connection.fetchval(query, *values)
                
                results.append(result)
            return results
        
        try:
            results = await db_manager.execute_transaction(_bulk_process, subscribers)
            logger.info(f"Bulk processed {len(subscribers)} subscribers")
            return results
        except Exception as e:
            logger.error(f"Error in bulk processing subscribers: {e}")
            raise
    @staticmethod
    async def save_subscriber_with_mapping(subscriber_data: Dict[str, Any]) -> Optional[str]:
        """
        Save a subscriber with field mapping from API to database schema.
        
        Args:
            subscriber_data: Dictionary of subscriber data from API
            
        Returns:
            The ID of the saved subscriber or None if failed
        """
        try:
            # Create a copy to avoid modifying the original
            db_subscriber = {}
            
            # Log the original ID and its type
            original_id = subscriber_data.get('id')
            logger.info(f"Original subscriber ID: {original_id}, Type: {type(original_id)}")
            
            if 'id' in subscriber_data and subscriber_data['id'] is not None:
                db_subscriber['id'] = str(subscriber_data['id'])
                logger.info(f"Converted subscriber ID to string: {db_subscriber['id']}, Type: {type(db_subscriber['id'])}")
            else:
                logger.error("Cannot update or create subscriber without ID")
                return None
            
            # Map base fields from ConvertKit API to database schema
            base_field_mapping = {
                'id': 'id',
                'email_address': 'email_address',
                'state': 'status',
                'first_name': 'first_name',
                
                # Location fields
                'location_city': 'city',
                'location_state': 'state',
                'location_country': 'country',
                
                # New region and timezone fields
                'subscriber_region': 'subscriber_region',
                'timezone': 'timezone',
                
                # Other fields
                'referrer_domain': 'referrer_domain',
                'referrer_utm_source': 'referrer_utm_source',
                'referrer_utm_medium': 'referrer_utm_medium',
                'referrer_utm_campaign': 'referrer_utm_campaign',
                'referrer_utm_content': 'referrer_utm_content',
                'referrer_utm_term': 'referrer_utm_term',
                'form_name': 'form_name',
                'purchase_power': 'purchase_power',
                'purchase_score': 'purchase_score',
                'email_domain_type': 'email_domain_type',
                'linkedin_profile_url': 'linkedin_profile_url',
            }
            
            # Copy mapped base fields
            for api_field, db_field in base_field_mapping.items():
                if api_field in subscriber_data and subscriber_data[api_field] is not None:
                    db_subscriber[db_field] = subscriber_data[api_field]
            
            if 'subscriber_region' in db_subscriber:
                region = db_subscriber['subscriber_region']
                if region and region.lower() == 'unknown':
                    db_subscriber['subscriber_region'] = None
                elif region:
                    region_lower = region.lower()
                    if region_lower == 'asia':
                        db_subscriber['subscriber_region'] = 'Asia'
                    elif region_lower == 'africa':
                        db_subscriber['subscriber_region'] = 'Africa'
                    elif region_lower == 'europe':
                        db_subscriber['subscriber_region'] = 'Europe'
                    elif region_lower in ['north_america', 'north america']:
                        db_subscriber['subscriber_region'] = 'North America'
                    elif region_lower in ['latin_america', 'latin america', 'south america']:
                        db_subscriber['subscriber_region'] = 'South America'
                    elif region_lower in ['oceania']:
                        db_subscriber['subscriber_region'] = 'Oceania'
            
            if 'created_at' in subscriber_data and subscriber_data['created_at']:
                db_subscriber['created_at'] = parse_iso_datetime(subscriber_data['created_at'])
            
            # Extract custom fields one by one
            if 'fields' in subscriber_data and subscriber_data['fields']:
                custom_fields = subscriber_data['fields']
                # Add each custom field directly to the subscriber record
                for field_name, field_value in custom_fields.items():
                    if field_value is not None:
                        # Check if field_value is a datetime string
                        if isinstance(field_value, str) and 'T' in field_value and (field_value.endswith('Z') or '+' in field_value):
                            field_value = parse_iso_datetime(field_value)
                        
                        # Add each custom field directly (don't add a 'fields' field)
                        db_subscriber[field_name] = field_value
            
            # Call update_or_create with explicit type checking of result
            logger.info(f"Calling update_or_create with db_subscriber['id']: {db_subscriber['id']}")
            subscriber_id = await SubscriberModel.update_or_create(db_subscriber)
            logger.info(f"Result from update_or_create: {subscriber_id}, Type: {type(subscriber_id)}")
            
            # Ensure subscriber_id is a string before continuing
            subscriber_id_str = str(subscriber_id) if subscriber_id is not None else None
            logger.info(f"Converted subscriber_id to string: {subscriber_id_str}")
            
            if subscriber_id_str:
                # Determine which components have processed this subscriber
                has_location = (
                    subscriber_data.get('location_city') is not None or
                    subscriber_data.get('location_state') is not None or
                    subscriber_data.get('location_country') is not None
                )
                
                has_referrer = subscriber_data.get('referrer_info') is not None
                
                # Update metadata with explicitly string ID
                logger.info(f"Calling update_metadata with subscriber_id_str: {subscriber_id_str}, Type: {type(subscriber_id_str)}")
                await SubscriberMetadataModel.update_metadata(
                    subscriber_id=subscriber_id_str,  # Explicitly use string ID
                    has_location=has_location,
                    has_referrer=has_referrer,
                    processed_complete=True
                )
                
                logger.info(f"Successfully saved subscriber {subscriber_id_str} with field mapping")
                return subscriber_id_str  # Return string ID
                
        except Exception as e:
            logger.error(f"Error saving subscriber with mapping: {e}")
            # Add more detailed error info
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

class PipelineStateModel:
    """
    Model for interacting with the pipeline_state table.
    """
    
    TABLE_NAME = "pipeline_state"
    
    @staticmethod
    async def ensure_table_exists():
        """
        Ensure the pipeline_state table exists.
        
        Creates the table if it doesn't exist.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {PipelineStateModel.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            pipeline_name VARCHAR(50) NOT NULL,
            last_run_id VARCHAR(100),
            last_run_timestamp TIMESTAMP WITH TIME ZONE,
            last_processed_id VARCHAR(100),
            status VARCHAR(20) NOT NULL DEFAULT 'idle',
            records_processed INTEGER DEFAULT 0,
            UNIQUE(pipeline_name)
        )
        """
        
        try:
            await db_manager.execute(query)
            logger.info(f"Ensured {PipelineStateModel.TABLE_NAME} table exists")
        except Exception as e:
            logger.error(f"Error ensuring {PipelineStateModel.TABLE_NAME} table exists: {e}")
            raise
    
    @staticmethod
    async def get_state(pipeline_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the state of a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Dict containing pipeline state or None if not found
        """
        query = f"""
        SELECT * FROM {PipelineStateModel.TABLE_NAME}
        WHERE pipeline_name = $1
        """
        
        record = await db_manager.fetchrow(query, pipeline_name)
        return dict(record) if record else None
    
    @staticmethod
    async def update_state(
        pipeline_name: str,
        status: Optional[str] = None,
        last_processed_id: Optional[str] = None,
        last_run_id: Optional[str] = None,
        records_processed: Optional[int] = None
    ) -> bool:
        """
        Update the state of a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            status: New status
            last_processed_id: Last processed ID
            last_run_id: Last run ID
            records_processed: Number of records processed
            
        Returns:
            True if the update was successful, False otherwise
        """
        # Get current state
        current_state = await PipelineStateModel.get_state(pipeline_name)
        
        if current_state:
            # Update existing state
            updates = {}
            if status is not None:
                updates["status"] = status
            if last_processed_id is not None:
                updates["last_processed_id"] = last_processed_id
            if last_run_id is not None:
                updates["last_run_id"] = last_run_id
            if records_processed is not None:
                updates["records_processed"] = records_processed
            
            # Always update timestamp
            updates["last_run_timestamp"] = datetime.now()
            
            # Create SET clause
            set_clause = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(updates.keys())])
            
            query = f"""
            UPDATE {PipelineStateModel.TABLE_NAME}
            SET {set_clause}
            WHERE pipeline_name = $1
            """
            
            values = [pipeline_name] + list(updates.values())
            
        else:
            # Create new state
            new_state = {
                "pipeline_name": pipeline_name,
                "status": status or "idle",
                "last_processed_id": last_processed_id,
                "last_run_id": last_run_id,
                "records_processed": records_processed or 0,
                "last_run_timestamp": datetime.now()
            }
            
            # Filter out None values
            new_state = {k: v for k, v in new_state.items() if v is not None}
            
            # Create INSERT statement
            columns = list(new_state.keys())
            placeholders = [f"${i+1}" for i in range(len(columns))]
            
            query = f"""
            INSERT INTO {PipelineStateModel.TABLE_NAME} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (pipeline_name) DO UPDATE
            SET last_run_timestamp = EXCLUDED.last_run_timestamp
            """
            
            values = [new_state.get(column) for column in columns]
        
        try:
            await db_manager.execute(query, *values)
            logger.info(f"Updated state for pipeline '{pipeline_name}'")
            return True
        except Exception as e:
            logger.error(f"Error updating state for pipeline '{pipeline_name}': {e}")
            return False

class PipelineRunModel:
    """
    Model for interacting with the pipeline_runs table.
    """
    
    TABLE_NAME = "pipeline_runs"
    
    @staticmethod
    async def ensure_table_exists():
        """
        Ensure the pipeline_runs table exists.
        
        Creates the table if it doesn't exist.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {PipelineRunModel.TABLE_NAME} (
            run_id UUID PRIMARY KEY,
            pipeline_name VARCHAR(50) NOT NULL,
            start_time TIMESTAMP WITH TIME ZONE NOT NULL,
            end_time TIMESTAMP WITH TIME ZONE,
            status VARCHAR(20) NOT NULL,
            records_processed INTEGER DEFAULT 0,
            error_message TEXT,
            metadata JSONB
        )
        """
        
        try:
            await db_manager.execute(query)
            logger.info(f"Ensured {PipelineRunModel.TABLE_NAME} table exists")
        except Exception as e:
            logger.error(f"Error ensuring {PipelineRunModel.TABLE_NAME} table exists: {e}")
            raise
    
    @staticmethod
    async def create_run(
        pipeline_name: str,
        status: str = "running",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new pipeline run.
        
        Args:
            pipeline_name: Name of the pipeline
            status: Initial status of the run
            metadata: Additional metadata for the run
            
        Returns:
            The ID of the created run
        """
        run_id = str(uuid.uuid4())
        
        query = f"""
        INSERT INTO {PipelineRunModel.TABLE_NAME}
        (run_id, pipeline_name, start_time, status, metadata)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING run_id
        """
        
        try:
            result = await db_manager.fetchval(
                query,
                run_id,
                pipeline_name,
                datetime.now(),
                status,
                json.dumps(metadata) if metadata else None
            )
            
            logger.info(f"Created run {run_id} for pipeline '{pipeline_name}'")
            
            # Update pipeline state
            await PipelineStateModel.update_state(
                pipeline_name=pipeline_name,
                status=status,
                last_run_id=run_id
            )
            
            return result
        except Exception as e:
            logger.error(f"Error creating run for pipeline '{pipeline_name}': {e}")
            raise
    
    @staticmethod
    async def update_run(
        run_id: str,
        status: Optional[str] = None,
        end_time: Optional[bool] = None,
        records_processed: Optional[int] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update an existing pipeline run.
        
        Args:
            run_id: ID of the run to update
            status: New status
            end_time: Whether to set end_time to now
            records_processed: Number of records processed
            error_message: Error message
            metadata: Updated metadata
            
        Returns:
            True if the update was successful, False otherwise
        """
        # Get current run
        query = f"""
        SELECT * FROM {PipelineRunModel.TABLE_NAME}
        WHERE run_id = $1
        """
        
        record = await db_manager.fetchrow(query, run_id)
        
        if not record:
            logger.error(f"Run {run_id} not found")
            return False
        
        # Build update dict
        updates = {}
        if status is not None:
            updates["status"] = status
        if end_time:
            updates["end_time"] = datetime.now()
        if records_processed is not None:
            updates["records_processed"] = records_processed
        if error_message is not None:
            updates["error_message"] = error_message
        if metadata is not None:
            current_metadata = json.loads(record["metadata"]) if record["metadata"] else {}
            current_metadata.update(metadata)
            updates["metadata"] = json.dumps(current_metadata)
        
        if not updates:
            logger.warning(f"No updates provided for run {run_id}")
            return True
        
        # Create SET clause
        set_clause = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(updates.keys())])
        
        query = f"""
        UPDATE {PipelineRunModel.TABLE_NAME}
        SET {set_clause}
        WHERE run_id = $1
        """
        
        values = [run_id] + list(updates.values())
        
        try:
            await db_manager.execute(query, *values)
            logger.info(f"Updated run {run_id}")
            
            # If this is a terminal status, update pipeline state
            if status in ["completed", "failed"]:
                # Get pipeline name
                pipeline_name = record["pipeline_name"]
                
                # Update pipeline state
                await PipelineStateModel.update_state(
                    pipeline_name=pipeline_name,
                    status="idle" if status == "completed" else "error",
                    records_processed=records_processed
                )
            
            return True
        except Exception as e:
            logger.error(f"Error updating run {run_id}: {e}")
            return False
    
    @staticmethod
    async def get_recent_runs(pipeline_name: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent pipeline runs.
        
        Args:
            pipeline_name: Filter by pipeline name
            limit: Maximum number of runs to return
            
        Returns:
            List of run records
        """
        if pipeline_name:
            query = f"""
            SELECT * FROM {PipelineRunModel.TABLE_NAME}
            WHERE pipeline_name = $1
            ORDER BY start_time DESC
            LIMIT $2
            """
            records = await db_manager.fetch(query, pipeline_name, limit)
        else:
            query = f"""
            SELECT * FROM {PipelineRunModel.TABLE_NAME}
            ORDER BY start_time DESC
            LIMIT $1
            """
            records = await db_manager.fetch(query, limit)
        
        return [dict(record) for record in records]

class SubscriberMetadataModel:
    """
    Model for interacting with the subscriber_metadata table.
    """
    
    TABLE_NAME = "subscriber_metadata"
    
    @staticmethod
    async def ensure_table_exists():
        """
        Ensure the subscriber_metadata table exists.
        
        Creates the table if it doesn't exist.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS {SubscriberMetadataModel.TABLE_NAME} (
            subscriber_id VARCHAR(100) PRIMARY KEY,
            has_location BOOLEAN DEFAULT FALSE,
            has_referrer BOOLEAN DEFAULT FALSE,
            processed_complete BOOLEAN DEFAULT FALSE
        )
        """
        
        try:
            await db_manager.execute(query)
            logger.info(f"Ensured {SubscriberMetadataModel.TABLE_NAME} table exists")
        except Exception as e:
            logger.error(f"Error ensuring {SubscriberMetadataModel.TABLE_NAME} table exists: {e}")
            raise
    
    @staticmethod
    async def update_metadata(
        subscriber_id: int,
        has_location: Optional[bool] = None,
        has_referrer: Optional[bool] = None,
        processed_complete: Optional[bool] = None
    ) -> bool:
        """
        Update metadata for a subscriber.
        
        Args:
            subscriber_id: ID of the subscriber
            has_location: Whether location data has been processed
            has_referrer: Whether referrer data has been processed
            processed_complete: Whether processing is complete
            
        Returns:
            True if the update was successful, False otherwise
        """
        # Check if record exists
        query = f"""
        SELECT subscriber_id FROM {SubscriberMetadataModel.TABLE_NAME}
        WHERE subscriber_id = $1
        """
        
        existing = await db_manager.fetchval(query, subscriber_id)
        
        if existing:
            # Update existing record
            updates = {}
            if has_location is not None:
                updates["has_location"] = has_location
            if has_referrer is not None:
                updates["has_referrer"] = has_referrer
            if processed_complete is not None:
                updates["processed_complete"] = processed_complete
            
            if not updates:
                logger.warning(f"No updates provided for subscriber {subscriber_id}")
                return True
            
            # Create SET clause
            set_clause = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(updates.keys())])
            
            query = f"""
            UPDATE {SubscriberMetadataModel.TABLE_NAME}
            SET {set_clause}
            WHERE subscriber_id = $1
            """
            
            values = [subscriber_id] + list(updates.values())
        else:
            # Create new record
            columns = ["subscriber_id"]
            values = [subscriber_id]
            
            if has_location is not None:
                columns.append("has_location")
                values.append(has_location)
            if has_referrer is not None:
                columns.append("has_referrer")
                values.append(has_referrer)
            if processed_complete is not None:
                columns.append("processed_complete")
                values.append(processed_complete)
            
            # Create placeholders
            placeholders = [f"${i+1}" for i in range(len(values))]
            
            query = f"""
            INSERT INTO {SubscriberMetadataModel.TABLE_NAME} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """
        
        try:
            await db_manager.execute(query, *values)
            logger.info(f"Updated metadata for subscriber {subscriber_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating metadata for subscriber {subscriber_id}: {e}")
            return False
    
    @staticmethod
    async def get_incomplete_subscribers(limit: int = 100) -> List[str]:
        """
        Get subscribers that haven't been fully processed.
        
        Args:
            limit: Maximum number of subscribers to return
            
        Returns:
            List of subscriber IDs
        """
        query = f"""
        SELECT subscriber_id FROM {SubscriberMetadataModel.TABLE_NAME}
        WHERE processed_complete = FALSE
        LIMIT $1
        """
        
        records = await db_manager.fetch(query, limit)
        return [record["subscriber_id"] for record in records]

# Initialize the necessary tables
async def initialize_db_tables():
    """Initialize all database tables needed by the pipeline."""
    try:
        # Ensure pipeline management tables exist
        await PipelineStateModel.ensure_table_exists()
        await PipelineRunModel.ensure_table_exists()
        await SubscriberMetadataModel.ensure_table_exists()
        
        logger.info("Database tables initialized")
        return True
    except Exception as e:
        logger.error(f"Error initializing database tables: {e}")
        return False
