"""
Database connection management for the NBO Pipeline.

This module provides a connection pool for PostgreSQL and
handles database connections throughout the pipeline.
"""
import asyncio
import logging
import asyncpg
from typing import Optional

from config import settings

logger = logging.getLogger(__name__)

class DatabaseConnectionManager:
    """
    Manages PostgreSQL database connections using a connection pool.
    """
    
    def __init__(self):
        """Initialize the connection manager."""
        self.pool = None
        self._init_lock = asyncio.Lock()
    
    async def initialize(self):
        """
        Initialize the connection pool.
        
        This method is safe to call multiple times; it will only
        create the pool once.
        """
        async with self._init_lock:
            if self.pool is not None:
                return
            
            try:
                logger.info(
                    f"Initializing database connection pool to "
                    f"{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
                )
                
                # Create connection pool
                self.pool = await asyncpg.create_pool(
                    host=settings.DB_HOST,
                    port=settings.DB_PORT,
                    user=settings.DB_USER,
                    password=settings.DB_PASSWORD,
                    database=settings.DB_NAME,
                    min_size=settings.DB_POOL_MIN_SIZE,
                    max_size=settings.DB_POOL_MAX_SIZE,
                    command_timeout=60.0,
                )
                
                logger.info("Database connection pool initialized successfully")
                
            except Exception as e:
                logger.error(f"Failed to initialize database connection pool: {e}")
                raise
    
    async def get_connection(self) -> asyncpg.Connection:
        """
        Get a connection from the pool.
        
        Returns:
            asyncpg.Connection: Database connection
            
        Raises:
            Exception: If the pool has not been initialized or another error occurs
        """
        if self.pool is None:
            await self.initialize()
            
        return await self.pool.acquire()
    
    async def release_connection(self, connection: asyncpg.Connection):
        """
        Release a connection back to the pool.
        
        Args:
            connection: The connection to release
        """
        if self.pool is not None:
            await self.pool.release(connection)
    
    async def execute(self, query, *args, **kwargs):
        """
        Execute a query and return the result.
        
        Args:
            query: The query to execute
            *args: Positional arguments for the query
            **kwargs: Keyword arguments for the query
            
        Returns:
            The result of the query
        """
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args, **kwargs)
    
    async def fetch(self, query, *args, **kwargs):
        """
        Execute a query and return all rows.
        
        Args:
            query: The query to execute
            *args: Positional arguments for the query
            **kwargs: Keyword arguments for the query
            
        Returns:
            List of records returned by the query
        """
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args, **kwargs)
    
    async def fetchrow(self, query, *args, **kwargs):
        """
        Execute a query and return the first row.
        
        Args:
            query: The query to execute
            *args: Positional arguments for the query
            **kwargs: Keyword arguments for the query
            
        Returns:
            First record returned by the query or None
        """
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args, **kwargs)
    
    async def fetchval(self, query, *args, **kwargs):
        """
        Execute a query and return a single value.
        
        Args:
            query: The query to execute
            *args: Positional arguments for the query
            **kwargs: Keyword arguments for the query
            
        Returns:
            First value of the first record returned by the query or None
        """
        async with self.pool.acquire() as connection:
            return await connection.fetchval(query, *args, **kwargs)
    
    async def execute_transaction(self, callback, *args, **kwargs):
        """
        Execute multiple operations in a transaction.
        
        Args:
            callback: Async function that takes a connection as the first argument
            *args: Additional positional arguments for the callback
            **kwargs: Additional keyword arguments for the callback
            
        Returns:
            The result of the callback
        """
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                return await callback(connection, *args, **kwargs)
    
    async def close(self):
        """Close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed")

# Create a singleton instance of the connection manager
db_manager = DatabaseConnectionManager()