"""
In-memory cache implementation for the NBO Pipeline.

This module provides a simple in-memory cache for storing
already processed subscriber IDs.
"""
import time
import threading
import logging
from typing import Dict, Any, Optional, List, Set

logger = logging.getLogger(__name__)

class MemoryCache:
    """
    In-memory cache implementation using dictionaries as hash tables
    for fast lookups. Thread-safe for concurrent access.
    """
    
    def __init__(self, max_size: int = 100000, ttl: int = 86400):
        """
        Initialize the memory cache.
        
        Args:
            max_size (int): Maximum number of items to store
            ttl (int): Time-to-live in seconds (default: 24 hours)
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        logger.info(f"Initialized memory cache with max_size={max_size}, ttl={ttl}")
        
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache if it exists and is not expired.
        
        Args:
            key (str): Cache key
            
        Returns:
            Optional[Any]: Cached value or None if not found
        """
        with self._lock:
            if key not in self._cache:
                return None
                
            item = self._cache[key]
            
            # Check if expired
            if self.ttl > 0 and time.time() > item['expires_at']:
                del self._cache[key]
                return None
                
            return item['value']
    
    def set(self, key: str, value: Any) -> bool:
        """
        Set a value in the cache.
        
        Args:
            key (str): Cache key
            value (Any): Value to cache
            
        Returns:
            bool: True if successfully set
        """
        with self._lock:
            # If at max size, remove oldest item
            if len(self._cache) >= self.max_size and key not in self._cache:
                oldest_key = min(self._cache.items(), key=lambda x: x[1]['created_at'])[0]
                del self._cache[oldest_key]
            
            self._cache[key] = {
                'value': value,
                'created_at': time.time(),
                'expires_at': time.time() + self.ttl if self.ttl > 0 else float('inf')
            }
            
            return True
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from the cache.
        
        Args:
            key (str): Cache key
            
        Returns:
            bool: True if key was found and deleted
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache and is not expired.
        
        Args:
            key (str): Cache key
            
        Returns:
            bool: True if key exists and is not expired
        """
        with self._lock:
            if key not in self._cache:
                return False
                
            item = self._cache[key]
            
            # Check if expired
            if self.ttl > 0 and time.time() > item['expires_at']:
                del self._cache[key]
                return False
                
            return True
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """
        Get multiple values from cache.
        
        Args:
            keys (List[str]): List of cache keys
            
        Returns:
            Dict[str, Any]: Dictionary of found keys and their values
        """
        result = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        return result
    
    def set_many(self, items: Dict[str, Any]) -> bool:
        """
        Set multiple values in the cache.
        
        Args:
            items (Dict[str, Any]): Dictionary of keys and values
            
        Returns:
            bool: True if all items were set
        """
        with self._lock:
            for key, value in items.items():
                self.set(key, value)
            return True
    
    def clear(self) -> bool:
        """
        Clear all items from the cache.
        
        Returns:
            bool: True if successfully cleared
        """
        with self._lock:
            self._cache.clear()
            return True
            
    def size(self) -> int:
        """
        Get the current size of the cache.
        
        Returns:
            int: Number of items in the cache
        """
        with self._lock:
            return len(self._cache)
            
    def contains_all(self, items: List[str]) -> bool:
        """
        Check if all provided items exist in the cache.
        
        Args:
            items (List[str]): List of keys to check
            
        Returns:
            bool: True if all keys exist
        """
        return all(self.exists(key) for key in items)
            
    def contains_any(self, items: List[str]) -> bool:
        """
        Check if any of the provided items exist in the cache.
        
        Args:
            items (List[str]): List of keys to check
            
        Returns:
            bool: True if any key exists
        """
        return any(self.exists(key) for key in items)
    
    def filter_existing(self, items: List[str]) -> List[str]:
        """
        Filter a list to only include items that exist in the cache.
        
        Args:
            items (List[str]): List of keys to check
            
        Returns:
            List[str]: List of keys that exist in the cache
        """
        return [item for item in items if self.exists(item)]
    
    def filter_non_existing(self, items: List[str]) -> List[str]:
        """
        Filter a list to only include items that don't exist in the cache.
        
        Args:
            items (List[str]): List of keys to check
            
        Returns:
            List[str]: List of keys that don't exist in the cache
        """
        return [item for item in items if not self.exists(item)]