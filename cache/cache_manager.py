"""
Cache manager for the NBO Pipeline.

This module provides a unified interface for different cache backends.
"""
import logging
from typing import Dict, Any, Optional, List

from config import settings
from .memory_cache import MemoryCache

logger = logging.getLogger(__name__)

class CacheManager:
    """
    Unified interface for different cache backends.
    Currently supports in-memory cache.
    """
    
    def __init__(self, backend: str = 'memory', **options):
        """
        Initialize the cache manager.
        
        Args:
            backend (str): Cache backend to use ('memory')
            **options: Options to pass to the cache backend
        """
        self.backend = backend
        
        if backend == 'memory':
            max_size = options.get('max_size', settings.CACHE_MAX_SIZE)
            ttl = options.get('ttl', settings.CACHE_TTL)
            self.cache = MemoryCache(max_size=max_size, ttl=ttl)
        else:
            raise ValueError(f"Unknown cache backend: {backend}")
        
        logger.info(f"Initialized cache manager with backend '{backend}'")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        return self.cache.get(key)
    
    def set(self, key: str, value: Any) -> bool:
        """Set value in cache."""
        return self.cache.set(key, value)
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        return self.cache.delete(key)
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        return self.cache.exists(key)
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache."""
        return self.cache.get_many(keys)
    
    def set_many(self, items: Dict[str, Any]) -> bool:
        """Set multiple values in cache."""
        return self.cache.set_many(items)
    
    def clear(self) -> bool:
        """Clear all items from the cache."""
        return self.cache.clear()
    
    def size(self) -> int:
        """Get current cache size."""
        return self.cache.size()
    
    def filter_non_existing(self, items: List[str]) -> List[str]:
        """Filter out items that already exist in cache."""
        return self.cache.filter_non_existing(items)

# Singleton instance
cache_manager = CacheManager(
    backend='memory', 
    max_size=settings.CACHE_MAX_SIZE,
    ttl=settings.CACHE_TTL
)

def get_cache_manager() -> CacheManager:
    """
    Get the singleton cache manager.
    
    Returns:
        CacheManager instance
    """
    return cache_manager