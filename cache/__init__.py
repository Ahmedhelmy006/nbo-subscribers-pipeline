"""
Cache system for the NBO Pipeline.

This package provides a caching system to speed up lookups
of already processed subscriber IDs.
"""
from .cache_manager import get_cache_manager, cache_manager

__all__ = ["get_cache_manager", "cache_manager"]