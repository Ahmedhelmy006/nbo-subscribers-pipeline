"""
Base pipeline class for the NBO Pipeline.

This module provides the base class for all pipeline components.
"""
import asyncio
import logging
from rich.console import Console
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class FetchPipeline:
    """Base class for fetch pipelines"""
    
    def __init__(self, max_concurrent=20):
        """
        Initialize the pipeline with concurrency control
        
        Args:
            max_concurrent (int): Maximum number of concurrent requests
        """
        self.max_concurrent = max_concurrent
        self.console = Console()
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of items
        
        Args:
            items (list): List of items to process
            
        Returns:
            list: Processed items
        """
        raise NotImplementedError("Subclasses must implement process_batch")
    
    async def process_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single item
        
        Args:
            item: Item to process
            
        Returns:
            Processed item
        """
        raise NotImplementedError("Subclasses must implement process_item")