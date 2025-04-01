"""
Base stack implementation for the NBO Pipeline.

This module provides a base class for file-based stacks.
"""
import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from config import settings

logger = logging.getLogger(__name__)

class BaseStack:
    """
    Base class for file-based stacks.
    
    Provides common functionality for all stack implementations.
    """
    
    def __init__(self, 
                 stack_dir: str,
                 prefix: str,
                 max_size: int = 1000,
                 file_extension: str = '.json'):
        """
        Initialize the base stack.
        
        Args:
            stack_dir (str): Directory to store stack files
            prefix (str): Prefix for stack file names
            max_size (int): Maximum number of items in a batch
            file_extension (str): File extension for stack files
        """
        self.stack_dir = Path(stack_dir)
        self.prefix = prefix
        self.max_size = max_size
        self.file_extension = file_extension
        self.current_batch_id = None
        self.current_batch_file = None
        self.current_batch_items = []
        
        # Create the stack directory if it doesn't exist
        self.stack_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized stack in {self.stack_dir} with prefix '{self.prefix}'")
    
    def _generate_batch_id(self) -> str:
        """
        Generate a unique batch ID.
        
        Returns:
            str: Unique batch ID
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.prefix}_{timestamp}"
    
    def _get_batch_filename(self, batch_id: str) -> Path:
        """
        Get the filename for a batch.
        
        Args:
            batch_id (str): Batch ID
            
        Returns:
            Path: Path to the batch file
        """
        return self.stack_dir / f"{batch_id}{self.file_extension}"
    
    def _create_new_batch(self) -> str:
        """
        Create a new batch file.
        
        Returns:
            str: Batch ID
        """
        self.current_batch_id = self._generate_batch_id()
        self.current_batch_file = self._get_batch_filename(self.current_batch_id)
        self.current_batch_items = []
        
        # Create initial batch file
        batch_data = {
            "batch_id": self.current_batch_id,
            "created_at": datetime.now().isoformat(),
            "items": []
        }
        
        with open(self.current_batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, indent=2)
        
        logger.info(f"Created new batch {self.current_batch_id}")
        return self.current_batch_id
    
    # [Rest of the methods remain the same as in the original implementation]
    
    def add_item(self, item: Dict[str, Any]) -> bool:
        """
        Add an item to the stack.
        
        Args:
            item (Dict): Item to add
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Get current batch
        if not self.current_batch_id or not self._get_batch_filename(self.current_batch_id).exists():
            self._create_new_batch()
        
        # Read current batch
        try:
            with open(self.current_batch_file, 'r', encoding='utf-8') as f:
                batch_data = json.load(f)
        except Exception as e:
            logger.error(f"Error reading batch file: {e}")
            return False
        
        # Add the item
        items = batch_data.get('items', [])
        items.append(item)
        batch_data['items'] = items
        batch_data['updated_at'] = datetime.now().isoformat()
        
        # Write back to file
        try:
            with open(self.current_batch_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, indent=2)
            
            logger.info(f"Added item to batch {self.current_batch_id}")
            return True
        except Exception as e:
            logger.error(f"Error writing batch file: {e}")
            return False
    
    # [Additional methods as needed]

# Example usage
if __name__ == "__main__":
    # Demonstration of basic usage
    stack = BaseStack(
        stack_dir="./stacks", 
        prefix="example_stack", 
        max_size=100
    )
    
    # Add some sample items
    stack.add_item({"key": "value1"})
    stack.add_item({"key": "value2"})