"""
Stack manager for the NBO Pipeline.

This module provides functions for managing stacks.
"""
import logging
from typing import Dict, Any

from .linkedin_stack import LinkedInStack

logger = logging.getLogger(__name__)

# Dictionary of stack instances
_stacks = {}

def get_stack_manager(stack_type: str) -> Any:
    """
    Get a stack manager for a specific type.
    
    Args:
        stack_type: Type of stack ('linkedin')
        
    Returns:
        Stack manager instance
    """
    global _stacks
    
    if stack_type not in _stacks:
        if stack_type == 'linkedin':
            _stacks[stack_type] = LinkedInStack()
        else:
            raise ValueError(f"Unknown stack type: {stack_type}")
    
    return _stacks[stack_type]

def get_linkedin_stack() -> LinkedInStack:
    """
    Get the LinkedIn stack manager.
    
    Returns:
        LinkedInStack instance
    """
    return get_stack_manager('linkedin')