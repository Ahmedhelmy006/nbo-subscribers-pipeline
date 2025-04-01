"""
Stack system for the NBO Pipeline.

This package provides a file-based stack system for storing
LinkedIn URLs for external scraping.
"""
from .linkedin_stack import LinkedInStack
from .stack_manager import get_stack_manager

__all__ = ["LinkedInStack", "get_stack_manager"]