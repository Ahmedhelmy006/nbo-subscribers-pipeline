"""
API keys configuration for the NBO Pipeline.

This module manages API keys and authentication information for
various external services used by the pipeline.
"""
import os
import sys
import logging
from . import settings

logger = logging.getLogger(__name__)

# ConvertKit API key
CONVERTKIT_API_KEY = settings.API_KEY

# OpenAI API key for LinkedIn profile analysis
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

def validate_api_keys():
    """
    Validate that required API keys are available.
    
    Raises a warning for missing optional keys and an error for
    missing required keys.
    """
    missing_required = []
    missing_optional = []
    
    # Check required keys
    if not CONVERTKIT_API_KEY:
        missing_required.append('KIT_V4_API_KEY')
    
    # Check optional keys
    if not OPENAI_API_KEY:
        missing_optional.append('OPENAI_API_KEY')
    
    # Log warnings for missing optional keys
    if missing_optional:
        logger.warning(f"Missing optional API keys: {', '.join(missing_optional)}")
        logger.warning("Some features may not work correctly.")
    
    # Exit if required keys are missing
    if missing_required:
        logger.error(f"Missing required API keys: {', '.join(missing_required)}")
        logger.error("Please set these environment variables before running the pipeline.")
        sys.exit(1)

# Run validation when imported
validate_api_keys()