"""
Configuration package for the NBO Pipeline.

This package contains configuration settings, API keys, and HTTP headers
used by various components of the pipeline.
"""
from . import settings
from . import api_keys
from . import cookie_manager
from . import headers
from . import logging_config

# Re-export commonly used items
from .settings import (
    API_BASE_URL,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    RECORDS_PER_PAGE, MAX_RECORDS_PER_BATCH,
    COUNTRIES_JSON_PATH, COUNTRIES_METADATA_PATH
)

from .api_keys import CONVERTKIT_API_KEY, OPENAI_API_KEY

# Provide access to cookie manager singleton
cookie_manager = cookie_manager.cookie_manager

# Provide access to logger
logger = logging_config.logger