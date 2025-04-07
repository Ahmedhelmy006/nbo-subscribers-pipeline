"""
Core configuration settings for the NBO Pipeline.
"""
import os
import sys
import logging
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv


# Setup basic logging before config is fully loaded
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
STACKS_DIR = BASE_DIR / "stacks" / "output"
REPORTS_DIR = BASE_DIR / "reports"

# Create required directories
for directory in [DATA_DIR, OUTPUT_DIR, LOGS_DIR, STACKS_DIR, REPORTS_DIR]:
    directory.mkdir(exist_ok=True, parents=True)


def load_environment_variables():
    """Explicitly load environment variables from .env file"""
    try:
        # Attempt to load .env file from project root
        load_dotenv(dotenv_path=BASE_DIR / '.env')
        logger.info("Successfully loaded environment variables from .env")
    except Exception as e:
        logger.error(f"Error loading .env file: {e}")

load_environment_variables()


# API Configuration
API_KEY = os.getenv('KIT_V4_API_KEY')
API_BASE_URL = "https://api.kit.com/v4"

# Webhook Configuration (replacing direct Slack integration)
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

# Database Configuration
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')  # Default to localhost
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')  # Empty default for security
DB_POOL_MIN_SIZE = int(os.getenv('DB_POOL_MIN_SIZE', '5'))
DB_POOL_MAX_SIZE = int(os.getenv('DB_POOL_MAX_SIZE', '20'))

# Pipeline Configuration
MAX_CONCURRENT_SUBSCRIBER_REQUESTS = int(os.getenv('MAX_CONCURRENT_SUBSCRIBER', '20'))
MAX_CONCURRENT_LOCATION_REQUESTS = int(os.getenv('MAX_CONCURRENT_LOCATION', '100'))
MAX_CONCURRENT_REFERRER_REQUESTS = int(os.getenv('MAX_CONCURRENT_REFERRER', '100'))
MAX_CONCURRENT_LINKEDIN_REQUESTS = int(os.getenv('MAX_CONCURRENT_LINKEDIN', '10'))

# Batch Processing
SCHEDULER_INTERVAL = int(os.getenv('SCHEDULER_INTERVAL', '300'))  # 5 minutes in seconds
RECORDS_PER_PAGE = int(os.getenv('RECORDS_PER_PAGE', '1000'))
MAX_RECORDS_PER_BATCH = int(os.getenv('MAX_RECORDS_PER_BATCH', '5000'))
SUBSCRIBER_STATUS = os.getenv('SUBSCRIBER_STATUS', 'all')  # all, active, inactive

# File Paths
COUNTRIES_JSON_PATH = DATA_DIR / "Countries Metadata.json"
COUNTRIES_METADATA_PATH = DATA_DIR / "Countries Metadata.json"
PERSONAL_DOMAINS_FILE = DATA_DIR / "personal_domains.txt"
PERSONAL_PROVIDERS_FILE = DATA_DIR / "personal_domains_providers.txt"

# Cookie file path - more flexible
default_cookie_path = BASE_DIR / "data" / "cookies.json"
COOKIE_FILE = os.getenv('COOKIE_FILE', str(default_cookie_path))

# Stack Configuration
LINKEDIN_STACK_PREFIX = "linkedin_stack"
LINKEDIN_STACK_MAX_SIZE = int(os.getenv('LINKEDIN_STACK_SIZE', '1000'))
LINKEDIN_STACK_ROTATION_INTERVAL = timedelta(hours=int(os.getenv('STACK_ROTATION_HOURS', '24')))

# Cache Configuration
CACHE_MAX_SIZE = int(os.getenv('CACHE_MAX_SIZE', '1000000'))
CACHE_TTL = int(os.getenv('CACHE_TTL', '604800'))  # 7 days in seconds

# LinkedIn Lookup Configuration
GOOGLE_SEARCH_MAX_RESULTS = int(os.getenv('GOOGLE_SEARCH_MAX_RESULTS', '5'))
GOOGLE_SEARCH_HEADLESS = os.getenv('GOOGLE_SEARCH_HEADLESS', 'True').lower() == 'true'

# OpenAI Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = LOGS_DIR / "pipeline.log"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Browser Settings for LinkedIn Lookup
BROWSER_ARGS = [
    # Existing arguments
    '--disable-blink-features=AutomationControlled',
    '--disable-features=IsolateOrigins,site-per-process',
    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    '--no-sandbox',
    '--disable-web-security',
    '--disable-features=IsolateOrigins',
    '--disable-site-isolation-trials',
    
    # Additional arguments to help avoid detection
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--start-maximized',
    '--lang=en-US,en;q=0.9',
    '--hide-scrollbars',
    '--mute-audio'
]
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"

# Check for required environment variables
def check_required_env_vars():
    """Check if all required environment variables are set."""
    missing_vars = []
    
    # Critical environment variables
    if not API_KEY:
        missing_vars.append('KIT_V4_API_KEY')
    
    # Warning only for non-critical variables
    warning_vars = []
    if not WEBHOOK_URL:
        warning_vars.append('WEBHOOK_URL')
    
    if not OPENAI_API_KEY:
        warning_vars.append('OPENAI_API_KEY')
    
    # In testing mode, don't exit on missing variables
    is_testing = 'PYTEST_CURRENT_TEST' in os.environ or 'unittest' in sys.modules
    
    # Exit if critical variables are missing (but not in testing mode)
    if missing_vars and not is_testing:
        logger.error(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables before running the pipeline.")
        sys.exit(1)
    elif missing_vars:
        logger.warning(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.warning("Running in test mode, so continuing anyway.")
    
    # Just warn for non-critical variables
    if warning_vars:
        logger.warning(f"WARNING: Missing recommended environment variables: {', '.join(warning_vars)}")
        logger.warning("Some features may not work correctly.")

# Run the check when this module is imported, unless in a test environment
if 'PYTEST_CURRENT_TEST' not in os.environ and 'unittest' not in sys.modules:
    check_required_env_vars()