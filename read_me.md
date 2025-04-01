# NBO Pipeline Project Structure

```
DATABASE_PIPELINE/
│
├── __pycache__/
├── .pytest_cache/
├── cache/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── cache_manager.py
│   └── memory_cache.py
│
├── classification_results/
│
├── config/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── api_keys.py
│   ├── cookie_manager.py
│   ├── headers.py
│   ├── logging_config.py
│   └── settings.py
│
├── data/
│   ├── cookies.json
│   ├── Countries Metadata.json
│   ├── countries+states+cities.json
│   ├── personal_domains.txt
│   └── personal_domains_providers.txt
│
├── db/
│   ├── __pycache__/
│   ├── connection.py
│   ├── models.py
│   └── state.py
│
├── email_classifier/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── classifier.py
│   └── validator.py
│
├── logs/
│
├── lookup/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── google_search.py
│   ├── lookup_processor.py
│   └── name_extractor.py
│
├── monitoring/
│   ├── __init__.py
│   ├── metrics.py
│   ├── slack_reporter.py
│   ├── slack_scheduler.py
│   └── visualization.py
│
├── output/
│
├── pipelines/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── base.py
│   ├── linkedin_pipeline.py
│   ├── location_pipeline.py
│   ├── referrer_pipeline.py
│   ├── subscriber_pipeline.py
│   └── worker_pools.py
│
├── reports/
│
├── stacks/
│   ├── output/
│   ├── __init__.py
│   ├── base_stack.py
│   ├── linkedin_stack.py
│   └── stack_manager.py
│
├── tests/
│   ├── integration_tests/
│   │   ├── db_connectivity.py
│   │   ├── integrate_classifier.py
│   │   └── live_test.py
│   │   └── test_convertkit_updater.py
│   │   └── test_cookies_load.py

│   │
│   └── unit_tests/
│       ├── __pycache__/
│       ├── test_name_ext...
│       ├── test_email_classifie...
│       └── test_helper.py
│       └── test_google_search.py

│
├── utils/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── country_utils.py
│   ├── location_fetcher.py
│   ├── helpers.py
│   ├── worker_pool.py
│
├── convertkit/
│   ├── __init__.py
│   ├── cli.py
│   ├── updater.py
│
│
├── main.py
├── run_batch.py
└── scheduler.py
├── .env
```

## Main Components

- **cache**: In-memory caching system for the pipeline
- **config**: Configuration settings, API keys, and HTTP headers
- **data**: Static data files including domain lists and country metadata
- **db**: Database connectivity and models
- **email_classifier**: Classifies emails as work or personal
- **lookup**: LinkedIn profile lookup functionality
- **monitoring**: Metrics and notification systems
- **pipelines**: Core processing pipelines for different data types
- **stacks**: Stack system for storing LinkedIn URLs
- **tests**: Unit and integration tests

## Key Files

- **config/settings.py**: Core configuration settings
- **db/connection.py**: Database connection management
- **db/models.py**: Database table models
- **db/state.py**: Pipeline state management
- **lookup/lookup_processor.py**: Orchestrates LinkedIn profile lookups
- **pipelines/linkedin_pipeline.py**: Pipeline for LinkedIn profile discovery
- **stacks/linkedin_stack.py**: Storage for LinkedIn URLs
- **tests/integration_tests/live_test.py**: Live testing script