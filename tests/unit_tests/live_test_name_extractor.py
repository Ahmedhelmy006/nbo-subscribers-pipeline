"""
Test script to evaluate the name extractor with real OpenAI API calls.
"""
import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Import the necessary components
from lookup.name_extractor import NameExtractor
from config.api_keys import OPENAI_API_KEY
from utils.helpers import mask_email

# Set up basic logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_real_examples():
    """Test the name extractor with real API calls on selected examples."""
    # Make sure we have an API key
    if not OPENAI_API_KEY:
        logger.error("No OpenAI API key found in environment variables. Set OPENAI_API_KEY first.")
        return False
    
    # Initialize the extractor
    extractor = NameExtractor()
    
    # Test cases from our comprehensive list
    test_cases = [
        # Email, Given Name, Expected result (for reference only)
        ("tiffany.jessup@whitecase.com", "Tiffany", "Tiffany Jessup"),
        ("linda.hine@stakkdco.com", "Linda", "Linda Hine"),
        ("mlindholm@hlcsweden.com", "Marko", "Marko Lindholm"),
        ("ngeorges@pinnacleclimate.com", "Nick", "Nick Georges"),
        ("steve.desalvo@kzf.com", "Steve", "Steve Desalvo"),
        ("judith@imaginenew.com", "Judith", "Judith Hively"),
        ("igspam@wevalueprivacy.com", "MindYourBusiness", None),
        ("electrical@amilifesciences.com", "P", "P Patel"),
        ("john_doe@example.com", None, "John Doe"),
        ("jane-doe@example.com", None, "Jane Doe"),
        ("jsmith@example.com", None, "John Smith"),
        ("qzmlnhgzwwuzgdhgv@poplk.com", "ko", None),
    ]
    
    successes = 0
    failures = 0
    
    # Process each test case
    for email, given_name, expected in test_cases:
        try:
            logger.info(f"Testing: {mask_email(email)}, Given name: {given_name}")
            
            # Make the actual API call
            extracted_name, method = extractor.extract_name_from_email(email, given_name)
            
            # Log the result
            logger.info(f"Result: {extracted_name} (Method: {method})")
            logger.info(f"Expected (for reference): {expected}")
            logger.info("-" * 50)
            
            # Count success/failure for summary
            if extracted_name is not None:
                successes += 1
            else:
                failures += 1
                
        except Exception as e:
            logger.error(f"Error processing {mask_email(email)}: {e}")
            failures += 1
    
    # Log summary
    logger.info(f"Test complete. Processed {len(test_cases)} examples.")
    logger.info(f"Successes: {successes}, Failures: {failures}")
    
    return successes, failures

if __name__ == "__main__":
    logger.info("Starting real API test...")
    test_real_examples()