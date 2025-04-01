"""
Comprehensive tests for email classification functionality in the NBO Pipeline.

This module tests the email classifier in email/classifier.py.
"""
import os
import tempfile
import unittest
from unittest.mock import patch, mock_open, MagicMock
import re
import sys
import pytest  # Still needed for skip functionality


# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


from email_classifier.classifier import (
    EmailClassifier,
    classify_email,
    is_work_email,
    get_classifier,
    reload_classification_data
)

class TestEmailClassifierInitialization(unittest.TestCase):
    """Tests for initializing the EmailClassifier."""
    
    def test_init_with_default_files(self):
        """Test initializing the classifier with default files."""
        classifier = EmailClassifier()
        # Should have loaded some personal domains
        self.assertGreater(len(classifier.personal_domains), 0)
        # Should have loaded some personal providers
        self.assertGreater(len(classifier.personal_providers), 0)
        # Should have compiled a regex pattern
        self.assertIsNotNone(classifier.provider_pattern)
    
    def test_init_with_custom_files(self):
        """Test initializing the classifier with custom files."""
        # Create temporary files with test domains
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as domains_file:
            domains_file.write("custom-domain.com\n")
            domains_file.write("testdomain.org\n")
            domains_path = domains_file.name
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as providers_file:
            providers_file.write("custom\n")
            providers_file.write("test\n")
            providers_path = providers_file.name
        
        try:
            classifier = EmailClassifier(
                domains_file=domains_path,
                providers_file=providers_path
            )
            
            # Should have loaded our custom domains
            self.assertIn("custom-domain.com", classifier.personal_domains)
            self.assertIn("testdomain.org", classifier.personal_domains)
            
            # Should have loaded our custom providers
            self.assertIn("custom", classifier.personal_providers)
            self.assertIn("test", classifier.personal_providers)
        finally:
            # Clean up temp files
            os.unlink(domains_path)
            os.unlink(providers_path)
    
    def test_load_personal_domains_with_nonexistent_file(self):
        """Test loading personal domains from a nonexistent file."""
        classifier = EmailClassifier(domains_file="nonexistent_file.txt")
        # Should fall back to default domains
        self.assertGreater(len(classifier.personal_domains), 0)
        # Common domains should be included in the defaults
        self.assertIn("gmail.com", classifier.personal_domains)
        self.assertIn("hotmail.com", classifier.personal_domains)
    
    def test_load_personal_providers_with_nonexistent_file(self):
        """Test loading personal providers from a nonexistent file."""
        classifier = EmailClassifier(providers_file="nonexistent_file.txt")
        # Should fall back to default providers
        self.assertGreater(len(classifier.personal_providers), 0)
        # Common providers should be included in the defaults
        self.assertIn("gmail", classifier.personal_providers)
        self.assertIn("yahoo", classifier.personal_providers)
    
    def test_load_personal_domains_with_empty_file(self):
        """Test loading personal domains from an empty file."""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as empty_file:
            empty_file.write("")
            empty_path = empty_file.name
        
        try:
            classifier = EmailClassifier(domains_file=empty_path)
            # Should fall back to default domains
            self.assertGreater(len(classifier.personal_domains), 0)
        finally:
            os.unlink(empty_path)
    
    def test_load_personal_domains_with_comments(self):
        """Test loading personal domains from a file with comments."""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as domains_file:
            domains_file.write("# This is a comment\n")
            domains_file.write("gmail.com\n")
            domains_file.write("# Another comment\n")
            domains_file.write("example.com\n")
            domains_path = domains_file.name
        
        try:
            classifier = EmailClassifier(domains_file=domains_path)
            # Should have loaded domains but not comments
            self.assertIn("gmail.com", classifier.personal_domains)
            self.assertIn("example.com", classifier.personal_domains)
            self.assertNotIn("# This is a comment", classifier.personal_domains)
            self.assertNotIn("# Another comment", classifier.personal_domains)
        finally:
            os.unlink(domains_path)
    
    def test_compile_provider_pattern(self):
        """Test compiling the provider pattern regex."""
        classifier = EmailClassifier()
        
        # Test that the pattern matches correctly
        for provider in ['gmail', 'yahoo', 'hotmail']:
            # Should match the provider as a complete domain
            self.assertIsNotNone(classifier.provider_pattern.search(provider))
            # Should match the provider as a subdomain
            self.assertIsNotNone(classifier.provider_pattern.search(f"{provider}.com"))
            # Should match the provider as a domain prefix
            self.assertIsNotNone(classifier.provider_pattern.search(f"{provider}.co.uk"))
        
        # Test that the pattern doesn't match irrelevant domains
        self.assertIsNone(classifier.provider_pattern.search("example.com"))
        self.assertIsNone(classifier.provider_pattern.search("company.org"))
    
    def test_compile_provider_pattern_with_empty_list(self):
        """Test compiling the provider pattern with an empty list."""
        classifier = EmailClassifier()
        # Replace providers with an empty list
        classifier.personal_providers = []
        
        # Shouldn't crash, should compile an empty pattern
        pattern = classifier._compile_provider_pattern()
        self.assertIsNotNone(pattern)
        # Pattern shouldn't match anything
        self.assertIsNone(pattern.search("gmail.com"))


class TestEmailClassification(unittest.TestCase):
    """Tests for email classification functionality."""
    
    def test_is_work_email_with_known_personal_domains(self):
        """Test is_work_email with known personal domains."""
        classifier = EmailClassifier()
        
        # Test common personal email domains
        self.assertFalse(classifier.is_work_email("test@gmail.com"))
        self.assertFalse(classifier.is_work_email("test@yahoo.com"))
        self.assertFalse(classifier.is_work_email("test@hotmail.com"))
        self.assertFalse(classifier.is_work_email("test@outlook.com"))
    
    def test_is_work_email_with_work_domains(self):
        """Test is_work_email with work domains."""
        classifier = EmailClassifier()
        
        # Test common work email domains
        self.assertTrue(classifier.is_work_email("test@company.com"))
        self.assertTrue(classifier.is_work_email("test@enterprise.org"))
        self.assertTrue(classifier.is_work_email("test@business.net"))
        self.assertTrue(classifier.is_work_email("test@corp.co"))
    
    def test_is_work_email_with_educational_domains(self):
        """Test is_work_email with educational domains."""
        classifier = EmailClassifier()
        
        # Educational domains should not be considered work emails
        self.assertFalse(classifier.is_work_email("test@university.edu"))
        self.assertFalse(classifier.is_work_email("test@school.edu.uk"))
        self.assertFalse(classifier.is_work_email("test@college.edu"))
    
    def test_is_work_email_with_government_domains(self):
        """Test is_work_email with government domains."""
        classifier = EmailClassifier()
        
        # Government domains should be considered work emails
        self.assertTrue(classifier.is_work_email("test@agency.gov"))
        self.assertTrue(classifier.is_work_email("test@department.gov.uk"))
    
    def test_is_work_email_with_personal_provider_patterns(self):
        """Test is_work_email with domains containing personal provider patterns."""
        classifier = EmailClassifier()
        
        # Domains containing personal provider patterns
        self.assertFalse(classifier.is_work_email("test@gmail-notifications.com"))
        self.assertFalse(classifier.is_work_email("test@yahoo-mail.com"))
        self.assertFalse(classifier.is_work_email("test@mail.yahoo.co.uk"))
    
    def test_is_work_email_with_invalid_emails(self):
        """Test is_work_email with invalid emails."""
        classifier = EmailClassifier()
        
        # Invalid emails should return False
        self.assertFalse(classifier.is_work_email("not-an-email"))
        self.assertFalse(classifier.is_work_email(""))
        self.assertFalse(classifier.is_work_email(None))
    
    def test_classify_email(self):
        """Test classify_email method."""
        classifier = EmailClassifier()
        
        # Personal email
        domain_type, domain = classifier.classify_email("test@gmail.com")
        self.assertEqual(domain_type, "personal")
        self.assertEqual(domain, "gmail.com")
        
        # Work email
        domain_type, domain = classifier.classify_email("test@company.com")
        self.assertEqual(domain_type, "work")
        self.assertEqual(domain, "company.com")
        
        # Invalid email
        domain_type, domain = classifier.classify_email("not-an-email")
        self.assertEqual(domain_type, "unknown")
        self.assertEqual(domain, "")
    
    def test_classify_email_exceptions(self):
        """Test classify_email handles exceptions."""
        classifier = EmailClassifier()
        
        # Should handle exceptions gracefully
        with patch.object(classifier, 'is_work_email', side_effect=Exception("Test exception")):
            domain_type, domain = classifier.classify_email("test@example.com")
            self.assertEqual(domain_type, "unknown")
            self.assertEqual(domain, "")


class TestEmailClassifierModule(unittest.TestCase):
    """Tests for the module-level functions in the email classifier."""
    
    def test_get_classifier(self):
        """Test that get_classifier returns a singleton instance."""
        # Reset the singleton
        import email_classifier.classifier
        email_classifier.classifier._classifier = None
        
        classifier1 = get_classifier()
        classifier2 = get_classifier()
        
        # Should be the same instance
        self.assertIs(classifier1, classifier2)
        self.assertIsInstance(classifier1, EmailClassifier)
    
    def test_is_work_email_function(self):
        """Test the is_work_email function."""
        # Test with sample emails
        self.assertFalse(is_work_email("test@gmail.com"))
        self.assertTrue(is_work_email("test@company.com"))
        
        # Verify it uses the classifier's method
        with patch('email_classifier.classifier.get_classifier') as mock_get:
            mock_classifier = MagicMock()
            mock_classifier.is_work_email.return_value = False
            mock_get.return_value = mock_classifier
            
            # Should use the classifier's result
            result = is_work_email("any@email.com")
            self.assertFalse(result)
            mock_classifier.is_work_email.assert_called_once_with("any@email.com")
    
    def test_classify_email_function(self):
        """Test the classify_email function."""
        # Test with sample emails
        domain_type, domain = classify_email("test@gmail.com")
        self.assertEqual(domain_type, "personal")
        self.assertEqual(domain, "gmail.com")
        
        # Verify it uses the classifier's method
        with patch('email_classifier.classifier.get_classifier') as mock_get:
            mock_classifier = MagicMock()
            mock_classifier.classify_email.return_value = ("test-type", "test-domain")
            mock_get.return_value = mock_classifier
            
            # Should use the classifier's result
            result = classify_email("any@email.com")
            self.assertEqual(result, ("test-type", "test-domain"))
            mock_classifier.classify_email.assert_called_once_with("any@email.com")
    
    def test_reload_classification_data(self):
        """Test reloading classification data."""
        # Create mock classifier
        mock_classifier = MagicMock()
        
        # Mock get_classifier to return our mock
        with patch('email_classifier.classifier.get_classifier', return_value=mock_classifier):
            # Call reload_classification_data
            reload_classification_data()
            
            # Verify reload_domains was called on the classifier
            mock_classifier.reload_domains.assert_called_once()


class TestEmailClassifierWithActualFiles(unittest.TestCase):
    """Tests that use the actual data files in the project."""
    
    def test_with_actual_domains_file(self):
        """Test with the actual domains file from the project."""
        # This test requires the actual domains file to exist
        # Skip if it doesn't exist
        import config
        if not os.path.exists(config.settings.PERSONAL_DOMAINS_FILE):
            self.skipTest(f"Domains file {config.settings.PERSONAL_DOMAINS_FILE} not found")
        
        classifier = EmailClassifier(domains_file=config.settings.PERSONAL_DOMAINS_FILE)
        
        # Check that at least some common domains were loaded
        self.assertGreater(len(classifier.personal_domains), 10)
        
        # Check some common known personal domains
        self.assertIn("gmail.com", classifier.personal_domains)
        self.assertIn("yahoo.com", classifier.personal_domains)
        self.assertIn("hotmail.com", classifier.personal_domains)
    
    def test_with_actual_providers_file(self):
        """Test with the actual providers file from the project."""
        # This test requires the actual providers file to exist
        # Skip if it doesn't exist
        import config
        if not os.path.exists(config.settings.PERSONAL_PROVIDERS_FILE):
            self.skipTest(f"Providers file {config.settings.PERSONAL_PROVIDERS_FILE} not found")
        
        classifier = EmailClassifier(providers_file=config.settings.PERSONAL_PROVIDERS_FILE)
        
        # Check that at least some common providers were loaded
        self.assertGreater(len(classifier.personal_providers), 5)
        
        # Check some common known personal providers
        self.assertIn("gmail", classifier.personal_providers)
        self.assertIn("yahoo", classifier.personal_providers)
        self.assertIn("hotmail", classifier.personal_providers)


if __name__ == "__main__":
    """
    Run the tests directly without pytest.
    This simulates running pytest on this file.
    """
    import os
    import sys
    import unittest
    
    # Set required environment variables for testing
    os.environ['KIT_V4_API_KEY'] = 'test_api_key'
    
    # Add project root to path if needed
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    
    # Run the tests using unittest
    test_loader = unittest.TestLoader()
    
    # Load all test cases from this module
    current_module = sys.modules[__name__]
    test_suite = test_loader.loadTestsFromModule(current_module)
    
    # Run the tests
    print("=" * 80)
    print("Running Email Classifier Tests")
    print("=" * 80)
    
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Print a summary
    print("\n" + "=" * 80)
    print(f"Results: {result.testsRun} tests run")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    print("=" * 80)
    
    # Exit with appropriate status code (0 for success, 1 for failure)
    sys.exit(len(result.failures) > 0 or len(result.errors) > 0)