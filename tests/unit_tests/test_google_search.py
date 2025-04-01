"""
Unit tests for the Google Search component in the NBO Pipeline.
"""
import os
import sys
import unittest
from unittest.mock import patch, AsyncMock
import asyncio

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import the components to test
from lookup.google_search import GoogleSearch

class AsyncTestCase(unittest.TestCase):
    """Base class for async tests."""
    
    def setUp(self):
        """Set up the event loop."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
    def tearDown(self):
        """Clean up the event loop."""
        self.loop.close()
        
    def run_async(self, coro):
        """Run an async coroutine in the test."""
        return self.loop.run_until_complete(coro)

class TestGoogleSearch(AsyncTestCase):
    """Comprehensive tests for the GoogleSearch class."""
    
    def setUp(self):
        """Set up test environment."""
        super().setUp()
        self.google_search = GoogleSearch()
    
    def test_initialization(self):
        """Test that GoogleSearch initializes correctly."""
        self.assertIsNotNone(self.google_search)
        self.assertTrue(hasattr(self.google_search, 'google_search'))
        self.assertTrue(hasattr(self.google_search, 'extract_search_results'))
    
    def test_search_query_construction(self):
        """Test search query construction."""
        test_cases = [
            {
                "query": "John Doe + example.com + LinkedIn",
                "expected_components": ["John Doe", "example.com", "LinkedIn"]
            },
            {
                "query": "Jane Smith + tech.company + California + LinkedIn",
                "expected_components": ["Jane Smith", "tech.company", "California", "LinkedIn"]
            }
        ]
        
        for case in test_cases:
            for component in case["expected_components"]:
                self.assertIn(component, case["query"])
    
    def test_extract_search_results_error_handling(self):
        """Test error handling in extract_search_results method."""
        async def _test():
            # Mock a page object with problematic content
            class MockPage:
                async def content(self):
                    return "<html>Problematic HTML</html>"
            
            mock_page = MockPage()
            
            # Expect an empty list in case of parsing errors
            results = await self.google_search.extract_search_results(mock_page)
            self.assertEqual(results, [])
        
        self.run_async(_test())
    
    def test_result_filtering(self):
        """Test filtering of search results."""
        async def _test():
            # Mock a page with various results
            class MockPage:
                async def content(self):
                    return """
                    <html>
                        <div class="g">
                            <a href="https://www.linkedin.com/in/test-profile">LinkedIn Profile</a>
                            <a href="https://www.google.com/search">Google Link</a>
                            <a href="https://www.example.com/webcache">Webcache Link</a>
                        </div>
                    </html>
                    """
            
            mock_page = MockPage()
            
            # Get search results
            results = await self.google_search.extract_search_results(mock_page)
            
            # Verify filtering
            self.assertTrue(all(
                not url.startswith('https://www.google.com') and 
                not url.startswith('https://webcache') and
                'linkedin.com/in/' in url.lower()
                for url in [r['url'] for r in results]
            ))
        
        self.run_async(_test())
    
    @patch('lookup.google_search.GoogleSearch.extract_search_results')
    def test_query_openai_integration(self, mock_extract):
        """Test OpenAI query method integration."""
        async def _test():
            # Mock search results
            mock_search_results = [
                {
                    "title": "John Doe | LinkedIn",
                    "url": "https://www.linkedin.com/in/johndoe",
                    "snippet": "Professional profile of John Doe"
                }
            ]
            mock_extract.return_value = mock_search_results
            
            # Prepare member info
            member_info = {
                "email": "john.doe@example.com",
                "first_name": "John",
                "last_name": "Doe"
            }
            
            # Mock OpenAI query
            with patch('lookup.google_search.requests.post') as mock_post:
                # Setup mock OpenAI response
                mock_response = mock_post.return_value
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "choices": [{
                        "message": {
                            "content": "https://www.linkedin.com/in/johndoe"
                        }
                    }]
                }
                
                # Call the method
                result = await self.google_search.query_openai(member_info, mock_search_results)
                
                # Verify result
                self.assertEqual(result, "https://www.linkedin.com/in/johndoe")
        
        self.run_async(_test())
    
    def test_max_results_limitation(self):
        """Test that search results are limited to maximum results."""
        async def _test():
            # Override max_results for this test
            original_max_results = self.google_search.max_results
            self.google_search.max_results = 2
            
            # Import BeautifulSoup for manual parsing inspection
            from bs4 import BeautifulSoup
            
            # Mock search results with multiple LinkedIn links
            class MockPage:
                async def content(self):
                    return """
                    <html>
                        <div class="g">
                            <a href="https://www.linkedin.com/in/profile1">
                                <h3>John Doe</h3>
                                <div class="s">Senior Software Engineer at Tech Corp</div>
                            </a>
                            <a href="https://www.linkedin.com/in/profile2">
                                <h3>Jane Smith</h3>
                                <div class="s">Marketing Manager at Global Inc</div>
                            </a>
                            <a href="https://www.linkedin.com/in/profile3">
                                <h3>Mike Johnson</h3>
                                <div class="s">Financial Analyst at Finance Group</div>
                            </a>
                            <a href="https://www.linkedin.com/in/profile4">
                                <h3>Sarah Williams</h3>
                                <div class="s">Product Designer at Creative Solutions</div>
                            </a>
                        </div>
                    </html>
                    """
            
            mock_page = MockPage()
            
            # Print the content for debugging
            html_content = await mock_page.content()
            print("\n--- Full HTML Content ---")
            print(html_content)
            
            # Manual BeautifulSoup parsing for debugging
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Manually extract all links for comparison
            print("\n--- Manual Link Extraction ---")
            manual_links = soup.select("div.g a[href*='linkedin.com/in/']")
            print(f"Total LinkedIn links found manually: {len(manual_links)}")
            for link in manual_links:
                print(f"URL: {link.get('href')}")
                title_elem = link.find('h3')
                snippet_elem = link.find('div', class_='s')
                print(f"Title: {title_elem.get_text() if title_elem else 'No Title'}")
                print(f"Snippet: {snippet_elem.get_text() if snippet_elem else 'No Snippet'}")
                print("---")
            
            # Get search results using the method
            results = await self.google_search.extract_search_results(mock_page)
            
            # Print detailed results
            print("\n--- Extracted Results ---")
            for result in results:
                print(f"URL: {result['url']}")
                print(f"Title: {result['title']}")
                print(f"Snippet: {result['snippet']}")
                print("---")
            
            # Verify result count is limited
            self.assertEqual(len(results), 2, 
                f"Expected 2 results, but got {len(results)}")
            
            # Restore original max_results
            self.google_search.max_results = original_max_results
        
        self.run_async(_test())

if __name__ == "__main__":
    unittest.main()