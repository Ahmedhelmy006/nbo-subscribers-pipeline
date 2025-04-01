"""
Tests for utility helper functions in the NBO Pipeline.

This module tests the helper utility functions in utils/helpers.py.
"""
import os
import json
import tempfile
import pytest
from pathlib import Path
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Import the helper functions to test
from utils.helpers import (
    ensure_directory,
    save_json,
    load_json,
    format_time,
    generate_filename,
    log_progress,
    mask_email,
    safe_extract
)

class TestDirectoryFunctions:
    """Tests for directory-related utility functions."""
    
    def test_ensure_directory_creates_new_directory(self):
        """Test that ensure_directory creates a directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = Path(temp_dir) / "test_dir"
            assert not test_dir.exists()
            
            ensure_directory(test_dir)
            assert test_dir.exists()
            assert test_dir.is_dir()
    
    def test_ensure_directory_with_existing_directory(self):
        """Test that ensure_directory handles existing directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = Path(temp_dir)
            assert test_dir.exists()
            
            # This should not raise an exception
            ensure_directory(test_dir)
            assert test_dir.exists()
    
    def test_ensure_directory_with_nested_paths(self):
        """Test that ensure_directory creates nested directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_dir = Path(temp_dir) / "level1" / "level2" / "level3"
            assert not nested_dir.exists()
            
            ensure_directory(nested_dir)
            assert nested_dir.exists()
            assert nested_dir.is_dir()

class TestJsonFunctions:
    """Tests for JSON-related utility functions."""
    
    def test_save_json_creates_file(self):
        """Test that save_json creates a file with the correct content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "test.json"
            test_data = {"key": "value", "nested": {"inner": "data"}}
            
            result = save_json(test_data, test_file)
            assert result is True
            assert test_file.exists()
            
            # Verify content
            with open(test_file, 'r') as f:
                saved_data = json.load(f)
            assert saved_data == test_data
    
    def test_save_json_creates_directories(self):
        """Test that save_json creates directories if they don't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_file = Path(temp_dir) / "nested" / "path" / "test.json"
            test_data = {"key": "value"}
            
            result = save_json(test_data, nested_file)
            assert result is True
            assert nested_file.exists()
    
    def test_load_json_reads_file(self):
        """Test that load_json correctly reads JSON data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "test.json"
            test_data = {"key": "value", "list": [1, 2, 3]}
            
            # Create the file
            with open(test_file, 'w') as f:
                json.dump(test_data, f)
            
            loaded_data = load_json(test_file)
            assert loaded_data == test_data
    
    def test_load_json_with_nonexistent_file(self):
        """Test that load_json returns None for nonexistent files."""
        loaded_data = load_json("nonexistent_file.json")
        assert loaded_data is None

class TestFormattingFunctions:
    """Tests for string formatting utility functions."""
    
    def test_format_time_seconds(self):
        """Test formatting time in seconds."""
        assert format_time(30) == "30.0 seconds"
        assert format_time(0.5) == "0.5 seconds"
        assert format_time(59.9) == "59.9 seconds"
    
    def test_format_time_minutes(self):
        """Test formatting time in minutes."""
        assert format_time(60) == "1.0 minutes"
        assert format_time(90) == "1.5 minutes"
        assert format_time(3599) == "60.0 minutes"
    
    def test_format_time_hours(self):
        """Test formatting time in hours."""
        assert format_time(3600) == "1.0 hours"
        assert format_time(7200) == "2.0 hours"
        assert format_time(5400) == "1.5 hours"
    
    def test_generate_filename_basic(self):
        """Test basic filename generation."""
        filename = generate_filename("test", timestamp=False)
        assert filename == "test.json"
    
    def test_generate_filename_with_timestamp(self):
        """Test filename generation with timestamp."""
        filename = generate_filename("test", timestamp=True)
        # Check that it matches the expected pattern
        assert filename.startswith("test_")
        assert filename.endswith(".json")
        # There should be a timestamp in the middle
        parts = filename.split("_")
        assert len(parts) >= 3  # At least "test", date, time
    
    def test_generate_filename_with_chunk(self):
        """Test filename generation with chunk number."""
        filename = generate_filename("test", chunk_num=5, timestamp=False)
        assert filename == "test_chunk_5.json"
    
    def test_generate_filename_with_timestamp_and_chunk(self):
        """Test filename generation with both timestamp and chunk."""
        filename = generate_filename("test", chunk_num=5, timestamp=True)
        # Should have prefix, timestamp, and chunk
        assert filename.startswith("test_")
        assert "chunk_5" in filename
        assert filename.endswith(".json")

class TestEmailFunctions:
    """Tests for email-related utility functions."""
    
    def test_mask_email_with_valid_email(self):
        """Test masking valid email addresses."""
        # Standard email
        assert mask_email("john.doe@example.com") == "j****e@example.com"
        # Short username
        assert mask_email("joe@example.com") == "j*e@example.com"
        # Very short username
        assert mask_email("jo@example.com") == "**@example.com"
        # Single character username
        assert mask_email("j@example.com") == "*@example.com"
    
    def test_mask_email_with_invalid_input(self):
        """Test masking invalid email addresses."""
        # No @ symbol
        assert mask_email("invalid-email") == "invalid-email"
        # Empty string
        assert mask_email("") == ""
        # None
        assert mask_email(None) is None

class TestDataExtractionFunctions:
    """Tests for data extraction utility functions."""
    
    def test_safe_extract_with_valid_path(self):
        """Test extracting values from nested dictionaries with valid paths."""
        data = {
            "level1": {
                "level2": {
                    "level3": "value"
                }
            }
        }
        
        assert safe_extract(data, "level1", "level2", "level3") == "value"
    
    def test_safe_extract_with_invalid_path(self):
        """Test extracting values from nested dictionaries with invalid paths."""
        data = {
            "level1": {
                "level2": "value"
            }
        }
        
        # Missing key
        assert safe_extract(data, "level1", "missing", "level3") is None
        # Default value
        assert safe_extract(data, "level1", "missing", default="default") == "default"
    
    def test_safe_extract_with_none_data(self):
        """Test extracting values when data is None."""
        assert safe_extract(None, "key") is None
        assert safe_extract(None, "key", default="default") == "default"
    
    def test_safe_extract_with_empty_keys(self):
        """Test extracting values with no keys provided."""
        data = {"key": "value"}
        assert safe_extract(data) == data

class TestProgressLogging:
    """Tests for progress logging utility functions."""
    
    def test_log_progress(self):
        """Test progress logging function."""
        # This test now simply verifies the function doesn't raise exceptions
        start_time = datetime.now().timestamp() - 60  # 60 seconds ago
        
        # This shouldn't raise an exception
        log_progress(50, 100, start_time)
        
        # No assertion needed - if we get here, the test passes
    
    def test_log_progress_zero_processed(self):
        """Test progress logging when nothing has been processed."""
        start_time = datetime.now().timestamp()
        
        # This shouldn't raise an exception
        log_progress(0, 100, start_time)
        
        # No assertion needed - if we get here, the test passes