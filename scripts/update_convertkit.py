#!/usr/bin/env python
"""
Script to update ConvertKit with data from our database.

This script is a simple wrapper around the ConvertKit CLI module.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from convertkit.cli import main

if __name__ == "__main__":
    main()