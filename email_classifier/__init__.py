"""
Email classification package for the NBO Pipeline.

This package provides functionality for classifying emails as work or personal,
which is used by the LinkedIn pipeline.
"""
from .classifier import EmailClassifier, classify_email, is_work_email

__all__ = ["EmailClassifier", "classify_email", "is_work_email"]