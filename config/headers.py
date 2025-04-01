"""
HTTP headers configuration for API requests.

This module provides functions to get appropriate headers for different
API endpoints, using cookies from the cookie manager when needed.
"""
from .cookie_manager import cookie_manager
from . import settings

def get_convertkit_api_headers(api_key=None):
    """
    Get headers for ConvertKit API requests.
    
    Args:
        api_key (str, optional): API key to use. If not provided,
            the key from settings is used.
            
    Returns:
        dict: Headers for ConvertKit API requests
    """
    return {
        'Accept': 'application/json',
        'X-Kit-Api-Key': api_key or settings.API_KEY
    }

def get_convertkit_cookie_headers():
    """
    Get headers with cookies for ConvertKit web requests.
    
    Returns:
        dict: Headers for ConvertKit web requests with cookies
    """
    return {
        "authority": "app.kit.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "cookie": cookie_manager.get_cookie("convertkit"),
        "priority": "u=0, i",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    }

def get_referrer_info_headers():
    """
    Get headers for referrer info requests.
    
    Returns:
        dict: Headers for referrer info requests
    """
    return {
        "accept": "application/json",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "cookie": cookie_manager.get_cookie("convertkit"),
        "priority": "u=1, i",
        "referer": "https://app.kit.com/subscribers",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-csrf-token": "0HpVWpiBX8iDR7XEcBms1IW46VG44YOg1bNJLIkrxPTlTRo2SJrhQX-tmTchAM3SD5o54pbqtXA7JmbP5EjZ-g",
        "x-react-rb": "t"
    }

def get_google_search_headers():
    """
    Get headers for Google search requests.
    
    Returns:
        dict: Headers for Google search requests
    """
    return {
        "User-Agent": settings.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cookie": cookie_manager.get_cookie("google"),
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0"
    }

def get_openai_headers(api_key=None):
    """
    Get headers for OpenAI API requests.
    
    Args:
        api_key (str, optional): API key to use. If not provided,
            the key from environment is used.
            
    Returns:
        dict: Headers for OpenAI API requests
    """
    import os
    api_key = api_key or os.getenv("OPENAI_API_KEY")
    
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }