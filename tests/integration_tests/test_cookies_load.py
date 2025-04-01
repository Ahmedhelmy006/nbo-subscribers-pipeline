#!/usr/bin/env python
"""
Cookie Debugging Script for NBO Pipeline.

This script provides detailed debugging information about cookie loading and usage.
"""
import sys
import os
import json
import asyncio
import requests
from pathlib import Path
from datetime import datetime
from rich.console import Console

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import from project
from config import settings
from config.cookie_manager import cookie_manager
from config.headers import get_referrer_info_headers, get_convertkit_cookie_headers

console = Console()

def debug_cookie_file():
    """Debug the cookie file location and content."""
    console.print("[bold yellow]Debugging Cookie File[/bold yellow]")
    
    # Check cookie file location
    cookie_file = settings.COOKIE_FILE
    console.print(f"Cookie file path: {cookie_file}")
    
    # Check if file exists
    cookie_path = Path(cookie_file)
    if not cookie_path.exists():
        console.print(f"[bold red]ERROR: Cookie file does not exist at {cookie_file}[/bold red]")
        return False
    
    # Check file permissions
    try:
        with open(cookie_path, 'r') as f:
            console.print(f"[green]Successfully opened cookie file for reading[/green]")
    except Exception as e:
        console.print(f"[bold red]ERROR: Could not open cookie file: {e}[/bold red]")
        return False
    
    # Try to parse the JSON
    try:
        with open(cookie_path, 'r') as f:
            cookie_data = json.load(f)
        console.print(f"[green]Successfully loaded JSON data from cookie file[/green]")
        
        # Check for expected structure
        if "convertkit" not in cookie_data:
            console.print(f"[bold red]ERROR: Missing 'convertkit' key in cookie file[/bold red]")
            console.print(f"Cookie file content: {json.dumps(cookie_data, indent=2)}")
            return False
        
        convertkit_data = cookie_data["convertkit"]
        if "value" not in convertkit_data:
            console.print(f"[bold red]ERROR: Missing 'value' key in convertkit cookie data[/bold red]")
            return False
        
        cookie_value = convertkit_data["value"]
        if not cookie_value:
            console.print(f"[bold red]ERROR: Cookie value is empty[/bold red]")
            return False
        
        # Check cookie value for key cookies
        important_cookies = ["_mailapp_session", "remember_user_token", "XSRF-TOKEN"]
        found_cookies = []
        for cookie in important_cookies:
            if cookie in cookie_value:
                found_cookies.append(cookie)
                
        console.print(f"Found important cookies: {', '.join(found_cookies)}")
        missing_cookies = set(important_cookies) - set(found_cookies)
        if missing_cookies:
            console.print(f"[bold yellow]WARNING: Missing important cookies: {', '.join(missing_cookies)}[/bold yellow]")
        
        # Check cookie length
        console.print(f"Cookie value length: {len(cookie_value)} characters")
        if len(cookie_value) < 100:
            console.print(f"[bold yellow]WARNING: Cookie value seems too short[/bold yellow]")
        
        # Check expiration (updated_at)
        if "updated_at" in convertkit_data:
            updated_at = convertkit_data["updated_at"]
            console.print(f"Cookies updated at: {updated_at}")
            
            try:
                updated_timestamp = datetime.fromisoformat(updated_at)
                age_days = (datetime.now() - updated_timestamp).days
                console.print(f"Cookie age: {age_days} days")
                if age_days > 7:
                    console.print(f"[bold yellow]WARNING: Cookies might be expired (older than 7 days)[/bold yellow]")
            except Exception as e:
                console.print(f"[yellow]Could not parse updated_at timestamp: {e}[/yellow]")
    
    except json.JSONDecodeError as e:
        console.print(f"[bold red]ERROR: Invalid JSON in cookie file: {e}[/bold red]")
        return False
    except Exception as e:
        console.print(f"[bold red]ERROR: Failed to process cookie file: {e}[/bold red]")
        return False
    
    return True

def debug_cookie_manager():
    """Debug the cookie manager functionality."""
    console.print("\n[bold yellow]Debugging Cookie Manager[/bold yellow]")
    
    # Check if manager is initialized
    if not hasattr(cookie_manager, 'cookies'):
        console.print(f"[bold red]ERROR: Cookie manager is not properly initialized[/bold red]")
        return False
    
    # Check if convertkit cookies are loaded
    if "convertkit" not in cookie_manager.cookies:
        console.print(f"[bold red]ERROR: No 'convertkit' cookies found in cookie manager[/bold red]")
        return False
    
    # Get the cookie value
    cookie_value = cookie_manager.get_cookie("convertkit")
    console.print(f"Cookie value from manager: {'**present**' if cookie_value else '**not present**'}")
    
    # Check if cookie is expired
    is_expired = cookie_manager.is_cookie_expired("convertkit")
    console.print(f"Is cookie expired: {is_expired}")
    
    # Get cookie info
    cookie_info = cookie_manager.get_cookie_info("convertkit")
    if cookie_info:
        console.print(f"Cookie info: value is {'present' if cookie_info.get('value') else 'missing'}, " +
                     f"updated_at is {'present' if cookie_info.get('updated_at') else 'missing'}")
    else:
        console.print(f"[bold red]ERROR: No cookie info found[/bold red]")
    
    return True

def debug_headers():
    """Debug the headers used for requests."""
    console.print("\n[bold yellow]Debugging API Headers[/bold yellow]")
    
    # Get referrer info headers
    referrer_headers = get_referrer_info_headers()
    console.print(f"Referrer info headers: {', '.join(referrer_headers.keys())}")
    
    # Check for key headers
    required_headers = ["accept", "content-type", "cookie", "user-agent"]
    missing_headers = [h for h in required_headers if h not in referrer_headers]
    if missing_headers:
        console.print(f"[bold red]ERROR: Missing required headers: {', '.join(missing_headers)}[/bold red]")
    
    # Check cookie header
    if "cookie" in referrer_headers:
        cookie_header = referrer_headers["cookie"]
        console.print(f"Cookie header: {'**present**' if cookie_header else '**not present**'}")
    else:
        console.print(f"[bold red]ERROR: No cookie header found[/bold red]")
    
    # Check for CSRF token header
    if "x-csrf-token" in referrer_headers:
        csrf_token = referrer_headers["x-csrf-token"]
        console.print(f"CSRF token header: {'**present**' if csrf_token else '**not present**'}")
    else:
        console.print(f"[yellow]NOTE: No CSRF token in headers (might be handled by cookies)[/yellow]")
    
    return True

def make_test_request():
    """Make a test request to check if cookies work."""
    console.print("\n[bold yellow]Making Test Request[/bold yellow]")
    
    try:
        # Get headers from your system
        headers = get_referrer_info_headers()
        
        # Make a test request to get a subscriber's referrer info
        subscriber_id = "3269225656"  # Use a real subscriber ID that failed earlier
        url = f"https://app.kit.com/subscribers/{subscriber_id}/referrer_info"
        
        response = requests.get(url, headers=headers, timeout=10)
        
        # Print response details
        console.print(f"Response status code: {response.status_code}")
        console.print(f"Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            console.print(f"[bold green]SUCCESS: Referrer info request worked![/bold green]")
            console.print(f"Response content: {response.text[:100]}...")
            return True
        elif response.status_code == 401:
            console.print(f"[bold red]ERROR: Unauthorized (401) - Authentication failed[/bold red]")
            if response.text:
                console.print(f"Error message: {response.text}")
            return False
        else:
            console.print(f"[bold yellow]WARNING: Unexpected status code: {response.status_code}[/bold yellow]")
            if response.text:
                console.print(f"Response content: {response.text[:100]}...")
            return False
            
    except Exception as e:
        console.print(f"[bold red]ERROR: Failed to make test request: {e}[/bold red]")
        return False

def suggest_fixes():
    """Suggest possible fixes based on diagnosis."""
    console.print("\n[bold yellow]Suggested Fixes[/bold yellow]")
    
    console.print("1. Refresh your cookies by logging in to ConvertKit again")
    console.print("   - Go to https://app.kit.com and log in")
    console.print("   - Open browser DevTools (F12)")
    console.print("   - Go to Application tab → Cookies → app.kit.com")
    console.print("   - Export all cookies from app.kit.com domain")
    
    console.print("\n2. Update your cookie file with the correct format:")
    console.print("""
    {
      "convertkit": {
        "value": "your_cookie_string_here",
        "updated_at": "2025-03-27T16:00:00.000Z"
      }
    }
    """)
    
    console.print("\n3. Check if the XSRF token is needed in headers:")
    console.print("   - Look for 'XSRF-TOKEN' in your cookies")
    console.print("   - Add the value as 'x-csrf-token' in get_referrer_info_headers()")
    
    console.print("\n4. Check for other authentication requirements:")
    console.print("   - ConvertKit might have updated their authentication")
    console.print("   - Inspect network requests in browser to see what headers they use")
    
    console.print("\n5. Verify cookie file path:")
    console.print(f"   - Current path: {settings.COOKIE_FILE}")
    console.print("   - Make sure this path exists and is accessible")

async def main():
    """Main function to run all debugging checks."""
    console.print("[bold]ConvertKit API Authentication Debugger[/bold]\n")
    
    # Debug cookie file
    cookie_file_ok = debug_cookie_file()
    
    # Debug cookie manager
    cookie_manager_ok = debug_cookie_manager()
    
    # Debug headers
    headers_ok = debug_headers()
    
    # Make test request
    request_ok = make_test_request()
    
    # Summary
    console.print("\n[bold]Debugging Summary[/bold]")
    console.print(f"Cookie file: {'✅ OK' if cookie_file_ok else '❌ ISSUE'}")
    console.print(f"Cookie manager: {'✅ OK' if cookie_manager_ok else '❌ ISSUE'}")
    console.print(f"API headers: {'✅ OK' if headers_ok else '❌ ISSUE'}")
    console.print(f"Test request: {'✅ OK' if request_ok else '❌ ISSUE'}")
    
    # Suggest fixes if needed
    if not (cookie_file_ok and cookie_manager_ok and headers_ok and request_ok):
        suggest_fixes()
    else:
        console.print("\n[bold green]All checks passed! Authentication should be working correctly.[/bold green]")

if __name__ == "__main__":
    asyncio.run(main())