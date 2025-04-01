#!/usr/bin/env python
"""
Test script for the ConvertKit updater module.

This script tests the functionality of the ConvertKit updater by updating
a sample subscriber and displaying the results.
"""
import asyncio
import sys
import os
import json
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import settings
# Import the database functions directly from their modules
from db.connection import db_manager

async def initialize_database():
    """Initialize the database connection."""
    try:
        await db_manager.initialize()
        return True
    except Exception as e:
        console.print(f"[bold red]Database initialization error: {e}[/bold red]")
        return False

async def cleanup_database():
    """Clean up database resources."""
    try:
        await db_manager.close()
    except Exception as e:
        console.print(f"[bold red]Database cleanup error: {e}[/bold red]")
from db.models import SubscriberModel
from convertkit.updater import ConvertKitUpdater, convertkit_updater

console = Console()

async def test_prepare_payload():
    """Test the payload preparation logic."""
    # Create a test subscriber data
    sample_subscriber = {
        "id": "123456789",
        "email_address": "test@example.com",
        "first_name": "Test",
        "created_at": "2025-03-28T00:00:00Z",
        "state": "active",
        "location_city": "San Francisco",
        "location_state": "California",
        "location_country": "United States",
        "referrer_domain": "google.com",
        "referrer_utm_source": "search",
        "linkedin_profile_url": "https://linkedin.com/in/test-user",
        "purchase_power": "High",
        "purchase_score": 0.92,
        "subscriber_region": "North America",
        "timezone": "UTC-08:00",
        "email_domain_type": "work"
    }
    
    # Create an instance of the updater
    updater = ConvertKitUpdater()
    
    # Prepare the payload
    payload = updater._prepare_update_payload(sample_subscriber)
    
    # Display the payload
    console.print("[bold cyan]Test Payload Preparation[/bold cyan]")
    console.print(f"Original Subscriber Data:")
    console.print(sample_subscriber)
    console.print("\nPrepared Payload for ConvertKit:")
    console.print(payload)
    
    # Verify fields that should and shouldn't be included
    fields = payload.get("fields", {})
    
    table = Table(title="Field Verification")
    table.add_column("Field", style="cyan")
    table.add_column("Expected", style="green")
    table.add_column("Actual", style="yellow")
    table.add_column("Status", style="bold")
    
    # Check fields that should be included
    should_include = [
        ("linkedin_profile_url", True),
        ("purchase_power", True),
        ("purchase_score", True),
        ("subscriber_region", True),
        ("timezone", True),
        ("email_domain_type", True),
        ("country_long", True),
    ]
    
    for field, expected in should_include:
        real_field = field
        if field == "country_long":
            # Special case for country_long which comes from location_country
            actual = "United States" in fields.get(field, "")
        else:
            actual = field in fields
        
        status = "✅" if actual == expected else "❌"
        table.add_row(field, str(expected), str(actual), status)
    
    # Check fields that should NOT be included
    should_exclude = [
        ("email_address", False),
        ("first_name", False),
        ("created_at", False),
        ("state", False),
        ("location_city", False),
        ("location_state", False),
        ("referrer_domain", False),
        ("referrer_utm_source", False),
    ]
    
    for field, expected in should_exclude:
        actual = field in fields
        status = "✅" if actual == expected else "❌"
        table.add_row(field, str(expected), str(actual), status)
    
    console.print(table)
    
    return payload

async def test_update_subscriber(subscriber_id):
    """Test updating a real subscriber."""
    console.print(f"[bold cyan]Testing Update for Subscriber {subscriber_id}[/bold cyan]")
    
    # First, get the subscriber data from database
    subscriber_data = await SubscriberModel.get_by_id(subscriber_id)
    
    if not subscriber_data:
        console.print(f"[bold red]Subscriber {subscriber_id} not found in database[/bold red]")
        return
    
    # Display subscriber data
    console.print(f"Subscriber Data from Database:")
    
    # Create a masked view for display
    masked_data = subscriber_data.copy()
    if "email_address" in masked_data:
        from utils.helpers import mask_email
        masked_data["email_address"] = mask_email(masked_data["email_address"])
    
    console.print(masked_data)
    
    # Prepare update payload
    updater = ConvertKitUpdater()
    payload = updater._prepare_update_payload(subscriber_data)
    
    console.print("\nPrepared Payload for ConvertKit:")
    console.print(payload)
    
    # Ask for confirmation before updating
    console.print("\n[bold yellow]Do you want to continue with the update? (y/n)[/bold yellow]")
    response = input().lower()
    
    if response != 'y':
        console.print("[yellow]Update cancelled[/yellow]")
        return
    
    # Perform the update
    success, message = await convertkit_updater.update_subscriber(subscriber_id)
    
    if success:
        console.print(f"[bold green]Successfully updated subscriber {subscriber_id}[/bold green]")
    else:
        console.print(f"[bold red]Failed to update subscriber {subscriber_id}: {message}[/bold red]")

async def main():
    """Main test function."""
    # Initialize database
    if not await initialize_database():
        console.print("[bold red]Failed to initialize database, exiting[/bold red]")
        return 1
    
    try:
        # Test payload preparation
        await test_prepare_payload()
        
        # If a subscriber ID is provided, test updating that subscriber
        if len(sys.argv) > 1:
            subscriber_id = sys.argv[1]
            await test_update_subscriber(subscriber_id)
        else:
            console.print("\n[yellow]No subscriber ID provided. To test updating a real subscriber, run:[/yellow]")
            console.print(f"python {sys.argv[0]} SUBSCRIBER_ID")
        
        return 0
    
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        import traceback
        console.print(traceback.format_exc())
        return 1
    
    finally:
        # Clean up
        await cleanup_database()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)