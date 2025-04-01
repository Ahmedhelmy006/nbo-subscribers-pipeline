import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def send_test_webhook(webhook_url):
    """
    Send a test message to the Zapier webhook.
    
    Args:
        webhook_url (str): Zapier webhook URL
    """
    # Prepare a test message
    message = "Hi There, I am Neon!"

    try:
        response = requests.post(
            webhook_url, 
            json=message,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code in [200, 201, 202]:
            print("✅ Test message sent successfully!")
            print(f"Response: {response.text}")
        else:
            print(f"❌ Failed to send test message. Status code: {response.status_code}")
            print(f"Response: {response.text}")
    
    except Exception as e:
        print(f"❌ Error sending test message: {e}")

def main():
    # Get webhook URL from environment variable
    webhook_url = os.getenv('WEBHOOK_URL')
    
    if not webhook_url:
        print("❌ WEBHOOK_URL not found in environment variables")
        return
    
    send_test_webhook(webhook_url)

if __name__ == "__main__":
    main()