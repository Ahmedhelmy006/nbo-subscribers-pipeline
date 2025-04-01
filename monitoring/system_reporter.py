import os
import sys
import requests
import logging
import json
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import settings

class SystemReporter:
    """
    Sends system notifications and batch summaries to Slack via Webhook.
    """
    
    def __init__(self, webhook_url=None):
        """
        Initialize the system reporter.
        
        Args:
            webhook_url: Webhook URL (defaults to settings or env var)
        """
        self.webhook_url = webhook_url or settings.WEBHOOK_URL or os.getenv('WEBHOOK_URL')
        self.batch_history = []
        self.batch_count = 0
        
        # Logging setup
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        if not self.webhook_url:
            self.logger.warning("No webhook URL provided. System notifications will be disabled.")
    
    def _format_message(self, message_type, details=None):
        """
        Format a human-readable message for Slack.
        
        Args:
            message_type: Type of message
            details: Additional details dictionary
        
        Returns:
            str: Formatted message
        """
        if not details:
            details = {}
        
        # Base message header
        message = f"🤖 NBO Pipeline: {message_type.upper()} Report\n"
        message += f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += "=" * 40 + "\n\n"
        
        # Add specific details based on message type
        if message_type == "startup":
            message += "Pipeline system has started successfully.\n"
        
        elif message_type == "batch_summary":
            # Add batch history details
            message += "Batch Processing Summary:\n"
            for batch in self.batch_history[-5:]:  # Last 5 batches
                message += f"- Batch #{batch.get('batch_number', 'N/A')}: "
                message += f"Processed {batch.get('total_processed', 0)} subscribers "
                message += f"in {batch.get('duration', 'N/A')}\n"
        
        elif message_type == "error":
            message += "❌ ERROR ENCOUNTERED:\n"
            message += f"Error: {details.get('error', 'Unknown error')}\n"
            
            if details.get('context'):
                message += f"Context: {details['context']}\n"
        
        # System health metrics
        if details.get('system_metrics'):
            metrics = details['system_metrics']
            message += "\nSystem Metrics:\n"
            message += f"- Total Subscribers Processed: {metrics.get('total_processed', 0)}\n"
            message += f"- Skipped Subscribers: {metrics.get('skipped', 0)}\n"
            message += f"- LinkedIn URLs Identified: {metrics.get('linkedin_urls', 0)}\n"
            message += f"- Database Entries Updated: {metrics.get('db_updates', 0)}\n"
            message += f"- ConvertKit API Updates: {metrics.get('convertkit_updates', 0)}\n"
            
            # Connection status
            message += f"- Database Connection: {'✅ Successful' if metrics.get('db_connection', False) else '❌ Failed'}\n"
            message += f"- ConvertKit API Connection: {'✅ Successful' if metrics.get('convertkit_connection', False) else '❌ Failed'}\n"
        
        return message
    
    def send_message(self, message_type, details=None):
        """
        Send a message to the webhook.
        
        Args:
            message_type: Type of message
            details: Additional details dictionary
        """
        if not self.webhook_url:
            self.logger.warning("No webhook URL configured. Skipping notification.")
            return

        # Format the message
        message_text = self._format_message(message_type, details)
        
        try:
            response = requests.post(
                self.webhook_url, 
                json={"text": message_text},
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code not in [200, 201, 202]:
                self.logger.error(f"Failed to send webhook: {response.status_code} - {response.text}")
        except Exception as e:
            self.logger.error(f"Error sending webhook: {e}")
    
    def record_batch_result(self, batch_info):
        """
        Record a batch result for future summary.
        
        Args:
            batch_info: Dictionary with batch processing details
        """
        # Limit to last 5 batches
        self.batch_history.append(batch_info)
        self.batch_count += 1
        
        if len(self.batch_history) > 5:
            self.batch_history.pop(0)
    
    def send_batch_summary(self, system_metrics=None):
        """
        Send a batch summary message.
        
        Args:
            system_metrics: Optional dictionary of system metrics
        """
        # Send summary every 5 batches
        if self.batch_count % 5 == 0:
            self.send_message("batch_summary", 
                              details={'system_metrics': system_metrics} if system_metrics else None)
    
    def send_startup_notification(self):
        """Send startup notification."""
        self.send_message("startup")
    
    def send_error_alert(self, error_message, context=None, system_metrics=None):
        """
        Send an error alert message immediately.
        
        Args:
            error_message: Error message to send
            context: Optional additional context
            system_metrics: Optional system metrics
        """
        details = {
            "error": error_message,
            "context": context,
            "system_metrics": system_metrics
        }
        self.send_message("error", details)

# Create a global singleton instance
system_reporter = SystemReporter()