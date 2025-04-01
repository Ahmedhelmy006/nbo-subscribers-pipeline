"""
Webhook reporter for the NBO Pipeline.

This module provides functionality for generating and sending
pipeline reports to webhooks (including Slack via Zapier).
"""
import os
import logging
import asyncio
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import settings
from .metrics import collect_pipeline_metrics
from .visualization import generate_all_charts

logger = logging.getLogger(__name__)

class WebhookReporter:
    """
    Generates and sends reports to webhooks.
    """
    
    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize the webhook reporter.
        
        Args:
            webhook_url: Webhook URL
        """
        self.webhook_url = webhook_url or settings.WEBHOOK_URL or os.getenv('WEBHOOK_URL')
        self.use_charts = False  # Default to not using charts since they require additional setup
    
    async def generate_report(self, days: int = 1) -> Dict[str, Any]:
        """
        Generate a full report with metrics and charts.
        
        Args:
            days: Number of days to include in the report
            
        Returns:
            Dictionary with report data and chart buffers
        """
        try:
            # Collect metrics
            metrics = await collect_pipeline_metrics(days)
            
            # Generate charts if needed
            charts = {}
            if self.use_charts:
                try:
                    charts = generate_all_charts(metrics)
                except Exception as e:
                    logger.error(f"Error generating charts: {e}")
            
            return {
                'metrics': metrics,
                'charts': charts,
                'generated_at': datetime.now().isoformat(),
                'days': days
            }
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return {
                'error': str(e),
                'generated_at': datetime.now().isoformat(),
                'days': days
            }
    
    def _format_message(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format the report data into a webhook message.
        
        Args:
            report: Report data
            
        Returns:
            Formatted message
        """
        metrics = report.get('metrics', {})
        
        # Format database metrics
        db_metrics = metrics.get('database', {})
        total_subscribers = db_metrics.get('total_subscribers', 0)
        new_subscribers = db_metrics.get('new_subscribers', 0)
        with_linkedin = db_metrics.get('with_linkedin', 0)
        
        # Format pipeline metrics
        pipeline_runs = metrics.get('pipeline_runs', {})
        total_runs = pipeline_runs.get('total_runs', 0)
        completed_runs = pipeline_runs.get('completed_runs', 0)
        completion_rate = pipeline_runs.get('completion_rate', 0)
        
        # Format time range
        time_range = metrics.get('time_range', {})
        start_time = time_range.get('start', 'Unknown')
        end_time = time_range.get('end', 'Unknown')
        
        # Format message for webhook
        message = {
            "text": "📊 NBO Pipeline Performance Report",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "📊 NBO Pipeline Performance Report"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Report Period:* {start_time} to {end_time}"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Total Subscribers:*\n{total_subscribers:,}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*New Subscribers:*\n{new_subscribers:,}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*LinkedIn Mapped:*\n{with_linkedin:,}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Pipeline Runs:*\n{total_runs}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Completed Runs:*\n{completed_runs}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Completion Rate:*\n{completion_rate:.1f}%"
                        }
                    ]
                }
            ]
        }
        
        return message
    
    def _format_simple_message(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format a simpler message for non-Slack webhooks.
        
        Args:
            report: Report data
            
        Returns:
            Simple formatted message
        """
        metrics = report.get('metrics', {})
        
        # Format database metrics
        db_metrics = metrics.get('database', {})
        total_subscribers = db_metrics.get('total_subscribers', 0)
        new_subscribers = db_metrics.get('new_subscribers', 0)
        with_linkedin = db_metrics.get('with_linkedin', 0)
        
        # Format pipeline metrics
        pipeline_runs = metrics.get('pipeline_runs', {})
        total_runs = pipeline_runs.get('total_runs', 0)
        completed_runs = pipeline_runs.get('completed_runs', 0)
        completion_rate = pipeline_runs.get('completion_rate', 0)
        
        # Format time range
        time_range = metrics.get('time_range', {})
        start_time = time_range.get('start', 'Unknown')
        end_time = time_range.get('end', 'Unknown')
        
        # Create a simpler message that works with most webhooks
        message = {
            "title": "NBO Pipeline Performance Report",
            "report_period": f"{start_time} to {end_time}",
            "metrics": {
                "total_subscribers": total_subscribers,
                "new_subscribers": new_subscribers,
                "linkedin_mapped": with_linkedin,
                "pipeline_runs": total_runs,
                "completed_runs": completed_runs,
                "completion_rate": f"{completion_rate:.1f}%"
            }
        }
        
        return message
    
    def send_report_to_webhook(self, report: Dict[str, Any], use_slack_format: bool = True) -> bool:
        """
        Send the report to the webhook.
        
        Args:
            report: Report data
            use_slack_format: Whether to use Slack-specific formatting
            
        Returns:
            True if successful, False otherwise
        """
        if not self.webhook_url:
            logger.warning("No webhook URL provided, skipping report")
            return False
        
        try:
            # Format the message
            message = self._format_message(report) if use_slack_format else self._format_simple_message(report)
            
            # Send the message
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Successfully sent report to webhook: {response.status_code}")
                return True
            else:
                logger.error(f"Error sending webhook message: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending report to webhook: {e}")
            return False
    
    async def send_daily_report(self, days: int = 1, use_slack_format: bool = True) -> bool:
        """
        Generate and send a daily report to the webhook.
        
        Args:
            days: Number of days to include in the report
            use_slack_format: Whether to use Slack-specific formatting
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate the report
            report = await self.generate_report(days)
            
            # Send the report
            success = self.send_report_to_webhook(report, use_slack_format)
            
            return success
        except Exception as e:
            logger.error(f"Error sending daily report: {e}")
            return False

async def schedule_daily_report(webhook_url: Optional[str] = None, hour: int = 9, minute: int = 0, use_slack_format: bool = True):
    """
    Schedule a daily report to be sent at the specified time.
    
    Args:
        webhook_url: Webhook URL
        hour: Hour to send the report (24-hour format)
        minute: Minute to send the report
        use_slack_format: Whether to use Slack-specific formatting
    """
    reporter = WebhookReporter(webhook_url)
    
    while True:
        now = datetime.now()
        
        # Calculate time until next report
        target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # If the target time has already passed today, schedule for tomorrow
        if target_time <= now:
            target_time = target_time + timedelta(days=1)
        
        # Calculate seconds to wait
        wait_seconds = (target_time - now).total_seconds()
        
        logger.info(f"Next pipeline report scheduled for {target_time} ({wait_seconds / 3600:.2f} hours from now)")
        
        # Wait until the scheduled time
        await asyncio.sleep(wait_seconds)
        
        # Generate and send the report
        success = await reporter.send_daily_report(use_slack_format=use_slack_format)
        
        if success:
            logger.info("Daily pipeline report sent successfully")
        else:
            logger.error("Failed to send daily pipeline report")
        
        # Wait a bit to avoid sending multiple reports if execution takes time
        await asyncio.sleep(60)

# For backward compatibility
SlackReporter = WebhookReporter