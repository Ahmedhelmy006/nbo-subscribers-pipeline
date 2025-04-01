"""
Visualization for the NBO Pipeline.

This module provides functionality for creating visualizations
of pipeline metrics for reporting.
"""
import io
import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Use a clean, modern style for charts
plt.style.use('seaborn-v0_8-whitegrid')

def create_pipeline_status_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing pipeline run status distribution.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        pipeline_runs = metrics.get('pipeline_runs', {})
        pipelines = pipeline_runs.get('pipelines', {})
        
        if not pipelines:
            return create_error_chart("No pipeline data available")
        
        # Extract data for plotting
        names = list(pipelines.keys())
        completed = [p.get('completed_runs', 0) for p in pipelines.values()]
        failed = [p.get('failed_runs', 0) for p in pipelines.values()]
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Create the stacked bar chart
        width = 0.7
        ax.bar(names, completed, width, label='Completed', color='#4CAF50')
        ax.bar(names, failed, width, bottom=completed, label='Failed', color='#F44336')
        
        # Add labels and title
        ax.set_ylabel('Number of Runs')
        ax.set_title('Pipeline Runs by Status')
        ax.legend()
        
        # Add value labels
        for i, pipeline in enumerate(names):
            total = completed[i] + failed[i]
            if total > 0:
                # Label for the total
                ax.text(i, total + 0.1, f"{total}", ha='center', fontweight='bold')
                
                # Label for completion rate
                completion_rate = completed[i] / total * 100 if total > 0 else 0
                ax.text(i, total / 2, f"{completion_rate:.1f}%", ha='center', color='white', fontweight='bold')
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating pipeline status chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_processing_rate_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing processing rates for different pipelines.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        pipeline_runs = metrics.get('pipeline_runs', {})
        pipelines = pipeline_runs.get('pipelines', {})
        
        if not pipelines:
            return create_error_chart("No pipeline data available")
        
        # Extract data for plotting
        names = []
        rates = []
        colors = []
        
        for name, stats in pipelines.items():
            total_records = stats.get('total_records', 0)
            avg_duration = stats.get('avg_duration', 0)
            
            if avg_duration > 0:
                rate = total_records / avg_duration
                names.append(name)
                rates.append(rate)
                # Assign different colors for different pipelines
                if 'linkedin' in name.lower():
                    colors.append('#4285F4')  # Google Blue
                elif 'location' in name.lower():
                    colors.append('#34A853')  # Google Green
                elif 'referrer' in name.lower():
                    colors.append('#FBBC05')  # Google Yellow
                else:
                    colors.append('#EA4335')  # Google Red
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Create the bar chart
        ax.bar(names, rates, color=colors)
        
        # Add labels and title
        ax.set_ylabel('Records per Second')
        ax.set_title('Pipeline Processing Rates')
        
        # Add value labels
        for i, rate in enumerate(rates):
            ax.text(i, rate + 0.1, f"{rate:.2f}", ha='center')
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating processing rate chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_completion_rate_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing completion rates for different pipelines.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        pipeline_specific = metrics.get('pipelines', {})
        
        if not pipeline_specific:
            return create_error_chart("No pipeline-specific data available")
        
        # Extract completion rates
        names = []
        rates = []
        
        for name, stats in pipeline_specific.items():
            if 'success_rate' in stats:
                names.append(name)
                rates.append(stats['success_rate'])
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Create the bar chart with a color gradient based on rate
        cmap = plt.cm.get_cmap('RdYlGn')  # Red to Yellow to Green
        normalized_rates = [rate / 100 for rate in rates]  # Normalize to 0-1 range
        colors = [cmap(rate) for rate in normalized_rates]
        
        ax.bar(names, rates, color=colors)
        
        # Add labels and title
        ax.set_ylabel('Completion Rate (%)')
        ax.set_title('Pipeline Completion Rates')
        ax.set_ylim(0, 100)  # Set y-axis from 0 to 100%
        
        # Add value labels
        for i, rate in enumerate(rates):
            ax.text(i, rate + 2, f"{rate:.1f}%", ha='center')
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating completion rate chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_email_domain_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing email domain type distribution.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        database_metrics = metrics.get('database', {})
        domain_types = database_metrics.get('email_domain_types', {})
        
        if not domain_types:
            return create_error_chart("No email domain data available")
        
        # Extract data for plotting
        labels = list(domain_types.keys())
        values = list(domain_types.values())
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Create the pie chart
        wedges, texts, autotexts = ax.pie(
            values, 
            labels=None,  # No labels on the pie itself
            autopct='%1.1f%%',
            startangle=90,
            colors=['#4285F4', '#34A853'],  # Work = Blue, Personal = Green
            wedgeprops={'edgecolor': 'w', 'linewidth': 1}
        )
        
        # Customize text
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        # Add a legend
        ax.legend(wedges, labels, title="Email Types", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
        
        # Add title
        ax.set_title('Email Domain Type Distribution')
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating email domain chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_purchase_power_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing purchase power distribution.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        database_metrics = metrics.get('database', {})
        purchase_power = database_metrics.get('purchase_power', {})
        
        if not purchase_power:
            return create_error_chart("No purchase power data available")
        
        # Extract data for plotting
        labels = list(purchase_power.keys())
        values = list(purchase_power.values())
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Define colors for purchase power categories
        color_map = {
            'High': '#4CAF50',    # Green
            'Medium': '#FFC107',  # Amber
            'Low': '#F44336',     # Red
            'Unknown': '#9E9E9E'  # Grey
        }
        
        # Get colors, defaulting to blue for any unmapped categories
        colors = [color_map.get(label, '#2196F3') for label in labels]
        
        # Create the bar chart
        bars = ax.bar(labels, values, color=colors)
        
        # Add labels and title
        ax.set_ylabel('Number of Subscribers')
        ax.set_title('Subscriber Purchase Power Distribution')
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.,
                height + 0.1,
                f"{int(height)}",
                ha='center'
            )
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating purchase power chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_linkedin_success_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing LinkedIn lookup success rates.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        pipeline_specific = metrics.get('pipelines', {})
        linkedin_metrics = pipeline_specific.get('linkedin', {})
        
        if not linkedin_metrics:
            return create_error_chart("No LinkedIn data available")
        
        # Extract data
        work_emails = linkedin_metrics.get('work_emails', 0)
        personal_emails = linkedin_metrics.get('personal_emails', 0)
        with_linkedin = linkedin_metrics.get('with_linkedin', 0)
        success_rate = linkedin_metrics.get('success_rate', 0)
        
        # Calculate proportions
        total_emails = work_emails + personal_emails
        work_percent = work_emails / total_emails * 100 if total_emails > 0 else 0
        personal_percent = personal_emails / total_emails * 100 if total_emails > 0 else 0
        
        work_with_linkedin = with_linkedin
        work_without_linkedin = work_emails - with_linkedin
        
        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))
        
        # First subplot: Email type distribution
        email_labels = ['Work', 'Personal']
        email_values = [work_emails, personal_emails]
        email_colors = ['#4285F4', '#34A853']
        
        ax1.pie(
            email_values, 
            labels=email_labels,
            autopct='%1.1f%%',
            startangle=90,
            colors=email_colors,
            wedgeprops={'edgecolor': 'w', 'linewidth': 1}
        )
        ax1.set_title('Email Type Distribution')
        
        # Second subplot: LinkedIn success for work emails
        linkedin_labels = ['Found LinkedIn', 'No LinkedIn']
        linkedin_values = [work_with_linkedin, work_without_linkedin]
        linkedin_colors = ['#4CAF50', '#F44336']
        
        ax2.pie(
            linkedin_values, 
            labels=linkedin_labels,
            autopct='%1.1f%%',
            startangle=90,
            colors=linkedin_colors,
            wedgeprops={'edgecolor': 'w', 'linewidth': 1}
        )
        ax2.set_title(f'LinkedIn Success Rate: {success_rate:.1f}%')
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating LinkedIn success chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_system_metrics_chart(metrics: Dict[str, Any]) -> io.BytesIO:
    """
    Create a chart showing system metrics (CPU, memory, disk).
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        BytesIO object containing the chart image
    """
    try:
        system_metrics = metrics.get('system', {})
        
        if not system_metrics:
            return create_error_chart("No system metrics available")
        
        # Extract data
        cpu_percent = system_metrics.get('cpu_percent', 0)
        memory_percent = system_metrics.get('memory_percent', 0)
        disk_percent = system_metrics.get('disk_percent', 0)
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Define categories and values
        categories = ['CPU', 'Memory', 'Disk']
        values = [cpu_percent, memory_percent, disk_percent]
        
        # Define colors based on values (green to red)
        colors = []
        for value in values:
            if value < 50:
                colors.append('#4CAF50')  # Green
            elif value < 75:
                colors.append('#FFC107')  # Amber
            else:
                colors.append('#F44336')  # Red
        
        # Create the bar chart
        bars = ax.bar(categories, values, color=colors)
        
        # Add labels and title
        ax.set_ylabel('Percent Utilization')
        ax.set_title('System Resource Utilization')
        ax.set_ylim(0, 100)  # Set y-axis from 0 to 100%
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.,
                height + 2,
                f"{height:.1f}%",
                ha='center'
            )
        
        plt.tight_layout()
        
        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf
    
    except Exception as e:
        logger.error(f"Error creating system metrics chart: {e}")
        return create_error_chart(f"Error: {str(e)}")

def create_error_chart(error_message: str) -> io.BytesIO:
    """
    Create a chart showing an error message.
    
    Args:
        error_message: Error message to display
        
    Returns:
        BytesIO object containing the chart image
    """
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Hide axis
    ax.axis('off')
    
    # Add error message
    ax.text(
        0.5, 0.5,
        f"Error: {error_message}",
        fontsize=14,
        color='red',
        ha='center',
        va='center',
        wrap=True
    )
    
    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close(fig)
    
    return buf

def generate_all_charts(metrics: Dict[str, Any]) -> Dict[str, io.BytesIO]:
    """
    Generate all charts from the metrics data.
    
    Args:
        metrics: Dictionary of metrics data
        
    Returns:
        Dictionary of chart name to BytesIO object
    """
    charts = {}
    
    # Generate all charts
    charts['pipeline_status'] = create_pipeline_status_chart(metrics)
    charts['processing_rate'] = create_processing_rate_chart(metrics)
    charts['completion_rate'] = create_completion_rate_chart(metrics)
    charts['email_domain'] = create_email_domain_chart(metrics)
    charts['purchase_power'] = create_purchase_power_chart(metrics)
    charts['linkedin_success'] = create_linkedin_success_chart(metrics)
    charts['system_metrics'] = create_system_metrics_chart(metrics)
    
    return charts