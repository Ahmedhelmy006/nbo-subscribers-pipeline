"""
Metrics collection for the NBO Pipeline.

This module provides functionality for collecting and processing
pipeline performance metrics.
"""
import logging
import psutil
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from db.models import PipelineRunModel, SubscriberModel
from db.connection import db_manager
from cache import cache_manager
from stacks.stack_manager import get_linkedin_stack

logger = logging.getLogger(__name__)

async def collect_pipeline_metrics(days: int = 1) -> Dict[str, Any]:
    """
    Collect metrics about pipeline performance.
    
    Args:
        days: Number of days to include in the metrics
        
    Returns:
        Dictionary of metrics
    """
    metrics = {}
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    try:
        # System metrics
        metrics['system'] = collect_system_metrics()
        
        # Database metrics
        metrics['database'] = await collect_database_metrics(start_time, end_time)
        
        # Pipeline run metrics
        metrics['pipeline_runs'] = await collect_pipeline_run_metrics(start_time, end_time)
        
        # Pipeline-specific metrics
        metrics['pipelines'] = await collect_specific_pipeline_metrics()
        
        # Cache metrics
        metrics['cache'] = collect_cache_metrics()
        
        # LinkedIn stack metrics
        metrics['linkedin_stack'] = collect_linkedin_stack_metrics()
        
        # Time range
        metrics['time_range'] = {
            'start': start_time.isoformat(),
            'end': end_time.isoformat(),
            'days': days
        }
        
        return metrics
    
    except Exception as e:
        logger.error(f"Error collecting metrics: {e}")
        # Return partial metrics if available
        return metrics or {'error': str(e)}

def collect_system_metrics() -> Dict[str, Any]:
    """
    Collect system metrics like CPU and memory usage.
    
    Returns:
        Dictionary of system metrics
    """
    try:
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'memory_available_mb': psutil.virtual_memory().available / (1024 * 1024),
            'memory_used_mb': psutil.virtual_memory().used / (1024 * 1024),
            'disk_percent': psutil.disk_usage('/').percent,
            'disk_free_gb': psutil.disk_usage('/').free / (1024 * 1024 * 1024),
            'load_avg': psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
        }
    except Exception as e:
        logger.error(f"Error collecting system metrics: {e}")
        return {'error': str(e)}

async def collect_database_metrics(start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """
    Collect metrics about the database.
    
    Args:
        start_time: Start of the time range
        end_time: End of the time range
        
    Returns:
        Dictionary of database metrics
    """
    try:
        # Get total subscriber count
        query = "SELECT COUNT(*) FROM subscribers"
        total_subscribers = await db_manager.fetchval(query)
        
        # Get new subscribers in the time range
        query = "SELECT COUNT(*) FROM subscribers WHERE created_at >= $1 AND created_at <= $2"
        new_subscribers = await db_manager.fetchval(query, start_time, end_time)
        
        # Get subscribers with location data
        query = """
        SELECT COUNT(*) FROM subscribers 
        WHERE location_country IS NOT NULL AND location_country != ''
        """
        with_location = await db_manager.fetchval(query)
        
        # Get subscribers with referrer data
        query = """
        SELECT COUNT(*) FROM subscribers 
        WHERE referrer_domain IS NOT NULL
        """
        with_referrer = await db_manager.fetchval(query)
        
        # Get subscribers with LinkedIn data
        query = """
        SELECT COUNT(*) FROM subscribers 
        WHERE linkedin_profile_url IS NOT NULL AND linkedin_profile_url != ''
        """
        with_linkedin = await db_manager.fetchval(query)
        
        # Get count by email domain type
        query = """
        SELECT email_domain_type, COUNT(*) 
        FROM subscribers 
        WHERE email_domain_type IS NOT NULL
        GROUP BY email_domain_type
        """
        domain_type_counts = await db_manager.fetch(query)
        
        # Get purchase power distribution
        query = """
        SELECT purchase_power, COUNT(*) 
        FROM subscribers 
        WHERE purchase_power IS NOT NULL AND purchase_power != 'Unknown'
        GROUP BY purchase_power
        """
        purchase_power_counts = await db_manager.fetch(query)
        
        # Format the results
        return {
            'total_subscribers': total_subscribers,
            'new_subscribers': new_subscribers,
            'with_location': with_location,
            'with_referrer': with_referrer,
            'with_linkedin': with_linkedin,
            'completion_rates': {
                'location': (with_location / total_subscribers * 100) if total_subscribers else 0,
                'referrer': (with_referrer / total_subscribers * 100) if total_subscribers else 0,
                'linkedin': (with_linkedin / total_subscribers * 100) if total_subscribers else 0
            },
            'email_domain_types': {
                row['email_domain_type']: row['count'] 
                for row in domain_type_counts
            },
            'purchase_power': {
                row['purchase_power']: row['count'] 
                for row in purchase_power_counts
            }
        }
    except Exception as e:
        logger.error(f"Error collecting database metrics: {e}")
        return {'error': str(e)}

async def collect_pipeline_run_metrics(start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """
    Collect metrics about pipeline runs.
    
    Args:
        start_time: Start of the time range
        end_time: End of the time range
        
    Returns:
        Dictionary of pipeline run metrics
    """
    try:
        # Get all runs in the time range
        query = """
        SELECT 
            pipeline_name, 
            status, 
            start_time, 
            end_time, 
            records_processed,
            EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds
        FROM pipeline_runs
        WHERE start_time >= $1 AND start_time <= $2
        ORDER BY start_time DESC
        """
        runs = await db_manager.fetch(query, start_time, end_time)
        
        # Convert to list of dicts
        run_list = [dict(run) for run in runs]
        
        # Calculate summary statistics
        total_runs = len(run_list)
        completed_runs = sum(1 for run in run_list if run['status'] == 'completed')
        failed_runs = sum(1 for run in run_list if run['status'] == 'failed')
        total_records = sum(run['records_processed'] or 0 for run in run_list)
        
        # Calculate average duration
        durations = [run['duration_seconds'] for run in run_list if run['duration_seconds'] is not None]
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        # Group by pipeline
        pipeline_stats = {}
        for run in run_list:
            pipeline = run['pipeline_name']
            if pipeline not in pipeline_stats:
                pipeline_stats[pipeline] = {
                    'total_runs': 0,
                    'completed_runs': 0,
                    'failed_runs': 0,
                    'total_records': 0,
                    'durations': []
                }
            
            pipeline_stats[pipeline]['total_runs'] += 1
            
            if run['status'] == 'completed':
                pipeline_stats[pipeline]['completed_runs'] += 1
            elif run['status'] == 'failed':
                pipeline_stats[pipeline]['failed_runs'] += 1
                
            pipeline_stats[pipeline]['total_records'] += run['records_processed'] or 0
            
            if run['duration_seconds'] is not None:
                pipeline_stats[pipeline]['durations'].append(run['duration_seconds'])
        
        # Calculate average durations by pipeline
        for pipeline, stats in pipeline_stats.items():
            durations = stats['durations']
            stats['avg_duration'] = sum(durations) / len(durations) if durations else 0
            del stats['durations']  # Remove raw durations from output
        
        return {
            'total_runs': total_runs,
            'completed_runs': completed_runs,
            'failed_runs': failed_runs,
            'completion_rate': (completed_runs / total_runs * 100) if total_runs else 0,
            'total_records_processed': total_records,
            'avg_duration_seconds': avg_duration,
            'pipelines': pipeline_stats,
            'recent_runs': run_list[:10]  # Include 10 most recent runs
        }
    except Exception as e:
        logger.error(f"Error collecting pipeline run metrics: {e}")
        return {'error': str(e)}

async def collect_specific_pipeline_metrics() -> Dict[str, Any]:
    """
    Collect metrics specific to each pipeline component.
    
    Returns:
        Dictionary of pipeline-specific metrics
    """
    try:
        metrics = {}
        
        # LinkedIn pipeline metrics
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN email_domain_type = 'work' THEN 1 END) as work_emails,
            COUNT(CASE WHEN email_domain_type = 'personal' THEN 1 END) as personal_emails,
            COUNT(CASE WHEN linkedin_profile_url IS NOT NULL THEN 1 END) as with_linkedin
        FROM subscribers
        """
        linkedin_stats = await db_manager.fetchrow(query)
        
        if linkedin_stats:
            linkedin_dict = dict(linkedin_stats)
            work_emails = linkedin_dict['work_emails'] or 0
            with_linkedin = linkedin_dict['with_linkedin'] or 0
            
            # Calculate success rate for work emails
            linkedin_dict['success_rate'] = (
                with_linkedin / work_emails * 100
            ) if work_emails > 0 else 0
            
            metrics['linkedin'] = linkedin_dict
        
        # Location pipeline metrics
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN location_city IS NOT NULL THEN 1 END) as with_city,
            COUNT(CASE WHEN location_state IS NOT NULL THEN 1 END) as with_state,
            COUNT(CASE WHEN location_country IS NOT NULL THEN 1 END) as with_country,
            COUNT(CASE WHEN purchase_power IS NOT NULL AND purchase_power != 'Unknown' THEN 1 END) as with_purchase_power
        FROM subscribers
        """
        location_stats = await db_manager.fetchrow(query)
        
        if location_stats:
            location_dict = dict(location_stats)
            total = location_dict['total'] or 0
            with_country = location_dict['with_country'] or 0
            
            # Calculate success rate
            location_dict['success_rate'] = (
                with_country / total * 100
            ) if total > 0 else 0
            
            metrics['location'] = location_dict
        
        # Referrer pipeline metrics
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN referrer_domain IS NOT NULL THEN 1 END) as with_referrer,
            COUNT(CASE WHEN referrer_utm_source IS NOT NULL THEN 1 END) as with_utm_source
        FROM subscribers
        """
        referrer_stats = await db_manager.fetchrow(query)
        
        if referrer_stats:
            referrer_dict = dict(referrer_stats)
            total = referrer_dict['total'] or 0
            with_referrer = referrer_dict['with_referrer'] or 0
            
            # Calculate success rate
            referrer_dict['success_rate'] = (
                with_referrer / total * 100
            ) if total > 0 else 0
            
            metrics['referrer'] = referrer_dict
        
        return metrics
    except Exception as e:
        logger.error(f"Error collecting pipeline-specific metrics: {e}")
        return {'error': str(e)}

def collect_cache_metrics() -> Dict[str, Any]:
    """
    Collect metrics about the cache.
    
    Returns:
        Dictionary of cache metrics
    """
    try:
        return {
            'size': cache_manager.size(),
            'type': cache_manager.backend
        }
    except Exception as e:
        logger.error(f"Error collecting cache metrics: {e}")
        return {'error': str(e)}

def collect_linkedin_stack_metrics() -> Dict[str, Any]:
    """
    Collect metrics about the LinkedIn stack.
    
    Returns:
        Dictionary of LinkedIn stack metrics
    """
    try:
        linkedin_stack = get_linkedin_stack()
        stats = linkedin_stack.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Error collecting LinkedIn stack metrics: {e}")
        return {'error': str(e)}