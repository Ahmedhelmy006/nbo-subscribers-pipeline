"""
Pipeline state management for the NBO Pipeline.

This module provides functions for managing the state of the pipeline,
tracking progress, and handling failures.
"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from .models import PipelineStateModel, PipelineRunModel

logger = logging.getLogger(__name__)

class PipelineStateManager:
    """
    Manages the state of a pipeline.
    
    Provides methods for tracking progress, managing runs, and
    handling failures.
    """
    
    def __init__(self, pipeline_name: str):
        """
        Initialize the pipeline state manager.
        
        Args:
            pipeline_name: Name of the pipeline
        """
        self.pipeline_name = pipeline_name
        self.current_run_id = None
        self.records_processed = 0
        self.last_processed_id = None
        self.metadata = {}
        
    async def get_current_state(self) -> Dict[str, Any]:
        """
        Get the current state of the pipeline.
        
        Returns:
            Dict containing pipeline state
        """
        state = await PipelineStateModel.get_state(self.pipeline_name)
        if not state:
            # Create default state
            await PipelineStateModel.update_state(
                pipeline_name=self.pipeline_name,
                status="idle"
            )
            state = await PipelineStateModel.get_state(self.pipeline_name)
            
        return state
    
    async def start_run(self, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Start a new pipeline run.
        
        Args:
            metadata: Additional metadata for the run
            
        Returns:
            The ID of the created run
        """
        # Reset counters
        self.records_processed = 0
        
        # Create run
        self.current_run_id = await PipelineRunModel.create_run(
            pipeline_name=self.pipeline_name,
            status="running",
            metadata=metadata
        )
        
        # Store metadata
        self.metadata = metadata or {}
        
        # Get the last processed ID
        state = await self.get_current_state()
        self.last_processed_id = state.get("last_processed_id")
        
        logger.info(f"Started run {self.current_run_id} for pipeline '{self.pipeline_name}'")
        return self.current_run_id
    
    async def update_progress(
        self,
        records_processed: Optional[int] = None, 
        last_processed_id: Optional[str] = None,
        metadata_updates: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update the progress of the current run.
        
        Args:
            records_processed: Additional records processed
            last_processed_id: New last processed ID
            metadata_updates: Updates to the run metadata
            
        Returns:
            True if the update was successful, False otherwise
        """
        if not self.current_run_id:
            logger.error("Cannot update progress: No active run")
            return False
        
        # Update counters
        if records_processed:
            self.records_processed += records_processed
        
        if last_processed_id:
            self.last_processed_id = last_processed_id
        
        # Update metadata
        if metadata_updates:
            self.metadata.update(metadata_updates)
        
        # Update run
        success = await PipelineRunModel.update_run(
            run_id=self.current_run_id,
            records_processed=self.records_processed,
            metadata=self.metadata
        )
        
        if not success:
            logger.error(f"Failed to update run {self.current_run_id}")
            return False
        
        # Update pipeline state
        success = await PipelineStateModel.update_state(
            pipeline_name=self.pipeline_name,
            last_processed_id=self.last_processed_id,
            records_processed=self.records_processed
        )
        
        if not success:
            logger.error(f"Failed to update state for pipeline '{self.pipeline_name}'")
            return False
        
        return True
    
    async def complete_run(
        self,
        records_processed: Optional[int] = None,
        last_processed_id: Optional[str] = None,
        metadata_updates: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Complete the current run successfully.
        
        Args:
            records_processed: Final records processed count
            last_processed_id: Final last processed ID
            metadata_updates: Final updates to the run metadata
            
        Returns:
            True if completion was successful, False otherwise
        """
        if not self.current_run_id:
            logger.error("Cannot complete run: No active run")
            return False
        
        # Update counters
        if records_processed:
            self.records_processed = records_processed
        
        if last_processed_id:
            self.last_processed_id = last_processed_id
        
        # Update metadata
        if metadata_updates:
            self.metadata.update(metadata_updates)
        
        # Update run
        success = await PipelineRunModel.update_run(
            run_id=self.current_run_id,
            status="completed",
            end_time=True,
            records_processed=self.records_processed,
            metadata=self.metadata
        )
        
        if not success:
            logger.error(f"Failed to complete run {self.current_run_id}")
            return False
        
        # Update pipeline state
        success = await PipelineStateModel.update_state(
            pipeline_name=self.pipeline_name,
            status="idle",
            last_processed_id=self.last_processed_id,
            records_processed=self.records_processed
        )
        
        if not success:
            logger.error(f"Failed to update state for pipeline '{self.pipeline_name}'")
            return False
        
        logger.info(f"Completed run {self.current_run_id} for pipeline '{self.pipeline_name}'")
        logger.info(f"Processed {self.records_processed} records")
        
        # Reset run ID
        self.current_run_id = None
        
        return True
    
    async def fail_run(
        self,
        error_message: str,
        records_processed: Optional[int] = None,
        metadata_updates: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark the current run as failed.
        
        Args:
            error_message: Error message describing the failure
            records_processed: Final records processed count
            metadata_updates: Final updates to the run metadata
            
        Returns:
            True if failure was recorded successfully, False otherwise
        """
        if not self.current_run_id:
            logger.error("Cannot fail run: No active run")
            return False
        
        # Update counters
        if records_processed:
            self.records_processed = records_processed
        
        # Update metadata
        if metadata_updates:
            self.metadata.update(metadata_updates)
        
        # Add error information to metadata
        self.metadata["error"] = {
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        }
        
        # Update run
        success = await PipelineRunModel.update_run(
            run_id=self.current_run_id,
            status="failed",
            end_time=True,
            records_processed=self.records_processed,
            error_message=error_message,
            metadata=self.metadata
        )
        
        if not success:
            logger.error(f"Failed to mark run {self.current_run_id} as failed")
            return False
        
        # Update pipeline state
        success = await PipelineStateModel.update_state(
            pipeline_name=self.pipeline_name,
            status="error",
            records_processed=self.records_processed
        )
        
        if not success:
            logger.error(f"Failed to update state for pipeline '{self.pipeline_name}'")
            return False
        
        logger.error(f"Run {self.current_run_id} for pipeline '{self.pipeline_name}' failed: {error_message}")
        logger.info(f"Processed {self.records_processed} records before failure")
        
        # Reset run ID
        self.current_run_id = None
        
        return True
    
    async def is_pipeline_running(self) -> bool:
        """
        Check if the pipeline is currently running.
        
        Returns:
            True if the pipeline is running, False otherwise
        """
        state = await self.get_current_state()
        return state.get("status") == "running"
    
    async def get_last_processed_id(self) -> Optional[str]:
        """
        Get the ID of the last processed record.
        
        Returns:
            ID of the last processed record or None
        """
        state = await self.get_current_state()
        return state.get("last_processed_id")
    
    async def get_pipeline_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the history of pipeline runs.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            List of run records
        """
        return await PipelineRunModel.get_recent_runs(self.pipeline_name, limit)

# Create a dictionary to store pipeline state managers
_pipeline_managers = {}

def get_pipeline_state_manager(pipeline_name: str) -> PipelineStateManager:
    """
    Get a pipeline state manager for a pipeline.
    
    Creates a new manager if one doesn't exist.
    
    Args:
        pipeline_name: Name of the pipeline
        
    Returns:
        PipelineStateManager for the pipeline
    """
    if pipeline_name not in _pipeline_managers:
        _pipeline_managers[pipeline_name] = PipelineStateManager(pipeline_name)
    
    return _pipeline_managers[pipeline_name]