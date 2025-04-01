"""
Worker pool utility for the NBO Pipeline.

This module provides a base worker pool class for managing asyncio tasks.
"""
import asyncio
import os
import time
import logging
import signal
import psutil
from rich.console import Console

logger = logging.getLogger(__name__)

class WorkerPool:
    """
    Dynamically sized pool of workers for processing tasks.
    """
    
    def __init__(self, worker_func, name, min_workers=5, max_workers=None):
        """
        Initialize a worker pool.
        
        Args:
            worker_func: Async function for each worker to run
            name: Name of the worker pool (for logging)
            min_workers: Minimum number of workers
            max_workers: Maximum number of workers (None = auto-determine)
        """
        self.worker_func = worker_func
        self.name = name
        self.min_workers = min_workers
        
        # Auto-determine max workers if not provided
        if max_workers is None:
            cpu_count = os.cpu_count() or 4
            # For I/O bound tasks, we can use a multiple of CPU count
            self.max_workers = cpu_count * 4
        else:
            self.max_workers = max_workers
            
        self.console = Console()
        self.workers = []
        self.active_workers = 0
        self.throughput_history = []
        self.last_adjustment_time = time.time()
        self.adjustment_interval = 5  # seconds between worker adjustments
        self.throughput_window = 5    # number of intervals to track for throughput
        
        # System load thresholds for scaling
        self.cpu_threshold_high = 80  # Scale down if CPU usage above 80%
        self.cpu_threshold_low = 30   # Scale up if CPU usage below 30%
        
        # Signals
        self.should_stop = asyncio.Event()
        
    async def start(self):
        """Start the worker pool with the initial number of workers."""
        self.console.print(f"[bold blue]Starting {self.name} worker pool with {self.min_workers} initial workers")
        
        # Start the auto-scaler
        asyncio.create_task(self.auto_scale())
        
        # Start initial workers
        for _ in range(self.min_workers):
            await self.add_worker()
            
    async def add_worker(self):
        """
        Add a new worker to the pool.
        
        Returns:
            bool: True if worker was added, False otherwise
        """
        if len(self.workers) >= self.max_workers:
            return False
            
        worker_task = asyncio.create_task(self.worker_func())
        self.workers.append(worker_task)
        self.active_workers += 1
        self.console.print(f"[green]{self.name}: Added worker. Now at {self.active_workers} workers")
        return True
        
    async def remove_worker(self):
        """
        Remove a worker from the pool.
        
        Returns:
            bool: True if worker was removed, False otherwise
        """
        if self.active_workers <= self.min_workers:
            return False
            
        # Don't actually cancel any tasks, just reduce the number of active workers
        # When workers complete naturally, they won't be replaced
        self.active_workers -= 1
        self.console.print(f"[yellow]{self.name}: Removed worker. Now at {self.active_workers} workers")
        return True
        
    async def auto_scale(self):
        """Automatically scale the number of workers based on system load."""
        while not self.should_stop.is_set():
            current_time = time.time()
            
            # Only adjust periodically
            if current_time - self.last_adjustment_time > self.adjustment_interval:
                # Get current system metrics
                cpu_percent = psutil.cpu_percent()
                
                # Calculate current throughput
                current_throughput = self.calculate_throughput()
                self.throughput_history.append(current_throughput)
                
                # Keep only the most recent measurements
                if len(self.throughput_history) > self.throughput_window:
                    self.throughput_history = self.throughput_history[-self.throughput_window:]
                
                # Decision logic for scaling
                if cpu_percent > self.cpu_threshold_high:
                    # CPU is too high, scale down
                    await self.remove_worker()
                elif cpu_percent < self.cpu_threshold_low:
                    # CPU is low, can we scale up?
                    if self.should_scale_up():
                        await self.add_worker()
                        
                self.last_adjustment_time = current_time
                
            # Wait before checking again
            await asyncio.sleep(1)
            
    def calculate_throughput(self):
        """
        Calculate current throughput (to be implemented by child classes).
        
        Returns:
            float: Throughput measure
        """
        # This is a placeholder - specific worker pools should override this
        return 0
        
    def should_scale_up(self):
        """
        Determine if we should scale up based on throughput history.
        
        Returns:
            bool: True if we should scale up
        """
        if len(self.throughput_history) < 2:
            return True
            
        # If throughput is increasing with more workers, keep scaling up
        return self.throughput_history[-1] > self.throughput_history[-2]
        
    async def stop(self):
        """Stop all workers in the pool."""
        self.should_stop.set()
        
        # Wait for all workers to complete
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
        
        self.console.print(f"[bold blue]{self.name} worker pool stopped")