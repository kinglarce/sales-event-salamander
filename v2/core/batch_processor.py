"""
Advanced Batch Processing
Provides efficient batch processing with monitoring, error handling, and progress tracking.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from enum import Enum
import time
from math import ceil
import threading
from queue import Queue, Empty

from .config import BatchConfig
from .logging import PerformanceLogger, get_logger

logger = get_logger(__name__)


class BatchStatus(Enum):
    """Batch processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class BatchResult:
    """Result of batch processing"""
    batch_id: int
    status: BatchStatus
    processed_count: int = 0
    failed_count: int = 0
    error_message: Optional[str] = None
    duration: float = 0.0
    retry_count: int = 0


@dataclass
class ProcessingStats:
    """Processing statistics"""
    total_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0
    total_processed: int = 0
    total_failed: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_batches == 0:
            return 0.0
        return self.completed_batches / self.total_batches
    
    @property
    def duration(self) -> float:
        """Calculate total duration"""
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def throughput(self) -> float:
        """Calculate items per second"""
        if self.duration == 0:
            return 0.0
        return self.total_processed / self.duration


class BatchProcessor:
    """Advanced batch processor with monitoring and error handling"""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self._performance_logger = PerformanceLogger(logger)
        self._stats = ProcessingStats()
        self._results: Dict[int, BatchResult] = {}
        self._lock = threading.Lock()
    
    def process_batches_sync(
        self,
        items: List[Any],
        processor_func: Callable[[List[Any]], Tuple[int, int]],
        batch_size: Optional[int] = None
    ) -> ProcessingStats:
        """Process items in batches synchronously"""
        batch_size = batch_size or self.config.batch_size
        batches = self._create_batches(items, batch_size)
        
        self._stats = ProcessingStats(total_batches=len(batches))
        self._performance_logger.start_timer("batch_processing")
        
        try:
            # Process batches in chunks
            for chunk_start in range(0, len(batches), self.config.chunk_size):
                chunk_end = min(chunk_start + self.config.chunk_size, len(batches))
                chunk_batches = batches[chunk_start:chunk_end]
                
                # Process chunk with thread pool
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    futures = []
                    
                    for i, batch in enumerate(chunk_batches):
                        batch_id = chunk_start + i
                        future = executor.submit(
                            self._process_single_batch_sync,
                            batch_id, batch, processor_func
                        )
                        futures.append(future)
                    
                    # Collect results
                    for future in as_completed(futures):
                        self._handle_batch_result(future.result())
            
            self._stats.end_time = time.time()
            self._performance_logger.end_timer(
                "batch_processing",
                total_batches=self._stats.total_batches,
                completed_batches=self._stats.completed_batches,
                failed_batches=self._stats.failed_batches,
                success_rate=self._stats.success_rate,
                throughput=self._stats.throughput
            )
            
            return self._stats
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            self._stats.end_time = time.time()
            raise
    
    async def process_batches_async(
        self,
        items: List[Any],
        processor_func: Callable[[List[Any]], Tuple[int, int]],
        batch_size: Optional[int] = None
    ) -> ProcessingStats:
        """Process items in batches asynchronously"""
        batch_size = batch_size or self.config.batch_size
        batches = self._create_batches(items, batch_size)
        
        self._stats = ProcessingStats(total_batches=len(batches))
        self._performance_logger.start_timer("async_batch_processing")
        
        try:
            # Process batches in chunks
            for chunk_start in range(0, len(batches), self.config.chunk_size):
                chunk_end = min(chunk_start + self.config.chunk_size, len(batches))
                chunk_batches = batches[chunk_start:chunk_end]
                
                # Create tasks for async processing
                tasks = []
                for i, batch in enumerate(chunk_batches):
                    batch_id = chunk_start + i
                    task = asyncio.create_task(
                        self._process_single_batch_async(batch_id, batch, processor_func)
                    )
                    tasks.append(task)
                
                # Wait for all tasks in chunk to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Batch processing error: {result}")
                    else:
                        self._handle_batch_result(result)
            
            self._stats.end_time = time.time()
            self._performance_logger.end_timer(
                "async_batch_processing",
                total_batches=self._stats.total_batches,
                completed_batches=self._stats.completed_batches,
                failed_batches=self._stats.failed_batches,
                success_rate=self._stats.success_rate,
                throughput=self._stats.throughput
            )
            
            return self._stats
            
        except Exception as e:
            logger.error(f"Async batch processing failed: {e}")
            self._stats.end_time = time.time()
            raise
    
    def _create_batches(self, items: List[Any], batch_size: int) -> List[List[Any]]:
        """Create batches from items"""
        batches = []
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batches.append(batch)
        return batches
    
    def _process_single_batch_sync(
        self,
        batch_id: int,
        batch: List[Any],
        processor_func: Callable[[List[Any]], Tuple[int, int]]
    ) -> BatchResult:
        """Process a single batch synchronously"""
        start_time = time.time()
        result = BatchResult(batch_id=batch_id, status=BatchStatus.PROCESSING)
        
        try:
            processed, failed = processor_func(batch)
            result.processed_count = processed
            result.failed_count = failed
            result.status = BatchStatus.COMPLETED
            
        except Exception as e:
            result.status = BatchStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Batch {batch_id} failed: {e}")
        
        finally:
            result.duration = time.time() - start_time
        
        return result
    
    async def _process_single_batch_async(
        self,
        batch_id: int,
        batch: List[Any],
        processor_func: Callable[[List[Any]], Tuple[int, int]]
    ) -> BatchResult:
        """Process a single batch asynchronously"""
        start_time = time.time()
        result = BatchResult(batch_id=batch_id, status=BatchStatus.PROCESSING)
        
        try:
            # Run processor function in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            processed, failed = await loop.run_in_executor(
                None, processor_func, batch
            )
            result.processed_count = processed
            result.failed_count = failed
            result.status = BatchStatus.COMPLETED
            
        except Exception as e:
            result.status = BatchStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Async batch {batch_id} failed: {e}")
        
        finally:
            result.duration = time.time() - start_time
        
        return result
    
    def _handle_batch_result(self, result: BatchResult) -> None:
        """Handle batch processing result"""
        with self._lock:
            self._results[result.batch_id] = result
            
            if result.status == BatchStatus.COMPLETED:
                self._stats.completed_batches += 1
                self._stats.total_processed += result.processed_count
                self._stats.total_failed += result.failed_count
            else:
                self._stats.failed_batches += 1
            
            logger.info(
                f"Batch {result.batch_id} {result.status.value}: "
                f"{result.processed_count} processed, {result.failed_count} failed "
                f"({result.duration:.2f}s)"
            )
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current processing progress"""
        with self._lock:
            return {
                "total_batches": self._stats.total_batches,
                "completed_batches": self._stats.completed_batches,
                "failed_batches": self._stats.failed_batches,
                "success_rate": self._stats.success_rate,
                "total_processed": self._stats.total_processed,
                "total_failed": self._stats.total_failed,
                "duration": self._stats.duration,
                "throughput": self._stats.throughput
            }
    
    def get_failed_batches(self) -> List[BatchResult]:
        """Get list of failed batches"""
        with self._lock:
            return [
                result for result in self._results.values()
                if result.status == BatchStatus.FAILED
            ]
    
    def retry_failed_batches(
        self,
        processor_func: Callable[[List[Any]], Tuple[int, int]],
        max_retries: int = 3
    ) -> ProcessingStats:
        """Retry failed batches"""
        failed_batches = self.get_failed_batches()
        
        if not failed_batches:
            logger.info("No failed batches to retry")
            return self._stats
        
        logger.info(f"Retrying {len(failed_batches)} failed batches")
        
        for batch_result in failed_batches:
            if batch_result.retry_count >= max_retries:
                logger.warning(f"Batch {batch_result.batch_id} exceeded max retries")
                continue
            
            # This would need the original batch data to retry
            # Implementation depends on how batches are stored
            batch_result.retry_count += 1
            batch_result.status = BatchStatus.RETRYING
        
        return self._stats


class ProgressTracker:
    """Track and report processing progress"""
    
    def __init__(self, total_items: int, report_interval: int = 100):
        self.total_items = total_items
        self.processed_items = 0
        self.failed_items = 0
        self.report_interval = report_interval
        self.start_time = time.time()
        self._lock = threading.Lock()
    
    def update(self, processed: int, failed: int = 0) -> None:
        """Update progress counters"""
        with self._lock:
            self.processed_items += processed
            self.failed_items += failed
            
            if self.processed_items % self.report_interval == 0:
                self._report_progress()
    
    def _report_progress(self) -> None:
        """Report current progress"""
        elapsed = time.time() - self.start_time
        rate = self.processed_items / elapsed if elapsed > 0 else 0
        remaining = (self.total_items - self.processed_items) / rate if rate > 0 else 0
        
        logger.info(
            f"Progress: {self.processed_items}/{self.total_items} "
            f"({self.processed_items/self.total_items*100:.1f}%) "
            f"- {rate:.1f} items/sec - ETA: {remaining:.1f}s"
        )
    
    def get_summary(self) -> Dict[str, Any]:
        """Get progress summary"""
        elapsed = time.time() - self.start_time
        return {
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "failed_items": self.failed_items,
            "success_rate": self.processed_items / self.total_items if self.total_items > 0 else 0,
            "elapsed_time": elapsed,
            "rate": self.processed_items / elapsed if elapsed > 0 else 0
        }
