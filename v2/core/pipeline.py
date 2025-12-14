"""
Pipeline Configuration and Management
Provides a configurable pipeline system for running ingestion scripts in order.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import yaml

from core import get_logger, PerformanceLogger

logger = get_logger(__name__)


class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineStep:
    """A single step in the pipeline"""
    name: str
    function: Callable
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    retry_count: int = 0
    max_retries: int = 3
    timeout: Optional[float] = None
    depends_on: List[str] = field(default_factory=list)
    condition: Optional[Callable] = None


@dataclass
class PipelineResult:
    """Result of a pipeline step execution"""
    step_name: str
    status: PipelineStatus
    duration: float
    success: bool
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    retry_count: int = 0


@dataclass
class PipelineConfig:
    """Configuration for a pipeline"""
    name: str
    description: str
    steps: List[PipelineStep]
    parallel_execution: bool = False
    max_parallel_steps: int = 3
    stop_on_failure: bool = True
    retry_failed_steps: bool = True
    max_retries: int = 3


class PipelineManager:
    """Manages pipeline execution with monitoring and error handling"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(self.logger)
        self.results: List[PipelineResult] = []
        self._cancelled = False
    
    async def execute_pipeline(self) -> List[PipelineResult]:
        """Execute the pipeline"""
        self.performance_logger.start_timer(f"pipeline_{self.config.name}")
        
        try:
            self.logger.info(f"Starting pipeline: {self.config.name}")
            self.logger.info(f"Description: {self.config.description}")
            self.logger.info(f"Steps: {len(self.config.steps)}")
            self.logger.info(f"Parallel execution: {self.config.parallel_execution}")
            
            if self.config.parallel_execution:
                await self._execute_parallel()
            else:
                await self._execute_sequential()
            
            self.performance_logger.end_timer(f"pipeline_{self.config.name}")
            return self.results
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self.performance_logger.end_timer(f"pipeline_{self.config.name}", success=False)
            raise
    
    async def _execute_sequential(self):
        """Execute pipeline steps sequentially"""
        for step in self.config.steps:
            if self._cancelled:
                break
            
            if not step.enabled:
                self.logger.info(f"Skipping disabled step: {step.name}")
                continue
            
            # Check dependencies
            if not self._check_dependencies(step):
                self.logger.error(f"Dependencies not met for step: {step.name}")
                if self.config.stop_on_failure:
                    break
                continue
            
            # Check condition
            if step.condition and not step.condition():
                self.logger.info(f"Condition not met for step: {step.name}")
                continue
            
            # Execute step
            result = await self._execute_step(step)
            self.results.append(result)
            
            if not result.success and self.config.stop_on_failure:
                self.logger.error(f"Step {step.name} failed, stopping pipeline")
                break
    
    async def _execute_parallel(self):
        """Execute pipeline steps in parallel where possible"""
        # Group steps by dependencies
        step_groups = self._group_steps_by_dependencies()
        
        for group in step_groups:
            if self._cancelled:
                break
            
            # Execute steps in this group in parallel
            tasks = []
            for step in group:
                if not step.enabled:
                    continue
                
                if not self._check_dependencies(step):
                    self.logger.error(f"Dependencies not met for step: {step.name}")
                    continue
                
                if step.condition and not step.condition():
                    self.logger.info(f"Condition not met for step: {step.name}")
                    continue
                
                task = asyncio.create_task(self._execute_step(step))
                tasks.append(task)
            
            if tasks:
                # Wait for all tasks in this group to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Step execution failed: {result}")
                    else:
                        self.results.append(result)
                
                # Check if we should stop on failure
                if self.config.stop_on_failure:
                    failed_steps = [r for r in results if not r.success]
                    if failed_steps:
                        self.logger.error(f"Failed steps in group: {[r.step_name for r in failed_steps]}")
                        break
    
    def _group_steps_by_dependencies(self) -> List[List[PipelineStep]]:
        """Group steps by their dependencies for parallel execution"""
        groups = []
        remaining_steps = self.config.steps.copy()
        
        while remaining_steps:
            # Find steps with no unmet dependencies
            current_group = []
            for step in remaining_steps[:]:
                if self._check_dependencies(step):
                    current_group.append(step)
                    remaining_steps.remove(step)
            
            if not current_group:
                # If no steps can be executed, add remaining steps to a group
                # This prevents infinite loops
                groups.append(remaining_steps)
                break
            
            groups.append(current_group)
        
        return groups
    
    def _check_dependencies(self, step: PipelineStep) -> bool:
        """Check if step dependencies are met"""
        for dep_name in step.depends_on:
            # Find the dependency result
            dep_result = None
            for result in self.results:
                if result.step_name == dep_name:
                    dep_result = result
                    break
            
            if not dep_result or not dep_result.success:
                return False
        
        return True
    
    async def _execute_step(self, step: PipelineStep) -> PipelineResult:
        """Execute a single pipeline step"""
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Executing step: {step.name}")
            
            # Execute the step function
            if step.timeout:
                result = await asyncio.wait_for(
                    step.function(*step.args, **step.kwargs),
                    timeout=step.timeout
                )
            else:
                result = await step.function(*step.args, **step.kwargs)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"Step {step.name} completed in {duration:.2f}s")
            
            return PipelineResult(
                step_name=step.name,
                status=PipelineStatus.COMPLETED,
                duration=duration,
                success=True,
                details={"result": result} if result else None
            )
            
        except asyncio.TimeoutError:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Step {step.name} timed out after {step.timeout}s"
            self.logger.error(error_msg)
            
            return PipelineResult(
                step_name=step.name,
                status=PipelineStatus.FAILED,
                duration=duration,
                success=False,
                error_message=error_msg
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Step {step.name} failed: {str(e)}"
            self.logger.error(error_msg)
            
            return PipelineResult(
                step_name=step.name,
                status=PipelineStatus.FAILED,
                duration=duration,
                success=False,
                error_message=error_msg
            )
    
    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary"""
        total_duration = sum(result.duration for result in self.results)
        successful_steps = sum(1 for result in self.results if result.success)
        failed_steps = len(self.results) - successful_steps
        
        return {
            "pipeline_name": self.config.name,
            "total_steps": len(self.results),
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "total_duration": total_duration,
            "success_rate": successful_steps / len(self.results) if self.results else 0,
            "results": [
                {
                    "step_name": result.step_name,
                    "status": result.status.value,
                    "duration": result.duration,
                    "success": result.success,
                    "error": result.error_message
                }
                for result in self.results
            ]
        }


class PipelineBuilder:
    """Builder for creating pipeline configurations"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.steps: List[PipelineStep] = []
        self.parallel_execution = False
        self.max_parallel_steps = 3
        self.stop_on_failure = True
        self.retry_failed_steps = True
        self.max_retries = 3
    
    def add_step(self, name: str, function: Callable, *args, **kwargs) -> 'PipelineBuilder':
        """Add a step to the pipeline"""
        step = PipelineStep(
            name=name,
            function=function,
            args=list(args),
            kwargs=kwargs
        )
        self.steps.append(step)
        return self
    
    def add_step_with_dependencies(self, name: str, function: Callable, 
                                 depends_on: List[str], *args, **kwargs) -> 'PipelineBuilder':
        """Add a step with dependencies"""
        step = PipelineStep(
            name=name,
            function=function,
            args=list(args),
            kwargs=kwargs,
            depends_on=depends_on
        )
        self.steps.append(step)
        return self
    
    def add_conditional_step(self, name: str, function: Callable, 
                           condition: Callable, *args, **kwargs) -> 'PipelineBuilder':
        """Add a step with a condition"""
        step = PipelineStep(
            name=name,
            function=function,
            args=list(args),
            kwargs=kwargs,
            condition=condition
        )
        self.steps.append(step)
        return self
    
    def enable_parallel_execution(self, max_parallel_steps: int = 3) -> 'PipelineBuilder':
        """Enable parallel execution"""
        self.parallel_execution = True
        self.max_parallel_steps = max_parallel_steps
        return self
    
    def disable_parallel_execution(self) -> 'PipelineBuilder':
        """Disable parallel execution"""
        self.parallel_execution = False
        return self
    
    def set_stop_on_failure(self, stop: bool) -> 'PipelineBuilder':
        """Set whether to stop on failure"""
        self.stop_on_failure = stop
        return self
    
    def set_retry_policy(self, retry_failed: bool, max_retries: int = 3) -> 'PipelineBuilder':
        """Set retry policy"""
        self.retry_failed_steps = retry_failed
        self.max_retries = max_retries
        return self
    
    def build(self) -> PipelineConfig:
        """Build the pipeline configuration"""
        return PipelineConfig(
            name=self.name,
            description=self.description,
            steps=self.steps,
            parallel_execution=self.parallel_execution,
            max_parallel_steps=self.max_parallel_steps,
            stop_on_failure=self.stop_on_failure,
            retry_failed_steps=self.retry_failed_steps,
            max_retries=self.max_retries
        )


def load_pipeline_from_config(config_path: str) -> PipelineConfig:
    """Load pipeline configuration from file"""
    with open(config_path, 'r') as f:
        if config_path.endswith('.yaml') or config_path.endswith('.yml'):
            config_data = yaml.safe_load(f)
        else:
            config_data = json.load(f)
    
    # Convert config data to PipelineConfig
    steps = []
    for step_data in config_data.get('steps', []):
        step = PipelineStep(
            name=step_data['name'],
            function=step_data['function'],  # This would need to be resolved
            args=step_data.get('args', []),
            kwargs=step_data.get('kwargs', {}),
            enabled=step_data.get('enabled', True),
            retry_count=step_data.get('retry_count', 0),
            max_retries=step_data.get('max_retries', 3),
            timeout=step_data.get('timeout'),
            depends_on=step_data.get('depends_on', []),
            condition=step_data.get('condition')  # This would need to be resolved
        )
        steps.append(step)
    
    return PipelineConfig(
        name=config_data['name'],
        description=config_data.get('description', ''),
        steps=steps,
        parallel_execution=config_data.get('parallel_execution', False),
        max_parallel_steps=config_data.get('max_parallel_steps', 3),
        stop_on_failure=config_data.get('stop_on_failure', True),
        retry_failed_steps=config_data.get('retry_failed_steps', True),
        max_retries=config_data.get('max_retries', 3)
    )
