"""
Custom Pipeline Example
Demonstrates how to create and use custom pipelines with the v2 system.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pipeline import PipelineBuilder, PipelineManager
from core import get_config, get_logger

logger = get_logger(__name__)


async def static_data_ingestion():
    """Example static data ingestion function"""
    logger.info("Running static data ingestion...")
    await asyncio.sleep(2)  # Simulate work
    logger.info("Static data ingestion completed")
    return {"configs_loaded": 10, "schemas_processed": 3}


async def events_tickets_ingestion():
    """Example events and tickets ingestion function"""
    logger.info("Running events and tickets ingestion...")
    await asyncio.sleep(5)  # Simulate work
    logger.info("Events and tickets ingestion completed")
    return {"events_processed": 5, "tickets_processed": 1000}


async def coupon_ingestion():
    """Example coupon ingestion function"""
    logger.info("Running coupon ingestion...")
    await asyncio.sleep(3)  # Simulate work
    logger.info("Coupon ingestion completed")
    return {"coupons_processed": 500, "series_processed": 10}


async def age_groups_ingestion():
    """Example age groups ingestion function"""
    logger.info("Running age groups ingestion...")
    await asyncio.sleep(2)  # Simulate work
    logger.info("Age groups ingestion completed")
    return {"age_groups_processed": 50}


async def analytics_processing():
    """Example analytics processing function"""
    logger.info("Running analytics processing...")
    await asyncio.sleep(1)  # Simulate work
    logger.info("Analytics processing completed")
    return {"reports_generated": 5}


def create_sequential_pipeline():
    """Create a sequential pipeline"""
    return (PipelineBuilder("sequential_example", "Sequential pipeline example")
            .add_step("static_data", static_data_ingestion)
            .add_step_with_dependencies("events_tickets", events_tickets_ingestion, ["static_data"])
            .add_step_with_dependencies("coupons", coupon_ingestion, ["events_tickets"])
            .add_step_with_dependencies("age_groups", age_groups_ingestion, ["events_tickets"])
            .add_step_with_dependencies("analytics", analytics_processing, ["events_tickets", "age_groups"])
            .set_stop_on_failure(True)
            .build())


def create_parallel_pipeline():
    """Create a parallel pipeline"""
    return (PipelineBuilder("parallel_example", "Parallel pipeline example")
            .add_step("static_data", static_data_ingestion)
            .add_step_with_dependencies("events_tickets", events_tickets_ingestion, ["static_data"])
            .add_step_with_dependencies("coupons", coupon_ingestion, ["events_tickets"])
            .add_step_with_dependencies("age_groups", age_groups_ingestion, ["events_tickets"])
            .add_step_with_dependencies("analytics", analytics_processing, ["events_tickets", "age_groups"])
            .enable_parallel_execution(max_parallel_steps=3)
            .set_stop_on_failure(True)
            .build())


def create_conditional_pipeline():
    """Create a pipeline with conditional steps"""
    def should_run_analytics():
        """Condition for running analytics"""
        # In a real scenario, this would check some condition
        return True
    
    return (PipelineBuilder("conditional_example", "Conditional pipeline example")
            .add_step("static_data", static_data_ingestion)
            .add_step_with_dependencies("events_tickets", events_tickets_ingestion, ["static_data"])
            .add_step_with_dependencies("coupons", coupon_ingestion, ["events_tickets"])
            .add_step_with_dependencies("age_groups", age_groups_ingestion, ["events_tickets"])
            .add_conditional_step("analytics", analytics_processing, should_run_analytics)
            .set_stop_on_failure(True)
            .build())


async def run_pipeline_example():
    """Run pipeline examples"""
    logger.info("Starting pipeline examples...")
    
    # Example 1: Sequential pipeline
    logger.info("\n=== Sequential Pipeline Example ===")
    sequential_pipeline = create_sequential_pipeline()
    sequential_manager = PipelineManager(sequential_pipeline)
    sequential_results = await sequential_manager.execute_pipeline()
    
    logger.info(f"Sequential pipeline completed: {len(sequential_results)} steps")
    for result in sequential_results:
        logger.info(f"  {result.step_name}: {result.status.value} ({result.duration:.2f}s)")
    
    # Example 2: Parallel pipeline
    logger.info("\n=== Parallel Pipeline Example ===")
    parallel_pipeline = create_parallel_pipeline()
    parallel_manager = PipelineManager(parallel_pipeline)
    parallel_results = await parallel_manager.execute_pipeline()
    
    logger.info(f"Parallel pipeline completed: {len(parallel_results)} steps")
    for result in parallel_results:
        logger.info(f"  {result.step_name}: {result.status.value} ({result.duration:.2f}s)")
    
    # Example 3: Conditional pipeline
    logger.info("\n=== Conditional Pipeline Example ===")
    conditional_pipeline = create_conditional_pipeline()
    conditional_manager = PipelineManager(conditional_pipeline)
    conditional_results = await conditional_manager.execute_pipeline()
    
    logger.info(f"Conditional pipeline completed: {len(conditional_results)} steps")
    for result in conditional_results:
        logger.info(f"  {result.step_name}: {result.status.value} ({result.duration:.2f}s)")
    
    # Print summaries
    logger.info("\n=== Pipeline Summaries ===")
    logger.info(f"Sequential: {sequential_manager.get_summary()}")
    logger.info(f"Parallel: {parallel_manager.get_summary()}")
    logger.info(f"Conditional: {conditional_manager.get_summary()}")


if __name__ == "__main__":
    asyncio.run(run_pipeline_example())
