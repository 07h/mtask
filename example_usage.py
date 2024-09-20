# example_usage.py

import asyncio
from datetime import datetime
import logging
from pydantic import BaseModel
from mTask import mTask

# Configure logging for the example script
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ExampleUsage")


# Define a Pydantic model for task data
class ExampleTaskData(BaseModel):
    param1: int
    param2: str


# Initialize the mTask instance
mtask = mTask(
    redis_url="redis://localhost:6379/0",
    enable_logging=False,
    retry_limit=3,
)


# Define a regular task using the @task decorator
@mtask.task(queue_name="default", concurrency=3)
async def example_task(data: ExampleTaskData):
    """
    Example task that simulates work by sleeping.

    Args:
        data (ExampleTaskData): Data model containing task parameters.
    """
    logger.info(
        f"🔥 Executing example_task with param1={data.param1}, param2='{data.param2}'"
    )
    await asyncio.sleep(10)  # Simulate a task taking some time
    logger.info("example_task completed.")


# Define an interval-based scheduled task using the @interval decorator
@mtask.interval(
    seconds=10,  # Set to 10 seconds
)
async def scheduled_interval_task():
    """
    Example interval-based scheduled task.
    """
    logger.info("🌀 Executing scheduled_interval_task.")
    await asyncio.sleep(1)  # Simulate task duration
    logger.info("scheduled_interval_task completed.")


@mtask.interval(
    seconds=20,  # Set to 10 seconds
)
async def scheduled_interval_task2():
    """
    Example interval-based scheduled task.
    """
    data = ExampleTaskData(
        param1=10, param2=f"⭐️ ADD TASK FROM SCHEDULED TASK: {datetime.now()}"
    )
    await mtask.add_task(
        queue_name="default",
        data_model=data,
    )


# Define a cron-based scheduled task using the @cron decorator
@mtask.cron(
    cron_expression="*/1 * * * *",  # Every 1 minute
)
async def scheduled_cron_task():
    """
    Example cron-based scheduled task.
    """
    logger.info("💎 Executing scheduled_cron_task.")
    await asyncio.sleep(1)  # Simulate task duration
    logger.info("scheduled_cron_task completed.")


# Main coroutine to run the mTask manager
async def main():
    """
    Main function to start the mTask manager.
    """

    # Start the mTask manager (connect, start workers, run scheduled tasks)
    manager_task = asyncio.create_task(mtask.run())

    # Wait a short moment to ensure the connection is established
    await asyncio.sleep(1)

    # Enqueue a manual task using add_task with Pydantic model
    data = ExampleTaskData(param1=10, param2="test")
    await mtask.add_task(
        queue_name="default",
        data_model=data,
    )
    logger.info("Enqueued a manual example_task.")

    # Keep the main coroutine running to allow scheduled tasks to execute
    await manager_task


if __name__ == "__main__":
    asyncio.run(main())
