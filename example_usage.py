# example_usage.py

import asyncio
import logging
from pydantic import BaseModel

# Import the mTask class from the mTask library
from mtask import mTask

# Create an instance of mTask
mtask = mTask(
    redis_url="redis://localhost:6379/9",
    retry_limit=3,
    log_level=logging.INFO,
    enable_logging=True,
)


# Define a Pydantic model for task data
class TaskData(BaseModel):
    value: int
    string: str = "default"


# Define a task using the @agent decorator with concurrency
@mtask.agent(queue_name="default", concurrency=2)
async def process_data(data: TaskData):
    """Process the data"""
    print(f"Processing data: {data.value}")
    await asyncio.sleep(1)  # Simulate some processing time

    # Enqueue a new task from within a task
    if data.value < 5:
        new_data = TaskData(value=data.value + 1)
        await mtask.add_task(queue_name="default", data_model=new_data)
        print(f"Enqueued new task with value: {new_data.value}")


# Define a scheduled task using the @interval decorator
@mtask.interval(seconds=10)
async def scheduled_task():
    """Scheduled task that runs every 10 seconds"""
    print("Scheduled task is running...")
    # Enqueue a task
    data = TaskData(value=0)
    await mtask.add_task(queue_name="default", data_model=data)


# Define another scheduled task using the @cron decorator
@mtask.cron(cron_expression="*/1 * * * *")  # Every minute
async def scheduled_cron_task():
    """Scheduled task that runs every minute"""
    print("Scheduled cron task is running...")
    # Pause the queue for 30 seconds
    await mtask.pause_queue(queue_name="default", duration=30)
    print("Paused the 'default' queue for 30 seconds.")


async def main():
    # Start the mTask manager
    manager_task = asyncio.create_task(mtask.run())

    # Keep the program running indefinitely
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        print("Shutting down...")
    finally:
        # Stop the manager
        manager_task.cancel()
        await asyncio.gather(manager_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
