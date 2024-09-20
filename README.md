# mTask

The `mTask` library offers a simple yet professional solution for managing task queues and scheduling using Redis and asyncio, enhanced with greenlet-based workers for concurrency. By following best practices and ensuring flexibility, `mTask` is suitable for production environments and can be easily integrated into various projects.

**Key Features:**

- **Ease of Use:** Register tasks using intuitive decorators.
- **Asynchronous Processing:** Leverage asyncio for efficient I/O-bound task handling.
- **Concurrency Control:** Manage the number of concurrent workers using greenlets.
- **Flexible Scheduling:** Support for both interval-based and cron-based task scheduling.
- **Reliable Task Execution:** Automatic retries for failed tasks with configurable limits.

Feel free to extend and customize `mTask` to fit your project's specific needs. If you encounter any issues or have suggestions for improvements, consider contributing to the project or reaching out for support.

## Quick Start

```python
import asyncio
from mTask import mTaskQueue, mScheduler, mWorker, task, scheduled, cron

# Initialize components
task_queue = mTaskQueue(redis_url="redis://localhost", queue_name="task_queue")
scheduler = mScheduler()
worker = mWorker(task_queue, concurrency=5, retry_limit=3)

@scheduled(interval=60)  # Run every 60 seconds
async def periodic_task():
    print("Executing periodic task...")

@cron("*/5 * * * *")  # Run every 5 minutes
async def cron_task():
    print("Executing cron task...")

@task()
async def example_task(param1, param2):
    print(f"Executing example_task with parameters: {param1}, {param2}")
    await asyncio.sleep(2)
    print("example_task completed.")

async def main():
    await task_queue.connect()
    scheduler_task = asyncio.create_task(scheduler.start())
    worker_task = asyncio.create_task(worker.start())

    # Enqueue a task
    await example_task(10, param2="test")

    try:
        await asyncio.gather(scheduler_task, worker_task)
    except KeyboardInterrupt:
        print("Shutting down...")
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())

```

## Documentation

### `mTask`

#### **1. `decorators.py`**

Decorators for registering tasks.

- `@scheduled(interval: float)`
- `@cron(cron_expression: str)`
- `@task()`

#### **2. `scheduler.py`**

Scheduler for running tasks at intervals and cron schedules.

#### **3. `queue.py`**

Manages the task queue using Redis.

#### **4. `worker.py`**

Worker processes for executing tasks using greenlets.



## Example Usage

Create a separate project that uses the `mTask` library.

### Project Structure

```
my_project/
├── tasks.py
├── main.py
├── requirements.txt
└── README.md
```

### `my_project/tasks.py`

Define your own tasks using the `mTask` decorators.

```python
# my_project/tasks.py

from mTask import task, scheduled, cron
import asyncio
import logging

logger = logging.getLogger(__name__)

@scheduled(interval=30)  # Run every 30 seconds
async def my_periodic_task():
    logger.info("Executing my periodic task...")
    # Your task logic here
    await asyncio.sleep(1)
    logger.info("My periodic task completed.")

@cron("0 * * * *")  # Run every hour
async def my_cron_task():
    logger.info("Executing my cron task...")
    # Your task logic here
    await asyncio.sleep(1)
    logger.info("My cron task completed.")

@task()
async def my_example_task(data):
    logger.info(f"Executing my_example_task with data: {data}")
    await asyncio.sleep(2)
    logger.info("my_example_task completed.")
```

### `my_project/main.py`

Initialize and run the `mTask` system.

```python
# my_project/main.py

import asyncio
import logging
from mTask import mTaskQueue, mScheduler, mWorker
import tasks  # Your tasks module

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Initialize components
    task_queue = mTaskQueue(redis_url="redis://localhost", queue_name="my_task_queue")
    scheduler = mScheduler()
    worker = mWorker(task_queue, concurrency=5, retry_limit=3)

    # Register tasks (assumes decorators have registered them upon import)
    
    # Connect to Redis
    await task_queue.connect()

    # Start scheduler and worker
    scheduler_task = asyncio.create_task(scheduler.start())
    worker_task = asyncio.create_task(worker.start())

    # Enqueue a task manually
    await tasks.my_example_task(data="Sample Data")

    try:
        await asyncio.gather(scheduler_task, worker_task)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

### `my_project/requirements.txt`

List project dependencies.

```txt
# my_project/requirements.txt

mTask
aioredis>=2.0.0
croniter>=1.0.15
greenlet>=1.1.2
```

### Running the Example

1. **Install the `mTask` library locally:**

   Navigate to the `mTask` library directory and install it.

   ```bash
   cd mTask
   pip install .
   ```

2. **Set up the user's project:**

   Navigate to `my_project` and install dependencies.

   ```bash
   cd my_project
   pip install -r requirements.txt
   ```

3. **Ensure Redis is running:**

   Make sure Redis is running on `localhost:6379`. You can start it using Docker:

   ```bash
   docker run -p 6379:6379 redis
   ```

4. **Run the main script:**

   ```bash
   python main.py
   ```

---

## Testing

Create tests to ensure the `mTask` library functions as expected.

### `tests/test_mTask.py`

Example tests using `pytest` and `pytest-asyncio`.

```python
# tests/test_mTask.py

import pytest
import asyncio
from mTask import mTaskQueue

@pytest.fixture
async def task_queue():
    tq = mTaskQueue(redis_url="redis://localhost", queue_name="test_queue")
    await tq.connect()
    yield tq
    await tq.disconnect()

@pytest.mark.asyncio
async def test_enqueue_dequeue(task_queue):
    task_id = await task_queue.enqueue("test_task", args=[1, 2], kwargs={"key": "value"})
    task = await task_queue.dequeue()
    assert task["id"] == task_id
    assert task["name"] == "test_task"
    assert task["args"] == [1, 2]
    assert task["kwargs"] == {"key": "value"}
    assert task["status"] == "pending"

@pytest.mark.asyncio
async def test_requeue(task_queue):
    task_id = await task_queue.enqueue("test_task")
    task = await task_queue.dequeue()
    await task_queue.requeue(task)
    task_requeued = await task_queue.dequeue()
    assert task_requeued["id"] == task_id
    assert task_requeued["status"] == "pending"

@pytest.mark.asyncio
async def test_mark_completed(task_queue):
    task_id = await task_queue.enqueue("test_task")
    await task_queue.mark_completed(task_id)
    # Implement logic to verify completion if storage is implemented
```

---

## Additional Enhancements and Best Practices

### Logging

Utilize Python's `logging` module for comprehensive logging. Configure log handlers and formats as needed.

### Configuration Management

Allow users to configure the library via configuration files or environment variables. Libraries like `pydantic` or `python-dotenv` can help manage configurations.

### Error Handling and Retries

Ensure robust error handling within tasks and the worker loop. Implement exponential backoff for retries to avoid overwhelming the system.

### Documentation and Docstrings

Provide thorough docstrings for all classes and methods to facilitate understanding and maintenance.

### Security Considerations

Secure Redis connections, especially in production environments. Use authentication and encryption as necessary.

### Extensibility

Allow users to extend the library with additional features like task dependencies, prioritization, and more.


