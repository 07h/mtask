# mTask.py

import asyncio
import inspect
import json
import logging
import uuid
from functools import wraps
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, List

from croniter import croniter
import redis.asyncio as redis
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mTask")


class TaskQueue:
    """
    Manages task queues using Redis.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        """
        Initialize the TaskQueue.

        Args:
            redis_url (str): Redis connection URL.
        """
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None

    async def connect(self):
        """Establish a connection to Redis."""
        try:
            self.redis = redis.from_url(
                self.redis_url, encoding="utf-8", decode_responses=True
            )
            await self.redis.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.exception(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self):
        """Close the connection to Redis."""
        if self.redis:
            await self.redis.close()
            logger.info("Disconnected from Redis")

    async def enqueue(
        self,
        queue_name: str,
        kwargs: Dict[str, Any] = None,
    ) -> str:
        """Add a task to the specified Redis queue."""
        if not self.redis:
            raise ConnectionError("Redis is not connected.")
        task = {
            "id": str(uuid.uuid4()),
            "name": queue_name,  # 'name' соответствует 'queue_name'
            "kwargs": kwargs or {},
            "status": "pending",
            "retry_count": 0,
        }
        try:
            await self.redis.rpush(queue_name, json.dumps(task))
            logger.debug(f"Enqueued task {task['id']} to queue '{queue_name}'")
            return task["id"]
        except Exception as e:
            logger.exception(f"Failed to enqueue task: {e}")
            raise

    async def dequeue(self, queue_name: str = "default") -> Optional[Dict[str, Any]]:
        """
        Retrieve a task from the specified Redis queue.

        Args:
            queue_name (str, optional): Name of the Redis queue.

        Returns:
            Optional[Dict[str, Any]]: The task data or None if the queue is empty.
        """
        if not self.redis:
            logger.error("Redis is not connected.")
            return None

        try:
            task_json = await self.redis.lpop(queue_name)
            if task_json:
                task = json.loads(task_json)
                logger.debug(
                    f"Dequeued task {task['id']} with name '{task['name']}' from queue '{queue_name}'"
                )
                return task
            return None
        except Exception as e:
            logger.exception(f"Failed to dequeue task from queue '{queue_name}': {e}")
            return None

    async def requeue(self, task: Dict[str, Any], queue_name: str = "default") -> None:
        """
        Re-add a task to the specified Redis queue for retrying.

        Args:
            task (Dict[str, Any]): The task data.
            queue_name (str, optional): Name of the Redis queue.
        """
        task["status"] = "pending"
        try:
            await self.redis.rpush(queue_name, json.dumps(task))
            logger.debug(
                f"Requeued task {task['id']} to queue '{queue_name}' (retry {task['retry_count']})"
            )
        except Exception as e:
            logger.exception(f"Failed to requeue task '{task['id']}': {e}")

    async def mark_completed(self, task_id: str) -> None:
        """
        Mark a task as completed.

        Args:
            task_id (str): Unique identifier of the task.
        """
        logger.info(f"Task {task_id} marked as completed.")
        # Implement storage of completed tasks if necessary


class Worker:
    """
    Processes tasks from a specific Redis queue.
    """

    def __init__(
        self,
        task_queue: TaskQueue,
        task_registry: Dict[str, Dict[str, Any]],
        retry_limit: int = 3,
        queue_name: str = "default",
        semaphore: asyncio.Semaphore = None,  # Добавлен параметр semaphore
    ):
        """
        Initialize the Worker.

        Args:
            task_queue (TaskQueue): Instance of TaskQueue.
            task_registry (Dict[str, Dict[str, Any]]): Registry of task functions and their concurrency.
            retry_limit (int, optional): Max retry attempts for failed tasks.
            queue_name (str, optional): Name of the Redis queue to process.
            semaphore (asyncio.Semaphore, optional): Semaphore to limit concurrency.
        """
        self.task_queue = task_queue
        self.retry_limit = retry_limit
        self.queue_name = queue_name
        self.task_registry = task_registry
        self.semaphore = semaphore if semaphore else asyncio.Semaphore(1)
        self._running = False
        self._workers: list[asyncio.Task] = []
        self.logger = logging.getLogger(f"Worker-{queue_name}")

        self.logger.debug(
            f"Worker initialized with concurrency={self.semaphore._value} for queue '{queue_name}'"
        )

    async def start(self):
        """Start the worker coroutines based on concurrency."""
        if self._running:
            self.logger.warning("Worker is already running.")
            return

        self._running = True
        concurrency = self.semaphore._value
        self.logger.info(
            f"Starting {concurrency} worker(s) for queue '{self.queue_name}'"
        )

        for i in range(concurrency):
            worker_task = asyncio.create_task(self._worker_loop(worker_id=i + 1))
            self._workers.append(worker_task)

    async def stop(self):
        """Stop all worker coroutines."""
        if not self._running:
            self.logger.warning("Worker is not running.")
            return

        self._running = False
        self.logger.info("Stopping workers...")
        for worker_task in self._workers:
            worker_task.cancel()

        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self.logger.info("Workers stopped.")

    async def _worker_loop(self, worker_id: int):
        """Main loop for each worker coroutine."""
        while self._running:
            try:
                task = await self.task_queue.dequeue(queue_name=self.queue_name)
                if task:
                    self.logger.info(
                        f"Worker {worker_id}: Dequeued task {task['id']} from queue '{self.queue_name}'"
                    )
                    await self.process_task(task, worker_id)
                else:
                    await asyncio.sleep(1)  # Prevent busy waiting
            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_id}: Received shutdown signal.")
                break
            except Exception as e:
                self.logger.exception(f"Worker {worker_id}: Unexpected error: {e}")

    async def process_task(self, task: Dict[str, Any], worker_id: int):
        """
        Process a single task.

        Args:
            task (Dict[str, Any]): The task data.
            worker_id (int): Identifier for the worker.
        """
        queue_name = task["name"]  # 'name' соответствует 'queue_name'
        kwargs = task.get("kwargs", {})
        func = self.task_registry.get(queue_name, {}).get("func")  # Получаем функцию
        if not func:
            raise ValueError(f"Task function for queue '{queue_name}' not found.")
        try:
            self.logger.info(
                f"Worker {worker_id}: Executing task {task['id']} from queue '{queue_name}'"
            )

            # Проверяем, ожидает ли функция Pydantic модель
            sig = inspect.signature(func)
            expects_model = False
            model_class: Optional[BaseModel] = None
            for name, param in sig.parameters.items():
                if (
                    param.annotation
                    and inspect.isclass(param.annotation)
                    and issubclass(param.annotation, BaseModel)
                ):
                    expects_model = True
                    model_class = param.annotation
                    break

            async with self.semaphore:  # Используем семафор для ограничения concurrency
                if expects_model and model_class:
                    # Десериализуем kwargs в Pydantic модель
                    data_model = model_class(**kwargs)
                    self.logger.info(
                        f"Worker {worker_id}: Executing task {task['id']} - '{queue_name}' with data={data_model}"
                    )
                    await func(data=data_model)
                else:
                    self.logger.info(
                        f"Worker {worker_id}: Executing task {task['id']} - '{queue_name}' with kwargs={kwargs}"
                    )
                    await func(**kwargs)

                await self.task_queue.mark_completed(task["id"])
                self.logger.info(
                    f"Worker {worker_id}: Task {task['id']} completed successfully."
                )
        except Exception as e:
            self.logger.exception(
                f"Worker {worker_id}: Error executing task {task['id']}: {e}"
            )
            if task["retry_count"] < self.retry_limit:
                task["retry_count"] += 1
                await self.task_queue.requeue(task, queue_name=self.queue_name)
                self.logger.info(
                    f"Worker {worker_id}: Requeued task {task['id']} (retry {task['retry_count']})"
                )
            else:
                self.logger.error(
                    f"Worker {worker_id}: Task {task['id']} failed after {self.retry_limit} retries."
                )


class ScheduledTask:
    def __init__(
        self,
        func: Callable,
        interval: Optional[int] = None,
        cron_expression: Optional[str] = None,
    ):
        self.func = func
        self.interval = interval
        self.cron_expression = cron_expression
        self.last_run = None
        self.next_run = self._get_next_run()
        self.is_running = False

    def _get_next_run(self) -> datetime:
        now = datetime.now()
        if self.interval is not None:
            return now + timedelta(seconds=self.interval)
        elif self.cron_expression is not None:
            cron = croniter(self.cron_expression, now)
            return cron.get_next(datetime)
        else:
            raise ValueError("Either interval or cron_expression must be set")

    async def run(self):
        if self.is_running:
            return
        self.is_running = True
        try:
            await self.func()
        except Exception as e:
            logger.exception(f"Error in scheduled task {self.func.__name__}: {e}")
        finally:
            self.last_run = datetime.now()
            self.next_run = self._get_next_run()
            self.is_running = False

    def should_run(self) -> bool:
        return datetime.now() >= self.next_run and not self.is_running


class mTask:
    """
    Main class to manage tasks, scheduling, and workers.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        retry_limit: int = 3,
    ):
        self.task_queue = TaskQueue(redis_url=redis_url)
        self.task_registry: Dict[str, Dict[str, Any]] = {}
        self.workers: Dict[str, Worker] = {}
        self.scheduled_tasks: List[ScheduledTask] = []
        self.retry_limit = retry_limit
        self.semaphores: Dict[str, asyncio.Semaphore] = {}
        logger.debug("Initialized mTask instance.")

    def task(
        self,
        queue_name: str = "default",
        concurrency: int = 1,
    ):
        """
        Decorator to define and register a task.

        Args:
            queue_name (str, optional): Redis queue name to enqueue the task.
            concurrency (int, optional): Max concurrent executions for the task.

        Returns:
            Callable: The decorator.
        """

        def decorator(func: Callable):
            # Register the function and concurrency in the task_registry
            self.task_registry[queue_name] = {"func": func, "concurrency": concurrency}

            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Determine if the function expects a Pydantic model
                sig = inspect.signature(func)
                model = None
                for name, param in sig.parameters.items():
                    if (
                        param.annotation
                        and inspect.isclass(param.annotation)
                        and issubclass(param.annotation, BaseModel)
                    ):
                        model = param.annotation
                        break

                if model:
                    try:
                        data = model(**kwargs).dict()
                    except Exception as e:
                        logger.error(f"Error parsing data with model {model}: {e}")
                        raise

                    # Enqueue the task without passing concurrency
                    task_id = await self.task_queue.enqueue(
                        queue_name=queue_name,
                        kwargs=data,
                    )
                    logger.info(
                        f"Task for queue '{queue_name}' enqueued with ID {task_id}"
                    )
                else:
                    # No Pydantic model; pass kwargs directly
                    task_id = await self.task_queue.enqueue(
                        queue_name=queue_name,
                        kwargs=kwargs,
                    )
                    logger.info(
                        f"Task for queue '{queue_name}' enqueued with ID {task_id}"
                    )

            return wrapper

        return decorator

    def interval(
        self,
        seconds: int = 60,
    ):
        """
        Decorator to schedule a task to run at fixed intervals.

        Args:
            seconds (int, optional): Interval in seconds between executions.

        Returns:
            Callable: The decorator.
        """

        def decorator(func: Callable):
            scheduled_task = ScheduledTask(func, interval=seconds)
            self.scheduled_tasks.append(scheduled_task)
            logger.debug(
                f"Scheduled interval task '{func.__name__}' every {seconds} seconds."
            )
            return func

        return decorator

    def cron(
        self,
        cron_expression: str,
    ):
        """
        Decorator to schedule a task based on a cron expression.

        Args:
            cron_expression (str): Cron expression defining the schedule.

        Returns:
            Callable: The decorator.
        """

        def decorator(func: Callable):
            scheduled_task = ScheduledTask(func, cron_expression=cron_expression)
            self.scheduled_tasks.append(scheduled_task)
            logger.debug(
                f"Scheduled cron task '{func.__name__}' with cron expression '{cron_expression}'."
            )
            return func

        return decorator

    async def add_task(
        self,
        queue_name: str,
        data_model: BaseModel,
    ) -> str:
        """
        Manually add a task to the queue with Pydantic model data.

        Args:
            queue_name (str): Name of the Redis queue.
            data_model (BaseModel): Pydantic model containing task parameters.

        Returns:
            str: Unique identifier of the enqueued task.
        """
        task_id = await self.task_queue.enqueue(
            queue_name=queue_name,
            kwargs=data_model.dict(),
        )
        logger.info(f"Manual task enqueued in queue '{queue_name}' with ID {task_id}")
        return task_id

    def start_worker(
        self,
        queue_name: str,
        semaphore: asyncio.Semaphore = None,
    ):
        """Start a worker for a specific queue."""
        if queue_name in self.workers:
            logger.warning(f"Worker for queue '{queue_name}' is already running.")
            return
        semaphore = semaphore if semaphore else asyncio.Semaphore(1)
        worker = Worker(
            task_queue=self.task_queue,
            task_registry=self.task_registry,
            retry_limit=self.retry_limit,
            queue_name=queue_name,
            semaphore=semaphore,  # Передаём семафор воркеру
        )
        self.workers[queue_name] = worker
        asyncio.create_task(worker.start())
        logger.debug(
            f"Started worker for queue '{queue_name}' with concurrency {self.task_registry[queue_name].get('concurrency', 1)}"
        )

    async def run_scheduled_tasks(self):
        """Continuously check and run scheduled tasks."""
        while True:
            for task in self.scheduled_tasks:
                if task.should_run():
                    asyncio.create_task(task.run())
            await asyncio.sleep(1)

    async def connect_and_start_workers(self):
        """Connect to Redis and start all workers."""
        await self.task_queue.connect()

        # Create and start a worker for each queue based on concurrency
        for queue_name, task_info in self.task_registry.items():
            concurrency = task_info.get("concurrency", 1)
            semaphore = asyncio.Semaphore(concurrency)
            self.semaphores[queue_name] = semaphore  # Save semaphore
            self.start_worker(
                queue_name=queue_name,
                semaphore=semaphore,
            )

        # Start the scheduled tasks runner
        asyncio.create_task(self.run_scheduled_tasks())

    async def run(self):
        """
        Start the TaskQueue, Workers, Scheduled Tasks, and keep the event loop running.
        """
        await self.connect_and_start_workers()

        logger.info("mTask is running. Press Ctrl+C to exit.")

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down mTask...")
            # Stop all workers
            for worker in self.workers.values():
                await worker.stop()
            # Disconnect from Redis
            await self.task_queue.disconnect()
            logger.info("mTask has been shut down.")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            # Stop all workers
            for worker in self.workers.values():
                await worker.stop()
            # Disconnect from Redis
            await self.task_queue.disconnect()
            logger.info("mTask has been shut down due to an error.")
