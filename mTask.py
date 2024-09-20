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


# ============================
# Custom Exception Classes
# ============================


class mTaskError(Exception):
    """Base exception class for mTask."""

    pass


class RedisConnectionError(mTaskError):
    """Raised when there is a connection error with Redis."""

    pass


class TaskEnqueueError(mTaskError):
    """Raised when enqueueing a task fails."""

    pass


class TaskDequeueError(mTaskError):
    """Raised when dequeueing a task fails."""

    pass


class TaskRequeueError(mTaskError):
    """Raised when requeueing a task fails."""

    pass


class TaskProcessingError(mTaskError):
    """Raised when processing a task fails."""

    pass


class TaskFunctionNotFoundError(mTaskError):
    """Raised when the task function is not found in the registry."""

    pass


# ============================
# TaskQueue Class
# ============================


class TaskQueue:
    """
    Manages task queues using Redis.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the TaskQueue.

        Args:
            redis_url (str): Redis connection URL.
            logger (logging.Logger, optional): Logger instance for logging.
        """
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.logger = logger or logging.getLogger("TaskQueue")

    async def connect(self):
        """
        Establish a connection to Redis.

        Raises:
            RedisConnectionError: If unable to connect to Redis.
        """
        try:
            self.redis = redis.from_url(
                self.redis_url, encoding="utf-8", decode_responses=True
            )
            await self.redis.ping()
        except Exception as e:
            self.logger.exception(f"Failed to connect to Redis: {e}")
            raise RedisConnectionError(f"Failed to connect to Redis: {e}") from e

    async def disconnect(self):
        """
        Close the connection to Redis.
        """
        if self.redis:
            await self.redis.close()

    async def enqueue(
        self,
        queue_name: str,
        kwargs: Dict[str, Any] = None,
    ) -> str:
        """
        Add a task to the specified Redis queue.

        Args:
            queue_name (str): Name of the Redis queue.
            kwargs (Dict[str, Any], optional): Keyword arguments for the task.

        Returns:
            str: Unique identifier of the enqueued task.

        Raises:
            TaskEnqueueError: If enqueueing the task fails.
        """
        if not self.redis:
            self.logger.error("Redis is not connected.")
            raise RedisConnectionError("Redis is not connected.")

        task = {
            "id": str(uuid.uuid4()),
            "name": queue_name,  # 'name' corresponds to 'queue_name'
            "kwargs": kwargs or {},
            "status": "pending",
            "retry_count": 0,
        }

        try:
            await self.redis.rpush(queue_name, json.dumps(task))
            self.logger.debug(f"Enqueued task {task['id']} to queue '{queue_name}'")
            return task["id"]
        except Exception as e:
            self.logger.exception(f"Failed to enqueue task: {e}")
            raise TaskEnqueueError(f"Failed to enqueue task: {e}") from e

    async def dequeue(self, queue_name: str = "default") -> Optional[Dict[str, Any]]:
        """
        Retrieve a task from the specified Redis queue.

        Args:
            queue_name (str, optional): Name of the Redis queue.

        Returns:
            Optional[Dict[str, Any]]: The task data or None if the queue is empty.

        Raises:
            TaskDequeueError: If dequeueing the task fails.
        """
        if not self.redis:
            self.logger.error("Redis is not connected.")
            raise RedisConnectionError("Redis is not connected.")

        try:
            task_json = await self.redis.lpop(queue_name)
            if task_json:
                task = json.loads(task_json)
                self.logger.debug(
                    f"Dequeued task {task['id']} with name '{task['name']}' from queue '{queue_name}'"
                )
                return task
            return None
        except Exception as e:
            self.logger.exception(
                f"Failed to dequeue task from queue '{queue_name}': {e}"
            )
            raise TaskDequeueError(
                f"Failed to dequeue task from queue '{queue_name}': {e}"
            ) from e

    async def requeue(self, task: Dict[str, Any], queue_name: str = "default") -> None:
        """
        Re-add a task to the specified Redis queue for retrying.

        Args:
            task (Dict[str, Any]): The task data.
            queue_name (str, optional): Name of the Redis queue.

        Raises:
            TaskRequeueError: If requeueing the task fails.
        """
        task["status"] = "pending"
        try:
            await self.redis.rpush(queue_name, json.dumps(task))
            self.logger.debug(
                f"Requeued task {task['id']} to queue '{queue_name}' (retry {task['retry_count']})"
            )
        except Exception as e:
            self.logger.exception(f"Failed to requeue task '{task['id']}': {e}")
            raise TaskRequeueError(f"Failed to requeue task '{task['id']}': {e}") from e


# ============================
# Worker Class
# ============================


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
        semaphore: Optional[asyncio.Semaphore] = None,
        logger: Optional[logging.Logger] = None,
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
        self.logger = logger or logging.getLogger(f"Worker-{queue_name}")

        self.logger.debug(
            f"Worker initialized with concurrency={self.semaphore._value} for queue '{queue_name}'"
        )

    async def start(self):
        """
        Start the worker coroutines based on concurrency.
        """
        if self._running:
            self.logger.warning("Worker is already running.")
            return

        self._running = True
        concurrency = self.semaphore._value

        for i in range(concurrency):
            worker_task = asyncio.create_task(self._worker_loop(worker_id=i + 1))
            self._workers.append(worker_task)

    async def stop(self):
        """
        Stop all worker coroutines.
        """
        if not self._running:
            self.logger.warning("Worker is not running.")
            return

        self._running = False
        for worker_task in self._workers:
            worker_task.cancel()

        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def _worker_loop(self, worker_id: int):
        """
        Main loop for each worker coroutine.

        Args:
            worker_id (int): Identifier for the worker.
        """
        while self._running:
            try:
                task = await self.task_queue.dequeue(queue_name=self.queue_name)
                if task:
                    await self.process_task(task, worker_id)
                else:
                    await asyncio.sleep(1)  # Prevent busy waiting
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.exception(f"Worker {worker_id}: Unexpected error: {e}")
                # Depending on the error, you may choose to continue or stop

    async def process_task(self, task: Dict[str, Any], worker_id: int):
        """
        Process a single task.

        Args:
            task (Dict[str, Any]): The task data.
            worker_id (int): Identifier for the worker.

        Raises:
            TaskFunctionNotFoundError: If the task function is not found in the registry.
        """
        queue_name = task["name"]  # 'name' corresponds to 'queue_name'
        kwargs = task.get("kwargs", {})
        func = self.task_registry.get(queue_name, {}).get("func")  # Get the function
        if not func:
            self.logger.error(f"Task function for queue '{queue_name}' not found.")
            raise TaskFunctionNotFoundError(
                f"Task function for queue '{queue_name}' not found."
            )

        try:

            # Check if the function expects a Pydantic model
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

            async with self.semaphore:  # Use semaphore to limit concurrency
                if expects_model and model_class:
                    # Deserialize kwargs into Pydantic model
                    data_model = model_class(**kwargs)
                    await func(data=data_model)
                else:
                    await func(**kwargs)

        except Exception as e:
            self.logger.exception(
                f"Worker {worker_id}: Error executing task {task['id']}: {e}"
            )
            if task["retry_count"] < self.retry_limit:
                task["retry_count"] += 1
                try:
                    await self.task_queue.requeue(task, queue_name=self.queue_name)
                except TaskRequeueError as re:
                    self.logger.error(
                        f"Worker {worker_id}: Failed to requeue task {task['id']}: {re}"
                    )
            else:
                self.logger.error(
                    f"Worker {worker_id}: Task {task['id']} failed after {self.retry_limit} retries."
                )
                # Optionally, you can implement further failure handling here


# ============================
# ScheduledTask Class
# ============================


class ScheduledTask:
    """
    Represents a task scheduled to run at fixed intervals or based on cron expressions.
    """

    def __init__(
        self,
        func: Callable,
        interval: Optional[int] = None,
        cron_expression: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the ScheduledTask.

        Args:
            func (Callable): The function to execute.
            interval (Optional[int], optional): Interval in seconds between executions.
            cron_expression (Optional[str], optional): Cron expression defining the schedule.
            logger (logging.Logger, optional): Logger instance for logging.
        """
        self.func = func
        self.interval = interval
        self.cron_expression = cron_expression
        self.last_run = None
        self.next_run = self._get_next_run()
        self.is_running = False
        self.logger = logger or logging.getLogger("ScheduledTask")

    def _get_next_run(self) -> datetime:
        """
        Calculate the next run time based on the schedule.

        Returns:
            datetime: The next scheduled run time.

        Raises:
            ValueError: If neither interval nor cron_expression is set.
        """
        now = datetime.now()
        if self.interval is not None:
            return now + timedelta(seconds=self.interval)
        elif self.cron_expression is not None:
            cron = croniter(self.cron_expression, now)
            return cron.get_next(datetime)
        else:
            raise ValueError("Either interval or cron_expression must be set")

    async def run(self):
        """
        Execute the scheduled task.
        """
        if self.is_running:
            return
        self.is_running = True
        try:
            await self.func()
        except Exception as e:
            self.logger.exception(f"Error in scheduled task {self.func.__name__}: {e}")
        finally:
            self.last_run = datetime.now()
            self.next_run = self._get_next_run()
            self.is_running = False

    def should_run(self) -> bool:
        """
        Determine if the task should run at the current time.

        Returns:
            bool: True if the task should run, False otherwise.
        """
        return datetime.now() >= self.next_run and not self.is_running


# ============================
# mTask Class
# ============================


class mTask:
    """
    Main class to manage tasks, scheduling, and workers.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        retry_limit: int = 3,
        log_level: int = logging.INFO,
        enable_logging: bool = True,
    ):
        """
        Initialize the mTask manager.

        Args:
            redis_url (str, optional): Redis connection URL.
            retry_limit (int, optional): Maximum number of retry attempts for failed tasks.
            log_level (int, optional): Logging level (e.g., logging.INFO, logging.DEBUG).
            enable_logging (bool, optional): Enable or disable logging.
        """
        self.task_registry: Dict[str, Dict[str, Any]] = {}
        self.workers: Dict[str, Worker] = {}
        self.scheduled_tasks: List[ScheduledTask] = []
        self.retry_limit = retry_limit
        self.semaphores: Dict[str, asyncio.Semaphore] = {}

        # Set up logging
        # Set up logging
        self.logger = logging.getLogger("mTask")
        if enable_logging:
            self.logger.setLevel(log_level)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        else:
            self.logger.disabled = True  # Disable logging

        self.logger.debug("Initialized mTask instance.")

        self.task_queue = TaskQueue(redis_url=redis_url, logger=self.logger)

    def task(
        self,
        queue_name: str = "default",
        concurrency: int = 1,
    ):
        """
        Decorator to define and register a task.

        Args:
            queue_name (str, optional): Redis queue name to enqueue the task.
            concurrency (int, optional): Maximum concurrent executions for the task.

        Returns:
            Callable: The decorator.
        """

        def decorator(func: Callable):
            """
            Decorator function to register the task.

            Args:
                func (Callable): The task function to register.

            Returns:
                Callable: The wrapped function.
            """
            # Register the function and concurrency in the task_registry
            self.task_registry[queue_name] = {"func": func, "concurrency": concurrency}

            @wraps(func)
            async def wrapper(*args, **kwargs):
                """
                Wrapper function to enqueue the task.

                Args:
                    *args: Variable length argument list.
                    **kwargs: Arbitrary keyword arguments.
                """
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
                        self.logger.error(f"Error parsing data with model {model}: {e}")
                        raise

                    # Enqueue the task without passing concurrency
                    try:
                        task_id = await self.task_queue.enqueue(
                            queue_name=queue_name,
                            kwargs=data,
                        )
                        return task_id
                    except TaskEnqueueError as e:
                        self.logger.error(f"Failed to enqueue task: {e}")
                        raise
                else:
                    # No Pydantic model; pass kwargs directly
                    try:
                        task_id = await self.task_queue.enqueue(
                            queue_name=queue_name,
                            kwargs=kwargs,
                        )
                        return task_id
                    except TaskEnqueueError as e:
                        self.logger.error(f"Failed to enqueue task: {e}")
                        raise

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
            """
            Decorator function to schedule the task.

            Args:
                func (Callable): The scheduled task function.

            Returns:
                Callable: The original function.
            """
            scheduled_task = ScheduledTask(func, interval=seconds, logger=self.logger)
            self.scheduled_tasks.append(scheduled_task)
            self.logger.debug(
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
            """
            Decorator function to schedule the task.

            Args:
                func (Callable): The scheduled task function.

            Returns:
                Callable: The original function.
            """
            scheduled_task = ScheduledTask(
                func, cron_expression=cron_expression, logger=self.logger
            )
            self.scheduled_tasks.append(scheduled_task)
            self.logger.debug(
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

        Raises:
            TaskEnqueueError: If enqueueing the task fails.
        """
        try:
            task_id = await self.task_queue.enqueue(
                queue_name=queue_name,
                kwargs=data_model.model_dump(),
            )
            return task_id
        except TaskEnqueueError as e:
            self.logger.error(f"Failed to enqueue manual task: {e}")
            raise

    def start_worker(
        self,
        queue_name: str,
        semaphore: asyncio.Semaphore = None,
    ):
        """
        Start a worker for a specific queue.

        Args:
            queue_name (str): Name of the Redis queue.
            semaphore (asyncio.Semaphore, optional): Semaphore to limit concurrency.
        """
        if queue_name in self.workers:
            self.logger.warning(f"Worker for queue '{queue_name}' is already running.")
            return
        semaphore = semaphore if semaphore else asyncio.Semaphore(1)
        worker = Worker(
            task_queue=self.task_queue,
            task_registry=self.task_registry,
            retry_limit=self.retry_limit,
            queue_name=queue_name,
            semaphore=semaphore,  # Pass semaphore to worker
            logger=self.logger,
        )
        self.workers[queue_name] = worker
        asyncio.create_task(worker.start())
        self.logger.debug(
            f"Started worker for queue '{queue_name}' with concurrency {self.task_registry[queue_name].get('concurrency', 1)}"
        )

    async def run_scheduled_tasks(self):
        """
        Continuously check and run scheduled tasks.
        """
        while True:
            for task in self.scheduled_tasks:
                if task.should_run():
                    asyncio.create_task(task.run())
            await asyncio.sleep(1)

    async def connect_and_start_workers(self):
        """
        Connect to Redis and start all workers.
        """
        try:
            await self.task_queue.connect()
        except RedisConnectionError as e:
            self.logger.error(f"Cannot start workers without Redis connection: {e}")
            raise

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
        try:
            await self.connect_and_start_workers()
        except mTaskError as e:
            self.logger.error(f"Failed to start mTask: {e}")
            return

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            # Stop all workers
            for worker in self.workers.values():
                await worker.stop()
            # Disconnect from Redis
            await self.task_queue.disconnect()
        except Exception as e:
            self.logger.exception(f"Unexpected error: {e}")
            # Stop all workers
            for worker in self.workers.values():
                await worker.stop()
            # Disconnect from Redis
            await self.task_queue.disconnect()
