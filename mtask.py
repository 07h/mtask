# mTask.py

import sys
import asyncio

# Use uvloop for better performance on Unix systems
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

import inspect
import json
import logging
import uuid
from functools import wraps
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, List, Awaitable
from enum import Enum
import random

from croniter import croniter
import redis.asyncio as redis
from pydantic import BaseModel

# ============================
# Helper Functions
# ============================


def calculate_backoff(retry_count: int, base_delay: int = 1, max_delay: int = 60) -> float:
    """Calculate exponential backoff with jitter."""
    delay = min(base_delay * (2 ** retry_count), max_delay)
    jitter = random.uniform(0, delay * 0.1)
    return delay + jitter


# ============================
# Task Models
# ============================


class TaskStatus(str, Enum):
    """Task status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class InternalTask(BaseModel):
    """Internal task representation with type safety."""
    id: str
    name: str
    kwargs: Dict[str, Any]
    status: str  # Using str for backward compatibility
    retry_count: int
    start_time: Optional[float] = None
    error: Optional[str] = None
    created_at: Optional[float] = None
    
    class Config:
        use_enum_values = True


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
    Manages task queues using Redis with support for processing queues.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        logger: Optional[logging.Logger] = None,
        health_check_interval: int = 30,
        max_reconnect_attempts: int = 5,
        operation_timeout: float = 5.0,
        connect_timeout: float = 10.0,
        max_connections: int = 150,
    ):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.logger = logger or logging.getLogger("TaskQueue")
        self.health_check_interval = health_check_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.operation_timeout = operation_timeout
        self.connect_timeout = connect_timeout
        self.max_connections = max_connections
        self._connection_healthy = False
        self._health_check_task: Optional[asyncio.Task] = None
        self._reconnecting = False

    async def connect(self):
        try:
            self.redis = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=self.max_connections,
                socket_timeout=self.operation_timeout,
                socket_connect_timeout=self.connect_timeout,
                retry_on_timeout=True,
            )
            await asyncio.wait_for(self.redis.ping(), timeout=self.connect_timeout)
            self._connection_healthy = True
            self.logger.info(f"Connected to Redis at {self.redis_url}")
            
            # Start health check in background
            if not self._health_check_task or self._health_check_task.done():
                self._health_check_task = asyncio.create_task(self._health_check_loop())
        except asyncio.TimeoutError:
            self._connection_healthy = False
            self.logger.error(f"Connection to Redis timed out after {self.connect_timeout}s")
            raise RedisConnectionError(f"Connection to Redis timed out after {self.connect_timeout}s")
        except Exception as e:
            self._connection_healthy = False
            self.logger.exception(f"Failed to connect to Redis: {e}")
            raise RedisConnectionError(f"Failed to connect to Redis: {e}") from e

    async def disconnect(self):
        # Stop health check
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        if self.redis:
            await self.redis.close()
            self._connection_healthy = False
            self.logger.info("Disconnected from Redis")
    
    async def _reconnect(self):
        """Attempt to reconnect to Redis with exponential backoff."""
        if self._reconnecting:
            return
        
        self._reconnecting = True
        attempt = 0
        
        while attempt < self.max_reconnect_attempts:
            attempt += 1
            wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60s
            
            self.logger.warning(
                f"Attempting to reconnect to Redis (attempt {attempt}/{self.max_reconnect_attempts}) "
                f"in {wait_time}s..."
            )
            await asyncio.sleep(wait_time)
            
            try:
                if self.redis:
                    await self.redis.close()
                
                self.redis = redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=self.max_connections,
                    socket_timeout=self.operation_timeout,
                    socket_connect_timeout=self.connect_timeout,
                    retry_on_timeout=True,
                )
                await asyncio.wait_for(self.redis.ping(), timeout=self.connect_timeout)
                self._connection_healthy = True
                self.logger.info("Successfully reconnected to Redis")
                self._reconnecting = False
                return
            except asyncio.TimeoutError:
                self.logger.error(f"Reconnection attempt {attempt} timed out after {self.connect_timeout}s")
            except Exception as e:
                self.logger.error(f"Reconnection attempt {attempt} failed: {e}")
        
        self._reconnecting = False
        self.logger.error(
            f"Failed to reconnect to Redis after {self.max_reconnect_attempts} attempts"
        )
    
    async def _health_check_loop(self):
        """Periodically check Redis connection health."""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                if self.redis:
                    await asyncio.wait_for(self.redis.ping(), timeout=self.operation_timeout)
                    if not self._connection_healthy:
                        self._connection_healthy = True
                        self.logger.info("Redis connection healthy")
                else:
                    raise Exception("Redis client is None")
                    
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                if self._connection_healthy:
                    self._connection_healthy = False
                    self.logger.error(f"Redis health check timed out after {self.operation_timeout}s")
                # Attempt to reconnect in background task to not block health check loop
                if not self._reconnecting:
                    asyncio.create_task(self._reconnect())
            except Exception as e:
                if self._connection_healthy:
                    self._connection_healthy = False
                    self.logger.error(f"Redis health check failed: {e}")
                
                # Attempt to reconnect in background task to not block health check loop
                if not self._reconnecting:
                    asyncio.create_task(self._reconnect())

    async def enqueue(
        self,
        queue_name: str,
        kwargs: Dict[str, Any] = None,
        priority: int = 0,
        max_task_size: int = 1024 * 1024,
    ) -> str:
        if not self.redis:
            self.logger.error("Redis is not connected.")
            raise RedisConnectionError("Redis is not connected.")

        task = {
            "id": str(uuid.uuid4()),
            "name": queue_name,
            "kwargs": kwargs or {},
            "status": "pending",
            "retry_count": 0,
            "priority": priority,
        }

        # Check task size
        task_json = json.dumps(task)
        task_size = len(task_json.encode('utf-8'))
        if task_size > max_task_size:
            error_msg = f"Task size ({task_size} bytes) exceeds maximum allowed size ({max_task_size} bytes)"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            if priority > 0:
                # Use sorted set for priority queues (higher score = higher priority)
                # Use negative timestamp as tiebreaker for same priority
                score = priority * 1000000 - datetime.utcnow().timestamp()
                await asyncio.wait_for(
                    self.redis.zadd(f"{queue_name}:priority", {task_json: score}),
                    timeout=self.operation_timeout
                )
                self.logger.debug(f"Enqueued task {task['id']} to priority queue '{queue_name}' with priority {priority}")
            else:
                # Use regular list for non-priority tasks (backward compatible)
                await asyncio.wait_for(
                    self.redis.rpush(queue_name, task_json),
                    timeout=self.operation_timeout
                )
                self.logger.debug(f"Enqueued task {task['id']} to queue '{queue_name}'")
            return task["id"]
        except asyncio.TimeoutError:
            self.logger.error(f"Enqueue operation timed out after {self.operation_timeout}s")
            raise TaskEnqueueError(f"Enqueue operation timed out after {self.operation_timeout}s")
        except Exception as e:
            self.logger.exception(f"Failed to enqueue task: {e}")
            raise TaskEnqueueError(f"Failed to enqueue task: {e}") from e

    async def dequeue(self, queue_name: str = "default") -> Optional[Dict[str, Any]]:
        if not self.redis:
            self.logger.error("Redis is not connected.")
            raise RedisConnectionError("Redis is not connected.")

        processing_queue = f"{queue_name}:processing"
        priority_queue = f"{queue_name}:priority"
        
        try:
            # Try priority queue first
            priority_tasks = await asyncio.wait_for(
                self.redis.zpopmax(priority_queue, 1),
                timeout=self.operation_timeout
            )
            if priority_tasks:
                task_json, _ = priority_tasks[0]
                task = json.loads(task_json)
                
                # Check if task has backoff delay (retry_after)
                retry_after = task.get("retry_after")
                if retry_after:
                    current_time = datetime.utcnow().timestamp()
                    if current_time < retry_after:
                        # Task is in backoff period, re-enqueue with lower priority
                        wait_time = retry_after - current_time
                        self.logger.debug(
                            f"Task {task['id']} still in backoff period, re-enqueueing "
                            f"(wait {wait_time:.1f}s more)"
                        )
                        # Re-add with adjusted score to maintain order
                        score = task.get("priority", 0) * 1000000 - retry_after
                        await asyncio.wait_for(
                            self.redis.zadd(priority_queue, {task_json: score}),
                            timeout=self.operation_timeout
                        )
                        return None
                    else:
                        # Backoff period expired, remove the marker
                        task.pop("retry_after", None)
                
                # Store in processing Hash (O(1) for mark_completed)
                final_task_json = task_json if "retry_after" not in task_json else json.dumps(task)
                await asyncio.wait_for(
                    self.redis.hset(processing_queue, task["id"], final_task_json),
                    timeout=self.operation_timeout
                )
                self.logger.debug(
                    f"Dequeued priority task {task['id']} from queue '{queue_name}' to processing '{processing_queue}'"
                )
                return task
            
            # Fall back to regular queue - try atomic LMOVE first (Redis 6.2+)
            task_json = None
            try:
                # LMOVE atomically moves element from source to destination
                # We move to a temp list first, then to hash
                task_json = await asyncio.wait_for(
                    self.redis.lpop(queue_name),
                    timeout=self.operation_timeout
                )
            except Exception:
                # Fallback for older Redis or if lpop fails
                pass
            
            if not task_json:
                # No task available, short sleep to avoid busy loop
                return None
                
            task = json.loads(task_json)
            
            # Check if task has backoff delay
            retry_after = task.get("retry_after")
            if retry_after:
                current_time = datetime.utcnow().timestamp()
                if current_time < retry_after:
                    wait_time = retry_after - current_time
                    self.logger.debug(
                        f"Task {task['id']} still in backoff period, re-enqueueing "
                        f"(wait {wait_time:.1f}s more)"
                    )
                    # Re-add to back of queue
                    await asyncio.wait_for(
                        self.redis.rpush(queue_name, task_json),
                        timeout=self.operation_timeout
                    )
                    return None
                else:
                    # Backoff period expired, remove the marker
                    task.pop("retry_after", None)
            
            # Store in processing Hash (O(1) for mark_completed)
            final_task_json = task_json if "retry_after" not in task_json else json.dumps(task)
            await asyncio.wait_for(
                self.redis.hset(processing_queue, task["id"], final_task_json),
                timeout=self.operation_timeout
            )
            self.logger.debug(
                f"Dequeued task {task['id']} from queue '{queue_name}' to processing '{processing_queue}'"
            )
            return task
        except asyncio.TimeoutError:
            self.logger.error(f"Dequeue operation timed out after {self.operation_timeout}s")
            raise TaskDequeueError(f"Dequeue operation timed out after {self.operation_timeout}s")
        except Exception as e:
            self.logger.exception(
                f"Failed to dequeue task from queue '{queue_name}': {e}"
            )
            raise TaskDequeueError(
                f"Failed to dequeue task from queue '{queue_name}': {e}"
            ) from e

    async def requeue(self, task: Dict[str, Any], queue_name: str = "default", apply_backoff: bool = True) -> None:
        if not self.redis:
            self.logger.error("Redis is not connected.")
            raise RedisConnectionError("Redis is not connected.")

        task["status"] = "pending"
        task.pop("start_time", None)
        priority = task.get("priority", 0)
        
        # Store backoff timestamp in task instead of sleeping (to avoid task loss on crash)
        if apply_backoff and task.get("retry_count", 0) > 0:
            backoff_delay = calculate_backoff(task["retry_count"])
            task["retry_after"] = datetime.utcnow().timestamp() + backoff_delay
            self.logger.debug(
                f"Task {task['id']} will be retried after {backoff_delay:.2f}s backoff "
                f"(at timestamp {task['retry_after']})"
            )
        
        try:
            if priority > 0:
                # Re-enqueue to priority queue
                score = priority * 1000000 - datetime.utcnow().timestamp()
                await asyncio.wait_for(
                    self.redis.zadd(f"{queue_name}:priority", {json.dumps(task): score}),
                    timeout=self.operation_timeout
                )
                self.logger.debug(
                    f"Requeued task {task['id']} to priority queue '{queue_name}' (retry {task['retry_count']})"
                )
            else:
                # Re-enqueue to regular queue
                await asyncio.wait_for(
                    self.redis.rpush(queue_name, json.dumps(task)),
                    timeout=self.operation_timeout
                )
                self.logger.debug(
                    f"Requeued task {task['id']} to queue '{queue_name}' (retry {task['retry_count']})"
                )
        except asyncio.TimeoutError:
            self.logger.error(f"Requeue operation timed out after {self.operation_timeout}s")
            raise TaskRequeueError(f"Requeue operation timed out after {self.operation_timeout}s")
        except Exception as e:
            self.logger.exception(f"Failed to requeue task '{task['id']}': {e}")
            raise TaskRequeueError(f"Failed to requeue task '{task['id']}': {e}") from e

    async def mark_completed(self, task_id: str, queue_name: str) -> None:
        processing_queue = f"{queue_name}:processing"
        try:
            # O(1) operation using Hash instead of O(n) list scan
            deleted = await asyncio.wait_for(
                self.redis.hdel(processing_queue, task_id),
                timeout=self.operation_timeout
            )
            if deleted:
                self.logger.info(
                    f"Task {task_id} marked as completed and removed from processing '{processing_queue}'."
                )
            else:
                self.logger.warning(
                    f"Task {task_id} was not found in processing queue '{processing_queue}'."
                )
        except asyncio.TimeoutError:
            self.logger.error(f"Mark completed operation timed out after {self.operation_timeout}s")
            raise TaskProcessingError(f"Mark completed operation timed out after {self.operation_timeout}s")
        except Exception as e:
            self.logger.exception(f"Failed to mark task {task_id} as completed: {e}")
            raise TaskProcessingError(
                f"Failed to mark task {task_id} as completed: {e}"
            ) from e

    async def recover_processing_tasks(self, queue_name: str, batch_size: int = 100):
        """Recover tasks from processing queue (Hash) back to main queue using batches."""
        processing_queue = f"{queue_name}:processing"
        main_queue = queue_name
        try:
            # First check if the key exists and its type
            key_type = await asyncio.wait_for(
                self.redis.type(processing_queue),
                timeout=self.operation_timeout
            )
            
            if key_type == "none":
                return
            
            # Handle migration from old List format to new Hash format
            if key_type == "list":
                await self._migrate_processing_list_to_hash(queue_name)
                key_type = "hash"
            
            if key_type != "hash":
                self.logger.warning(
                    f"Processing queue '{processing_queue}' has unexpected type '{key_type}', skipping recovery"
                )
                return
            
            # Get all tasks from Hash
            tasks = await asyncio.wait_for(
                self.redis.hgetall(processing_queue),
                timeout=self.operation_timeout
            )
            
            if not tasks:
                return
            
            total_tasks = len(tasks)
            recovered = 0
            
            # Process in batches using pipeline
            task_items = list(tasks.items())
            for i in range(0, len(task_items), batch_size):
                batch = task_items[i:i + batch_size]
                
                pipe = self.redis.pipeline()
                for task_id, task_json in batch:
                    pipe.lpush(main_queue, task_json)
                await pipe.execute()
                
                recovered += len(batch)
                
                if recovered % 500 == 0 and recovered < total_tasks:
                    self.logger.info(
                        f"Recovery progress: {recovered}/{total_tasks} tasks moved from '{processing_queue}' to '{main_queue}'"
                    )
            
            # Delete processing hash after successful recovery
            await asyncio.wait_for(
                self.redis.delete(processing_queue),
                timeout=self.operation_timeout
            )
            self.logger.info(
                f"Recovered {recovered} tasks from processing '{processing_queue}' back to main '{main_queue}'."
            )
        except asyncio.TimeoutError:
            self.logger.error(f"Recovery operation timed out after {self.operation_timeout}s")
        except Exception as e:
            self.logger.exception(
                f"Failed to recover tasks from processing queue '{processing_queue}': {e}"
            )
    
    async def _migrate_processing_list_to_hash(self, queue_name: str):
        """Migrate processing queue from List to Hash format (one-time migration)."""
        processing_queue = f"{queue_name}:processing"
        self.logger.info(f"Migrating processing queue '{processing_queue}' from List to Hash format...")
        
        try:
            # Get all tasks from List
            tasks = await asyncio.wait_for(
                self.redis.lrange(processing_queue, 0, -1),
                timeout=self.operation_timeout
            )
            
            if not tasks:
                await self.redis.delete(processing_queue)
                return
            
            # Convert to Hash
            temp_hash = f"{processing_queue}:migration_temp"
            pipe = self.redis.pipeline()
            for task_json in tasks:
                task = json.loads(task_json)
                pipe.hset(temp_hash, task["id"], task_json)
            await pipe.execute()
            
            # Atomic rename (delete old, rename temp)
            await self.redis.delete(processing_queue)
            await self.redis.rename(temp_hash, processing_queue)
            
            self.logger.info(
                f"Successfully migrated {len(tasks)} tasks in '{processing_queue}' from List to Hash format."
            )
        except Exception as e:
            self.logger.exception(f"Failed to migrate processing queue '{processing_queue}': {e}")

    async def get_task_count(self, queue_name: str) -> int:
        try:
            count = await asyncio.wait_for(
                self.redis.llen(queue_name),
                timeout=self.operation_timeout
            )
            return count
        except asyncio.TimeoutError:
            self.logger.error(f"Get task count timed out after {self.operation_timeout}s")
            return 0
        except Exception as e:
            self.logger.error(f"Failed to get task count for '{queue_name}': {e}")
            return 0

    async def get_processing_task_count(self, queue_name: str) -> int:
        processing_queue = f"{queue_name}:processing"
        try:
            # Use hlen for Hash (new format)
            key_type = await self.redis.type(processing_queue)
            if key_type == "hash":
                count = await self.redis.hlen(processing_queue)
            elif key_type == "list":
                # Backward compatibility with old List format
                count = await self.redis.llen(processing_queue)
            else:
                count = 0
            return count
        except Exception as e:
            self.logger.error(
                f"Failed to get processing task count for '{queue_name}': {e}"
            )
            return 0


# ============================
# Worker Class
# ============================


class Worker:
    def __init__(
        self,
        task_queue: TaskQueue,
        task_registry: Dict[str, Dict[str, Any]],
        task_registry_lock: asyncio.Lock,
        retry_limit: int = 3,
        queue_name: str = "default",
        semaphore: Optional[asyncio.Semaphore] = None,
        logger: Optional[logging.Logger] = None,
        enable_dlq: bool = True,
        move_to_dlq_callback: Optional[Callable] = None,
        record_metric_callback: Optional[Callable] = None,
    ):
        self.task_queue = task_queue
        self.retry_limit = retry_limit
        self.queue_name = queue_name
        self.task_registry = task_registry
        self.task_registry_lock = task_registry_lock
        self.semaphore = semaphore if semaphore else asyncio.Semaphore(1)
        self._running = False
        self._workers: List[asyncio.Task] = []
        self._active_tasks: int = 0
        self.logger = logger or logging.getLogger(f"Worker-{queue_name}")
        self.enable_dlq = enable_dlq
        self.move_to_dlq_callback = move_to_dlq_callback
        self.record_metric_callback = record_metric_callback

        self.logger.debug(
            f"Worker initialized with concurrency={self.semaphore._value} for queue '{queue_name}'"
        )

    async def start(self):
        if self._running:
            self.logger.warning("Worker is already running.")
            return

        self._running = True
        concurrency = self.semaphore._value
        self.logger.info(
            f"Starting {concurrency} worker(s) for queue '{self.queue_name}'"
        )

        for i in range(concurrency):
            monitor_task = asyncio.create_task(self._monitor_worker(i + 1))
            self._workers.append(monitor_task)

    async def _monitor_worker(self, worker_id: int):
        while self._running:
            worker_task = asyncio.create_task(self._worker_loop(worker_id))
            try:
                await worker_task
            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_id}: Received shutdown signal.")
                break
            except Exception as e:
                self.logger.exception(f"Worker {worker_id}: Unexpected error: {e}")
                await asyncio.sleep(1)
                self.logger.info(
                    f"Worker {worker_id}: Restarting after unexpected termination."
                )

    async def _worker_loop(self, worker_id: int):
        while self._running:
            try:
                # Dequeue outside semaphore to avoid blocking other workers
                task = await self.task_queue.dequeue(queue_name=self.queue_name)
                if task:
                    self.logger.info(
                        f"Worker {worker_id}: Dequeued task {task['id']} from queue '{self.queue_name}'"
                    )
                    # Only acquire semaphore for task execution
                    async with self.semaphore:
                        await self.process_task(task, worker_id)
                else:
                    # Sleep outside semaphore - short sleep to be responsive
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_id}: Received shutdown signal.")
                break
            except Exception as e:
                self.logger.exception(f"Worker {worker_id}: Error in worker loop: {e}")
                await asyncio.sleep(1)

    async def stop(self, graceful: bool = True, timeout: int = 30):
        """Stop workers, optionally waiting for active tasks to complete."""
        if not self._running:
            self.logger.warning("Worker is not running.")
            return

        self._running = False
        
        if graceful and self._active_tasks > 0:
            self.logger.info(f"Gracefully stopping workers, waiting for {self._active_tasks} active tasks...")
            start_time = datetime.utcnow().timestamp()
            
            while self._active_tasks > 0:
                elapsed = datetime.utcnow().timestamp() - start_time
                if elapsed >= timeout:
                    self.logger.warning(
                        f"Graceful shutdown timeout reached after {timeout}s, "
                        f"{self._active_tasks} tasks still active. Forcing shutdown."
                    )
                    break
                await asyncio.sleep(0.5)
        
        self.logger.info("Stopping workers...")
        for worker_task in self._workers:
            worker_task.cancel()

        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self.logger.info("Workers stopped.")

    async def process_task(self, task: Dict[str, Any], worker_id: int):
        queue_name = task["name"]
        kwargs = task.get("kwargs", {})
        self._active_tasks += 1
        
        try:
            # Read without lock - task_registry is immutable after startup
            # Python dict.get() is thread-safe for simple reads due to GIL
            task_info = self.task_registry.get(queue_name, {})
            func = task_info.get("func")
            timeout = task_info.get("timeout")
            on_task_requeued = task_info.get("on_task_requeued")
            
            if not func:
                self.logger.error(f"Task function for queue '{queue_name}' not found.")
                raise TaskFunctionNotFoundError(
                    f"Task function for queue '{queue_name}' not found."
                )
    
            # Execute the task
            task["start_time"] = datetime.utcnow().timestamp()
            self.logger.info(
                f"Worker {worker_id}: Executing task {task['id']} from queue '{queue_name}'"
            )

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

            if expects_model and model_class:
                data_model = model_class(**kwargs)
                self.logger.info(
                    f"Worker {worker_id}: Executing task {task['id']} - '{queue_name}' with data={data_model}"
                )
                if timeout:
                    await asyncio.wait_for(func(data=data_model), timeout=timeout)
                else:
                    await func(data=data_model)
            else:
                self.logger.info(
                    f"Worker {worker_id}: Executing task {task['id']} - '{queue_name}' with kwargs={kwargs}"
                )
                if timeout:
                    await asyncio.wait_for(func(**kwargs), timeout=timeout)
                else:
                    await func(**kwargs)

            # Record successful completion metrics
            execution_time = datetime.utcnow().timestamp() - task["start_time"]
            if self.record_metric_callback:
                await self.record_metric_callback(self.queue_name, "completed")
                await self.record_metric_callback(self.queue_name, "execution_time", execution_time)
            
            self.logger.info(
                f"Worker {worker_id}: Task {task['id']} completed successfully in {execution_time:.2f}s."
            )
        except asyncio.TimeoutError:
            self.logger.error(
                f"Worker {worker_id}: Task {task['id']} exceeded timeout of {timeout} seconds."
            )
            if task["retry_count"] < self.retry_limit:
                task["retry_count"] += 1
                try:
                    await self.task_queue.requeue(task, queue_name=self.queue_name)
                    self.logger.info(
                        f"Worker {worker_id}: Requeued task {task['id']} due to timeout (retry {task['retry_count']})"
                    )
                    # Record requeue metric
                    if self.record_metric_callback:
                        await self.record_metric_callback(self.queue_name, "requeued")
                    # Вызов колбэка, если задан
                    if on_task_requeued:
                        await on_task_requeued(task, "timeout")
                except TaskRequeueError as re:
                    self.logger.error(
                        f"Worker {worker_id}: Failed to requeue task {task['id']}: {re}"
                    )
            else:
                error_msg = f"Task exceeded timeout of {timeout}s after {self.retry_limit} retries"
                self.logger.error(f"Worker {worker_id}: Task {task['id']} {error_msg}")
                # Record failure metric
                if self.record_metric_callback:
                    await self.record_metric_callback(self.queue_name, "failed")
                if self.enable_dlq and self.move_to_dlq_callback:
                    await self.move_to_dlq_callback(task, self.queue_name, error_msg)
        except Exception as e:
            self.logger.exception(
                f"Worker {worker_id}: Error executing task {task['id']}: {e}"
            )
            if task["retry_count"] < self.retry_limit:
                task["retry_count"] += 1
                try:
                    await self.task_queue.requeue(task, queue_name=self.queue_name)
                    self.logger.info(
                        f"Worker {worker_id}: Requeued task {task['id']} (retry {task['retry_count']})"
                    )
                    # Record requeue metric
                    if self.record_metric_callback:
                        await self.record_metric_callback(self.queue_name, "requeued")
                    # Можно также вызвать колбэк здесь для других причин (например 'error'),
                    # если это понадобится
                except TaskRequeueError as re:
                    self.logger.error(
                        f"Worker {worker_id}: Failed to requeue task {task['id']}: {re}"
                    )
            else:
                error_msg = str(e) if e else "Unknown error"
                self.logger.error(
                    f"Worker {worker_id}: Task {task['id']} failed after {self.retry_limit} retries. Error: {error_msg}"
                )
                # Record failure metric
                if self.record_metric_callback:
                    await self.record_metric_callback(self.queue_name, "failed")
                if self.enable_dlq and self.move_to_dlq_callback:
                    await self.move_to_dlq_callback(task, self.queue_name, error_msg)
        finally:
            self._active_tasks -= 1
            try:
                await self.task_queue.mark_completed(task["id"], queue_name)
                self.logger.info(
                    f"Worker {worker_id}: Task {task['id']} marked as completed."
                )
            except Exception as e:
                self.logger.exception(
                    f"Worker {worker_id}: Failed to mark task {task['id']} as completed: {e}"
                )


# ============================
# ScheduledTask Class
# ============================


class ScheduledTask:
    def __init__(
        self,
        func: Callable,
        interval: Optional[int] = None,
        cron_expression: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.func = func
        self.interval = interval
        self.cron_expression = cron_expression
        self.last_run = None
        self.next_run = self._get_next_run()
        self.is_running = False
        self.logger = logger or logging.getLogger("ScheduledTask")

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
            self.logger.exception(f"Error in scheduled task {self.func.__name__}: {e}")
        finally:
            self.last_run = datetime.now()
            self.next_run = self._get_next_run()
            self.is_running = False

    def should_run(self) -> bool:
        return datetime.now() >= self.next_run and not self.is_running


# ============================
# mTask Class
# ============================


class mTask:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        retry_limit: int = 3,
        log_level: int = logging.INFO,
        enable_logging: bool = True,
        shutdown_timeout: int = 30,
        enable_dlq: bool = True,
        max_task_size: int = 1024 * 1024,
        # New optional Redis parameters
        redis_operation_timeout: float = 5.0,
        redis_connect_timeout: float = 10.0,
        redis_max_connections: int = 50,
        redis_health_check_interval: int = 30,
    ):
        # Validate parameters
        if retry_limit < 0:
            raise ValueError("retry_limit must be >= 0")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be > 0")
        if max_task_size <= 0:
            raise ValueError("max_task_size must be > 0")
        if not redis_url or not isinstance(redis_url, str):
            raise ValueError("redis_url must be a non-empty string")
        if redis_operation_timeout <= 0:
            raise ValueError("redis_operation_timeout must be > 0")
        if redis_connect_timeout <= 0:
            raise ValueError("redis_connect_timeout must be > 0")
        if redis_max_connections <= 0:
            raise ValueError("redis_max_connections must be > 0")
        
        # Store Redis parameters for TaskQueue creation
        self._redis_url = redis_url
        self._redis_operation_timeout = redis_operation_timeout
        self._redis_connect_timeout = redis_connect_timeout
        self._redis_max_connections = redis_max_connections
        self._redis_health_check_interval = redis_health_check_interval
        
        self.task_queue = TaskQueue(
            redis_url=redis_url,
            operation_timeout=redis_operation_timeout,
            connect_timeout=redis_connect_timeout,
            max_connections=redis_max_connections,
            health_check_interval=redis_health_check_interval,
        )
        self.task_registry: Dict[str, Dict[str, Any]] = {}
        self.workers: Dict[str, Worker] = {}
        self.scheduled_tasks: List[ScheduledTask] = []
        self.retry_limit = retry_limit
        self.semaphores: Dict[str, asyncio.Semaphore] = {}
        self.queue_status: Dict[str, str] = {}
        self.shutdown_timeout = shutdown_timeout
        self.enable_dlq = enable_dlq
        self.max_task_size = max_task_size
        self._is_shutting_down = False
        self._metrics: Dict[str, Dict[str, Any]] = {}
        self._initialize_metrics()

        # Use only asyncio locks to avoid race conditions
        self.task_registry_lock = asyncio.Lock()
        self.queue_status_lock = asyncio.Lock()
        self.metrics_lock = asyncio.Lock()

        self.logger = logging.getLogger("mTask")
        if enable_logging:
            self.logger.setLevel(log_level)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
                )
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        else:
            self.logger.disabled = True

        self.logger.debug("Initialized mTask instance.")
        self._initialize_status_report_task()

    def _initialize_metrics(self):
        """Initialize metrics structure for all queues."""
        # Metrics will be populated lazily as queues are used
        pass
    
    async def _record_metric(self, queue_name: str, metric_type: str, value: Any = 1):
        """Record a metric for a specific queue."""
        async with self.metrics_lock:
            if queue_name not in self._metrics:
                self._metrics[queue_name] = {
                    "tasks_completed": 0,
                    "tasks_failed": 0,
                    "tasks_requeued": 0,
                    "total_execution_time": 0.0,
                    "task_count": 0,
                    "last_updated": datetime.utcnow().isoformat(),
                }
            
            if metric_type == "completed":
                self._metrics[queue_name]["tasks_completed"] += 1
            elif metric_type == "failed":
                self._metrics[queue_name]["tasks_failed"] += 1
            elif metric_type == "requeued":
                self._metrics[queue_name]["tasks_requeued"] += 1
            elif metric_type == "execution_time":
                self._metrics[queue_name]["total_execution_time"] += value
                self._metrics[queue_name]["task_count"] += 1
            
            self._metrics[queue_name]["last_updated"] = datetime.utcnow().isoformat()
    
    async def get_metrics(self, queue_name: Optional[str] = None) -> Dict[str, Any]:
        """Get metrics for a specific queue or all queues."""
        async with self.metrics_lock:
            if queue_name:
                metrics = self._metrics.get(queue_name, {})
                if metrics and metrics.get("task_count", 0) > 0:
                    metrics["avg_execution_time"] = (
                        metrics["total_execution_time"] / metrics["task_count"]
                    )
                return metrics
            else:
                # Return metrics for all queues
                result = {}
                for qname, metrics in self._metrics.items():
                    result[qname] = metrics.copy()
                    if metrics.get("task_count", 0) > 0:
                        result[qname]["avg_execution_time"] = (
                            metrics["total_execution_time"] / metrics["task_count"]
                        )
                return result

    def _initialize_status_report_task(self):
        @self.interval(seconds=300)
        async def status_report_task():
            if not self.task_registry:
                self.logger.info("No queues registered.")
                return

            report_data = []
            queue_names = list(self.task_registry.keys())

            for queue_name in queue_names:
                task_info = self.task_registry.get(queue_name, {})
                concurrency = task_info.get("concurrency", 1)
                status = await self._get_queue_status(queue_name)

                queue_length = await self.task_queue.get_task_count(queue_name)
                processing_length = await self.task_queue.get_processing_task_count(
                    queue_name
                )
                
                # Get metrics
                metrics = await self.get_metrics(queue_name)
                completed = metrics.get("tasks_completed", 0)
                failed = metrics.get("tasks_failed", 0)
                avg_time = metrics.get("avg_execution_time", 0)

                report_data.append(
                    {
                        "Queue Name": queue_name,
                        "Concurrency": concurrency,
                        "Queue": queue_length,
                        "Processing": processing_length,
                        "Completed": completed,
                        "Failed": failed,
                        "Avg Time (s)": f"{avg_time:.2f}" if avg_time else "N/A",
                        "Status": status,
                    }
                )

            table = self._format_table(report_data)
            self.logger.info(
                f"\n\t=== Queue Status Report ===\n\n{table}\n\n\t============================\n"
            )

    def _format_table(self, data: List[Dict[str, Any]]) -> str:
        if not data:
            return "No data to display."

        headers = data[0].keys()
        column_widths = {header: len(header) for header in headers}
        for row in data:
            for header, value in row.items():
                column_widths[header] = max(column_widths[header], len(str(value)))

        header_row = " | ".join(
            f"{header:<{column_widths[header]}}" for header in headers
        )
        separator = "-+-".join("-" * column_widths[header] for header in headers)
        data_rows = "\n".join(
            " | ".join(
                f"{str(value):<{column_widths[header]}}"
                for header, value in row.items()
            )
            for row in data
        )
        table = f"{header_row}\n{separator}\n{data_rows}"
        return table

    def agent(
        self,
        queue_name: str = "default",
        concurrency: int = 1,
        timeout: Optional[int] = None,
        on_task_requeued: Optional[
            Callable[[Dict[str, Any], str], Awaitable[None]]
        ] = None,
        rate_limit: Optional[int] = None,
    ):
        """
        Decorator to define and register a task.

        Args:
            queue_name (str, optional): Redis queue name.
            concurrency (int, optional): Max concurrency.
            timeout (int, optional): Timeout in seconds.
            on_task_requeued (Callable, optional): Callback called when task is requeued (e.g. due to timeout).
            rate_limit (int, optional): Max tasks per minute (None for unlimited).
        """
        # Validate parameters
        if concurrency <= 0:
            raise ValueError("concurrency must be > 0")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0 or None")
        if rate_limit is not None and rate_limit <= 0:
            raise ValueError("rate_limit must be > 0 or None")
        if not queue_name or not isinstance(queue_name, str):
            raise ValueError("queue_name must be a non-empty string")

        def decorator(func: Callable):
            # Registration happens at startup, no concurrency issues
            self.task_registry[queue_name] = {
                "func": func,
                "concurrency": concurrency,
                "timeout": timeout,
                "on_task_requeued": on_task_requeued,
                "rate_limit": rate_limit,
            }
            self.queue_status[queue_name] = "Running"

            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Check rate limit if configured
                if rate_limit:
                    current_minute = int(datetime.utcnow().timestamp() / 60)
                    rate_key = f"rate_limit:{queue_name}:{current_minute}"
                    
                    try:
                        current_count = await self.task_queue.redis.get(rate_key)
                        current_count = int(current_count) if current_count else 0
                        
                        if current_count >= rate_limit:
                            self.logger.warning(
                                f"Rate limit exceeded for queue '{queue_name}': {current_count}/{rate_limit} per minute"
                            )
                            raise mTaskError(
                                f"Rate limit exceeded for queue '{queue_name}': {current_count}/{rate_limit} per minute"
                            )
                        
                        # Increment counter with 2 minute expiry
                        await self.task_queue.redis.incr(rate_key)
                        await self.task_queue.redis.expire(rate_key, 120)
                    except Exception as e:
                        if isinstance(e, mTaskError):
                            raise
                        self.logger.error(f"Error checking rate limit: {e}")
                        # Continue if rate limit check fails (fail open)
                
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
                        data = model(**kwargs).model_dump()
                    except Exception as e:
                        self.logger.error(f"Error parsing data with model {model}: {e}")
                        raise

                    try:
                        task_id = await self.task_queue.enqueue(
                            queue_name=queue_name,
                            kwargs=data,
                            priority=0,
                            max_task_size=self.max_task_size,
                        )
                        self.logger.info(
                            f"Task for queue '{queue_name}' enqueued with ID {task_id}"
                        )
                        return task_id
                    except TaskEnqueueError as e:
                        self.logger.error(f"Failed to enqueue task: {e}")
                        raise
                    except ValueError as e:
                        self.logger.error(f"Task validation failed: {e}")
                        raise
                else:
                    try:
                        task_id = await self.task_queue.enqueue(
                            queue_name=queue_name,
                            kwargs=kwargs,
                            priority=0,
                            max_task_size=self.max_task_size,
                        )
                        self.logger.info(
                            f"Task for queue '{queue_name}' enqueued with ID {task_id}"
                        )
                        return task_id
                    except TaskEnqueueError as e:
                        self.logger.error(f"Failed to enqueue task: {e}")
                        raise
                    except ValueError as e:
                        self.logger.error(f"Task validation failed: {e}")
                        raise

            return wrapper

        return decorator

    def interval(
        self,
        seconds: int = 60,
    ):
        # Validate parameters
        if seconds <= 0:
            raise ValueError("seconds must be > 0")
        
        def decorator(func: Callable):
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
        # Validate cron expression
        if not cron_expression or not isinstance(cron_expression, str):
            raise ValueError("cron_expression must be a non-empty string")
        try:
            # Test if cron expression is valid
            croniter(cron_expression, datetime.now())
        except Exception as e:
            raise ValueError(f"Invalid cron expression: {e}")
        
        def decorator(func: Callable):
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
        data_model: Any,
        priority: int = 0,
    ) -> str:
        if isinstance(data_model, BaseModel):
            data = data_model.model_dump()
        elif isinstance(data_model, dict):
            data = data_model
        else:
            self.logger.error("data_model must be a Pydantic model or a dictionary.")
            raise ValueError("data_model must be a Pydantic model or a dictionary.")

        try:
            task_id = await self.task_queue.enqueue(
                queue_name=queue_name,
                kwargs=data,
                priority=priority,
                max_task_size=self.max_task_size,
            )
            self.logger.info(
                f"Manual task enqueued in queue '{queue_name}' with ID {task_id} (priority={priority})"
            )
            return task_id
        except TaskEnqueueError as e:
            self.logger.error(f"Failed to enqueue manual task: {e}")
            raise

    async def _move_to_dlq(self, task: Dict[str, Any], queue_name: str, error: str):
        """Move a failed task to the Dead Letter Queue."""
        if not self.enable_dlq:
            self.logger.debug(f"DLQ disabled, task {task['id']} will not be moved to DLQ")
            return
        
        dlq_name = f"{queue_name}:dlq"
        task['error'] = error
        task['failed_at'] = datetime.utcnow().isoformat()
        task['status'] = 'failed'
        
        try:
            await self.task_queue.redis.rpush(dlq_name, json.dumps(task))
            self.logger.error(
                f"Task {task['id']} moved to DLQ '{dlq_name}' after {self.retry_limit} retries. Error: {error}"
            )
        except Exception as e:
            self.logger.exception(f"Failed to move task {task['id']} to DLQ: {e}")

    async def get_dlq_tasks(self, queue_name: str) -> List[Dict[str, Any]]:
        """Get all tasks from the Dead Letter Queue for a specific queue."""
        dlq_name = f"{queue_name}:dlq"
        try:
            tasks_json = await self.task_queue.redis.lrange(dlq_name, 0, -1)
            tasks = [json.loads(task_json) for task_json in tasks_json]
            return tasks
        except Exception as e:
            self.logger.exception(f"Failed to get DLQ tasks from '{dlq_name}': {e}")
            return []

    async def retry_dlq_task(self, task_id: str, queue_name: str) -> bool:
        """Retry a task from the Dead Letter Queue by re-enqueueing it."""
        dlq_name = f"{queue_name}:dlq"
        try:
            tasks_json = await self.task_queue.redis.lrange(dlq_name, 0, -1)
            for index, task_json in enumerate(tasks_json):
                task = json.loads(task_json)
                if task['id'] == task_id:
                    # Remove from DLQ
                    await self.task_queue.redis.lrem(dlq_name, 0, task_json)
                    
                    # Reset task for retry
                    task['retry_count'] = 0
                    task['status'] = 'pending'
                    task.pop('error', None)
                    task.pop('failed_at', None)
                    task.pop('start_time', None)
                    
                    # Re-enqueue
                    await self.task_queue.redis.rpush(queue_name, json.dumps(task))
                    self.logger.info(f"Task {task_id} moved from DLQ back to queue '{queue_name}'")
                    return True
            
            self.logger.warning(f"Task {task_id} not found in DLQ '{dlq_name}'")
            return False
        except Exception as e:
            self.logger.exception(f"Failed to retry DLQ task {task_id}: {e}")
            return False

    async def clear_dlq(self, queue_name: str) -> int:
        """Clear all tasks from the Dead Letter Queue for a specific queue."""
        dlq_name = f"{queue_name}:dlq"
        try:
            count = await self.task_queue.redis.llen(dlq_name)
            if count > 0:
                await self.task_queue.redis.delete(dlq_name)
                self.logger.info(f"Cleared {count} tasks from DLQ '{dlq_name}'")
            return count
        except Exception as e:
            self.logger.exception(f"Failed to clear DLQ '{dlq_name}': {e}")
            return 0

    def start_worker(
        self,
        queue_name: str,
        semaphore: Optional[asyncio.Semaphore] = None,
    ):
        if queue_name in self.workers:
            self.logger.warning(f"Worker for queue '{queue_name}' is already running.")
            return

        task_info = self.task_registry.get(queue_name, {})
        concurrency = task_info.get("concurrency", 1)
        semaphore = semaphore if semaphore else asyncio.Semaphore(concurrency)
        # Worker уже извлекает on_task_requeued в конструкторе по queue_name

        worker = Worker(
            task_queue=self.task_queue,
            task_registry=self.task_registry,
            task_registry_lock=self.task_registry_lock,
            retry_limit=self.retry_limit,
            queue_name=queue_name,
            semaphore=semaphore,
            logger=self.logger,
            enable_dlq=self.enable_dlq,
            move_to_dlq_callback=self._move_to_dlq,
            record_metric_callback=self._record_metric,
        )
        self.workers[queue_name] = worker
        asyncio.create_task(worker.start())
        self.logger.debug(
            f"Started worker for queue '{queue_name}' with concurrency {semaphore._value}"
        )

    async def run_scheduled_tasks(self):
        while True:
            for task in self.scheduled_tasks:
                if task.should_run():
                    asyncio.create_task(task.run())
            await asyncio.sleep(1)

    async def connect_and_start_workers(self):
        try:
            self.task_queue = TaskQueue(
                redis_url=self._redis_url,
                logger=self.logger,
                operation_timeout=self._redis_operation_timeout,
                connect_timeout=self._redis_connect_timeout,
                max_connections=self._redis_max_connections,
                health_check_interval=self._redis_health_check_interval,
            )
            await self.task_queue.connect()
        except RedisConnectionError as e:
            self.logger.error(f"Cannot start workers without Redis connection: {e}")
            raise

        queue_names = list(self.task_registry.keys())

        for queue_name in queue_names:
            await self.task_queue.recover_processing_tasks(queue_name)

        for queue_name in queue_names:
            task_info = self.task_registry.get(queue_name, {})
            concurrency = task_info.get("concurrency", 1)
            semaphore = asyncio.Semaphore(concurrency)
            self.semaphores[queue_name] = semaphore
            self.start_worker(queue_name=queue_name, semaphore=semaphore)

        asyncio.create_task(self.run_scheduled_tasks())

    async def graceful_shutdown(self):
        """Perform graceful shutdown of all workers and disconnect from Redis."""
        if self._is_shutting_down:
            self.logger.warning("Shutdown already in progress.")
            return
        
        self._is_shutting_down = True
        self.logger.info("Starting graceful shutdown...")
        
        # Stop all workers with timeout
        shutdown_tasks = []
        for queue_name, worker in self.workers.items():
            self.logger.info(f"Stopping worker for queue '{queue_name}'...")
            shutdown_tasks.append(worker.stop(graceful=True, timeout=self.shutdown_timeout))
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        # Disconnect from Redis
        await self.task_queue.disconnect()
        self.logger.info("Graceful shutdown completed.")

    async def run(self):
        try:
            await self.connect_and_start_workers()
        except mTaskError as e:
            self.logger.error(f"Failed to start mTask: {e}")
            return

        asyncio.create_task(self.monitor_queue_status())

        self.logger.info("mTask is running. Press Ctrl+C to exit.")

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal (Ctrl+C)...")
            await self.graceful_shutdown()
        except Exception as e:
            self.logger.exception(f"Unexpected error: {e}")
            await self.graceful_shutdown()

    async def _get_queue_status(self, queue_name: str) -> str:
        status_key = f"queue_status:{queue_name}"
        status = await self.task_queue.redis.get(status_key) or "Running"
        return status

    async def pause_queue(self, queue_name: str, duration: int):
        # Validate parameters
        if duration <= 0:
            raise ValueError("duration must be > 0")
        if not queue_name or not isinstance(queue_name, str):
            raise ValueError("queue_name must be a non-empty string")
        
        if queue_name not in self.workers:
            self.logger.error(f"Queue '{queue_name}' not found.")
            raise mTaskError(f"Queue '{queue_name}' not found.")

        status_key = f"queue_status:{queue_name}"
        try:
            current_status = await self.task_queue.redis.get(status_key) or "Running"
            if current_status == "Paused":
                self.logger.warning(f"Queue '{queue_name}' is already paused.")
                return

            await self.task_queue.redis.set(status_key, "Paused", ex=duration)

            await self.workers[queue_name].stop()
            del self.workers[queue_name]

            # Move tasks from processing queue back to main queue
            processing_queue = f"{queue_name}:processing"
            key_type = await self.task_queue.redis.type(processing_queue)
            
            if key_type == "hash":
                # New Hash format
                tasks = await self.task_queue.redis.hgetall(processing_queue)
                if tasks:
                    pipe = self.task_queue.redis.pipeline()
                    for task_id, task_json in tasks.items():
                        pipe.lpush(queue_name, task_json)
                    await pipe.execute()
                    await self.task_queue.redis.delete(processing_queue)
                    self.logger.info(
                        f"Moved {len(tasks)} tasks from processing queue back to main queue '{queue_name}'."
                    )
            elif key_type == "list":
                # Old List format (backward compatibility)
                tasks = await self.task_queue.redis.lrange(processing_queue, 0, -1)
                if tasks:
                    await self.task_queue.redis.lpush(queue_name, *tasks[::-1])
                    await self.task_queue.redis.delete(processing_queue)
                    self.logger.info(
                        f"Moved {len(tasks)} tasks from processing queue back to main queue '{queue_name}'."
                    )

            if queue_name in self.semaphores:
                del self.semaphores[queue_name]

            async with self.queue_status_lock:
                self.queue_status[queue_name] = "Paused"
            self.logger.info(f"Queue '{queue_name}' paused for {duration} seconds.")
        except Exception as e:
            self.logger.exception(f"Failed to pause queue '{queue_name}': {e}")
            raise mTaskError(f"Failed to pause queue '{queue_name}': {e}") from e

    async def monitor_queue_status(self):
        while True:
            queue_names = list(self.task_registry.keys())

            for queue_name in queue_names:
                status_key = f"queue_status:{queue_name}"
                current_status = (
                    await self.task_queue.redis.get(status_key) or "Running"
                )
                previous_status = self.queue_status.get(queue_name, "Running")

                if current_status != previous_status:
                    if current_status == "Paused":
                        async with self.queue_status_lock:
                            self.queue_status[queue_name] = "Paused"
                        self.logger.info(f"Queue '{queue_name}' is paused.")
                        if queue_name in self.workers:
                            await self.workers[queue_name].stop()
                            del self.workers[queue_name]
                    elif current_status == "Running" or current_status is None:
                        async with self.queue_status_lock:
                            self.queue_status[queue_name] = "Running"
                        self.logger.info(f"Queue '{queue_name}' is resumed.")
                        async with self.task_registry_lock:
                            task_info = self.task_registry.get(queue_name, {})
                            concurrency = task_info.get("concurrency", 1)
                        semaphore = asyncio.Semaphore(concurrency)
                        self.semaphores[queue_name] = semaphore
                        self.start_worker(queue_name, semaphore)
                    else:
                        pass
            await asyncio.sleep(5)
