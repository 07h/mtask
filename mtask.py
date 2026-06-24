# mTask.py

import asyncio
import inspect
import json
import logging
import signal
import time
import uuid
from contextvars import ContextVar
from functools import wraps
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, List, Awaitable
from enum import Enum
import random

from croniter import croniter
import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisClientConnectionError
from pydantic import BaseModel, ValidationError

# NOTE: mtask no longer installs the uvloop event loop policy on import.
# Enable uvloop in your application entrypoint if desired (see example_usage.py).

# Queue name of the task currently being processed in this asyncio context.
# Used to detect pause_queue() calls made from inside a running task.
_current_worker_queue: ContextVar[Optional[str]] = ContextVar(
    "mtask_current_worker_queue", default=None
)

# ============================
# Helper Functions
# ============================


def calculate_backoff(retry_count: int, base_delay: int = 1, max_delay: int = 60) -> float:
    """Calculate exponential backoff with jitter."""
    delay = min(base_delay * (2 ** retry_count), max_delay)
    jitter = random.uniform(0, delay * 0.1)
    return delay + jitter


# Optional faster JSON via orjson (V9). Falls back to stdlib json when not
# installed. Safe for LREM matching: we always store and later compare the
# exact same string, so cross-process formatting differences do not matter.
try:
    import orjson as _orjson

    def _dumps(obj: Any) -> str:
        return _orjson.dumps(obj).decode("utf-8")

    def _loads(s: Any) -> Any:
        return _orjson.loads(s)
except ImportError:  # pragma: no cover - depends on environment
    def _dumps(obj: Any) -> str:
        return json.dumps(obj)

    def _loads(s: Any) -> Any:
        return json.loads(s)


# ============================
# Lua Scripts
# ============================

# Atomically pop the highest-priority task from the priority zset and push it
# into the processing list. Without this, a crash between ZPOPMAX and RPUSH
# would lose the task forever (it exists only in process memory).
PRIORITY_DEQUEUE_LUA = """
local popped = redis.call('ZPOPMAX', KEYS[1], 1)
if popped[1] == nil then
    return false
end
redis.call('RPUSH', KEYS[2], popped[1])
return popped[1]
"""

# Atomically move the head of the main list into the processing list.
# Equivalent to LMOVE (Redis 6.2+) but works on any Redis with scripting.
LIST_DEQUEUE_LUA = """
local task = redis.call('LPOP', KEYS[1])
if task == false then
    return false
end
redis.call('RPUSH', KEYS[2], task)
return task
"""

# Atomically move tasks whose backoff period has expired from the delayed
# zset (score = ready-at timestamp) back into their target queue. Tasks with
# priority > 0 go to the priority zset, the rest to the main list.
PROMOTE_DELAYED_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, task_json in ipairs(due) do
    redis.call('ZREM', KEYS[1], task_json)
    local priority = 0
    local ok, task = pcall(cjson.decode, task_json)
    if ok and type(task) == 'table' and tonumber(task['priority']) then
        priority = tonumber(task['priority'])
    end
    if priority > 0 then
        redis.call('ZADD', KEYS[2], priority * 1000000 - tonumber(ARGV[1]), task_json)
    else
        redis.call('RPUSH', KEYS[3], task_json)
    end
end
return #due
"""


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


# Deterministic failures that cannot succeed on retry: such tasks go
# straight to the DLQ instead of being retried with backoff.
NON_RETRYABLE_ERRORS = (ValidationError, TaskFunctionNotFoundError)


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
        self._reconnect_task: Optional[asyncio.Task] = None
        # Lock is created lazily inside the running event loop
        # (Python 3.8/3.9 bind asyncio.Lock to the loop at creation time)
        self._connect_lock: Optional[asyncio.Lock] = None
        # Lua scripts are (re-)registered lazily per Redis client instance
        self._scripts_client: Optional[redis.Redis] = None
        self._priority_dequeue_script = None
        self._promote_delayed_script = None
        self._list_dequeue_script = None
        # V6: throttle the delayed-promote script so it does not run on every
        # poll of every worker. Maps queue_name -> last promote monotonic ts.
        self._last_promote: Dict[str, float] = {}
        self._promote_interval: float = 0.5

    def _get_connect_lock(self) -> asyncio.Lock:
        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()
        return self._connect_lock

    def _ensure_scripts(self) -> None:
        """(Re-)register Lua scripts when the Redis client changes.

        register_script() is local (computes SHA only); the Script object
        falls back to EVAL on NOSCRIPT, so this survives reconnects and
        Redis restarts.
        """
        if self._scripts_client is not self.redis:
            self._priority_dequeue_script = self.redis.register_script(
                PRIORITY_DEQUEUE_LUA
            )
            self._promote_delayed_script = self.redis.register_script(
                PROMOTE_DELAYED_LUA
            )
            self._list_dequeue_script = self.redis.register_script(
                LIST_DEQUEUE_LUA
            )
            self._scripts_client = self.redis

    async def connect(self):
        # Avoid repeated connects/log spam if we're already connected.
        if self.redis is not None and self._connection_healthy:
            return

        async with self._get_connect_lock():
            # Re-check under lock
            if self.redis is not None and self._connection_healthy:
                return

            try:
                # If a client exists but is unhealthy, close it before recreating.
                if self.redis is not None:
                    try:
                        await self.redis.close()
                    except Exception:
                        pass

                self.redis = redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=self.max_connections,
                    socket_timeout=self.operation_timeout,
                    socket_connect_timeout=self.connect_timeout,
                    # No client-level retries: mtask has its own retry logic,
                    # and silent command retries can duplicate writes (RPUSH)
                    retry_on_timeout=False,
                )
                await asyncio.wait_for(self.redis.ping(), timeout=self.connect_timeout)
                self._connection_healthy = True
                self.logger.info(f"Connected to Redis at {self.redis_url}")

                # Start health check in background
                if not self._health_check_task or self._health_check_task.done():
                    self._health_check_task = asyncio.create_task(self._health_check_loop())
            except asyncio.TimeoutError:
                self._connection_healthy = False
                self.logger.error(
                    f"Connection to Redis timed out after {self.connect_timeout}s"
                )
                raise RedisConnectionError(
                    f"Connection to Redis timed out after {self.connect_timeout}s"
                )
            except Exception as e:
                self._connection_healthy = False
                self.logger.exception(f"Failed to connect to Redis: {e}")
                raise RedisConnectionError(f"Failed to connect to Redis: {e}") from e

    async def disconnect(self):
        # Stop health check and any in-flight reconnect
        for bg_task in (self._health_check_task, self._reconnect_task):
            if bg_task and not bg_task.done():
                bg_task.cancel()
                try:
                    await bg_task
                except asyncio.CancelledError:
                    pass
        
        if self.redis:
            await self.redis.close()
            self._connection_healthy = False
            self.logger.info("Disconnected from Redis")

    async def ensure_connected(self) -> None:
        """Ensure Redis client is initialized and responsive.

        This prevents producer-side usage errors where enqueue() is called
        before connect(), and provides a fast path during normal operation.

        When the connection is unhealthy this method FAILS FAST: it kicks off
        a single background reconnect (with backoff) and raises immediately
        instead of blocking every caller behind the connect lock for the
        whole reconnect cycle (which can take minutes).
        """
        # Fast path: client exists and marked healthy
        if self.redis is not None and self._connection_healthy:
            return

        # No client yet: perform the initial connect (short, bounded by
        # connect_timeout, so holding the lock here is fine)
        if self.redis is None:
            async with self._get_connect_lock():
                if self.redis is None:
                    await self.connect()
            if self.redis is not None and self._connection_healthy:
                return
            raise RedisConnectionError("Redis is not connected.")

        # Client exists but marked unhealthy: try one quick ping
        try:
            await asyncio.wait_for(self.redis.ping(), timeout=self.operation_timeout)
            self._connection_healthy = True
            return
        except Exception:
            self._connection_healthy = False

        # Fail fast for the caller; reconnect happens in the background
        self._start_background_reconnect()
        raise RedisConnectionError("Redis is not connected (reconnect in progress).")

    def _start_background_reconnect(self) -> None:
        """Start a single background reconnect task (idempotent)."""
        if self._reconnecting:
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect())

    async def _reconnect(self):
        """Attempt to reconnect to Redis with exponential backoff.

        The first attempt happens immediately; backoff sleeps occur only
        between failed attempts.
        """
        if self._reconnecting:
            return
        
        self._reconnecting = True
        try:
            attempt = 0
            while attempt < self.max_reconnect_attempts:
                attempt += 1
                self.logger.warning(
                    f"Attempting to reconnect to Redis "
                    f"(attempt {attempt}/{self.max_reconnect_attempts})..."
                )
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
                        retry_on_timeout=False,
                    )
                    await asyncio.wait_for(self.redis.ping(), timeout=self.connect_timeout)
                    self._connection_healthy = True
                    self.logger.info("Successfully reconnected to Redis")
                    return
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    self.logger.error(
                        f"Reconnection attempt {attempt} timed out after {self.connect_timeout}s"
                    )
                except Exception as e:
                    self.logger.error(f"Reconnection attempt {attempt} failed: {e}")

                if attempt < self.max_reconnect_attempts:
                    wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60s
                    self.logger.warning(f"Next reconnection attempt in {wait_time}s...")
                    await asyncio.sleep(wait_time)

            self.logger.error(
                f"Failed to reconnect to Redis after {self.max_reconnect_attempts} attempts"
            )
        finally:
            self._reconnecting = False
    
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
                self._start_background_reconnect()
            except Exception as e:
                if self._connection_healthy:
                    self._connection_healthy = False
                    self.logger.error(f"Redis health check failed: {e}")
                
                # Attempt to reconnect in background task to not block health check loop
                self._start_background_reconnect()

    async def enqueue(
        self,
        queue_name: str,
        kwargs: Dict[str, Any] = None,
        priority: int = 0,
        max_task_size: int = 1024 * 1024,
    ) -> str:
        await self.ensure_connected()

        task = {
            "id": str(uuid.uuid4()),
            "name": queue_name,
            "kwargs": kwargs or {},
            "status": "pending",
            "retry_count": 0,
            "priority": priority,
        }

        try:
            task_json = _dumps(task)
        except (TypeError, ValueError) as e:
            self.logger.error(f"Task kwargs are not JSON-serializable: {e}")
            raise TaskEnqueueError(
                f"Task kwargs are not JSON-serializable: {e}"
            ) from e

        # Check task size
        task_size = len(task_json.encode('utf-8'))
        if task_size > max_task_size:
            error_msg = f"Task size ({task_size} bytes) exceeds maximum allowed size ({max_task_size} bytes)"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            if priority > 0:
                # Use sorted set for priority queues (higher score = higher priority)
                # Use negative timestamp as tiebreaker for same priority
                score = priority * 1000000 - time.time()
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
        """
        Dequeue a task from the queue.
        
        Returns a task dict with '_task_json' field containing the JSON string
        stored in processing queue (needed for mark_completed with LREM).
        """
        await self.ensure_connected()
        self._ensure_scripts()

        processing_queue = f"{queue_name}:processing"
        priority_queue = f"{queue_name}:priority"
        delayed_queue = f"{queue_name}:delayed"
        
        try:
            # Move tasks whose backoff expired from the delayed zset back
            # into the main/priority queues (atomic Lua script). Throttled
            # per queue (V6) so N concurrent workers do not each run this on
            # every poll; correctness is unaffected since a slightly delayed
            # promotion only postpones a retry by < _promote_interval.
            now = time.monotonic()
            if now - self._last_promote.get(queue_name, 0.0) >= self._promote_interval:
                self._last_promote[queue_name] = now
                await asyncio.wait_for(
                    self._promote_delayed_script(
                        keys=[delayed_queue, priority_queue, queue_name],
                        args=[time.time(), 100],
                    ),
                    timeout=self.operation_timeout,
                )

            # Try priority queue first: ZPOPMAX + RPUSH-to-processing is done
            # atomically in Lua, so a crash cannot lose the task in between
            task_json = await asyncio.wait_for(
                self._priority_dequeue_script(
                    keys=[priority_queue, processing_queue]
                ),
                timeout=self.operation_timeout,
            )
            if task_json:
                task = await self._prepare_dequeued_task(
                    task_json, queue_name, processing_queue, delayed_queue
                )
                if task is not None:
                    self.logger.debug(
                        f"Dequeued priority task {task['id']} from queue '{queue_name}' "
                        f"to processing '{processing_queue}'"
                    )
                    return task
                # Poison message or legacy backoff task was filtered out:
                # fall through to the regular queue so it is not starved
            
            # Regular queue: atomic LPOP+RPUSH via Lua (works on any Redis
            # version, unlike LMOVE which needs 6.2+). Loop to skip over
            # poison messages / legacy backoff entries without starving.
            while True:
                task_json = await asyncio.wait_for(
                    self._list_dequeue_script(
                        keys=[queue_name, processing_queue]
                    ),
                    timeout=self.operation_timeout
                )
                if not task_json:
                    # No task available
                    return None

                task = await self._prepare_dequeued_task(
                    task_json, queue_name, processing_queue, delayed_queue
                )
                if task is not None:
                    self.logger.debug(
                        f"Dequeued task {task['id']} from queue '{queue_name}' "
                        f"to processing '{processing_queue}'"
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

    async def _prepare_dequeued_task(
        self,
        task_json: str,
        queue_name: str,
        processing_queue: str,
        delayed_queue: str,
    ) -> Optional[Dict[str, Any]]:
        """Validate a task that was just moved into the processing list.

        Returns the parsed task dict (with "_task_json" set for later LREM),
        or None when the entry was filtered out:
        - unparseable JSON (poison message) is moved to the DLQ instead of
          cycling through processing/recovery forever;
        - legacy tasks carrying a future "retry_after" field (written by
          older mtask versions) are migrated into the delayed zset.
        """
        try:
            task = _loads(task_json)
        except (json.JSONDecodeError, ValueError):
            self.logger.error(
                f"Unparseable task in queue '{queue_name}', moving to DLQ: {task_json[:200]!r}"
            )
            await asyncio.wait_for(
                self.redis.lrem(processing_queue, 1, task_json),
                timeout=self.operation_timeout
            )
            await asyncio.wait_for(
                self.redis.rpush(f"{queue_name}:dlq", task_json),
                timeout=self.operation_timeout
            )
            return None

        # Legacy backoff format: "retry_after" stored in the task body
        retry_after = task.get("retry_after") if isinstance(task, dict) else None
        if retry_after:
            if time.time() < retry_after:
                # Still in backoff: migrate to the delayed zset
                self.logger.debug(
                    f"Task {task.get('id')} still in backoff "
                    f"(legacy retry_after), moving to delayed zset"
                )
                task.pop("retry_after", None)
                migrated_json = _dumps(task)
                await asyncio.wait_for(
                    self.redis.lrem(processing_queue, 1, task_json),
                    timeout=self.operation_timeout
                )
                await asyncio.wait_for(
                    self.redis.zadd(delayed_queue, {migrated_json: retry_after}),
                    timeout=self.operation_timeout
                )
                return None
            # Backoff expired: strip the marker and refresh the processing entry
            task.pop("retry_after", None)
            new_task_json = _dumps(task)
            await asyncio.wait_for(
                self.redis.lrem(processing_queue, 1, task_json),
                timeout=self.operation_timeout
            )
            await asyncio.wait_for(
                self.redis.rpush(processing_queue, new_task_json),
                timeout=self.operation_timeout
            )
            task_json = new_task_json

        if not isinstance(task, dict) or "id" not in task:
            self.logger.error(
                f"Malformed task in queue '{queue_name}', moving to DLQ: {task_json[:200]!r}"
            )
            await asyncio.wait_for(
                self.redis.lrem(processing_queue, 1, task_json),
                timeout=self.operation_timeout
            )
            await asyncio.wait_for(
                self.redis.rpush(f"{queue_name}:dlq", task_json),
                timeout=self.operation_timeout
            )
            return None

        # Crash-loop protection (V1): bump the per-task delivery counter in
        # Redis. It is stored OUTSIDE the task body (a hash) so it survives a
        # process crash and does not require rewriting the processing entry.
        try:
            attempts = await asyncio.wait_for(
                self.redis.hincrby(f"{queue_name}:attempts", task["id"], 1),
                timeout=self.operation_timeout,
            )
            task["_delivery_attempts"] = attempts
        except Exception as e:
            # Counter is best-effort; never block delivery on it
            self.logger.warning(
                f"Failed to track delivery attempts for task {task.get('id')}: {e}"
            )

        # Store task_json for mark_completed (LREM needs exact value)
        task["_task_json"] = task_json
        return task

    async def clear_delivery_attempts(self, queue_name: str, task_id: str) -> None:
        """Drop the delivery counter for a task that reached a terminal state.

        Called on success / DLQ / non-retryable so the {queue}:attempts hash
        does not accumulate stale entries (V1).
        """
        try:
            await self.ensure_connected()
            await asyncio.wait_for(
                self.redis.hdel(f"{queue_name}:attempts", task_id),
                timeout=self.operation_timeout,
            )
        except Exception as e:
            self.logger.debug(
                f"Failed to clear delivery attempts for task {task_id}: {e}"
            )

    async def requeue(self, task: Dict[str, Any], queue_name: str = "default", apply_backoff: bool = True) -> None:
        await self.ensure_connected()

        task["status"] = "pending"
        task.pop("start_time", None)
        # Remove internal fields before serialization to avoid data growth
        task.pop("_task_json", None)
        # Delivery counter lives in Redis ({queue}:attempts), not in the body
        task.pop("_delivery_attempts", None)
        # Legacy backoff marker (older mtask versions): backoff is now
        # scheduled via the delayed zset, not stored in the task body
        task.pop("retry_after", None)
        priority = task.get("priority", 0)
        
        try:
            task_json = _dumps(task)

            if apply_backoff and task.get("retry_count", 0) > 0:
                # Park the task in the delayed zset until the backoff expires.
                # It is promoted back to its queue by dequeue() (Lua script),
                # so it neither blocks the queue nor gets shuffled around.
                backoff_delay = calculate_backoff(task["retry_count"])
                retry_at = time.time() + backoff_delay
                await asyncio.wait_for(
                    self.redis.zadd(f"{queue_name}:delayed", {task_json: retry_at}),
                    timeout=self.operation_timeout
                )
                self.logger.debug(
                    f"Task {task['id']} parked in delayed queue for {backoff_delay:.2f}s "
                    f"(retry {task['retry_count']}, ready at {retry_at})"
                )
            elif priority > 0:
                # Re-enqueue to priority queue
                score = priority * 1000000 - time.time()
                await asyncio.wait_for(
                    self.redis.zadd(f"{queue_name}:priority", {task_json: score}),
                    timeout=self.operation_timeout
                )
                self.logger.debug(
                    f"Requeued task {task['id']} to priority queue '{queue_name}' (retry {task['retry_count']})"
                )
            else:
                # Re-enqueue to regular queue
                await asyncio.wait_for(
                    self.redis.rpush(queue_name, task_json),
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

    async def mark_completed(
        self, 
        task_json: str, 
        queue_name: str,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> None:
        """
        Mark a task as completed by removing it from the processing queue.
        
        Includes retry logic for timeout errors to handle connection pool contention.
        
        Args:
            task_json: The exact JSON string of the task (as stored in processing list)
            queue_name: Name of the queue
            max_retries: Maximum number of retry attempts on timeout (default: 3)
            retry_delay: Base delay between retries in seconds (default: 1.0)
        """
        processing_queue = f"{queue_name}:processing"
        
        # Extract task_id for logging
        try:
            task = _loads(task_json)
            task_id = task.get("id", "unknown")
        except (json.JSONDecodeError, ValueError):
            task_id = "unknown"
        
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                await self.ensure_connected()
                # LREM removes element by value from List
                # count=1 means remove first occurrence (from head)
                deleted = await asyncio.wait_for(
                    self.redis.lrem(processing_queue, 1, task_json),
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
                return  # Success, exit
                
            except asyncio.TimeoutError as e:
                last_error = e
                if attempt < max_retries:
                    delay = retry_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.warning(
                        f"Mark completed timed out for task {task_id}, "
                        f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})"
                    )
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(
                        f"Mark completed failed for task {task_id} after {max_retries + 1} attempts"
                    )
            except (RedisClientConnectionError, RedisConnectionError) as e:
                self.logger.warning(
                    "Mark completed connection error for task %s (attempt %s/%s): %s",
                    task_id,
                    attempt + 1,
                    max_retries + 1,
                    e,
                )
                self._connection_healthy = False
                if attempt < max_retries:
                    delay = retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    raise TaskProcessingError(
                        f"Failed to mark task {task_id} as completed: {e}"
                    ) from e
            except Exception as e:
                self.logger.exception(f"Failed to mark task {task_id} as completed: {e}")
                raise TaskProcessingError(
                    f"Failed to mark task {task_id} as completed: {e}"
                ) from e
        
        # All retries exhausted
        raise TaskProcessingError(
            f"Mark completed operation timed out for task {task_id} after {max_retries + 1} attempts"
        )

    async def recover_processing_tasks(self, queue_name: str):
        """
        Recover tasks from processing queue (List) back to main queue.
        
        This is called at startup to recover tasks that were left in processing
        due to a crash or unexpected shutdown.
        """
        await self.ensure_connected()
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
            
            # Handle migration from old Hash format to new List format
            if key_type == "hash":
                await self._migrate_processing_hash_to_list(queue_name)
                key_type = "list"
            
            if key_type != "list":
                self.logger.warning(
                    f"Processing queue '{processing_queue}' has unexpected type '{key_type}', skipping recovery"
                )
                return
            
            # Get count of tasks in processing
            total_tasks = await asyncio.wait_for(
                self.redis.llen(processing_queue),
                timeout=self.operation_timeout
            )
            
            if total_tasks == 0:
                return
            
            recovered = 0
            
            # Move all tasks from processing back to main queue using RPOPLPUSH
            # This is atomic and preserves order
            while True:
                try:
                    task_json = await asyncio.wait_for(
                        self.redis.rpoplpush(processing_queue, main_queue),
                        timeout=self.operation_timeout
                    )
                    if not task_json:
                        break
                    recovered += 1
                    
                    if recovered % 500 == 0 and recovered < total_tasks:
                        self.logger.info(
                            f"Recovery progress: {recovered}/{total_tasks} tasks moved from '{processing_queue}' to '{main_queue}'"
                        )
                except Exception:
                    break
            
            if recovered > 0:
                self.logger.info(
                    f"Recovered {recovered} tasks from processing '{processing_queue}' back to main '{main_queue}'."
                )
        except asyncio.TimeoutError:
            self.logger.error(f"Recovery operation timed out after {self.operation_timeout}s")
        except Exception as e:
            self.logger.exception(
                f"Failed to recover tasks from processing queue '{processing_queue}': {e}"
            )
    
    async def _migrate_processing_hash_to_list(self, queue_name: str):
        """Migrate processing queue from Hash to List format (one-time migration)."""
        processing_queue = f"{queue_name}:processing"
        self.logger.info(f"Migrating processing queue '{processing_queue}' from Hash to List format...")
        
        try:
            # Get all tasks from Hash
            tasks = await asyncio.wait_for(
                self.redis.hgetall(processing_queue),
                timeout=self.operation_timeout
            )
            
            if not tasks:
                await self.redis.delete(processing_queue)
                return
            
            # Convert to List
            temp_list = f"{processing_queue}:migration_temp"
            pipe = self.redis.pipeline()
            for task_id, task_json in tasks.items():
                pipe.rpush(temp_list, task_json)
            await pipe.execute()
            
            # Atomic rename (delete old, rename temp)
            await self.redis.delete(processing_queue)
            await self.redis.rename(temp_list, processing_queue)
            
            self.logger.info(
                f"Successfully migrated {len(tasks)} tasks in '{processing_queue}' from Hash to List format."
            )
        except Exception as e:
            self.logger.exception(f"Failed to migrate processing queue '{processing_queue}': {e}")

    async def get_task_count(self, queue_name: str) -> int:
        try:
            await self.ensure_connected()
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
            await self.ensure_connected()
            count = await asyncio.wait_for(
                self.redis.llen(processing_queue),
                timeout=self.operation_timeout
            )
            return count
        except asyncio.TimeoutError:
            self.logger.error(f"Get processing task count timed out after {self.operation_timeout}s")
            return 0
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
        concurrency: int = 1,
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
        self.concurrency = concurrency
        self._running = False
        self._workers: List[asyncio.Task] = []
        self._active_tasks: int = 0
        self.logger = logger or logging.getLogger(f"Worker-{queue_name}")
        self.enable_dlq = enable_dlq
        self.move_to_dlq_callback = move_to_dlq_callback
        self.record_metric_callback = record_metric_callback
        # Grace period after a timeout cancellation before the slot is freed
        # regardless of whether the coroutine honoured the cancel (V3).
        self.cancel_grace_period: float = 1.0
        # Strong references so detached/background tasks are not GC'd (V3/V7)
        self._detached_tasks: set = set()
        self._bg_tasks: set = set()

        self.logger.debug(
            f"Worker initialized with concurrency={concurrency} for queue '{queue_name}'"
        )

    def _spawn_callback(self, callback: Optional[Callable], task: Dict[str, Any], reason: str) -> None:
        """Run an on_task_requeued callback off the worker hot path (V7).

        A slow or hanging callback must not hold the concurrency slot, so it
        runs as a tracked background task with exceptions logged.
        """
        if callback is None:
            return

        cb_task = asyncio.ensure_future(callback(task, reason))
        self._bg_tasks.add(cb_task)

        def _on_done(t: asyncio.Task) -> None:
            self._bg_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self.logger.error(
                    "on_task_requeued callback failed: %r", t.exception()
                )

        cb_task.add_done_callback(_on_done)

    async def start(self):
        if self._running:
            self.logger.warning("Worker is already running.")
            return

        self._running = True
        self.logger.info(
            f"Starting {self.concurrency} worker(s) for queue '{self.queue_name}'"
        )

        for i in range(self.concurrency):
            monitor_task = asyncio.create_task(self._monitor_worker(i + 1))
            self._workers.append(monitor_task)

    async def _monitor_worker(self, worker_id: int):
        while self._running:
            worker_task = asyncio.create_task(self._worker_loop(worker_id))
            try:
                await worker_task
            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_id}: Received shutdown signal.")
                # Cancelling this monitor does NOT cancel the inner Task —
                # propagate the cancellation explicitly so the worker loop
                # actually stops and can requeue its in-flight task.
                if not worker_task.done():
                    worker_task.cancel()
                    await asyncio.gather(worker_task, return_exceptions=True)
                break
            except Exception as e:
                self.logger.exception(f"Worker {worker_id}: Unexpected error: {e}")
                await asyncio.sleep(1)
                self.logger.info(
                    f"Worker {worker_id}: Restarting after unexpected termination."
                )

    async def _worker_loop(self, worker_id: int):
        task = None
        idle_sleep = 0.1
        while self._running:
            try:
                task = await self.task_queue.dequeue(queue_name=self.queue_name)
                if task:
                    self.logger.info(
                        f"Worker {worker_id}: Dequeued task {task['id']} from queue '{self.queue_name}'"
                    )
                    # Transfer ownership: process_task handles requeue/cleanup
                    # itself (including on cancellation), so the CancelledError
                    # branch below must not requeue it a second time.
                    current_task, task = task, None
                    await self.process_task(current_task, worker_id)
                    idle_sleep = 0.1
                else:
                    # Adaptive idle backoff: poll fast when busy, ease off
                    # to 1s on an empty queue to reduce Redis load
                    await asyncio.sleep(idle_sleep)
                    idle_sleep = min(idle_sleep * 2, 1.0)
            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_id}: Received shutdown signal.")
                # Return task to queue if it was dequeued but not processed
                if task:
                    # Save _task_json before requeue (requeue removes it)
                    task_json = task.get("_task_json")
                    try:
                        await self.task_queue.requeue(task, queue_name=self.queue_name, apply_backoff=False)
                        self.logger.info(f"Worker {worker_id}: Returned task {task['id']} to queue due to shutdown.")
                        # Remove from processing queue to prevent duplicate execution after recovery
                        if task_json:
                            try:
                                await self.task_queue.mark_completed(task_json, self.queue_name)
                            except Exception as mc_err:
                                self.logger.warning(
                                    f"Worker {worker_id}: Failed to remove task from processing: {mc_err}"
                                )
                    except Exception as e:
                        self.logger.error(f"Worker {worker_id}: Failed to return task {task['id']} to queue: {e}")
                break
            except Exception as e:
                self.logger.exception(f"Worker {worker_id}: Error in worker loop: {e}")
                task = None  # Clear task reference on error
                await asyncio.sleep(1)

    async def stop(self, graceful: bool = True, timeout: int = 30):
        """Stop workers, optionally waiting for active tasks to complete."""
        if not self._running:
            self.logger.warning("Worker is not running.")
            return

        self._running = False
        
        if graceful and self._active_tasks > 0:
            self.logger.info(f"Gracefully stopping workers, waiting for {self._active_tasks} active tasks...")
            start_time = time.monotonic()
            
            while self._active_tasks > 0:
                elapsed = time.monotonic() - start_time
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

    async def _run_with_timeout(self, coro: Awaitable, timeout: Optional[float]):
        """Run a coroutine, enforcing a timeout that frees the worker slot.

        Unlike asyncio.wait_for, which waits indefinitely for an
        uncooperative coroutine to finish cancelling, this method cancels the
        task, waits only a short grace period, and then returns control even
        if the task ignored the cancellation. The still-running coroutine is
        detached (a strong reference is kept so it is not garbage-collected)
        and raises asyncio.TimeoutError to the caller. This guarantees the
        concurrency slot is released on timeout (V3).
        """
        if not timeout:
            await coro
            return

        coro_task = asyncio.ensure_future(coro)
        done, _ = await asyncio.wait({coro_task}, timeout=timeout)
        if coro_task in done:
            # Propagate result/exception
            coro_task.result()
            return

        coro_task.cancel()
        # Give the task a short window to honour the cancellation
        await asyncio.wait({coro_task}, timeout=self.cancel_grace_period)
        if not coro_task.done():
            self.logger.warning(
                "Task did not stop within %.1fs grace after timeout; "
                "detaching it so the worker slot is freed (it may keep "
                "running in the background).",
                self.cancel_grace_period,
            )
            self._detached_tasks.add(coro_task)
            coro_task.add_done_callback(self._detached_tasks.discard)
        raise asyncio.TimeoutError

    async def process_task(self, task: Dict[str, Any], worker_id: int):
        queue_name = task["name"]
        kwargs = task.get("kwargs", {})
        self._active_tasks += 1
        # Save the exact JSON stored in the processing list BEFORE any
        # requeue() call: requeue() pops "_task_json" from the task dict,
        # and the finally block below needs it for LREM cleanup.
        task_json = task.get("_task_json")
        # When True, the finally block keeps the processing entry in place
        # (for startup recovery) instead of removing it (V4).
        keep_in_processing = False
        # Mark this asyncio context as "inside a worker task" so that
        # pause_queue() can detect re-entrant calls (see mTask.pause_queue).
        ctx_token = _current_worker_queue.set(self.queue_name)
        
        try:
            # Read without lock - task_registry is immutable after startup
            # Python dict.get() is thread-safe for simple reads due to GIL
            task_info = self.task_registry.get(queue_name, {})
            func = task_info.get("func")
            timeout = task_info.get("timeout")
            on_task_requeued = task_info.get("on_task_requeued")
            # Cached at registration (V5) - avoids inspect.signature per task
            model_param_name = task_info.get("model_param_name")
            model_class = task_info.get("model_class")
            
            if not func:
                self.logger.error(f"Task function for queue '{queue_name}' not found.")
                raise TaskFunctionNotFoundError(
                    f"Task function for queue '{queue_name}' not found."
                )

            # Crash-loop / redelivery protection (V1): the delivery counter
            # lives in Redis and survives process death, unlike retry_count
            # which only advances inside the except handlers below.
            delivery_attempts = task.get("_delivery_attempts")
            if (
                delivery_attempts is not None
                and delivery_attempts > self.retry_limit + 1
            ):
                error_msg = (
                    f"Task exceeded max delivery attempts "
                    f"({delivery_attempts} > {self.retry_limit + 1}); "
                    f"likely crashing the worker on each run"
                )
                self.logger.error(
                    f"Worker {worker_id}: Task {task['id']} {error_msg}"
                )
                if self.record_metric_callback:
                    await self.record_metric_callback(self.queue_name, "failed")
                if self.enable_dlq and self.move_to_dlq_callback:
                    await self.move_to_dlq_callback(task, self.queue_name, error_msg)
                await self.task_queue.clear_delivery_attempts(self.queue_name, task["id"])
                return
    
            # Execute the task
            task["start_time"] = time.time()
            self.logger.info(
                f"Worker {worker_id}: Executing task {task['id']} from queue '{queue_name}'"
            )

            if model_param_name and model_class:
                data_model = model_class(**kwargs)
                self.logger.info(
                    f"Worker {worker_id}: Executing task {task['id']} - '{queue_name}' with data={data_model}"
                )
                # Pass the model using the actual parameter name from the signature
                call_kwargs = {model_param_name: data_model}
                await self._run_with_timeout(func(**call_kwargs), timeout)
            else:
                self.logger.info(
                    f"Worker {worker_id}: Executing task {task['id']} - '{queue_name}' with kwargs={kwargs}"
                )
                await self._run_with_timeout(func(**kwargs), timeout)

            # Record successful completion metrics
            execution_time = time.time() - task["start_time"]
            if self.record_metric_callback:
                await self.record_metric_callback(self.queue_name, "completed")
                await self.record_metric_callback(self.queue_name, "execution_time", execution_time)
            # Task done for good: drop its delivery counter (V1)
            await self.task_queue.clear_delivery_attempts(self.queue_name, task["id"])
            
            self.logger.info(
                f"Worker {worker_id}: Task {task['id']} completed successfully in {execution_time:.2f}s."
            )
        except asyncio.CancelledError:
            # Worker is being cancelled mid-execution (shutdown/pause):
            # return the task to the queue for redelivery, then re-raise.
            # The finally block removes the old processing entry.
            self.logger.info(
                f"Worker {worker_id}: Task {task['id']} cancelled mid-execution, requeueing."
            )
            try:
                await self.task_queue.requeue(
                    task, queue_name=self.queue_name, apply_backoff=False
                )
            except Exception as req_e:
                self.logger.error(
                    f"Worker {worker_id}: Failed to requeue cancelled task {task['id']}: {req_e} "
                    f"(it will be recovered from the processing queue on next startup)"
                )
                # Keep the processing entry so recovery can pick it up
                task_json = None
            raise
        except NON_RETRYABLE_ERRORS as e:
            # Deterministic failures (validation, missing handler):
            # retrying cannot succeed, move straight to DLQ.
            error_msg = f"Non-retryable error: {e}"
            self.logger.error(
                f"Worker {worker_id}: Task {task['id']} failed with non-retryable error: {e}"
            )
            if self.record_metric_callback:
                await self.record_metric_callback(self.queue_name, "failed")
            if self.enable_dlq and self.move_to_dlq_callback:
                await self.move_to_dlq_callback(task, self.queue_name, error_msg)
            await self.task_queue.clear_delivery_attempts(self.queue_name, task["id"])
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
                    # Fire the requeue callback off the hot path (V7) so a slow
                    # or hanging callback cannot block the worker slot.
                    self._spawn_callback(on_task_requeued, task, "timeout")
                except TaskRequeueError as re:
                    # Requeue failed: keep the processing entry for recovery
                    # instead of dropping the task (V4).
                    keep_in_processing = True
                    self.logger.error(
                        f"Worker {worker_id}: Failed to requeue task {task['id']}: {re} "
                        f"(kept in processing for recovery)"
                    )
            else:
                error_msg = f"Task exceeded timeout of {timeout}s after {self.retry_limit} retries"
                self.logger.error(f"Worker {worker_id}: Task {task['id']} {error_msg}")
                # Record failure metric
                if self.record_metric_callback:
                    await self.record_metric_callback(self.queue_name, "failed")
                if self.enable_dlq and self.move_to_dlq_callback:
                    await self.move_to_dlq_callback(task, self.queue_name, error_msg)
                await self.task_queue.clear_delivery_attempts(self.queue_name, task["id"])
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
                except TaskRequeueError as re:
                    # Requeue failed: keep the processing entry for recovery
                    # instead of dropping the task (V4).
                    keep_in_processing = True
                    self.logger.error(
                        f"Worker {worker_id}: Failed to requeue task {task['id']}: {re} "
                        f"(kept in processing for recovery)"
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
                await self.task_queue.clear_delivery_attempts(self.queue_name, task["id"])
        finally:
            _current_worker_queue.reset(ctx_token)
            self._active_tasks -= 1
            try:
                # Use the task_json saved BEFORE any requeue() call (requeue
                # pops "_task_json" from the dict), so the old processing
                # entry is removed even after a retry was scheduled.
                # Note: use self.queue_name (worker's queue), not queue_name (task's name)
                # keep_in_processing means a requeue failed and we deliberately
                # leave the entry for startup recovery (V4).
                if task_json and not keep_in_processing:
                    await self.task_queue.mark_completed(task_json, self.queue_name)
                    self.logger.info(
                        f"Worker {worker_id}: Task {task['id']} removed from processing queue."
                    )
            except Exception as e:
                # Do NOT requeue here: the task either completed successfully,
                # was already requeued for retry, or sits in the DLQ. A stale
                # processing entry is recovered (at most once) on next startup,
                # which is preferable to actively creating a duplicate now.
                self.logger.exception(
                    f"Worker {worker_id}: Failed to remove task {task['id']} from processing queue: {e}"
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
        # Schedule the next run from the TRIGGER time, not from completion:
        # otherwise a 50s task with interval=60 effectively runs every ~110s
        # (interval drift). Overlap is still prevented by is_running.
        self.last_run = datetime.now()
        self.next_run = self._get_next_run()
        try:
            await self.func()
        except Exception as e:
            self.logger.exception(f"Error in scheduled task {self.func.__name__}: {e}")
        finally:
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
        self.queue_status: Dict[str, str] = {}
        self.shutdown_timeout = shutdown_timeout
        self.enable_dlq = enable_dlq
        self.max_task_size = max_task_size
        self._is_shutting_down = False
        self._metrics: Dict[str, Dict[str, Any]] = {}
        self._initialize_metrics()

        # Internal service tasks and references to fire-and-forget tasks
        # (keeping references prevents GC and lets us observe exceptions)
        self._monitor_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._background_tasks: set = set()

        # Locks are created lazily inside the running event loop:
        # on Python 3.8/3.9 asyncio.Lock() binds to the loop at creation
        # time, which breaks when mTask is instantiated at module level.
        self._task_registry_lock: Optional[asyncio.Lock] = None
        self._queue_status_lock: Optional[asyncio.Lock] = None
        self._metrics_lock: Optional[asyncio.Lock] = None

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

    @property
    def task_registry_lock(self) -> asyncio.Lock:
        if self._task_registry_lock is None:
            self._task_registry_lock = asyncio.Lock()
        return self._task_registry_lock

    @property
    def queue_status_lock(self) -> asyncio.Lock:
        if self._queue_status_lock is None:
            self._queue_status_lock = asyncio.Lock()
        return self._queue_status_lock

    @property
    def metrics_lock(self) -> asyncio.Lock:
        if self._metrics_lock is None:
            self._metrics_lock = asyncio.Lock()
        return self._metrics_lock

    def _spawn_background(self, coro: Awaitable, name: str) -> asyncio.Task:
        """Create a background task, keep a reference and log its failure.

        asyncio only keeps weak references to tasks: without storing a
        reference a fire-and-forget task can be garbage collected mid-flight
        and its exception silently dropped.
        """
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self.logger.error(
                    "Background task '%s' failed: %r", name, t.exception()
                )

        task.add_done_callback(_on_done)
        return task

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
                    "last_updated": datetime.now(timezone.utc).isoformat(),
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
            
            self._metrics[queue_name]["last_updated"] = datetime.now(timezone.utc).isoformat()
    
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
            rate_limit (int, optional): Max task ENQUEUES per minute via the
                decorated wrapper (None for unlimited). Note: this limits the
                producer side, not worker execution rate, and does not apply
                to add_task().
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
            # Fail fast on signatures the worker cannot call
            if not inspect.iscoroutinefunction(func):
                raise ValueError(
                    f"Task function '{func.__name__}' for queue '{queue_name}' "
                    f"must be an async function"
                )
            # Resolve the pydantic-model parameter once at registration time
            # (V5): process_task reuses this instead of calling
            # inspect.signature() on every single task execution.
            model_param_name: Optional[str] = None
            model_class: Optional[type] = None
            for param_name, param in inspect.signature(func).parameters.items():
                if (
                    param.annotation
                    and inspect.isclass(param.annotation)
                    and issubclass(param.annotation, BaseModel)
                ):
                    if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                        raise ValueError(
                            f"Model parameter '{param_name}' of task function "
                            f"'{func.__name__}' must be addressable by keyword "
                            f"(it is positional-only)"
                        )
                    if model_param_name is None:
                        model_param_name = param_name
                        model_class = param.annotation

            # Registration happens at startup, no concurrency issues
            self.task_registry[queue_name] = {
                "func": func,
                "concurrency": concurrency,
                "timeout": timeout,
                "on_task_requeued": on_task_requeued,
                "rate_limit": rate_limit,
                "model_param_name": model_param_name,
                "model_class": model_class,
            }
            self.queue_status[queue_name] = "Running"

            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Check rate limit if configured
                if rate_limit:
                    current_minute = int(time.time() / 60)
                    rate_key = f"rate_limit:{queue_name}:{current_minute}"
                    
                    try:
                        await self.task_queue.ensure_connected()
                        # INCR is atomic, so concurrent producers cannot
                        # slip past the limit (no GET-then-INCR race)
                        current_count = await asyncio.wait_for(
                            self.task_queue.redis.incr(rate_key),
                            timeout=self.task_queue.operation_timeout,
                        )
                        if current_count == 1:
                            # Fresh counter: set expiry (key is per-minute,
                            # 2 min TTL covers clock skew)
                            await asyncio.wait_for(
                                self.task_queue.redis.expire(rate_key, 120),
                                timeout=self.task_queue.operation_timeout,
                            )
                        
                        if current_count > rate_limit:
                            self.logger.warning(
                                f"Rate limit exceeded for queue '{queue_name}': {current_count}/{rate_limit} per minute"
                            )
                            raise mTaskError(
                                f"Rate limit exceeded for queue '{queue_name}': {current_count}/{rate_limit} per minute"
                            )
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
        # Strip internal fields: "_task_json" contains a full JSON copy of
        # the task and would double the DLQ entry size (and grow
        # exponentially across DLQ retry cycles)
        task.pop('_task_json', None)
        task.pop('start_time', None)
        task['error'] = error
        task['failed_at'] = datetime.now(timezone.utc).isoformat()
        task['status'] = 'failed'
        
        try:
            await self.task_queue.ensure_connected()
            await asyncio.wait_for(
                self.task_queue.redis.rpush(dlq_name, json.dumps(task)),
                timeout=self.task_queue.operation_timeout,
            )
            self.logger.error(
                f"Task {task['id']} moved to DLQ '{dlq_name}' after {self.retry_limit} retries. Error: {error}"
            )
        except Exception as e:
            self.logger.exception(f"Failed to move task {task['id']} to DLQ: {e}")

    async def get_dlq_tasks(self, queue_name: str) -> List[Dict[str, Any]]:
        """Get all tasks from the Dead Letter Queue for a specific queue."""
        dlq_name = f"{queue_name}:dlq"
        try:
            await self.task_queue.ensure_connected()
            tasks_json = await asyncio.wait_for(
                self.task_queue.redis.lrange(dlq_name, 0, -1),
                timeout=self.task_queue.operation_timeout,
            )
            tasks = []
            for task_json in tasks_json:
                try:
                    tasks.append(json.loads(task_json))
                except (json.JSONDecodeError, ValueError):
                    # Poison messages land in the DLQ as raw strings
                    tasks.append({"id": None, "raw": task_json, "status": "failed"})
            return tasks
        except Exception as e:
            self.logger.exception(f"Failed to get DLQ tasks from '{dlq_name}': {e}")
            return []

    async def retry_dlq_task(self, task_id: str, queue_name: str) -> bool:
        """Retry a task from the Dead Letter Queue by re-enqueueing it."""
        dlq_name = f"{queue_name}:dlq"
        try:
            await self.task_queue.ensure_connected()
            tasks_json = await asyncio.wait_for(
                self.task_queue.redis.lrange(dlq_name, 0, -1),
                timeout=self.task_queue.operation_timeout,
            )
            for index, task_json in enumerate(tasks_json):
                try:
                    task = json.loads(task_json)
                except (json.JSONDecodeError, ValueError):
                    continue
                if isinstance(task, dict) and task.get('id') == task_id:
                    # Remove from DLQ
                    await asyncio.wait_for(
                        self.task_queue.redis.lrem(dlq_name, 0, task_json),
                        timeout=self.task_queue.operation_timeout,
                    )
                    
                    # Reset task for retry and strip internal/stale fields
                    task['retry_count'] = 0
                    task['status'] = 'pending'
                    task.pop('error', None)
                    task.pop('failed_at', None)
                    task.pop('start_time', None)
                    task.pop('_task_json', None)
                    task.pop('retry_after', None)
                    
                    # Re-enqueue
                    await asyncio.wait_for(
                        self.task_queue.redis.rpush(queue_name, json.dumps(task)),
                        timeout=self.task_queue.operation_timeout,
                    )
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
            await self.task_queue.ensure_connected()
            count = await asyncio.wait_for(
                self.task_queue.redis.llen(dlq_name),
                timeout=self.task_queue.operation_timeout,
            )
            if count > 0:
                await asyncio.wait_for(
                    self.task_queue.redis.delete(dlq_name),
                    timeout=self.task_queue.operation_timeout,
                )
                self.logger.info(f"Cleared {count} tasks from DLQ '{dlq_name}'")
            return count
        except Exception as e:
            self.logger.exception(f"Failed to clear DLQ '{dlq_name}': {e}")
            return 0

    def start_worker(self, queue_name: str):
        if queue_name in self.workers:
            self.logger.warning(f"Worker for queue '{queue_name}' is already running.")
            return

        task_info = self.task_registry.get(queue_name, {})
        concurrency = task_info.get("concurrency", 1)

        worker = Worker(
            task_queue=self.task_queue,
            task_registry=self.task_registry,
            task_registry_lock=self.task_registry_lock,
            retry_limit=self.retry_limit,
            queue_name=queue_name,
            concurrency=concurrency,
            logger=self.logger,
            enable_dlq=self.enable_dlq,
            move_to_dlq_callback=self._move_to_dlq,
            record_metric_callback=self._record_metric,
        )
        self.workers[queue_name] = worker
        self._spawn_background(worker.start(), f"worker-start:{queue_name}")
        self.logger.debug(
            f"Started worker for queue '{queue_name}' with concurrency {concurrency}"
        )

    async def run_scheduled_tasks(self):
        while True:
            for task in self.scheduled_tasks:
                if task.should_run():
                    self._spawn_background(
                        task.run(), f"scheduled:{task.func.__name__}"
                    )
            await asyncio.sleep(1)

    async def connect_and_start_workers(self):
        try:
            # Reuse the TaskQueue created in __init__ to avoid repeated connects/log spam
            # and duplicated health-check loops.
            self.task_queue.logger = self.logger
            await self.task_queue.connect()
        except RedisConnectionError as e:
            self.logger.error(f"Cannot start workers without Redis connection: {e}")
            raise

        queue_names = list(self.task_registry.keys())

        for queue_name in queue_names:
            await self.task_queue.recover_processing_tasks(queue_name)

        for queue_name in queue_names:
            self.start_worker(queue_name=queue_name)

        self._scheduler_task = asyncio.create_task(self.run_scheduled_tasks())

    async def graceful_shutdown(self):
        """Perform graceful shutdown of all workers and disconnect from Redis."""
        if self._is_shutting_down:
            self.logger.warning("Shutdown already in progress.")
            return
        
        self._is_shutting_down = True
        self.logger.info("Starting graceful shutdown...")

        # Stop internal service tasks first so they don't react to the
        # workers/queues being torn down.
        for service_task in (self._monitor_task, self._scheduler_task):
            if service_task is not None and not service_task.done():
                service_task.cancel()
        for service_task in (self._monitor_task, self._scheduler_task):
            if service_task is not None:
                try:
                    await asyncio.gather(service_task, return_exceptions=True)
                except asyncio.CancelledError:
                    pass

        # Stop all workers with timeout (iterate over a copy: pause_queue /
        # monitor_queue_status may mutate self.workers concurrently)
        shutdown_tasks = []
        for queue_name, worker in list(self.workers.items()):
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

        self._monitor_task = asyncio.create_task(self.monitor_queue_status())

        # Register signal handlers so SIGINT/SIGTERM trigger a real graceful
        # shutdown. Note: "except KeyboardInterrupt" inside a coroutine almost
        # never fires (asyncio delivers CancelledError instead), so signals
        # and CancelledError are the actual shutdown paths.
        shutdown_event = asyncio.Event()
        loop = asyncio.get_event_loop()
        registered_signals: List[Any] = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
                registered_signals.append(sig)
            except (NotImplementedError, RuntimeError):
                # Windows / non-main thread: signals not supported here
                pass

        self.logger.info("mTask is running. Press Ctrl+C to exit.")

        try:
            while not shutdown_event.is_set():
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass
                self._restart_failed_service_tasks()
            self.logger.info("Received shutdown signal...")
            await self.graceful_shutdown()
        except asyncio.CancelledError:
            # run() task was cancelled (e.g. asyncio.run teardown or
            # manager_task.cancel() in the host application)
            self.logger.info("Received cancellation, shutting down gracefully...")
            await self.graceful_shutdown()
            raise
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal (Ctrl+C)...")
            await self.graceful_shutdown()
        except Exception as e:
            self.logger.exception(f"Unexpected error: {e}")
            await self.graceful_shutdown()
        finally:
            for sig in registered_signals:
                try:
                    loop.remove_signal_handler(sig)
                except (NotImplementedError, RuntimeError):
                    pass

    def _restart_failed_service_tasks(self) -> None:
        """Restart internal service tasks (monitor, scheduler) if they died."""
        if self._is_shutting_down:
            return

        if (
            self._monitor_task is not None
            and self._monitor_task.done()
            and not self._monitor_task.cancelled()
            and self._monitor_task.exception() is not None
        ):
            self.logger.warning(
                "Monitor queue status task exited with error, restarting: %s",
                self._monitor_task.exception(),
            )
            self._monitor_task = asyncio.create_task(self.monitor_queue_status())

        if (
            self._scheduler_task is not None
            and self._scheduler_task.done()
            and not self._scheduler_task.cancelled()
            and self._scheduler_task.exception() is not None
        ):
            self.logger.warning(
                "Scheduled tasks runner exited with error, restarting: %s",
                self._scheduler_task.exception(),
            )
            self._scheduler_task = asyncio.create_task(self.run_scheduled_tasks())

    async def _get_queue_status(self, queue_name: str) -> str:
        status_key = f"queue_status:{queue_name}"
        try:
            await self.task_queue.ensure_connected()
            status = await asyncio.wait_for(
                self.task_queue.redis.get(status_key),
                timeout=self.task_queue.operation_timeout,
            )
            return status or "Running"
        except Exception as e:
            self.logger.error(f"Failed to get status for queue '{queue_name}': {e}")
            return "Unknown"

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
            await self.task_queue.ensure_connected()
            current_status = await asyncio.wait_for(
                self.task_queue.redis.get(status_key),
                timeout=self.task_queue.operation_timeout,
            ) or "Running"
            if current_status == "Paused":
                self.logger.warning(f"Queue '{queue_name}' is already paused.")
                return

            await asyncio.wait_for(
                self.task_queue.redis.set(status_key, "Paused", ex=duration),
                timeout=self.task_queue.operation_timeout,
            )
        except mTaskError:
            raise
        except Exception as e:
            self.logger.exception(f"Failed to pause queue '{queue_name}': {e}")
            raise mTaskError(f"Failed to pause queue '{queue_name}': {e}") from e

        # Detect a re-entrant call: pause_queue() invoked from INSIDE a task
        # of the very worker we are about to stop. A synchronous graceful
        # stop would wait for the calling task itself (self-deadlock until
        # the shutdown timeout), so finalize the pause in a background task
        # instead — the caller can finish, then the worker stops cleanly.
        if _current_worker_queue.get() == queue_name:
            self.logger.info(
                f"pause_queue('{queue_name}') called from inside a task of the same "
                f"queue; finishing the pause in the background."
            )
            self._spawn_background(
                self._finalize_pause(queue_name, duration),
                f"pause-queue:{queue_name}",
            )
            return

        await self._finalize_pause(queue_name, duration)

    async def _finalize_pause(self, queue_name: str, duration: int):
        """Stop the worker for a paused queue and return its in-flight tasks."""
        try:
            worker = self.workers.pop(queue_name, None)
            if worker is not None:
                await worker.stop(graceful=True, timeout=self.shutdown_timeout)

            # Return tasks from the processing queue back to the main queue.
            # recover_processing_tasks uses an atomic RPOPLPUSH loop (and
            # handles the legacy Hash format), unlike the previous
            # LRANGE+LPUSH+DELETE which could drop concurrently added tasks.
            await self.task_queue.recover_processing_tasks(queue_name)

            async with self.queue_status_lock:
                self.queue_status[queue_name] = "Paused"
            self.logger.info(f"Queue '{queue_name}' paused for {duration} seconds.")
        except Exception as e:
            self.logger.exception(f"Failed to finalize pause of queue '{queue_name}': {e}")
            raise mTaskError(f"Failed to pause queue '{queue_name}': {e}") from e

    async def monitor_queue_status(self):
        while True:
            try:
                await self.task_queue.ensure_connected()
            except RedisConnectionError:
                self.logger.warning("Monitor: Redis not connected, will retry after sleep.")
                await asyncio.sleep(5)
                continue

            try:
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
                            # pop() avoids a KeyError race with pause_queue /
                            # _finalize_pause removing the worker concurrently
                            worker = self.workers.pop(queue_name, None)
                            if worker is not None:
                                await worker.stop(
                                    graceful=True, timeout=self.shutdown_timeout
                                )
                                # Return in-flight tasks so they are not stuck
                                # in processing for the whole pause duration
                                await self.task_queue.recover_processing_tasks(
                                    queue_name
                                )
                        elif current_status == "Running" or current_status is None:
                            async with self.queue_status_lock:
                                self.queue_status[queue_name] = "Running"
                            self.logger.info(f"Queue '{queue_name}' is resumed.")
                            self.start_worker(queue_name)
                        else:
                            pass
            except (RedisClientConnectionError, RedisConnectionError) as e:
                self.logger.warning(
                    "Monitor: Redis connection error while checking queue status: %s. "
                    "Will reconnect and retry.",
                    e,
                )
                self.task_queue._connection_healthy = False
                await asyncio.sleep(5)
                continue

            await asyncio.sleep(5)
