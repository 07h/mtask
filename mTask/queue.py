# mTask/queue.py


import json
import uuid
from typing import Any, Dict, Optional
import aioredis
import logging

logger = logging.getLogger(__name__)


class mTaskQueue:
    def __init__(
        self, redis_url: str = "redis://localhost", queue_name: str = "task_queue"
    ):
        """
        Initializes the task queue.

        Args:
            redis_url (str): Redis connection URL.
            queue_name (str): Name of the Redis list to use as the queue.
        """
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self):
        """
        Establishes a connection to the Redis server.
        """
        self.redis = await aioredis.from_url(
            self.redis_url, encoding="utf-8", decode_responses=True
        )
        logger.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect(self):
        """
        Closes the connection to the Redis server.
        """
        if self.redis:
            await self.redis.close()
            await self.redis.wait_closed()
            logger.info("Disconnected from Redis")

    async def enqueue(
        self, task_name: str, args: Any = None, kwargs: Dict[str, Any] = None
    ) -> str:
        """
        Adds a task to the queue.

        Args:
            task_name (str): Name of the task function.
            args (Any): Positional arguments for the task.
            kwargs (Dict[str, Any]): Keyword arguments for the task.

        Returns:
            str: The unique ID of the enqueued task.
        """
        task = {
            "id": str(uuid.uuid4()),
            "name": task_name,
            "args": args or [],
            "kwargs": kwargs or {},
            "status": "pending",
            "retry_count": 0,
        }
        await self.redis.rpush(self.queue_name, json.dumps(task))
        logger.debug(f"Enqueued task {task['id']} with name {task_name}")
        return task["id"]

    async def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Removes and returns the first task from the queue.

        Returns:
            Optional[Dict[str, Any]]: The task data or None if the queue is empty.
        """
        task_json = await self.redis.lpop(self.queue_name)
        if task_json:
            task = json.loads(task_json)
            logger.debug(f"Dequeued task {task['id']} with name {task['name']}")
            return task
        return None

    async def requeue(self, task: Dict[str, Any]):
        """
        Requeues a failed task for retry.

        Args:
            task (Dict[str, Any]): The task data.
        """
        task["status"] = "pending"
        await self.redis.rpush(self.queue_name, json.dumps(task))
        logger.debug(f"Requeued task {task['id']} (retry {task['retry_count']})")

    async def mark_completed(self, task_id: str):
        """
        Marks a task as completed. Extend this method to store completed tasks if needed.

        Args:
            task_id (str): The unique ID of the task.
        """
        logger.info(f"Task {task_id} marked as completed.")
        # Implement storage of completed tasks if necessary
