# mTask/worker.py

import asyncio
from typing import Callable, Dict, Any
from .queue import mTaskQueue
import importlib
import logging
from greenlet import greenlet

logger = logging.getLogger(__name__)


class mWorker:
    def __init__(
        self, task_queue: mTaskQueue, concurrency: int = 5, retry_limit: int = 3
    ):
        """
        Initializes the worker.

        Args:
            task_queue (mTaskQueue): The task queue instance.
            concurrency (int): Number of concurrent workers.
            retry_limit (int): Maximum number of retries for failed tasks.
        """
        self.task_queue = task_queue
        self.concurrency = concurrency
        self.retry_limit = retry_limit
        self._running = False
        self._workers = []

    async def start(self):
        """
        Starts the worker processes.
        """
        if self._running:
            logger.warning("Worker is already running.")
            return

        self._running = True
        await self.task_queue.connect()
        logger.info(f"Starting worker with concurrency={self.concurrency}")
        for _ in range(self.concurrency):
            gr = greenlet(self._worker_loop)
            self._workers.append(gr)
            gr.switch()

    async def stop(self):
        """
        Stops all worker processes.
        """
        self._running = False
        logger.info("Stopping worker...")
        await self.task_queue.disconnect()
        for gr in self._workers:
            gr.parent.throw(KeyboardInterrupt)
        logger.info("Worker stopped.")

    def _worker_loop(self):
        """
        The main loop for each worker greenlet.
        """
        while self._running:
            try:
                task = asyncio.run(self.task_queue.dequeue())
                if task:
                    asyncio.run(self.process_task(task))
                else:
                    asyncio.run(asyncio.sleep(1))  # Avoid busy waiting
            except KeyboardInterrupt:
                logger.info("Worker received shutdown signal.")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in worker: {e}")

    async def process_task(self, task: Dict[str, Any]):
        """
        Processes a single task.

        Args:
            task (Dict[str, Any]): The task data.
        """
        task_name = task["name"]
        args = task.get("args", [])
        kwargs = task.get("kwargs", {})
        retry_count = task.get("retry_count", 0)

        try:
            func = self.get_task_function(task_name)
            logger.info(
                f"Executing task {task['id']}: {task_name} with args={args} kwargs={kwargs}"
            )
            await func(*args, **kwargs)
            await self.task_queue.mark_completed(task["id"])
            logger.info(f"Task {task['id']} completed successfully.")
        except Exception as e:
            logger.exception(f"Error executing task {task['id']}: {e}")
            if retry_count < self.retry_limit:
                task["retry_count"] = retry_count + 1
                await self.task_queue.requeue(task)
                logger.info(f"Requeued task {task['id']} (retry {task['retry_count']})")
            else:
                logger.error(
                    f"Task {task['id']} failed after {self.retry_limit} retries."
                )

    def get_task_function(self, task_name: str) -> Callable:
        """
        Retrieves the task function by name.

        Args:
            task_name (str): The name of the task function.

        Returns:
            Callable: The task function.

        Raises:
            ValueError: If the task function is not found.
        """
        try:
            tasks_module = importlib.import_module("tasks")
            func = getattr(tasks_module, task_name, None)
            if not func:
                raise ValueError(
                    f"No task function named '{task_name}' found in 'tasks' module."
                )
            return func
        except ModuleNotFoundError:
            raise ImportError(
                "Tasks module not found. Ensure that 'tasks.py' exists and is in PYTHONPATH."
            )
