# mTask/decorators.py

from typing import Callable, Any
import asyncio
from functools import wraps
from .scheduler import mScheduler
from .queue import mTaskQueue
import logging

logger = logging.getLogger(__name__)

# Singleton instances of Scheduler and TaskQueue
scheduler = mScheduler()
task_queue = mTaskQueue()


def scheduled(interval: float):
    """
    Decorator to register a task that runs at regular intervals.

    Args:
        interval (float): Time in seconds between task executions.
    """

    def decorator(func: Callable):
        scheduler.add_interval_task(func, interval)
        logger.debug(
            f"Registered scheduled task '{func.__name__}' with interval {interval} seconds"
        )
        return func

    return decorator


def cron(cron_expression: str):
    """
    Decorator to register a task that runs based on a cron expression.

    Args:
        cron_expression (str): Cron-formatted schedule string.
    """

    def decorator(func: Callable):
        scheduler.add_cron_task(func, cron_expression)
        logger.debug(
            f"Registered cron task '{func.__name__}' with cron '{cron_expression}'"
        )
        return func

    return decorator


def task():
    """
    Decorator to register a task that can be enqueued manually.
    """

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            await task_queue.enqueue(func.__name__, args, kwargs)
            logger.debug(
                f"Task '{func.__name__}' enqueued with args={args} kwargs={kwargs}"
            )

        return wrapper

    return decorator
