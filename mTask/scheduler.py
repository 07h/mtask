# mTask/scheduler.py

import asyncio
from typing import Callable, List, Tuple
from croniter import croniter
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class mScheduler:
    def __init__(self):
        """
        Initializes the scheduler with empty task lists.
        """
        self.interval_tasks: List[Tuple[Callable, float]] = []
        self.cron_tasks: List[Tuple[Callable, str]] = []
        self._running = False

    def add_interval_task(self, func: Callable, interval: float):
        """
        Adds a task to be run at regular intervals.

        Args:
            func (Callable): The task function.
            interval (float): Time in seconds between executions.
        """
        self.interval_tasks.append((func, interval))
        logger.info(
            f"Added interval task '{func.__name__}' with interval {interval} seconds"
        )

    def add_cron_task(self, func: Callable, cron_expression: str):
        """
        Adds a task to be run based on a cron schedule.

        Args:
            func (Callable): The task function.
            cron_expression (str): Cron-formatted schedule string.
        """
        self.cron_tasks.append((func, cron_expression))
        logger.info(f"Added cron task '{func.__name__}' with cron '{cron_expression}'")

    async def start(self):
        """
        Starts the scheduler to run all registered tasks.
        """
        if self._running:
            logger.warning("Scheduler is already running.")
            return

        self._running = True
        logger.info("Starting scheduler...")
        tasks = []
        for func, interval in self.interval_tasks:
            tasks.append(asyncio.create_task(self._run_interval(func, interval)))
        for func, cron_expr in self.cron_tasks:
            tasks.append(asyncio.create_task(self._run_cron(func, cron_expr)))
        await asyncio.gather(*tasks)

    async def _run_interval(self, func: Callable, interval: float):
        """
        Runs a task at regular intervals.

        Args:
            func (Callable): The task function.
            interval (float): Time in seconds between executions.
        """
        while self._running:
            try:
                logger.debug(f"Running interval task '{func.__name__}'")
                await func()
            except Exception as e:
                logger.exception(f"Error in interval task '{func.__name__}': {e}")
            await asyncio.sleep(interval)

    async def _run_cron(self, func: Callable, cron_expr: str):
        """
        Runs a task based on a cron schedule.

        Args:
            func (Callable): The task function.
            cron_expr (str): Cron-formatted schedule string.
        """
        itr = croniter(cron_expr, datetime.now())
        while self._running:
            next_run = itr.get_next(datetime)
            delay = (next_run - datetime.now()).total_seconds()
            if delay < 0:
                delay = 0
            logger.debug(
                f"Cron task '{func.__name__}' scheduled to run in {delay} seconds"
            )
            await asyncio.sleep(delay)
            try:
                logger.debug(f"Running cron task '{func.__name__}'")
                await func()
            except Exception as e:
                logger.exception(f"Error in cron task '{func.__name__}': {e}")

    async def stop(self):
        """
        Stops the scheduler from running any further tasks.
        """
        self._running = False
        logger.info("Scheduler stopped.")
