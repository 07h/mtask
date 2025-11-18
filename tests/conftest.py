"""Pytest fixtures for mTask tests."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
import fakeredis.aioredis
from mtask import mTask, TaskQueue


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def fake_redis():
    """Create a fake Redis client for testing."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
async def task_queue(fake_redis):
    """Create a TaskQueue instance with fake Redis."""
    queue = TaskQueue(redis_url="redis://localhost:6379")
    queue.redis = fake_redis
    queue._connection_healthy = True
    return queue


@pytest.fixture
async def mtask_instance():
    """Create an mTask instance for testing."""
    return mTask(
        redis_url="redis://localhost:6379",
        retry_limit=2,
        enable_logging=False,
    )

