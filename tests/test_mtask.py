"""Tests for mTask class."""
import pytest
import asyncio
from pydantic import BaseModel
from mtask import mTask, mTaskError


class TestData(BaseModel):
    value: int
    message: str = "test"


@pytest.mark.asyncio
async def test_mtask_initialization():
    """Test mTask initialization with various parameters."""
    mtask = mTask(
        redis_url="redis://localhost:6379",
        retry_limit=5,
        shutdown_timeout=60,
        enable_dlq=True,
        max_task_size=2 * 1024 * 1024,
    )
    
    assert mtask.retry_limit == 5
    assert mtask.shutdown_timeout == 60
    assert mtask.enable_dlq is True
    assert mtask.max_task_size == 2 * 1024 * 1024


def test_mtask_validation():
    """Test that mTask validates parameters correctly."""
    with pytest.raises(ValueError, match="retry_limit must be >= 0"):
        mTask(retry_limit=-1)
    
    with pytest.raises(ValueError, match="shutdown_timeout must be > 0"):
        mTask(shutdown_timeout=0)
    
    with pytest.raises(ValueError, match="max_task_size must be > 0"):
        mTask(max_task_size=-100)


def test_agent_decorator_validation():
    """Test that agent decorator validates parameters."""
    mtask = mTask()
    
    with pytest.raises(ValueError, match="concurrency must be > 0"):
        @mtask.agent(queue_name="test", concurrency=0)
        async def bad_task():
            pass
    
    with pytest.raises(ValueError, match="timeout must be > 0 or None"):
        @mtask.agent(queue_name="test", timeout=-1)
        async def bad_timeout_task():
            pass


def test_interval_decorator_validation():
    """Test that interval decorator validates parameters."""
    mtask = mTask()
    
    with pytest.raises(ValueError, match="seconds must be > 0"):
        @mtask.interval(seconds=0)
        async def bad_interval_task():
            pass


def test_cron_decorator_validation():
    """Test that cron decorator validates cron expression."""
    mtask = mTask()
    
    with pytest.raises(ValueError, match="Invalid cron expression"):
        @mtask.cron(cron_expression="invalid cron")
        async def bad_cron_task():
            pass


@pytest.mark.asyncio
async def test_agent_decorator_registration():
    """Test that agent decorator registers tasks correctly."""
    mtask = mTask()
    
    @mtask.agent(queue_name="test_queue", concurrency=3, timeout=10)
    async def test_task(data: TestData):
        return data.value
    
    assert "test_queue" in mtask.task_registry
    assert mtask.task_registry["test_queue"]["func"] == test_task.__wrapped__
    assert mtask.task_registry["test_queue"]["concurrency"] == 3
    assert mtask.task_registry["test_queue"]["timeout"] == 10


@pytest.mark.asyncio
async def test_metrics():
    """Test metrics recording and retrieval."""
    mtask = mTask()
    
    # Record some metrics
    await mtask._record_metric("test_queue", "completed")
    await mtask._record_metric("test_queue", "completed")
    await mtask._record_metric("test_queue", "failed")
    await mtask._record_metric("test_queue", "execution_time", 2.5)
    await mtask._record_metric("test_queue", "execution_time", 1.5)
    
    # Get metrics
    metrics = await mtask.get_metrics("test_queue")
    
    assert metrics["tasks_completed"] == 2
    assert metrics["tasks_failed"] == 1
    assert metrics["task_count"] == 2
    assert metrics["total_execution_time"] == 4.0
    assert metrics["avg_execution_time"] == 2.0


@pytest.mark.asyncio
async def test_rate_limiting(fake_redis):
    """Test rate limiting functionality."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis
    
    @mtask.agent(queue_name="limited_queue", rate_limit=2)
    async def limited_task(value: int):
        return value
    
    # First two tasks should succeed
    await limited_task(value=1)
    await limited_task(value=2)
    
    # Third task should be rate limited
    with pytest.raises(mTaskError, match="Rate limit exceeded"):
        await limited_task(value=3)

