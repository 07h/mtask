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


def test_max_inflight_and_worker_queues_validation():
    with pytest.raises(ValueError, match="max_inflight must be > 0"):
        mTask(max_inflight=0)

    mtask = mTask(worker_queues=["a", "b"], enable_logging=False)
    assert mtask.worker_queues == ["a", "b"]
    assert mtask.max_inflight is None


def test_unknown_worker_queues_fail_at_start():
    mtask = mTask(worker_queues=["missing"], enable_logging=False)

    @mtask.agent(queue_name="real")
    async def handler():
        pass

    with pytest.raises(ValueError, match="unregistered queue"):
        mtask._owned_queue_names()


def test_owned_queue_filter():
    mtask = mTask(worker_queues=["keep"], enable_logging=False)

    @mtask.agent(queue_name="keep")
    async def keep():
        pass

    @mtask.agent(queue_name="skip")
    async def skip():
        pass

    assert mtask._owned_queue_names() == ["keep"]


@pytest.mark.asyncio
async def test_queue_depth_metrics(fake_redis):
    mtask = mTask(enable_logging=False)
    mtask.task_queue.redis = fake_redis
    mtask.task_queue._connection_healthy = True

    await mtask.task_queue.enqueue("q", kwargs={"n": 1})
    await mtask.task_queue.enqueue("q", kwargs={"n": 2}, priority=5)
    depth = await mtask.get_queue_depth("q")
    assert depth == 2
    assert await mtask.get_processing_count("q") == 0
    assert mtask.get_inflight_count() == 0

    stats = await mtask.get_worker_stats()
    assert stats == {}  # no agents registered

    @mtask.agent(queue_name="q")
    async def handler(**kwargs):
        pass

    stats = await mtask.get_worker_stats()
    assert stats["q"]["depth"] == 2
    assert stats["q"]["owned"] is False


@pytest.mark.asyncio
async def test_run_scheduler_only_does_not_start_workers(fake_redis):
    mtask = mTask(enable_logging=False)
    mtask.task_queue.redis = fake_redis
    mtask.task_queue._connection_healthy = True

    async def fake_connect():
        mtask.task_queue._connection_healthy = True

    mtask.task_queue.connect = fake_connect

    @mtask.agent(queue_name="q")
    async def handler(**kwargs):
        pass

    run_task = asyncio.create_task(mtask.run(workers=False, scheduler=True))
    await asyncio.sleep(0.2)
    assert mtask.workers == {}
    assert mtask._scheduler_task is not None
    assert mtask._monitor_task is None
    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)


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

