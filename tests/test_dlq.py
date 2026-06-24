"""Tests for Dead Letter Queue functionality."""
import pytest
import json
from mtask import mTask


@pytest.mark.asyncio
async def test_move_to_dlq(fake_redis):
    """Test moving failed tasks to DLQ."""
    mtask = mTask(enable_dlq=True)
    mtask.task_queue.redis = fake_redis
    
    task = {
        "id": "test-task-123",
        "name": "test_queue",
        "kwargs": {"value": 1},
        "status": "processing",
        "retry_count": 3,
    }
    
    await mtask._move_to_dlq(task, "test_queue", "Test error")
    
    # Verify task in DLQ
    dlq_tasks = await fake_redis.lrange("test_queue:dlq", 0, -1)
    assert len(dlq_tasks) == 1
    
    dlq_task = json.loads(dlq_tasks[0])
    assert dlq_task["id"] == "test-task-123"
    assert dlq_task["error"] == "Test error"
    assert dlq_task["status"] == "failed"


@pytest.mark.asyncio
async def test_get_dlq_tasks(fake_redis):
    """Test retrieving tasks from DLQ."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis
    
    # Add tasks to DLQ manually
    for i in range(3):
        task = {
            "id": f"task-{i}",
            "name": "test_queue",
            "kwargs": {"index": i},
            "status": "failed",
            "error": f"Error {i}",
        }
        await fake_redis.rpush("test_queue:dlq", json.dumps(task))
    
    # Get DLQ tasks
    dlq_tasks = await mtask.get_dlq_tasks("test_queue")
    
    assert len(dlq_tasks) == 3
    assert all(task["status"] == "failed" for task in dlq_tasks)


@pytest.mark.asyncio
async def test_retry_dlq_task(fake_redis):
    """Test retrying a task from DLQ."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis
    
    task = {
        "id": "test-task-456",
        "name": "test_queue",
        "kwargs": {"value": 1},
        "status": "failed",
        "error": "Previous error",
        "retry_count": 3,
    }
    await fake_redis.rpush("test_queue:dlq", json.dumps(task))
    
    # Retry the task
    success = await mtask.retry_dlq_task("test-task-456", "test_queue")
    
    assert success is True
    
    # Verify task removed from DLQ
    dlq_tasks = await fake_redis.lrange("test_queue:dlq", 0, -1)
    assert len(dlq_tasks) == 0
    
    # Verify task back in main queue
    main_queue_tasks = await fake_redis.lrange("test_queue", 0, -1)
    assert len(main_queue_tasks) == 1
    
    retried_task = json.loads(main_queue_tasks[0])
    assert retried_task["id"] == "test-task-456"
    assert retried_task["retry_count"] == 0
    assert "error" not in retried_task


@pytest.mark.asyncio
async def test_clear_dlq(fake_redis):
    """Test clearing all tasks from DLQ."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis
    
    # Add multiple tasks to DLQ
    for i in range(5):
        task = {"id": f"task-{i}", "status": "failed"}
        await fake_redis.rpush("test_queue:dlq", json.dumps(task))
    
    # Clear DLQ
    count = await mtask.clear_dlq("test_queue")
    
    assert count == 5
    
    # Verify DLQ is empty
    dlq_tasks = await fake_redis.lrange("test_queue:dlq", 0, -1)
    assert len(dlq_tasks) == 0


@pytest.mark.asyncio
async def test_dlq_disabled(fake_redis):
    """Test that tasks are not moved to DLQ when disabled."""
    mtask = mTask(enable_dlq=False)
    mtask.task_queue.redis = fake_redis
    
    task = {
        "id": "test-task-789",
        "name": "test_queue",
        "kwargs": {"value": 1},
        "status": "processing",
        "retry_count": 3,
    }
    
    await mtask._move_to_dlq(task, "test_queue", "Test error")
    
    # Verify DLQ is empty
    dlq_tasks = await fake_redis.lrange("test_queue:dlq", 0, -1)
    assert len(dlq_tasks) == 0


@pytest.mark.asyncio
async def test_find_dlq_tasks_by_kwargs(fake_redis):
    """find_dlq_tasks returns matches without modifying the DLQ."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis

    for i, report_id in enumerate(["r1", "r1", "r2"]):
        task = {
            "id": f"task-{i}",
            "name": "test_queue",
            "kwargs": {"report_id": report_id, "domain": f"d{i}.com"},
            "status": "failed",
            "error": f"err-{i}",
        }
        await fake_redis.rpush("test_queue:dlq", json.dumps(task))

    matches = await mtask.find_dlq_tasks(
        "test_queue", kwargs_match={"report_id": "r1"}
    )
    assert len(matches) == 2
    assert all(m["kwargs"]["report_id"] == "r1" for m in matches)

    # DLQ unchanged
    assert await fake_redis.llen("test_queue:dlq") == 3


@pytest.mark.asyncio
async def test_remove_dlq_tasks_by_kwargs(fake_redis):
    """remove_dlq_tasks deletes all entries matching kwargs_match."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis
    mtask.task_queue._connection_healthy = True

    for i, report_id in enumerate(["r1", "r1", "r2"]):
        task = {
            "id": f"task-{i}",
            "name": "test_queue",
            "kwargs": {"report_id": report_id},
            "status": "failed",
            "error": "boom",
        }
        await fake_redis.rpush("test_queue:dlq", json.dumps(task))
        await fake_redis.hset("test_queue:attempts", f"task-{i}", 5)

    removed = await mtask.remove_dlq_tasks(
        "test_queue", kwargs_match={"report_id": "r1"}
    )
    assert removed == 2
    assert await fake_redis.llen("test_queue:dlq") == 1
    remaining = json.loads((await fake_redis.lrange("test_queue:dlq", 0, -1))[0])
    assert remaining["kwargs"]["report_id"] == "r2"
    # Delivery counters cleared for removed tasks only
    assert await fake_redis.hget("test_queue:attempts", "task-0") is None
    assert await fake_redis.hget("test_queue:attempts", "task-1") is None
    assert await fake_redis.hget("test_queue:attempts", "task-2") == "5"


@pytest.mark.asyncio
async def test_remove_dlq_tasks_by_task_id(fake_redis):
    """remove_dlq_tasks can drop a single entry by mtask task id."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis

    task = {
        "id": "dead-beef",
        "name": "test_queue",
        "kwargs": {"report_id": "r99"},
        "status": "failed",
    }
    await fake_redis.rpush("test_queue:dlq", json.dumps(task))

    assert await mtask.remove_dlq_tasks("test_queue", task_id="dead-beef") == 1
    assert await fake_redis.llen("test_queue:dlq") == 0
    assert await mtask.remove_dlq_tasks("test_queue", task_id="missing") == 0


@pytest.mark.asyncio
async def test_remove_dlq_tasks_requires_exactly_one_selector(fake_redis):
    """remove_dlq_tasks rejects missing or duplicate selectors."""
    mtask = mTask()
    mtask.task_queue.redis = fake_redis

    with pytest.raises(ValueError, match="Exactly one"):
        await mtask.remove_dlq_tasks("test_queue")

    with pytest.raises(ValueError, match="Exactly one"):
        await mtask.remove_dlq_tasks(
            "test_queue", kwargs_match={"a": 1}, task_id="x"
        )

    with pytest.raises(ValueError, match="non-empty"):
        await mtask.remove_dlq_tasks("test_queue", kwargs_match={})

