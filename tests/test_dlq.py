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

