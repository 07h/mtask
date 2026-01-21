"""Tests for TaskQueue class."""
import pytest
import json
from mtask import TaskQueue, RedisConnectionError, TaskEnqueueError


@pytest.mark.asyncio
async def test_enqueue_task(task_queue):
    """Test enqueueing a task."""
    task_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"key": "value"})
    
    assert task_id is not None
    assert isinstance(task_id, str)
    
    # Verify task was added to queue
    tasks = await task_queue.redis.lrange("test_queue", 0, -1)
    assert len(tasks) == 1
    
    task_data = json.loads(tasks[0])
    assert task_data["id"] == task_id
    assert task_data["kwargs"] == {"key": "value"}


@pytest.mark.asyncio
async def test_enqueue_priority_task(task_queue):
    """Test enqueueing a task with priority."""
    task_id = await task_queue.enqueue(
        queue_name="test_queue", 
        kwargs={"key": "value"},
        priority=5
    )
    
    # Verify task was added to priority queue
    tasks = await task_queue.redis.zrange("test_queue:priority", 0, -1)
    assert len(tasks) == 1


@pytest.mark.asyncio
async def test_dequeue_task(task_queue):
    """Test dequeueing a task."""
    task_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"key": "value"})
    
    task = await task_queue.dequeue(queue_name="test_queue")
    
    assert task is not None
    assert task["id"] == task_id
    assert task["kwargs"] == {"key": "value"}
    
    # Verify task moved to processing queue (now uses Hash)
    processing_tasks = await task_queue.redis.hgetall("test_queue:processing")
    assert len(processing_tasks) == 1
    assert task_id in processing_tasks


@pytest.mark.asyncio
async def test_dequeue_priority_task(task_queue):
    """Test dequeueing a priority task."""
    # Enqueue two tasks with different priorities
    await task_queue.enqueue(queue_name="test_queue", kwargs={"priority": 1}, priority=1)
    high_priority_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"priority": 5}, priority=5)
    
    # Should dequeue high priority task first
    task = await task_queue.dequeue(queue_name="test_queue")
    assert task["id"] == high_priority_id


@pytest.mark.asyncio
async def test_mark_completed(task_queue):
    """Test marking a task as completed."""
    task_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"key": "value"})
    task = await task_queue.dequeue(queue_name="test_queue")
    
    await task_queue.mark_completed(task_id, "test_queue")
    
    # Verify task removed from processing queue (now uses Hash)
    processing_count = await task_queue.redis.hlen("test_queue:processing")
    assert processing_count == 0


@pytest.mark.asyncio
async def test_requeue_task(task_queue):
    """Test requeueing a task."""
    task_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"key": "value"})
    task = await task_queue.dequeue(queue_name="test_queue")
    
    task["retry_count"] += 1
    await task_queue.requeue(task, queue_name="test_queue", apply_backoff=False)
    
    # Verify task back in main queue
    tasks = await task_queue.redis.lrange("test_queue", 0, -1)
    assert len(tasks) == 1
    
    requeued_task = json.loads(tasks[0])
    assert requeued_task["id"] == task_id
    assert requeued_task["retry_count"] == 1


@pytest.mark.asyncio
async def test_requeue_with_backoff(task_queue):
    """Test requeueing a task with backoff."""
    import time
    
    task_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"key": "value"})
    task = await task_queue.dequeue(queue_name="test_queue")
    
    task["retry_count"] = 1
    await task_queue.requeue(task, queue_name="test_queue", apply_backoff=True)
    
    # Task should be in queue
    tasks = await task_queue.redis.lrange("test_queue", 0, -1)
    assert len(tasks) == 1
    
    requeued_task = json.loads(tasks[0])
    assert "retry_after" in requeued_task
    
    # Try to dequeue immediately - should return None (still in backoff)
    task_dequeued = await task_queue.dequeue(queue_name="test_queue")
    assert task_dequeued is None
    
    # Task should still be in queue
    tasks = await task_queue.redis.lrange("test_queue", 0, -1)
    assert len(tasks) == 1


@pytest.mark.asyncio
async def test_recover_processing_tasks(task_queue):
    """Test recovering tasks from processing queue (Hash format)."""
    # Enqueue and dequeue tasks to populate processing queue
    task_ids = []
    for i in range(3):
        task_id = await task_queue.enqueue(queue_name="test_queue", kwargs={"index": i})
        task_ids.append(task_id)
        await task_queue.dequeue(queue_name="test_queue")
    
    # Verify 3 tasks in processing queue (now uses Hash)
    processing_tasks = await task_queue.redis.hgetall("test_queue:processing")
    assert len(processing_tasks) == 3
    
    # Recover tasks
    await task_queue.recover_processing_tasks("test_queue")
    
    # Verify tasks moved back to main queue
    main_queue_tasks = await task_queue.redis.lrange("test_queue", 0, -1)
    assert len(main_queue_tasks) == 3
    
    # Verify processing queue is empty
    processing_count = await task_queue.redis.hlen("test_queue:processing")
    assert processing_count == 0


@pytest.mark.asyncio
async def test_task_size_validation(task_queue):
    """Test that large tasks are rejected."""
    large_data = {"key": "x" * 2000000}  # > 1MB
    
    with pytest.raises(ValueError, match="exceeds maximum allowed size"):
        await task_queue.enqueue(
            queue_name="test_queue",
            kwargs=large_data,
            max_task_size=1024 * 1024
        )


@pytest.mark.asyncio
async def test_recover_processing_tasks_list_migration(task_queue):
    """Test recovering tasks from old List format processing queue (migration)."""
    # Manually add tasks in old List format
    for i in range(3):
        task = {
            "id": f"task-{i}",
            "name": "test_queue",
            "kwargs": {"index": i},
            "status": "processing",
            "retry_count": 0,
        }
        await task_queue.redis.rpush("test_queue:processing", json.dumps(task))
    
    # Verify 3 tasks in processing queue (List format)
    key_type = await task_queue.redis.type("test_queue:processing")
    assert key_type == "list"
    
    # Recover tasks - should migrate from List to Hash
    await task_queue.recover_processing_tasks("test_queue")
    
    # Verify tasks moved back to main queue
    main_queue_tasks = await task_queue.redis.lrange("test_queue", 0, -1)
    assert len(main_queue_tasks) == 3
    
    # Verify processing queue is deleted
    exists = await task_queue.redis.exists("test_queue:processing")
    assert exists == 0


@pytest.mark.asyncio
async def test_get_processing_task_count_hash(task_queue):
    """Test getting processing task count with Hash format."""
    # Enqueue and dequeue to create processing entries
    for i in range(2):
        await task_queue.enqueue(queue_name="test_queue", kwargs={"index": i})
        await task_queue.dequeue(queue_name="test_queue")
    
    count = await task_queue.get_processing_task_count("test_queue")
    assert count == 2

