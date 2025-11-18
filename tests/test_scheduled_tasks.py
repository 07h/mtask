"""Tests for scheduled tasks functionality."""
import pytest
import asyncio
from datetime import datetime
from mtask import ScheduledTask


@pytest.mark.asyncio
async def test_scheduled_task_interval():
    """Test scheduled task with interval."""
    call_count = 0
    
    async def test_func():
        nonlocal call_count
        call_count += 1
    
    task = ScheduledTask(test_func, interval=1)
    
    # Initially should be ready to run
    assert task.should_run() is True
    
    # After running, should not be ready immediately
    await task.run()
    assert call_count == 1
    assert task.should_run() is False
    
    # After waiting, should be ready again
    await asyncio.sleep(1.1)
    assert task.should_run() is True


@pytest.mark.asyncio
async def test_scheduled_task_cron():
    """Test scheduled task with cron expression."""
    call_count = 0
    
    async def test_func():
        nonlocal call_count
        call_count += 1
    
    # Run every minute
    task = ScheduledTask(test_func, cron_expression="* * * * *")
    
    # Should have a valid next_run time
    assert task.next_run is not None
    assert isinstance(task.next_run, datetime)
    

@pytest.mark.asyncio
async def test_scheduled_task_prevents_concurrent_runs():
    """Test that scheduled task doesn't run concurrently."""
    
    async def slow_task():
        await asyncio.sleep(0.5)
    
    task = ScheduledTask(slow_task, interval=1)
    
    # Start first run
    run1 = asyncio.create_task(task.run())
    await asyncio.sleep(0.1)
    
    # Try to run again while first is still running
    assert task.is_running is True
    assert task.should_run() is False
    
    # Wait for completion
    await run1
    assert task.is_running is False


@pytest.mark.asyncio
async def test_scheduled_task_error_handling():
    """Test that scheduled task handles errors gracefully."""
    
    async def failing_task():
        raise ValueError("Test error")
    
    task = ScheduledTask(failing_task, interval=1)
    
    # Should not raise error
    await task.run()
    
    # Should still update next_run after error
    assert task.last_run is not None
    assert task.next_run is not None

