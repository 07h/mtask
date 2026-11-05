"""Tests for scheduled tasks functionality."""
import logging
import pytest
import asyncio
from datetime import datetime
from mtask import ScheduledTask, mTask


@pytest.mark.asyncio
async def test_scheduled_task_interval():
    """Test scheduled task with interval."""
    call_count = 0

    async def test_func():
        nonlocal call_count
        call_count += 1

    task = ScheduledTask(test_func, interval=1)

    # First run happens one interval after registration (not immediately)
    assert task.should_run() is False

    # After the interval elapses, should be ready
    await asyncio.sleep(1.1)
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


@pytest.mark.asyncio
async def test_scheduled_task_max_runtime_cancels_and_allows_next_tick(caplog):
    """C1: max_runtime cancels a hung invocation and clears is_running."""
    started = 0
    finished = 0

    async def hung():
        nonlocal started, finished
        started += 1
        try:
            await asyncio.sleep(10)
            finished += 1
        except asyncio.CancelledError:
            raise

    task = ScheduledTask(hung, interval=1, max_runtime=0.05)
    # Force due so we can invoke run() directly
    task.next_run = datetime.now()

    with caplog.at_level(logging.WARNING):
        await task.run()

    assert started == 1
    assert finished == 0
    assert task.is_running is False
    assert any(
        "exceeded max_runtime" in record.message
        and "hung" in record.message
        for record in caplog.records
    )

    # Next tick must be allowed after the timeout cleared is_running
    task.next_run = datetime.now()
    assert task.should_run() is True
    await task.run()
    assert started == 2
    assert task.is_running is False


@pytest.mark.asyncio
async def test_scheduled_task_default_no_max_runtime_unchanged():
    """Default (max_runtime=None) still waits for the full body — no timeout."""
    completed = False

    async def body():
        nonlocal completed
        await asyncio.sleep(0.15)
        completed = True

    task = ScheduledTask(body, interval=1)
    assert task.max_runtime is None
    await asyncio.wait_for(task.run(), timeout=1.0)
    assert completed is True
    assert task.is_running is False


def test_interval_and_cron_accept_max_runtime():
    """Decorators forward optional max_runtime; omitting it stays compatible."""
    mtask = mTask(enable_logging=False)

    @mtask.interval(seconds=30)
    async def plain_interval():
        pass

    @mtask.interval(seconds=30, max_runtime=5)
    async def capped_interval():
        pass

    @mtask.cron("* * * * *")
    async def plain_cron():
        pass

    @mtask.cron("* * * * *", max_runtime=2.5)
    async def capped_cron():
        pass

    by_name = {t.func.__name__: t for t in mtask.scheduled_tasks}
    assert by_name["plain_interval"].max_runtime is None
    assert by_name["capped_interval"].max_runtime == 5
    assert by_name["plain_cron"].max_runtime is None
    assert by_name["capped_cron"].max_runtime == 2.5


def test_max_runtime_validation():
    with pytest.raises(ValueError):
        ScheduledTask(lambda: None, interval=1, max_runtime=0)

    mtask = mTask(enable_logging=False)
    with pytest.raises(ValueError):
        mtask.interval(seconds=10, max_runtime=-1)
    with pytest.raises(ValueError):
        mtask.cron("* * * * *", max_runtime=0)
