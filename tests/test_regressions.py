"""Regression tests for the bugs fixed in v0.3.0 and v0.3.1.

Each test references the finding it guards against (C1, C2, H2, M2, M3, M5,
and the v0.3.1 hardening items V1, V3, V4).
"""
import asyncio
import json

import pytest
from pydantic import BaseModel

from mtask import Worker, mTask, mTaskError


class PayloadModel(BaseModel):
    value: int


def make_worker(mtask: mTask, queue_name: str = "q", retry_limit: int = 3) -> Worker:
    return Worker(
        task_queue=mtask.task_queue,
        task_registry=mtask.task_registry,
        task_registry_lock=mtask.task_registry_lock,
        retry_limit=retry_limit,
        queue_name=queue_name,
        concurrency=1,
        logger=mtask.logger,
        enable_dlq=True,
        move_to_dlq_callback=mtask._move_to_dlq,
        record_metric_callback=mtask._record_metric,
    )


def make_mtask(fake_redis) -> mTask:
    mtask = mTask(enable_logging=False)
    mtask.task_queue.redis = fake_redis
    mtask.task_queue._connection_healthy = True
    return mtask


@pytest.mark.asyncio
async def test_retry_does_not_leave_zombie_in_processing(fake_redis):
    """C1: a retried task must not leave its old entry in :processing."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q")
    async def failing_task(**kwargs):
        raise RuntimeError("boom")

    worker = make_worker(mtask)

    await mtask.task_queue.enqueue(queue_name="q", kwargs={"n": 1})
    task = await mtask.task_queue.dequeue(queue_name="q")
    assert await fake_redis.llen("q:processing") == 1

    await worker.process_task(task, worker_id=1)

    # The retry copy is parked in the delayed zset...
    assert await fake_redis.zcard("q:delayed") == 1
    # ...and the old entry is GONE from the processing list (was the C1 bug:
    # requeue() popped "_task_json" so the finally block could not LREM it)
    assert await fake_redis.llen("q:processing") == 0


@pytest.mark.asyncio
async def test_stop_cancels_worker_loops_and_requeues_inflight_task(fake_redis):
    """C2: stop() must actually cancel _worker_loop tasks (not just monitors)
    and the in-flight task must be returned to the queue."""
    mtask = make_mtask(fake_redis)
    started = asyncio.Event()

    @mtask.agent(queue_name="q")
    async def slow_task(**kwargs):
        started.set()
        await asyncio.sleep(30)

    worker = make_worker(mtask)

    task_id = await mtask.task_queue.enqueue(queue_name="q", kwargs={"n": 1})
    await worker.start()
    await asyncio.wait_for(started.wait(), timeout=5)

    # Force stop while the task is mid-execution
    await worker.stop(graceful=False)

    # Worker loop tasks are really finished (before the fix the inner
    # _worker_loop kept running after "Workers stopped")
    assert worker._workers == []
    assert worker._active_tasks == 0

    # The in-flight task was requeued and the processing entry removed
    main_queue = await fake_redis.lrange("q", 0, -1)
    assert len(main_queue) == 1
    assert json.loads(main_queue[0])["id"] == task_id
    assert await fake_redis.llen("q:processing") == 0


@pytest.mark.asyncio
async def test_cancelling_run_triggers_graceful_shutdown(fake_redis):
    """C3: cancelling run() (asyncio.run teardown, manager_task.cancel())
    must execute graceful_shutdown instead of dying silently."""
    mtask = make_mtask(fake_redis)

    async def fake_connect():
        mtask.task_queue._connection_healthy = True

    mtask.task_queue.connect = fake_connect

    run_task = asyncio.create_task(mtask.run())
    await asyncio.sleep(0.3)
    assert mtask._is_shutting_down is False

    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)

    assert mtask._is_shutting_down is True


@pytest.mark.asyncio
async def test_model_parameter_with_custom_name(fake_redis):
    """H2: the model argument must be passed using the actual parameter name
    from the function signature (previously hardcoded to `data`)."""
    mtask = make_mtask(fake_redis)
    received = {}

    @mtask.agent(queue_name="q")
    async def handler(payload: PayloadModel):
        received["payload"] = payload

    worker = make_worker(mtask)

    await mtask.task_queue.enqueue(queue_name="q", kwargs={"value": 42})
    task = await mtask.task_queue.dequeue(queue_name="q")
    await worker.process_task(task, worker_id=1)

    assert received["payload"] == PayloadModel(value=42)
    # No retries, nothing in DLQ
    assert await fake_redis.zcard("q:delayed") == 0
    assert await fake_redis.llen("q:dlq") == 0


@pytest.mark.asyncio
async def test_dlq_entry_has_no_internal_fields(fake_redis):
    """M2: DLQ entries must not contain the nested `_task_json` copy
    (it doubled the entry size on every DLQ cycle)."""
    mtask = make_mtask(fake_redis)

    task = {
        "id": "t-1",
        "name": "q",
        "kwargs": {"n": 1},
        "status": "processing",
        "retry_count": 3,
        "start_time": 123.0,
        "_task_json": json.dumps({"id": "t-1", "huge": "copy"}),
    }
    await mtask._move_to_dlq(task, "q", "boom")

    entry = json.loads((await fake_redis.lrange("q:dlq", 0, -1))[0])
    assert "_task_json" not in entry
    assert "start_time" not in entry
    assert entry["error"] == "boom"


@pytest.mark.asyncio
async def test_validation_error_goes_straight_to_dlq(fake_redis):
    """M3: deterministic failures (pydantic ValidationError) must not be
    retried — they go straight to the DLQ with retry_count untouched."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q")
    async def handler(payload: PayloadModel):
        pass

    worker = make_worker(mtask)

    # "value" cannot be parsed as int -> ValidationError inside the worker
    await mtask.task_queue.enqueue(queue_name="q", kwargs={"value": "not-an-int"})
    task = await mtask.task_queue.dequeue(queue_name="q")
    await worker.process_task(task, worker_id=1)

    # Straight to DLQ: no retry copies anywhere
    dlq = await fake_redis.lrange("q:dlq", 0, -1)
    assert len(dlq) == 1
    dlq_task = json.loads(dlq[0])
    assert dlq_task["retry_count"] == 0
    assert "Non-retryable" in dlq_task["error"]
    assert await fake_redis.zcard("q:delayed") == 0
    assert await fake_redis.llen("q") == 0
    assert await fake_redis.llen("q:processing") == 0


@pytest.mark.asyncio
async def test_rate_limit_under_concurrency(fake_redis):
    """M5: the rate limiter must hold under concurrent producers
    (previously GET-then-INCR allowed everyone past the limit)."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q", rate_limit=2)
    async def producer(value: int):
        pass

    results = await asyncio.gather(
        *(producer(value=i) for i in range(6)), return_exceptions=True
    )

    succeeded = [r for r in results if isinstance(r, str)]
    limited = [r for r in results if isinstance(r, mTaskError)]
    assert len(succeeded) == 2
    assert len(limited) == 4


@pytest.mark.asyncio
async def test_unserializable_kwargs_raise_enqueue_error(fake_redis):
    """M9: non-JSON-serializable kwargs raise TaskEnqueueError, not a bare
    TypeError escaping from json.dumps."""
    from mtask import TaskEnqueueError

    mtask = make_mtask(fake_redis)
    with pytest.raises(TaskEnqueueError, match="not JSON-serializable"):
        await mtask.task_queue.enqueue(queue_name="q", kwargs={"bad": object()})


# ---------------------------------------------------------------------------
# v0.3.1 hardening regressions (V1, V3, V4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delivery_attempts_caps_crash_loop(fake_redis):
    """V1: a task that never advances retry_count (e.g. crashes the process,
    here simulated by repeated dequeue+recovery) is sent to DLQ after
    retry_limit + 1 deliveries, and its attempts counter is cleaned up."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q")
    async def handler(**kwargs):
        pass

    worker = make_worker(mtask, retry_limit=3)  # cap = 4 deliveries
    await mtask.task_queue.enqueue(queue_name="q", kwargs={"n": 1})

    # Simulate the poison-pill loop: dequeue (increments the Redis counter),
    # then "crash" before completion by recovering the processing entry back
    # to the main queue without touching retry_count.
    dlq_len = 0
    for _ in range(10):
        task = await mtask.task_queue.dequeue(queue_name="q")
        if task is None:
            break
        attempts = task["_delivery_attempts"]
        if attempts > worker.retry_limit + 1:
            # process_task should now divert it to the DLQ
            await worker.process_task(task, worker_id=1)
            break
        # crash before finishing: move processing entry back to main queue
        await mtask.task_queue.recover_processing_tasks("q")

    dlq_len = await fake_redis.llen("q:dlq")
    assert dlq_len == 1
    dlq_task = json.loads((await fake_redis.lrange("q:dlq", 0, -1))[0])
    assert "max delivery attempts" in dlq_task["error"]
    # Counter cleaned up
    assert await fake_redis.hlen("q:attempts") == 0


@pytest.mark.asyncio
async def test_delivery_attempts_cleared_on_success(fake_redis):
    """V1: a normally-completing task leaves no entry in {queue}:attempts."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q")
    async def handler(**kwargs):
        pass

    worker = make_worker(mtask)
    await mtask.task_queue.enqueue(queue_name="q", kwargs={"n": 1})
    task = await mtask.task_queue.dequeue(queue_name="q")
    assert task["_delivery_attempts"] == 1
    await worker.process_task(task, worker_id=1)
    assert await fake_redis.hlen("q:attempts") == 0


@pytest.mark.asyncio
async def test_timeout_frees_slot_for_uncooperative_handler(fake_redis):
    """V3: a handler that swallows CancelledError must not pin the worker
    slot; process_task returns and the task is retried."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q", timeout=1)
    async def stubborn(**kwargs):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            # Ignore the cancellation and keep "working" briefly
            await asyncio.sleep(30)

    worker = make_worker(mtask, retry_limit=3)
    worker.cancel_grace_period = 0.2  # keep the test fast
    await mtask.task_queue.enqueue(queue_name="q", kwargs={"n": 1})
    task = await mtask.task_queue.dequeue(queue_name="q")

    # Must return promptly (well under the handler's 30s sleeps)
    await asyncio.wait_for(worker.process_task(task, worker_id=1), timeout=5)

    # Slot freed and task scheduled for retry (parked in delayed zset)
    assert worker._active_tasks == 0
    assert await fake_redis.zcard("q:delayed") == 1
    # The detached coroutine is tracked; cancel it to keep the test clean
    assert worker._detached_tasks
    for t in list(worker._detached_tasks):
        t.cancel()
    await asyncio.gather(*worker._detached_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_no_task_loss_when_requeue_fails_after_timeout(fake_redis):
    """V4: if requeue fails after a timeout, the processing entry is kept
    for startup recovery instead of being silently dropped."""
    mtask = make_mtask(fake_redis)

    @mtask.agent(queue_name="q", timeout=1)
    async def slow(**kwargs):
        await asyncio.sleep(30)

    worker = make_worker(mtask, retry_limit=3)
    worker.cancel_grace_period = 0.2
    await mtask.task_queue.enqueue(queue_name="q", kwargs={"n": 1})
    task = await mtask.task_queue.dequeue(queue_name="q")
    assert await fake_redis.llen("q:processing") == 1

    # Make requeue fail
    from mtask import TaskRequeueError

    async def boom(*args, **kwargs):
        raise TaskRequeueError("redis down")

    mtask.task_queue.requeue = boom

    await asyncio.wait_for(worker.process_task(task, worker_id=1), timeout=5)

    # Entry kept in processing (recoverable), not lost
    assert await fake_redis.llen("q:processing") == 1
