"""Public and behavioural contracts introduced in mtask 0.5."""
import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from mtask import TaskOutcome, Worker, mTask


def make_mtask(fake_redis, **kwargs):
    instance = mTask(enable_logging=False, **kwargs)
    instance.task_queue.redis = fake_redis
    instance.task_queue._connection_healthy = True
    return instance


def make_worker(instance, queue_name="q"):
    return Worker(
        task_queue=instance.task_queue,
        task_registry=instance.task_registry,
        task_registry_lock=instance.task_registry_lock,
        retry_limit=instance.retry_limit,
        queue_name=queue_name,
        concurrency=1,
        logger=instance.logger,
        enable_dlq=instance.enable_dlq,
        move_to_dlq_callback=instance._move_to_dlq,
        record_metric_callback=instance._record_metric,
    )


async def run_one(instance, worker, queue_name="q"):
    task = await instance.task_queue.dequeue(queue_name)
    assert task is not None
    await worker.process_task(task, worker_id=1)
    await asyncio.sleep(0)
    return task


def test_v04_defaults_remain_compatible():
    instance = mTask(enable_logging=False)
    assert instance.fair_inflight is False
    assert instance.max_inflight_per_queue is None
    assert instance.dlq_index_fields == []
    assert instance.is_shutting_down is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "reason"),
    [(RuntimeError("boom"), "exception"), (asyncio.TimeoutError(), "timeout")],
)
async def test_requeue_callback_is_unified(fake_redis, failure, reason):
    instance = make_mtask(fake_redis)
    calls = []

    async def callback(task, callback_reason):
        calls.append((task["id"], callback_reason))

    @instance.agent("q", on_task_requeued=callback)
    async def handler(**kwargs):
        raise failure

    task_id = await instance.task_queue.enqueue("q", kwargs={})
    await run_one(instance, make_worker(instance))
    await asyncio.sleep(0.01)
    assert calls == [(task_id, reason)]


@pytest.mark.asyncio
async def test_stale_heartbeat_uses_requeue_callback(fake_redis):
    instance = make_mtask(fake_redis)
    calls = []

    async def callback(task, reason):
        calls.append((task["id"], reason))

    @instance.agent(
        "q", heartbeat_interval=0.01, on_task_requeued=callback
    )
    async def handler(**kwargs):
        pass

    task_id = await instance.task_queue.enqueue("q", kwargs={})
    task = await instance.task_queue.dequeue("q")
    await fake_redis.hset("q:heartbeats", task_id, 0)
    await instance._reap_stale_heartbeats("q")
    await asyncio.sleep(0.01)
    assert calls == [(task_id, "stale_heartbeat")]


@pytest.mark.asyncio
async def test_task_outcomes_and_legacy_returns(fake_redis):
    instance = make_mtask(fake_redis)
    outcomes = [
        TaskOutcome.SUCCESS,
        TaskOutcome.RETRY,
        TaskOutcome.DISCARD,
        "retry",
        False,
        None,
    ]

    @instance.agent("q")
    async def handler(index):
        return outcomes[index]

    worker = make_worker(instance)
    for index in range(len(outcomes)):
        await instance.task_queue.enqueue("q", kwargs={"index": index})
        await run_one(instance, worker)

    assert await fake_redis.zcard("q:delayed") == 1
    assert await fake_redis.llen("q:dlq") == 0
    metrics = await instance.get_metrics("q")
    assert metrics["tasks_completed"] == 4
    assert metrics["tasks_discarded"] == 1
    assert metrics["tasks_requeued"] == 1


@pytest.mark.asyncio
async def test_fair_admission_and_per_queue_cap():
    instance = mTask(
        enable_logging=False,
        max_inflight=2,
        fair_inflight=True,
        max_inflight_per_queue=1,
    )
    await instance.acquire("a")
    blocked_a = asyncio.create_task(instance.acquire("a"))
    admitted_b = asyncio.create_task(instance.acquire("b"))
    await asyncio.sleep(0)
    assert admitted_b.done()
    assert not blocked_a.done()
    instance.release("b")
    assert not blocked_a.done()
    instance.release("a")
    await asyncio.wait_for(blocked_a, timeout=1)
    instance.release("a")


@pytest.mark.asyncio
async def test_fair_admission_load_has_no_starvation_and_respects_caps():
    instance = mTask(
        enable_logging=False,
        max_inflight=4,
        fair_inflight=True,
        max_inflight_per_queue=2,
    )
    active = {}
    max_active = {}
    max_global = 0
    completed = []

    async def run(queue_name, count):
        nonlocal max_global
        for index in range(count):
            await instance.acquire(queue_name)
            active[queue_name] = active.get(queue_name, 0) + 1
            max_active[queue_name] = max(
                max_active.get(queue_name, 0), active[queue_name]
            )
            max_global = max(max_global, sum(active.values()))
            await asyncio.sleep(0)
            completed.append((queue_name, index))
            active[queue_name] -= 1
            instance.release(queue_name)

    await asyncio.wait_for(
        asyncio.gather(
            run("hot", 100),
            run("hot", 100),
            run("low-volume", 5),
        ),
        timeout=3,
    )

    assert max_global <= 4
    assert all(value <= 2 for value in max_active.values())
    assert len([item for item in completed if item[0] == "low-volume"]) == 5
    assert next(
        index
        for index, item in enumerate(completed)
        if item[0] == "low-volume"
    ) < 200


@pytest.mark.asyncio
async def test_empty_queue_does_not_reserve_global_inflight(fake_redis):
    instance = make_mtask(
        fake_redis,
        max_inflight=1,
        fair_inflight=True,
    )
    worker = make_worker(instance, queue_name="empty")
    worker.inflight_gate = instance

    await worker.start()
    await asyncio.sleep(0.1)

    assert instance._global_inflight == 0
    await worker.stop(graceful=True, timeout=1)


@pytest.mark.asyncio
async def test_dlq_indexes_fallback_consistency_and_rebuild(fake_redis):
    instance = make_mtask(fake_redis, dlq_index_fields=["report_id"])
    indexed = {
        "id": "new",
        "name": "q",
        "kwargs": {"report_id": "r1"},
        "retry_count": 3,
    }
    await instance._move_to_dlq(indexed, "q", "boom")
    assert await fake_redis.hlen("q:dlq:entries") == 1

    legacy = {
        "id": "old",
        "name": "q",
        "kwargs": {"report_id": "r1"},
        "status": "failed",
    }
    await fake_redis.rpush("q:dlq", json.dumps(legacy))
    matches = await instance.find_dlq_tasks(
        "q", kwargs_match={"report_id": "r1"}
    )
    assert {task["id"] for task in matches} == {"new", "old"}

    assert await instance.rebuild_dlq_index("q") == 2
    assert await fake_redis.hlen("q:dlq:entries") == 2
    original_lrange = fake_redis.lrange
    with patch.object(
        fake_redis,
        "lrange",
        new=AsyncMock(wraps=original_lrange),
    ) as lrange:
        indexed_matches = await instance.find_dlq_tasks(
            "q", kwargs_match={"report_id": "r1"}
        )
        assert {task["id"] for task in indexed_matches} == {"new", "old"}
        lrange.assert_not_awaited()
    assert await instance.retry_dlq_task("new", "q") is True
    assert await fake_redis.hget("q:dlq:entries", "new") is None
    assert await instance.remove_dlq_tasks("q", task_id="old") == 1
    assert await fake_redis.hlen("q:dlq:entries") == 0
    assert await instance.clear_dlq("q") == 0


@pytest.mark.asyncio
async def test_dlq_index_falls_back_when_list_is_ahead(fake_redis):
    instance = make_mtask(fake_redis, dlq_index_fields=["report_id"])
    await instance._move_to_dlq(
        {
            "id": "new",
            "name": "q",
            "kwargs": {"report_id": "r1"},
            "retry_count": 3,
        },
        "q",
        "boom",
    )
    assert await instance.rebuild_dlq_index("q") == 1
    await fake_redis.rpush(
        "q:dlq",
        json.dumps(
            {
                "id": "old",
                "name": "q",
                "kwargs": {"report_id": "r1"},
                "status": "failed",
            }
        ),
    )

    matches = await instance.find_dlq_tasks(
        "q", kwargs_match={"report_id": "r1"}
    )

    assert {task["id"] for task in matches} == {"new", "old"}


@pytest.mark.asyncio
async def test_public_shutdown_lifecycle(fake_redis):
    instance = make_mtask(fake_redis)

    async def disconnect():
        return None

    instance.task_queue.disconnect = disconnect
    await instance.shutdown()
    assert instance.is_shutting_down is True
    assert await instance.wait_shutdown_complete(timeout=0.1) is True
    stats = instance.get_shutdown_stats()
    assert stats["completed_at"] is not None
    assert stats["duration"] >= 0
    await instance.shutdown()
