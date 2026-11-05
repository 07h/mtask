"""H1/H2: heartbeat reap and DLQ listing use bounded LRANGE, not 0/-1."""
import json
from unittest.mock import patch

import pytest

from mtask import _REDIS_LIST_SCAN_BATCH, mTask
from tests.test_v05_contract import make_mtask


def _assert_bounded_lrange_calls(calls):
    assert len(calls) >= 1
    for args in calls:
        assert len(args) >= 3
        start, end = args[1], args[2]
        assert end != -1, f"unbounded LRANGE end=-1 in call {args}"
        assert start >= 0
        assert end >= start
        assert (end - start + 1) <= _REDIS_LIST_SCAN_BATCH


@pytest.mark.asyncio
async def test_dlq_iter_uses_bounded_lrange(fake_redis):
    """H2: _iter_dlq_entries / get_dlq_tasks never LRANGE 0,-1."""
    instance = make_mtask(fake_redis)
    total = _REDIS_LIST_SCAN_BATCH + 3
    for i in range(total):
        await fake_redis.rpush(
            "q:dlq",
            json.dumps(
                {
                    "id": f"t-{i}",
                    "name": "q",
                    "kwargs": {"i": i},
                    "status": "failed",
                }
            ),
        )

    calls = []
    real = fake_redis.lrange

    async def tracking_lrange(*args, **kwargs):
        calls.append(args)
        return await real(*args, **kwargs)

    with patch.object(fake_redis, "lrange", new=tracking_lrange):
        tasks = await instance.get_dlq_tasks("q")

    _assert_bounded_lrange_calls(calls)
    assert len(calls) >= 2
    assert len(tasks) == total
    assert tasks[0]["id"] == "t-0"
    assert tasks[-1]["id"] == f"t-{total - 1}"


@pytest.mark.asyncio
async def test_reap_stale_heartbeats_uses_bounded_lrange(fake_redis):
    """H1: stale-heartbeat reap scans :processing in bounded pages."""
    instance = make_mtask(fake_redis)

    @instance.agent("q", heartbeat_interval=0.01)
    async def handler(**kwargs):
        pass

    # Fill processing with many live entries plus one stale target.
    filler = _REDIS_LIST_SCAN_BATCH + 2
    for i in range(filler):
        entry = {
            "id": f"live-{i}",
            "name": "q",
            "kwargs": {},
            "status": "processing",
            "retry_count": 0,
        }
        await fake_redis.rpush("q:processing", json.dumps(entry))
        await fake_redis.hset("q:heartbeats", f"live-{i}", "9999999999")

    stale = {
        "id": "stale-1",
        "name": "q",
        "kwargs": {},
        "status": "processing",
        "retry_count": 0,
    }
    await fake_redis.rpush("q:processing", json.dumps(stale))
    await fake_redis.hset("q:heartbeats", "stale-1", "0")

    calls = []
    real = fake_redis.lrange

    async def tracking_lrange(*args, **kwargs):
        calls.append(args)
        return await real(*args, **kwargs)

    with patch.object(fake_redis, "lrange", new=tracking_lrange):
        await instance._reap_stale_heartbeats("q")

    _assert_bounded_lrange_calls(calls)
    assert len(calls) >= 2

    # Stale task returned to main queue; heartbeat cleared
    assert await fake_redis.llen("q") == 1
    assert await fake_redis.hget("q:heartbeats", "stale-1") is None
    # Live entries still in processing
    assert await fake_redis.llen("q:processing") == filler
