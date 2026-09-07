# Migrating from mtask 0.3.x to 0.4.0

Redis keys, task JSON, DLQ helpers, `add_task`, `pause_queue`, and the `@agent` / `@cron` / `@interval` decorators stay compatible. Existing 0.3 deployments can roll forward without draining queues.

## Opt-in APIs

```python
mtask = mTask(
    redis_url=...,
    worker_queues=["topic_a", "topic_b"],  # or env MTASK_WORKER_QUEUES=topic_a,topic_b
    max_inflight=30,
)

# Consumer-only worker process
await mtask.run(workers=True, scheduler=False)

# Producer-only (cron/interval + add_task)
await mtask.run(workers=False, scheduler=True)
```

Unknown names in `worker_queues` raise `ValueError` at startup.

## Consumer model

Each owned queue now has **one** blocking consumer. `concurrency` is a semaphore on handlers, not N Redis poll loops. Replicas that share the same `worker_queues` compete on `BLMOVE` / list pop — that is supported.

Startup still recovers `{queue}:processing` back onto the main list for **owned** queues only. During a rolling restart, a new replica can re-deliver tasks that siblings are still executing (at-least-once). Keep handlers idempotent. Do not run two *different* queue groups that overlap the same queue names.

## Metrics (for loader backpressure)

```python
depth = await mtask.get_queue_depth("topic_a")
inflight = mtask.get_inflight_count()
stats = await mtask.get_worker_stats()
```

Depth is `LLEN` + priority `ZCARD` + delayed `ZCARD`.

## Heartbeats (optional)

```python
@mtask.agent("slow_queue", concurrency=5, timeout=3600, heartbeat_interval=60)
async def slow(data: Model):
    ...
```

While the handler runs, mtask writes `{queue}:heartbeats`. If the timestamp is older than `3 * heartbeat_interval` and the task is still in processing, it is returned to the queue. Disable by omitting the argument (default).

## Rollback to 0.3.2

Pin `mtask==0.3.2`. Heartbeat hashes (`{queue}:heartbeats`) can be ignored or `DEL`'d; 0.3 does not read them. All other keys are shared.
