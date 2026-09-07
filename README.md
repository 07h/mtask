
`μTask` is a simple task queue and scheduler library using Redis and asyncio

```
async def requeue_callback(task: Dict[str, Any], reason: str):
    # {"id": "95729f66-c4d7-404b-a7da-3ebe5ff0ed5b", "name": "topic_second_checker", "kwargs": {"domain": "google.com", "report_id": "6755f85e8bb404bcbf536a2ae", "report_type": "auctions", "source": "godaddy", "item_id": "599444209", "end_time": 1734456120, "price": 25.0, "bids": 0}, "status": "pending", "retry_count": 0}
    print(f"Task {task['id']} was requeued due to {reason}, retry count: {task['retry_count']}")

@queues.agent(
    "topic_first_checker",
    config.settings.get("queues.first_checker.agent_concurrency"),
    timeout=config.settings.get("queues.agent_timeout"),
    on_task_requeued=requeue_callback
)
async def first_checker(data: CheckerAuctionTask):
    pass
```

## Deployment notes

- **Multi-process.** Use `worker_queues` / `MTASK_WORKER_QUEUES` so each process consumes a subset of `@agent` queues. Use `run(workers=False, scheduler=True)` for producer-only processes (cron/interval + `add_task`). Use `run(workers=True, scheduler=False)` for consumer pools. Scale a pool with replicas that share the same `worker_queues`; they compete on blocking dequeue.
- **Recovery.** On startup a worker recovers `{queue}:processing` for **its owned queues only**. A restarting replica can re-deliver work still running on a sibling — at-least-once. Do not overlap queue names across different worker groups.
- **`max_inflight`.** Optional process-wide cap on concurrent handlers (on top of per-queue `concurrency`).
- **Metrics.** `get_queue_depth()`, `get_processing_count()`, `get_inflight_count()`, `get_worker_stats()` for backpressure and dashboards.
- **Single worker process per set of queues (legacy note).** Same recovery caveat as 0.3: two processes that both recover the *same* queue on startup can duplicate in-flight work. Replicas of one group are still the supported scale-out model.
- **uvloop is no longer enabled on import.** Since v0.3.0 the library does not
  install the uvloop event loop policy as an import side effect. Enable it in
  your application entrypoint if desired:

  ```python
  import sys, asyncio
  if sys.platform != "win32":
      try:
          import uvloop
          asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
      except ImportError:
          pass
  ```
- **Delayed retries live in `{queue}:delayed`** (a sorted set scored by the
  ready-at timestamp). When rolling back to a pre-0.3.0 version, drain this
  key first — older versions do not read it.
- **Delivery semantics are at-least-once.** A task may be re-executed after a
  crash or forced shutdown; make handlers idempotent.
- **Make handlers cooperative to cancellation.** Timeouts cancel the handler
  coroutine; a handler that swallows `asyncio.CancelledError` (or runs blocking
  / CPU-bound work without `await`) cannot be interrupted promptly. Since
  v0.3.1 the worker slot is freed after a short grace period regardless, but a
  stubborn coroutine may keep running in the background. Offload blocking or
  CPU-bound work with `await asyncio.to_thread(...)` / an executor.
- **Long-running handlers.** Pass `heartbeat_interval=` on `@agent` so a hung
  process that never crashes can still return the task (stale after 3x interval).
- **Crash-loop protection.** A delivery counter is tracked per task in the
  Redis hash `{queue}:attempts`. A task delivered more than `retry_limit + 1`
  times (e.g. one that repeatedly crashes the process before its retry logic
  runs) is moved to the DLQ instead of looping forever.
- **Breaking a loader ↔ DLQ loop.** If a producer keeps enqueueing work that
  already failed (same `report_id` in kwargs but a new task uuid each time),
  check the DLQ at the start of the agent handler and finalize in your DB:

  ```python
  matches = await mtask.find_dlq_tasks(
      "my_queue", kwargs_match={"report_id": data.report_id}
  )
  if matches:
      await finalize_failed_report(data.report_id, matches[0].get("error"))
      await mtask.remove_dlq_tasks(
          "my_queue", kwargs_match={"report_id": data.report_id}
      )
      return
  ```
- **uvloop is no longer enabled on import.** Since v0.3.0 the library does not
  install the uvloop event loop policy as an import side effect. Enable it in
  your application entrypoint if desired:

  ```python
  import sys, asyncio
  if sys.platform != "win32":
      try:
          import uvloop
          asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
      except ImportError:
          pass
  ```
- **Delayed retries live in `{queue}:delayed`** (a sorted set scored by the
  ready-at timestamp). When rolling back to a pre-0.3.0 version, drain this
  key first — older versions do not read it.
- **Delivery semantics are at-least-once.** A task may be re-executed after a
  crash or forced shutdown; make handlers idempotent.
- **Make handlers cooperative to cancellation.** Timeouts cancel the handler
  coroutine; a handler that swallows `asyncio.CancelledError` (or runs blocking
  / CPU-bound work without `await`) cannot be interrupted promptly. Since
  v0.3.1 the worker slot is freed after a short grace period regardless, but a
  stubborn coroutine may keep running in the background. Offload blocking or
  CPU-bound work with `await asyncio.to_thread(...)` / an executor.
- **Crash-loop protection.** A delivery counter is tracked per task in the
  Redis hash `{queue}:attempts`. A task delivered more than `retry_limit + 1`
  times (e.g. one that repeatedly crashes the process before its retry logic
  runs) is moved to the DLQ instead of looping forever.
- **Breaking a loader ↔ DLQ loop.** If a producer keeps enqueueing work that
  already failed (same `report_id` in kwargs but a new task uuid each time),
  check the DLQ at the start of the agent handler and finalize in your DB:

  ```python
  matches = await mtask.find_dlq_tasks(
      "my_queue", kwargs_match={"report_id": data.report_id}
  )
  if matches:
      await finalize_failed_report(data.report_id, matches[0].get("error"))
      await mtask.remove_dlq_tasks(
          "my_queue", kwargs_match={"report_id": data.report_id}
      )
      return
  ```