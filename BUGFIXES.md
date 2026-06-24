# Bug Fixes Applied

## v0.3.2 — DLQ lookup and removal by kwargs

Additive API only; existing `get_dlq_tasks`, `retry_dlq_task`, and `clear_dlq`
are unchanged.

- **`find_dlq_tasks(queue_name, *, kwargs_match)`** — returns DLQ entries whose
  `kwargs` contain every key-value pair in `kwargs_match` (subset match). Does
  not modify the DLQ. Use at the start of an agent handler when a loader may
  re-enqueue the same business payload under a new task id after a prior failure.
- **`remove_dlq_tasks(queue_name, *, kwargs_match=None, task_id=None) -> int`**
  — removes matching entries without re-enqueueing (unlike `retry_dlq_task`).
  Exactly one selector is required. Also clears `{queue}:attempts` for each
  removed task id. `task_id` is for admin use when the DLQ entry uuid is known.
- Internal **`_iter_dlq_entries`** shared by DLQ helpers to avoid duplicated
  `LRANGE` + parse logic.

## v0.3.1 — Worker logic hardening

Public API unchanged (no changed signatures; existing karma code needs no edits). New internal field `_delivery_attempts` is stripped on requeue; a new Redis key `{queue}:attempts` is created automatically.

- **V1 — crash-loop / infinite redelivery protection (Critical).** `retry_count`
  only advances inside the `except` handlers of `process_task`, so a task that
  kills the whole process (OOM, container kill, segfault) before those run was
  recovered from `{queue}:processing` and re-executed forever. A delivery
  counter is now kept in a Redis hash `{queue}:attempts` (HINCRBY on every
  dequeue, survives process death). When deliveries exceed `retry_limit + 1`,
  the task goes straight to the DLQ. The counter is cleared (`HDEL`) on success,
  DLQ, and non-retryable failures. This also bounds V8 (sibling redelivery on
  re-entrant `pause_queue`).
- **V3 — timeout now frees the worker slot (High).** `asyncio.wait_for` waits
  indefinitely for an uncooperative coroutine to finish cancelling, so a handler
  that swallows `CancelledError` pinned the concurrency slot forever. Execution
  now goes through `Worker._run_with_timeout`: cancel, wait a short grace period
  (`cancel_grace_period`, default 1s), then free the slot regardless. A still
  -running coroutine is detached with a strong reference kept (so it is not GC'd)
  and the task is retried/DLQ'd as before. Handlers should be cooperative to
  cancellation; a timed-out handler may briefly keep running in the background
  (at-least-once).
- **V4 — no task loss when requeue fails after a timeout/error (High).**
  Previously the `finally` block always removed the processing entry, so if
  `requeue()` failed (e.g. Redis briefly down) the task was dropped entirely.
  A `keep_in_processing` flag now leaves the entry for startup recovery.
- **V5 — signature resolved once at registration (Medium).** The pydantic-model
  parameter name/class is computed in the `@agent` decorator and cached in
  `task_registry`, instead of calling `inspect.signature()` on every task.
- **V6 — delayed-promote throttled per queue (Medium).** The promote Lua script
  ran on every poll of every worker; it is now throttled to once per
  `_promote_interval` (0.5s) per queue, cutting redundant `ZRANGEBYSCORE` load
  under high concurrency on idle queues.
- **V7 — `on_task_requeued` off the hot path (Medium).** The callback runs as a
  tracked background task (`Worker._spawn_callback`) with exceptions logged, so
  a slow/hanging callback cannot hold the worker slot.
- **V9 — optional orjson (Low).** Internal `_dumps`/`_loads` use orjson when
  installed, falling back to stdlib `json`. Safe for LREM matching since the
  exact stored string is always reused.

Note: V2 (all queues/workers share one event loop, so a blocking or
cancellation-ignoring handler can slow every queue in the process) is
architectural and not fixable inside the library — mitigate via deployment
topology (multiple worker processes per queue group) and handler discipline
(`asyncio.to_thread` / executors for blocking or CPU-bound work).

## v0.3.0 — Audit fixes (27 findings)

The public API is unchanged: `mTask(...)`, `@agent` / `@interval` / `@cron`,
`add_task`, `pause_queue`, `run`, DLQ helpers and `task_queue.redis` keep
their signatures. Behavioral changes are listed below.

### Critical

- **C1 — zombie entries in `{queue}:processing` after a retry.**
  `requeue()` pops `_task_json` from the task dict, so the `finally` block in
  `process_task` could no longer LREM the old processing entry; every retried
  task left a duplicate that startup recovery re-executed. The JSON is now
  saved to a local variable before any requeue.
- **C2 — `Worker.stop()` never actually stopped worker loops.** Cancelling
  `_monitor_worker` did not cancel the inner `_worker_loop` task it was
  awaiting. The cancellation is now propagated explicitly
  (`worker_task.cancel()` + gather). Additionally, a task cancelled
  mid-execution is requeued (`apply_backoff=False`) before the cancellation
  is re-raised, so forced shutdowns do not lose in-flight tasks.
- **C3 — graceful shutdown was dead code.** `except KeyboardInterrupt` inside
  a coroutine is unreachable under asyncio. `run()` now registers
  `SIGINT`/`SIGTERM` handlers (with a Windows fallback) and handles
  `asyncio.CancelledError`; both paths execute `graceful_shutdown()`.
- **C4 — priority dequeue could lose a task.** `ZPOPMAX` and the `RPUSH` into
  processing happened as two separate commands; a crash in between lost the
  task. Both steps now run in a single Lua script. The regular-list dequeue
  also uses an atomic Lua LPOP+RPUSH (replacing LMOVE, so Redis < 6.2 works
  without the previous non-atomic fallback).
- **C5 — a task in backoff blocked the whole queue.** A task waiting out its
  backoff was repeatedly dequeued and pushed back, and `dequeue()` returned
  `None` even when other tasks were ready. Backoff tasks are now parked in a
  separate `{queue}:delayed` sorted set (score = ready-at timestamp) and are
  atomically promoted back by a Lua script on each dequeue. Legacy in-flight
  tasks with the old `retry_after` body field are migrated on the fly.

### High

- **H1** — naive `datetime.utcnow().timestamp()` (local-time interpretation
  of a UTC wall clock) replaced with `time.time()` / `time.monotonic()`;
  ISO timestamps now use `datetime.now(timezone.utc)`.
- **H2** — the pydantic-model argument was always passed as `data=`; handlers
  whose parameter had another name crashed with `TypeError` on every retry.
  The real parameter name from the signature is used, and `@agent` now fails
  fast at registration for non-async handlers and positional-only model
  parameters.
- **H3** — after a failed `mark_completed` the task was requeued even when it
  had completed successfully, guaranteeing duplicate execution. The stale
  processing entry is now left for startup recovery instead (duplicate is
  possible, no longer guaranteed).
- **H4** — `asyncio.Lock` instances are created lazily inside the running
  loop (Python 3.8/3.9 bind locks to the loop at creation time, which broke
  module-level `mTask(...)` instantiation).
- **H5** — multi-instance limitation documented in README (single worker
  process per set of queues; producers are unlimited).
- **H6** — `ensure_connected()` no longer holds the connect lock for an
  entire reconnect cycle (which froze all workers and producers for minutes).
  It fails fast with `RedisConnectionError` and kicks off a single background
  reconnect task.

### Medium

- **M1** — all fire-and-forget tasks keep strong references with done
  callbacks that log exceptions; the scheduler loop (`run_scheduled_tasks`)
  is restarted by `run()` if it dies, like the queue monitor.
- **M2** — DLQ entries no longer embed the nested `_task_json` copy (entry
  size doubled per DLQ cycle); `retry_dlq_task` strips internal fields too.
- **M3** — deterministic failures (pydantic `ValidationError`, missing task
  function) skip retries and go straight to the DLQ.
- **M4** — idle worker polling backs off adaptively from 0.1s to 1s.
- **M5** — the producer-side rate limiter uses atomic `INCR` (the previous
  GET-then-INCR let concurrent producers through over the limit).
- **M6** — DLQ APIs, `_get_queue_status` and `pause_queue` go through
  `ensure_connected()` + `asyncio.wait_for` like the core queue operations.
- **M7** — importing `mtask` no longer installs the uvloop event-loop policy
  globally; enable uvloop in your application entrypoint.
- **M8** — `pause_queue` reuses the atomic `recover_processing_tasks()`
  RPOPLPUSH loop instead of a racy LRANGE+LPUSH+DELETE sequence.
- **M9** — non-JSON-serializable kwargs raise `TaskEnqueueError` instead of a
  bare `TypeError` escaping from `json.dumps`.
- **M10** — calling `pause_queue()` from inside a task of the same queue
  self-deadlocked until the shutdown timeout (the graceful stop waited for
  the calling task itself). Re-entrant calls are detected via a `ContextVar`
  and the pause is finalized in a background task.

### Low

- **L1** — `Task.exception()` is only read after checking `.cancelled()`.
- **L2** — reconnect backoff sleeps after a failed attempt, not before the
  first one.
- **L3** — unparseable queue entries (poison messages) are moved to
  `{queue}:dlq` instead of cycling through processing/recovery forever.
- **L4** — scheduled tasks compute `next_run` at trigger time, not at
  completion, so intervals no longer drift by the task duration.
- **L6** — `graceful_shutdown` iterates over a copy of `workers`.
- **L7** — `retry_on_timeout=False` for the Redis client: silent client-level
  retries could duplicate `RPUSH` writes; mtask has its own retry logic.

### Notes for existing deployments

- Backoff retries are stored in `{queue}:delayed`. Before rolling back to
  v0.2.x, drain this zset (older code never reads it).
- Enable uvloop in your own entrypoints after upgrading (see README).
- Interval scheduled tasks still run their first iteration one interval after
  startup (unchanged), but no longer drift by the duration of each run.

---

# v0.2.x fixes (historical)

## Critical Issues Fixed

### 1. ✅ Fixed useless finally block in `_monitor_worker` (lines 486-499)
**Problem:** The finally block tried to remove `worker_task` from `self._workers`, but `worker_task` is from `_worker_loop` while `self._workers` contains `_monitor_worker` tasks. They never match.

**Solution:** Removed the useless finally block completely.

### 2. ✅ Fixed race condition with `on_task_requeued` callback
**Problem:** `on_task_requeued` was accessed from `task_registry` without lock in Worker's `__init__`, causing potential race condition.

**Solution:** Moved the callback retrieval inside the lock-protected section in `process_task()` method where it's actually used.

**Before:**
```python
# In __init__
task_info = self.task_registry.get(self.queue_name, {})
self.on_task_requeued = task_info.get("on_task_requeued", None)
```

**After:**
```python
# In process_task, inside async with self.task_registry_lock:
on_task_requeued = task_info.get("on_task_requeued")
```

### 3. ✅ Reduced blpop timeout from 5 to 1 second
**Problem:** With 5-second timeout, priority tasks could wait up to 5 seconds before being processed if the worker was blocked on the regular queue.

**Solution:** Changed timeout from 5 to 1 second for faster priority queue checking.

### 4. ✅ Fixed task loss during backoff period
**Problem:** The original `requeue()` implementation used `await asyncio.sleep(backoff_delay)`, keeping the task in memory. If the process crashed during this sleep, the task would be lost forever.

**Solution:** Implemented persistent backoff using timestamps:
- Store `retry_after` timestamp in the task itself
- Task is immediately re-enqueued to Redis (persistent)
- `dequeue()` checks `retry_after` and re-enqueues if backoff period hasn't expired
- When backoff expires, task is processed normally

**Benefits:**
- Tasks survive process crashes during backoff
- Backoff is maintained across restarts
- No tasks are lost

## Implementation Details

### Backoff with Persistence

**Before:**
```python
async def requeue(task, ...):
    if apply_backoff:
        backoff_delay = calculate_backoff(retry_count)
        await asyncio.sleep(backoff_delay)  # ← Task in memory, lost on crash
    await redis.rpush(queue, task)
```

**After:**
```python
async def requeue(task, ...):
    if apply_backoff:
        backoff_delay = calculate_backoff(retry_count)
        task["retry_after"] = now() + backoff_delay  # ← Stored in task
    await redis.rpush(queue, task)  # ← Immediately persisted

async def dequeue(queue):
    task = await redis.lpop(queue)
    if task.get("retry_after") and now() < task["retry_after"]:
        await redis.rpush(queue, task)  # ← Re-enqueue if still in backoff
        return None
    return task
```

## Testing Recommendations

1. **Test graceful shutdown:** Verify workers wait for active tasks
2. **Test priority queue responsiveness:** Ensure priority tasks are picked up within ~1 second
3. **Test backoff persistence:** Kill process during backoff, restart, verify task executes after backoff expires
4. **Test concurrent callback access:** Multiple workers using `on_task_requeued` without race conditions

## Backward Compatibility

✅ All changes are backward compatible:
- No API changes
- Existing code continues to work
- New `retry_after` field in tasks is optional and handled gracefully

