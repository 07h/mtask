# Changelog

## 0.4.1

### Fixed

- Resume after `pause_queue`: the status monitor now watches all owned queues, not only queues with a live worker. Previously a paused queue never restarted once its Redis TTL expired if any sibling worker was still running (first/second/third checkers stuck after webarchive pause).
- Delayed-task promote loop uses the same owned-queue list, so due retries are promoted even while a queue is paused.

## 0.4.0

### Added

- `worker_queues` (`mTask(...)` or `MTASK_WORKER_QUEUES`) — this process only consumes the listed queues.
- `max_inflight` — process-wide cap on concurrent handlers across all owned queues.
- `run(workers=True, scheduler=True)` — producer-only (`workers=False`) or consumer-only (`scheduler=False`) processes.
- Metrics: `get_queue_depth()`, `get_processing_count()`, `get_inflight_count()`, `get_worker_stats()`.
- `TaskQueue.dequeue_blocking()` — BLMOVE-based blocking dequeue (atomic move into processing).
- One consumer loop per queue; concurrency is a semaphore, not N poll loops.
- Background delayed-promote loop (no longer tied to every worker poll).
- Optional `@agent(..., heartbeat_interval=seconds)` plus stale-heartbeat requeue (3x interval).

### Changed

- Workers no longer spawn `concurrency` independent poll loops.
- `dequeue()` remains non-blocking for tests and callers that need a single shot.

### Compatibility

- Redis key layout and task JSON are unchanged from 0.3.x.
- `@agent` / `@interval` / `@cron` / `add_task` / DLQ / `pause_queue` signatures are unchanged except the new optional `heartbeat_interval`.
