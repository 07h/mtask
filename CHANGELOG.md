# Changelog

## 0.5.0

### Added

- `TaskOutcome.SUCCESS`, `TaskOutcome.RETRY`, and `TaskOutcome.DISCARD`.
  Existing handler return values, including strings and booleans, still mean
  success.
- Unified `on_task_requeued(task, reason)` notifications for timeout
  (`timeout`), ordinary exceptions (`exception`), explicit outcomes
  (`task_outcome_retry`), and stale heartbeats (`stale_heartbeat`).
- Opt-in fair process-wide admission with `fair_inflight=True`, plus
  `max_inflight_per_queue` (an integer or queue-to-limit mapping).
- Opt-in DLQ indexes with `dlq_index_fields=[...]`. The canonical LIST is
  dual-written with a HASH and value SETs; LIST-only entries remain readable.
  `rebuild_dlq_index(queue_name)` rebuilds indexes online.
- Public shutdown lifecycle: `shutdown()`, `is_shutting_down`,
  `wait_shutdown_complete()`, `shutdown_stats`, and `get_shutdown_stats()`.
- Aggregate `get_stats()` runtime snapshot for health endpoints.
- Worker stats now include each queue's admitted task count.

### Compatibility

- All new scheduling, indexing, and outcome behavior is opt-in or additive.
- Defaults retain 0.4 scheduling and LIST-only DLQ storage.
- Python 3.8 remains supported.

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
