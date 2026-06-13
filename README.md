
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

- **Single worker process per set of queues.** On startup the worker recovers
  everything left in `{queue}:processing` back into the main queue. If two or
  more worker processes consume the same queues, a restarting instance will
  "steal" tasks that are currently being executed by its siblings, causing
  duplicate execution. Producer-only processes (those that just call
  `add_task()` / decorated wrappers and register no `@agent`) are always safe
  in any number.
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