# Bug Fixes Applied

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

