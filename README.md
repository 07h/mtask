
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