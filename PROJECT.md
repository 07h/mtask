
## Архитектура проекта

### Основные компоненты

```
┌─────────────────────────────────────────────────────────────────────┐
│                            mTask                                     │
│  (главный класс-оркестратор)                                        │
├─────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐   │
│  │  TaskQueue    │  │   Worker(s)   │  │  ScheduledTask(s)     │   │
│  │  (Redis ops)  │  │  (обработка)  │  │  (cron/interval)      │   │
│  └───────────────┘  └───────────────┘  └───────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 1. TaskQueue — Управление очередями в Redis

Отвечает за все Redis-операции:

| Структура Redis | Описание |
|-----------------|----------|
| `{queue_name}` | Основная очередь (List) — обычные задачи |
| `{queue_name}:priority` | Приоритетная очередь (Sorted Set) |
| `{queue_name}:processing` | Задачи в обработке (Hash) — для отслеживания |
| `{queue_name}:dlq` | Dead Letter Queue — провалившиеся задачи |

### Ключевые методы:

- **`enqueue()`** — добавляет задачу в очередь (обычную или приоритетную)
- **`dequeue()`** — извлекает задачу (сначала из priority, потом из обычной)
- **`requeue()`** — возвращает задачу в очередь при ошибке (с backoff)
- **`mark_completed()`** — удаляет задачу из processing queue
- **`recover_processing_tasks()`** — восстановление задач после краша

### Структура задачи:

```python
{
    "id": "uuid",
    "name": "queue_name", 
    "kwargs": {...},           # параметры задачи
    "status": "pending",       # pending/processing/completed/failed
    "retry_count": 0,
    "priority": 0,
    "retry_after": timestamp,  # для backoff (опционально)
}
```

---

## 2. Worker — Обработчик задач

Каждый Worker работает в отдельном asyncio task с контролем concurrency через Semaphore.

### Логика обработки:

```
┌─────────────────────────────────────────────────────────────┐
│                    Worker Loop                               │
├─────────────────────────────────────────────────────────────┤
│  1. dequeue() — получить задачу                             │
│  2. Найти функцию в task_registry                           │
│  3. Если функция принимает Pydantic модель → распарсить     │
│  4. Выполнить с timeout (если задан)                        │
│  5. При успехе → mark_completed()                           │
│  6. При ошибке:                                              │
│     - retry_count < limit → requeue() с backoff              │
│     - retry_count >= limit → move_to_dlq()                  │
└─────────────────────────────────────────────────────────────┘
```

### Особенности:
- **Exponential backoff с jitter** — при повторных попытках
- **Persistent backoff** — задача сохраняется в Redis с `retry_after`, не теряется при краше
- **Graceful shutdown** — ждёт завершения активных задач

---

## 3. ScheduledTask — Планировщик

Два режима запуска:

| Режим | Пример | Описание |
|-------|--------|----------|
| **interval** | `@mtask.interval(seconds=60)` | Каждые N секунд |
| **cron** | `@mtask.cron("*/5 * * * *")` | По cron-выражению |

- Предотвращает одновременный запуск одной и той же задачи (`is_running` флаг)
- Обрабатывает ошибки без падения

---

## 4. mTask — Главный класс

### Декораторы:

```python
# Регистрация обработчика задач
@mtask.agent(
    queue_name="my_queue",
    concurrency=3,          # макс. параллельных задач
    timeout=30,             # таймаут в секундах
    rate_limit=100,         # макс. задач/минуту (опционально)
    on_task_requeued=callback  # колбэк при requeue
)
async def process(data: MyModel):
    ...

# Планируемые задачи
@mtask.interval(seconds=300)
async def cleanup():
    ...

@mtask.cron("0 2 * * *")  # в 2:00 каждый день
async def nightly_report():
    ...
```

### Функции управления:

| Метод | Описание |
|-------|----------|
| `add_task()` | Добавить задачу вручную (с приоритетом) |
| `pause_queue()` | Приостановить очередь на N секунд |
| `get_dlq_tasks()` | Получить провалившиеся задачи |
| `retry_dlq_task()` | Повторить задачу из DLQ |
| `clear_dlq()` | Очистить DLQ |
| `get_metrics()` | Получить метрики (completed, failed, avg_time) |
| `graceful_shutdown()` | Корректное завершение |

---

## 5. Особенности реализации

### Отказоустойчивость:
- **Health check loop** — периодическая проверка Redis
- **Auto-reconnect** — exponential backoff при потере соединения
- **Processing queue recovery** — восстановление задач после краша
- **Persistent backoff** — задачи не теряются при перезапуске

### Безопасность:
- Валидация параметров при создании
- Ограничение размера задачи (`max_task_size`)
- Rate limiting
- Таймауты на все Redis-операции

### Миграция:
- Поддержка старого формата processing queue (List → Hash)

---

## 6. Типичный flow задачи

```
1. @mtask.agent декорирует функцию → регистрация в task_registry
2. Вызов process(data=...) → enqueue() в Redis
3. Worker.dequeue() → получение задачи
4. Worker.process_task() → выполнение функции
5. Успех → mark_completed()
   Ошибка → requeue() (до retry_limit) → DLQ
```

---

## 7. Зависимости

- **redis** (async) — хранилище
- **pydantic** — валидация данных
- **croniter** — парсинг cron
- **uvloop** — ускорение event loop (Unix)
- **async-timeout** — таймауты

