# API Reference

All public symbols are exported from the top-level `unimessaging` package.

```python
import unimessaging
print(unimessaging.__version__)  # the installed distribution version, e.g. "0.16.0"
print(unimessaging.__all__)
```

---

## Top-Level Exports

| Symbol | Type | Module |
|--------|------|--------|
| `send_message` | function | `unimessaging.integrations.common.facade` |
| `Message` | class | `unimessaging.domain.entities` |
| `InvalidMessageError` | exception | `unimessaging.domain.exceptions` |
| `NotificationGateway` | protocol | `unimessaging.domain.ports` |
| `SendMessageRequest` | dataclass | `unimessaging.application.dto` |
| `SendMessageResponse` | dataclass | `unimessaging.application.dto` |
| `SendMessageUseCase` | class | `unimessaging.application.use_cases` |
| `InMemoryNotificationGateway` | class | `unimessaging.adapters.in_memory.gateway` |

---

## `send_message(message, recipient, *, gateway=None)`

The primary facade function.

```python
send_message(message: str, recipient: str | None = None, *, gateway: NotificationGateway | None = None) -> dict
```

**Parameters:**
- `message` (str) -- Message content. Must not be empty.
- `recipient` (str | None) -- Target recipient. Required.
- `gateway` (NotificationGateway | None) -- Custom gateway. Defaults to `InMemoryNotificationGateway`.

**Returns:** `dict` with keys `"status"` and `"payload"`.

**Raises:**
- `ValueError` -- if `recipient` is `None`
- `InvalidMessageError` -- if `message` or `recipient` is empty/whitespace

---

## `Message(content, recipient)`

Domain value object.

```python
Message(content: str, recipient: str)
```

**Fields:**
- `content` (str) -- Stripped on creation. Must not be empty.
- `recipient` (str) -- Stripped on creation. Must not be empty.

**Methods:**
- `to_dict() -> dict` -- Returns `{"message": content, "to": recipient}`

**Raises:** `InvalidMessageError` on construction if validation fails.

---

## `InvalidMessageError(detail)`

Domain exception. Subclass of `ValueError`.

```python
InvalidMessageError(detail: str)
```

**Attributes:**
- `detail` (str) -- Human-readable error description.

---

## `NotificationGateway` (Protocol)

Port contract for transport adapters.

**Required method:**
```python
def deliver(self, message: Message) -> dict
```

---

## `SendMessageRequest(message, recipient)`

Application-layer input DTO. Frozen dataclass.

**Fields:**
- `message` (str)
- `recipient` (str)

---

## `SendMessageResponse(status, payload)`

Application-layer output DTO. Frozen dataclass.

**Fields:**
- `status` (str)
- `payload` (dict)

---

## `SendMessageUseCase(gateway)`

Application-layer use case.

**Constructor:**
- `gateway` (NotificationGateway) -- The transport adapter.

**Methods:**
- `execute(request: SendMessageRequest) -> SendMessageResponse`

---

## `InMemoryNotificationGateway()`

Echo adapter. Returns `message.to_dict()` from `deliver()`. No dependencies.

**Methods:**
- `deliver(message: Message) -> dict`

---

## `InMemoryBrokerAdapter()`

**Module:** `unimessaging.adapters.in_memory_broker`
**Import:** `from unimessaging import InMemoryBrokerAdapter`

In-memory implementation of `AsyncMessagingPort` for testing pub/sub without a real NATS server.

---

## `NATSNotificationGateway(config=None)`

**Module:** `unimessaging.adapters.nats.gateway`
**Import:** `from unimessaging.adapters import NATSNotificationGateway`

NATS transport adapter. Requires `nats-py`.

**Constructor:**
- `config` (NATSConfig | None) -- Connection settings. Defaults to `NATSConfig()`.

**Methods:**
- `deliver(message: Message) -> dict` -- Publishes to NATS. Returns payload enriched with `"subject"` key.

**Raises:**
- `RuntimeError` -- if `nats-py` not installed, or if called from within an active event loop.

---

## `NATSConfig(url, subject, client_name, flush_timeout)`

**Module:** `unimessaging.adapters.nats.gateway`
**Import:** `from unimessaging.adapters import NATSConfig`

Frozen dataclass for NATS connection settings.

| Field | Type | Default |
|-------|------|---------|
| `url` | `str` | `"nats://localhost:4222"` |
| `subject` | `str` | `"notifications.default"` |
| `client_name` | `str` | `"unimessaging"` |
| `flush_timeout` | `float` | `2.0` |

---

## Outbox

> Requires the `outbox` extra: `pip install unimessaging[outbox]`

**Module:** `unimessaging.outbox`
**Import:** `from unimessaging.outbox import OutboxMixin, OutboxStatus, OutboxRepository, OutboxEventBus, OutboxRelay, relay_loop, run_standalone_relay, relay_in_process`

---

## `OutboxMixin`

SQLAlchemy mixin providing all outbox table columns. Inherit with your project's declarative `Base`:

```python
from unimessaging.outbox import OutboxMixin
from your_app.orm import Base

class OutboxORM(OutboxMixin, Base):
    pass
```

Alembic will auto-detect the table via `--autogenerate`.

---

## `OutboxStatus`

Enum with values: `PENDING`, `PUBLISHED`, `FAILED`.

---

## `OutboxRepository(session, model_class)`

Writes outbox rows within the caller's database transaction.

**Constructor Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `session` | `AsyncSession` | The SQLAlchemy async session (shared with the service's UoW) |
| `model_class` | class | The concrete ORM class inheriting from `OutboxMixin` + `Base` |

**Methods:**

### `async add(*, aggregate_type, aggregate_id, event_type, payload, headers=None, occurred_at) -> None`

Insert an outbox row and flush (making it visible within the current transaction).

---

## `OutboxEventBus(outbox_repo)`

Serializes dataclass domain events into outbox rows. Works with any dataclass event that has `aggregate_type`, `aggregate_id`, `event_id`, and `occurred_at` attributes — no domain imports required (structural typing).

**Constructor Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `outbox_repo` | `OutboxRepository` | Repository sharing the UoW's session |

**Methods:**

### `async publish(event) -> None`

Serialize a single dataclass event and write it to the outbox.

### `async publish_many(events) -> None`

Serialize and write multiple events.

---

## `OutboxRelay(session_factory, messaging, *, subject_prefix, table_name="outbox", max_retries=10, base_backoff=5)`

Polls a PostgreSQL outbox table for pending rows and publishes them via a messaging backend.

**Constructor Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_factory` | `async_sessionmaker[AsyncSession]` | *required* | SQLAlchemy async session factory |
| `messaging` | any with `async publish(subject, data)` | *required* | Messaging client (e.g. `UnifiedMessaging`) |
| `subject_prefix` | `str` | *required* | Prefix for NATS subjects (e.g. `"articles"` → `"articles.event"`) |
| `table_name` | `str` | `"outbox"` | Name of the outbox table |
| `max_retries` | `int` | `10` | Max attempts before marking a row `FAILED` |
| `base_backoff` | `int` | `5` | Base delay in seconds for exponential back-off |

**Methods:**

### `async process_batch(batch_size=50) -> int`

Process up to `batch_size` pending outbox rows. Returns the number of rows successfully published.

Rows are locked with `FOR UPDATE SKIP LOCKED` to allow concurrent relay instances. On success, rows are marked `PUBLISHED`. On failure, retries are incremented with exponential back-off. After `max_retries`, rows are marked `FAILED`.

---

## `async relay_loop(relay, *, poll_interval=0.5, on_tick=None) -> None`

Run the relay in an infinite loop, sleeping `poll_interval` seconds when idle. Designed to be run as a background `asyncio.Task`. Cancel the task to stop gracefully. `on_tick`, if given, is called (sync or async) once at the start of every iteration; an exception from it is logged and does not stop the loop.

```python
task = asyncio.create_task(relay_loop(relay))
# ... on shutdown:
task.cancel()
await task
```

---

## Django Outbox

> Requires the `django` extra: `pip install unimessaging[django]`

**Module:** `unimessaging.outbox_django`
**Import:** `from unimessaging.outbox_django import OutboxRecord, OutboxStatus, DjangoOutboxRepository, DjangoOutboxEventBus, DjangoOutboxRelay`

---

## `OutboxRecord`

Django model providing the outbox table. Add `"unimessaging.outbox_django"` to `INSTALLED_APPS` and run migrations.

Same schema as `OutboxMixin` but using Django field types.

**Meta:**
- `app_label = "unimessaging"`
- `db_table = "outbox"`

---

## `DjangoOutboxRepository()`

Writes outbox rows using Django ORM within the caller's `transaction.atomic()`.

**Methods:**

### `add(*, aggregate_type, aggregate_id, event_type, payload, headers=None, occurred_at) -> None`

Insert an outbox row via `OutboxRecord.objects.create()`.

---

## `DjangoOutboxEventBus(outbox_repo)`

Synchronous event bus for Django services. Serializes dataclass domain events into outbox rows using the same `_serialize` helper as the async `OutboxEventBus`.

**Constructor Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `outbox_repo` | `DjangoOutboxRepository` | Repository instance |

**Methods:**

### `publish(event) -> None`

Serialize a single dataclass event and write it to the outbox (sync).

### `publish_many(events) -> None`

Serialize and write multiple events (sync).

---

## `DjangoOutboxRelay(messaging, *, subject_prefix, table_name="outbox", max_retries=10, base_backoff=5)`

Polls the outbox table using raw SQL and publishes rows via async messaging, bridging sync DB operations with async NATS publishing.

**Constructor Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `messaging` | any with `async publish(subject, data)` | *required* | Messaging client (e.g. `UnifiedMessaging`) |
| `subject_prefix` | `str` | *required* | Prefix for NATS subjects |
| `table_name` | `str` | `"outbox"` | Name of the outbox table |
| `max_retries` | `int` | `10` | Max attempts before marking `FAILED` |
| `base_backoff` | `int` | `5` | Base delay in seconds for exponential back-off |

**Methods:**

### `process_batch(batch_size=50) -> int`

Process up to `batch_size` pending outbox rows. Uses `FOR UPDATE SKIP LOCKED` for safe concurrent processing. Returns the number of rows successfully published.

---

## Django Integration

**Module:** `unimessaging.integrations.django`
**Import:** `from unimessaging.integrations.django import start_messaging, stop_messaging, get_messaging, get_broker`

### `async start_messaging(*, subjects, service_name, url="nats://localhost:4222", enable_durable=False, registry=None) -> UnifiedMessageBroker`

Create and start a broker, storing it at module level (Django has no `app.state`).

### `async stop_messaging() -> None`

Stop the module-level broker.

### `get_messaging() -> UnifiedMessaging | None`

Return the messaging client (or `None` if not started).

### `get_broker() -> UnifiedMessageBroker | None`

Return the broker instance (or `None` if not started).

---

### Expected Outbox Table Schema

The relay operates on rows with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `aggregate_type` | `str` | Used to build the subject: `{prefix}.{aggregate_type}` |
| `payload` | JSONB | Event payload, published as JSON bytes |
| `status` | `str` | `PENDING`, `PUBLISHED`, or `FAILED` |
| `retries` | `int` | Current retry count |
| `available_at` | `datetime` | Next eligible processing time |
| `published_at` | `datetime | None` | Set on successful publish |
| `last_error` | `str | None` | Last error message on failure |
