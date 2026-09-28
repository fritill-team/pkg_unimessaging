# Outbox Relay

The outbox module provides reliable, at-least-once event publishing through the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html). Domain events are written to an `outbox` table within the same database transaction as the business data, then a background relay process picks them up and publishes to NATS.

## Installation

```bash
pip install unimessaging[outbox]
```

This adds `sqlalchemy[asyncio]>=2.0` as a dependency.

## Components

| Component | Responsibility |
|---|---|
| `OutboxMixin` | SQLAlchemy mixin defining the outbox table schema |
| `OutboxRepository` | Writes outbox rows within the caller's DB transaction |
| `OutboxEventBus` | Serializes dataclass events into outbox rows (generic, duck-typed) |
| `OutboxRelay` | Background poller that publishes pending rows to NATS |
| `relay_loop` | Infinite async loop wrapper for the relay |

## How It Works

```
Use Case ──→ OutboxEventBus.publish(event)
                     │
                     ▼
             OutboxRepository.add(...)  ← same DB transaction as domain write
                     │
                     ▼
              outbox table row (PENDING)
                     │
              OutboxRelay (background task)
                     │
                     ▼
              messaging.publish(subject, data)
                     │
              NATS subject: "{prefix}.{aggregate_type}"
```

1. Your use case calls `await bus.publish(event)`. The `OutboxEventBus` serializes the dataclass event and writes it to the outbox table via `OutboxRepository` — inside the same transaction as the domain write.
2. The `OutboxRelay` polls for `PENDING` rows with `FOR UPDATE SKIP LOCKED`, ensuring safe concurrent processing.
3. Each row is published to a NATS subject built from `subject_prefix` + `aggregate_type` (e.g. `"articles.event"`).
4. On success the row is marked `PUBLISHED`. On failure, retries increment with exponential back-off until `max_retries`, after which the row is marked `FAILED`.

## Setup Guide

### 1. Create the ORM model

Inherit `OutboxMixin` with your project's declarative `Base`:

```python
# infra/database/orm/outbox.py
from unimessaging.outbox import OutboxMixin
from .base import Base

class OutboxORM(OutboxMixin, Base):
    pass
```

Then generate the Alembic migration:

```bash
alembic revision --autogenerate -m "add outbox table"
alembic upgrade head
```

### 2. Wire the EventBus into your UoW

```python
# interface/common/uow.py
from unimessaging.outbox import OutboxRepository, OutboxEventBus
from infra.database.orm.outbox import OutboxORM

def _build_uow(session):
    outbox_repo = OutboxRepository(session, OutboxORM)
    event_bus = OutboxEventBus(outbox_repo)
    return YourUnitOfWork(session=session, event_bus=event_bus)
```

The `OutboxEventBus` satisfies any `EventBus` protocol with `async publish(event)` and `async publish_many(events)` via structural (duck) typing. No domain imports needed in the package.

### 3. Start the relay in your lifespan

```python
# infra/lifespan.py
import asyncio
from unimessaging.outbox import OutboxRelay, relay_loop
from unimessaging.integrations.fastapi import start_messaging, stop_messaging

@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_messaging(app, subjects=[...], service_name="my-service", url="nats://localhost:4222")

    messaging = app.state.messaging
    relay = OutboxRelay(
        session_factory=sessionmanager._sessionmaker,
        messaging=messaging,
        subject_prefix="articles",
    )
    relay_task = asyncio.create_task(relay_loop(relay))

    try:
        yield
    finally:
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        await stop_messaging(app)
```

### 4. Publish events from use cases

```python
async with uow:
    article = await uow.articles.save(article)
    await uow.event_bus.publish(ArticleCreated(aggregate_id=article.id, ...))
    # Both writes happen in the same transaction
```

## Configuration

```python
relay = OutboxRelay(
    session_factory=session_factory,
    messaging=messaging,
    subject_prefix="articles",      # required — builds "articles.{aggregate_type}"
    table_name="outbox",            # default: "outbox"
    max_retries=10,                 # default: 10 — FAILED after this many attempts
    base_backoff=5,                 # default: 5 seconds — exponential: 5, 10, 20, 40, ...
)

# Poll interval is configured on the loop
task = asyncio.create_task(relay_loop(relay, poll_interval=0.5))
```

### Running the relay as its own process

The relay needs only a session factory and a publisher, so it can run outside the web
process and scale separately from HTTP. The web pod reads the opt-out, and a separate
entrypoint runs the relay:

```python
# infra/lifespan.py: skip the in-process relay only when it runs elsewhere
from unimessaging.outbox import relay_in_process

if relay_in_process():   # OUTBOX_RELAY_IN_PROCESS, default true
    relay_task = asyncio.create_task(relay_loop(relay))

# interface/cli/relay.py: the standalone entrypoint
from unimessaging.outbox import run_standalone_relay

asyncio.run(run_standalone_relay(
    lambda messaging: ArticlesOutboxRelay(sessionmaker, messaging, subject_prefix="articles"),
    service_name="articles",
    url=settings.NATS_URL,
    enable_durable=True,
    stream_name=STREAM_NAME,            # same values the web pod passes;
    stream_subjects=STREAM_SUBJECTS,    # the stream is created if absent
    heartbeat_path="/tmp/outbox-relay.heartbeat",
))
```

- `OUTBOX_RELAY_IN_PROCESS=false` means "the relay runs elsewhere", never "off". Only
  `false`, `0`, `no`, `off` (case-insensitive) parse as false; anything unrecognised keeps
  the relay in-process and logs a warning.
- The runner's broker is publish-only: no subjects, no consumers, no handlers.
- SIGTERM/SIGINT cancels the loop, lets the in-flight batch roll back (its rows are claimed
  again later), stops the broker and returns. A row published but not yet marked is
  published again: delivery is at-least-once.
- Both relays may run at once (`SKIP LOCKED`), so cut over by starting the standalone relay
  first and only then setting the flag to `false` on the web pod.
- `relay_loop(..., on_tick=callback)` calls `callback` once per iteration; the runner uses
  it to touch `heartbeat_path`. Probe it with an exec liveness check:

```bash
python -m unimessaging.outbox.healthcheck --path /tmp/outbox-relay.heartbeat --max-age 30
```

It exits 1 when the heartbeat is missing or older than `--max-age` seconds, 0 otherwise.

### Processing a Single Batch

For testing or one-off processing, call `process_batch()` directly:

```python
count = await relay.process_batch(batch_size=100)
print(f"Published {count} events")
```

## Subject Routing

The relay builds NATS subjects as `{subject_prefix}.{aggregate_type}`:

| `subject_prefix` | `aggregate_type` | NATS Subject |
|-------------------|------------------|--------------|
| `"articles"` | `"event"` | `articles.event` |
| `"articles"` | `"therapy_session"` | `articles.therapy_session` |
| `"articles"` | `"article"` | `articles.article` |
| `"reservations"` | `"slot"` | `reservations.slot` |

Subscribers use standard NATS wildcard patterns:

```python
# Subscribe to all article events
register_handler("articles.*", handle_article_event)

# Subscribe to a specific kind
register_handler("articles.event", handle_event_only)
```

## Outbox Table Schema

The `OutboxMixin` defines this schema. You can also create it manually:

```sql
CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'PENDING',
    retries         INTEGER NOT NULL DEFAULT 0,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    occurred_at     TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX idx_outbox_pending ON outbox (available_at)
    WHERE status = 'PENDING';
```

## Retry Behavior

| Retry | Delay | Total Wait |
|-------|-------|------------|
| 1 | 5s | 5s |
| 2 | 10s | 15s |
| 3 | 20s | 35s |
| 4 | 40s | 75s |
| 5 | 80s | ~2.5min |
| 6 | 160s | ~5min |
| 7+ | 320s (capped) | ~10min+ |
| 10 | — | Marked `FAILED` |

## Logging

The relay uses Python's standard `logging` module under the logger name `unimessaging.outbox`. Configure it like any other logger:

```python
import logging
logging.getLogger("unimessaging.outbox").setLevel(logging.DEBUG)
```

## Django Setup

The package also provides a Django-native outbox module with sync Django ORM operations and a management command for running the relay.

### Installation

```bash
pip install unimessaging[django]
```

### Components

| Component | Responsibility |
|---|---|
| `OutboxRecord` | Django model defining the outbox table schema |
| `DjangoOutboxRepository` | Writes outbox rows within the caller's `transaction.atomic()` |
| `DjangoOutboxEventBus` | Sync event bus — serializes dataclass events into outbox rows |
| `DjangoOutboxRelay` | Sync DB polling + async NATS publish bridge |
| `outbox_relay` | Management command to run the relay as a process |

### 1. Add to INSTALLED_APPS

```python
# settings.py
INSTALLED_APPS = [
    ...,
    "unimessaging.outbox_django",
]
```

Then create and run migrations:

```bash
python manage.py makemigrations
python manage.py migrate
```

### 2. Wire the EventBus

```python
from django.db import transaction
from unimessaging.outbox_django import DjangoOutboxRepository, DjangoOutboxEventBus

repo = DjangoOutboxRepository()
bus = DjangoOutboxEventBus(repo)

with transaction.atomic():
    reservation.save()
    bus.publish(ReservationCreated(...))  # sync, same transaction
```

The `DjangoOutboxEventBus` satisfies any `EventBus` protocol with `publish(event)` and `publish_many(events)` via structural (duck) typing. It reuses the same serialization helpers as the async `OutboxEventBus`.

### 3. Run the relay

Start as a separate process via the management command:

```bash
python manage.py outbox_relay --subject-prefix reservations
python manage.py outbox_relay --subject-prefix reservations --nats-url nats://nats:4222 --poll-interval 1.0 --batch-size 100
```

**Command arguments:**

| Argument | Default | Description |
|---|---|---|
| `--subject-prefix` | *required* | Prefix for NATS subjects |
| `--nats-url` | `nats://localhost:4222` | NATS server URL |
| `--service-name` | `django-service` | Service name for the broker |
| `--poll-interval` | `0.5` | Seconds to sleep when idle |
| `--batch-size` | `50` | Max rows per batch |

### 4. Publish events from views / use cases

```python
with transaction.atomic():
    reservation.save()
    bus.publish(ReservationCreated(
        aggregate_type="reservation",
        aggregate_id=reservation.id,
        event_id=uuid4(),
        occurred_at=datetime.now(timezone.utc),
        ...
    ))
    # Both writes happen in the same transaction
```

### Django Configuration

```python
relay = DjangoOutboxRelay(
    messaging,
    subject_prefix="reservations",     # required — builds "reservations.{aggregate_type}"
    table_name="outbox",               # default: "outbox"
    max_retries=10,                    # default: 10 — FAILED after this many attempts
    base_backoff=5,                    # default: 5 seconds — exponential: 5, 10, 20, ...
)
```

---

## What Each Service Owns vs. What the Package Owns

| Owned by `unimessaging` | Owned by each service |
|---|---|
| `OutboxMixin` (table schema) | Concrete `OutboxORM(OutboxMixin, Base)` (2 lines) |
| `OutboxRepository` (generic write) | Alembic migration (auto-generated) |
| `OutboxEventBus` (generic serializer) | Domain event classes (`EventCreated`, etc.) |
| `OutboxRelay` + `relay_loop` | `EventBus` protocol in `domain/ports/` |
| `OutboxStatus` enum | UoW + lifespan wiring |

The package handles all outbox infrastructure. The service only defines its domain events and wires the shared session.
