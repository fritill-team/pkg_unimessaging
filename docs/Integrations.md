# Integrations

The integrations layer wires use cases with adapters and exposes consumer-friendly APIs. It lives under `src/unimessaging/integrations/`.

## The `send_message` Facade

**Module:** `unimessaging.integrations.common.facade`
**Also exported from:** `unimessaging` (top-level)

This is the primary entry point for simple notification use cases.

### Signature

```python
def send_message(
    message: str,
    recipient: str | None = None,
    *,
    gateway: NotificationGateway | None = None,
) -> dict:
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `message` | `str` | (required) | The message content |
| `recipient` | `str \| None` | `None` | The target recipient. **Required** -- raises `ValueError` if `None`. |
| `gateway` | `NotificationGateway \| None` | `None` | Optional custom gateway. Defaults to `InMemoryNotificationGateway`. |

### Return Value

```python
{
    "status": "sent",
    "payload": {
        "message": "...",
        "to": "..."
    }
}
```

The `payload` dict contents depend on the gateway implementation. The NATS gateway adds a `"subject"` key.

### How It Works

1. If no `gateway` is provided, uses a cached `InMemoryNotificationGateway` singleton
2. If a `gateway` is provided, creates a fresh `SendMessageUseCase` with that gateway
3. Constructs a `SendMessageRequest` DTO
4. Calls `use_case.execute(request)`
5. Unwraps the `SendMessageResponse` into a plain dict

### Caching

The default use case (no custom gateway) is cached with `@lru_cache(maxsize=1)`. This avoids reconstructing the gateway and use case on every call.

When a custom gateway is passed, a new use case is created each time -- no caching.

### Examples

Default gateway:

```python
from unimessaging import send_message

send_message("hello", "alice")
```

Custom gateway:

```python
from unimessaging import send_message
from unimessaging.adapters import NATSNotificationGateway, NATSConfig

nats_gw = NATSNotificationGateway(NATSConfig(subject="notifications.orders"))
send_message("Order shipped", "user-42", gateway=nats_gw)
```

### Errors

| Error | When |
|-------|------|
| `ValueError("recipient is required")` | `recipient` is `None` |
| `InvalidMessageError` | Empty/whitespace `message` or `recipient` |
| `RuntimeError` | NATS gateway called inside an active event loop |

---

## FastAPI Integration

**Module:** `unimessaging.integrations.fastapi`
**Extra:** `pip install unimessaging[fastapi]`

Provides lifespan helpers that manage the messaging broker lifecycle and attach it to FastAPI's `app.state`.

### `start_messaging()`

```python
async def start_messaging(
    app: FastAPI,
    *,
    subjects: list[str],
    service_name: str,
    url: str = "nats://localhost:4222",
    enable_durable: bool = False,
    registry: HandlerRegistry | None = None,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    creds_file: str | None = None,
) -> UnifiedMessageBroker
```

Creates and starts a `UnifiedMessageBroker`, then attaches it to `app.state`:
- `app.state.messaging_broker` — the full broker instance
- `app.state.messaging` — the `UnifiedMessaging` client facade

### `stop_messaging()`

```python
async def stop_messaging(app: FastAPI) -> None
```

Stops the broker and cleans up `app.state`.

### Usage

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from unimessaging.integrations.fastapi import start_messaging, stop_messaging

@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_messaging(
        app,
        subjects=["notifications.>"],
        service_name="my-service",
        url="nats://localhost:4222",
    )
    try:
        yield
    finally:
        await stop_messaging(app)

app = FastAPI(lifespan=lifespan)
```

See [FastAPI Example](FastAPI-Example) for a full walkthrough.

### NATS credentials

Both integrations, `UnifiedMessageBroker` and `create_messaging_client` accept
`user`, `password`, `token` and `creds_file`. Each reaches the NATS connection
only when set, so a service that passes none of them connects exactly as before.
Any left unset falls back to an env var; an empty value counts as unset:

| Argument | Env var |
|----------|---------|
| `user` | `NATS_USER` |
| `password` | `NATS_PASSWORD` |
| `token` | `NATS_TOKEN` |
| `creds_file` | `NATS_CREDS_FILE` (passed to nats-py as `user_credentials`) |

With several set, nats-py decides (2.13.1): creds file, then user+password,
then token, then credentials embedded in the URL. Keep `url` credential-free
once the credentials move out of it. URLs are masked in every log line either way.

A server `Permissions Violation` is logged at ERROR as `NATS permission
violation`; other async errors stay at WARNING.

---

## Django Integration

**Module:** `unimessaging.integrations.django`
**Extra:** `pip install unimessaging[django]`

Provides the same broker lifecycle helpers as the FastAPI integration, but uses module-level state instead of `app.state` (since Django has no equivalent).

### `start_messaging()`

```python
async def start_messaging(
    *,
    subjects: list[str],
    service_name: str,
    url: str = "nats://localhost:4222",
    enable_durable: bool = False,
    registry: HandlerRegistry | None = None,
    user: str | None = None,
    password: str | None = None,
    token: str | None = None,
    creds_file: str | None = None,
) -> UnifiedMessageBroker
```

Creates and starts a `UnifiedMessageBroker`, storing it at module level.

### `stop_messaging()`

```python
async def stop_messaging() -> None
```

Stops the module-level broker and clears state.

### `get_messaging()`

```python
def get_messaging() -> UnifiedMessaging | None
```

Returns the `UnifiedMessaging` client, or `None` if the broker hasn't been started.

### `get_broker()`

```python
def get_broker() -> UnifiedMessageBroker | None
```

Returns the broker instance, or `None` if not started.

### Usage

```python
import asyncio
from unimessaging.integrations.django import start_messaging, stop_messaging, get_messaging

# At startup (e.g. AppConfig.ready() or a management command):
loop = asyncio.get_event_loop()
loop.run_until_complete(start_messaging(
    subjects=["reservations.>"],
    service_name="reservations-service",
    url="nats://localhost:4222",
))

# In views or use cases:
messaging = get_messaging()
```

### Differences from FastAPI

| Aspect | FastAPI | Django |
|--------|---------|--------|
| State storage | `app.state.messaging` | Module-level `_client` |
| First argument | `app: FastAPI` (required) | None (no app object) |
| Access pattern | `request.app.state.messaging` | `get_messaging()` |
| Lifecycle | Async lifespan context manager | Manual start/stop calls |

See [Django Integration](Django-Integration) for a full guide including the outbox relay management command.
