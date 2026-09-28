"""Generic outbox relay: polls a PostgreSQL outbox table and publishes rows via messaging.

Requires the ``sqlalchemy`` optional extra::

    pip install unimessaging[outbox]

The outbox table is expected to have at least these columns:

    id, aggregate_type, payload (JSONB), status, retries, available_at,
    published_at, last_error

Rows with ``status='PENDING'`` and ``available_at <= now()`` are locked
(``FOR UPDATE SKIP LOCKED``) and published.  On success the row is marked
``PUBLISHED``; on failure retries are incremented with exponential back-off
up to ``max_retries`` before the row is marked ``FAILED``.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

try:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "SQLAlchemy is required for the outbox relay. "
        "Install it with: pip install unimessaging[outbox]"
    ) from _exc

logger = logging.getLogger("unimessaging.outbox")

DEFAULT_BATCH_SIZE = 50
DEFAULT_BASE_BACKOFF = 5  # seconds
DEFAULT_MAX_RETRIES = 10
DEFAULT_TABLE_NAME = "outbox"

_LOCK_SQL_TEMPLATE = """
SELECT id
FROM {table}
WHERE status = 'PENDING' AND available_at <= now()
ORDER BY available_at
LIMIT :batch
FOR UPDATE SKIP LOCKED
"""

_FETCH_SQL_TEMPLATE = "SELECT * FROM {table} WHERE id = ANY(:ids)"

_MARK_PUBLISHED_TEMPLATE = """
UPDATE {table}
SET status='PUBLISHED', published_at=now(), last_error=NULL
WHERE id=:id
"""

_MARK_RETRY_TEMPLATE = """
UPDATE {table}
SET status=:status, retries=:retries,
    available_at=:available_at, last_error=:err
WHERE id=:id
"""


class OutboxRelay:
    """Polls outbox rows and publishes them to a messaging backend.

    Parameters
    ----------
    session_factory:
        An ``async_sessionmaker`` used to create DB sessions.
    messaging:
        Any object with an async ``publish(subject, data)`` method
        (e.g. ``UnifiedMessaging``).
    subject_prefix:
        Prefix prepended to ``aggregate_type`` to form the NATS subject.
        E.g. ``"articles"`` → ``"articles.event"``, ``"articles.therapy_session"``.
    table_name:
        Name of the outbox table.  Defaults to ``"outbox"``.
    max_retries:
        Max publish attempts before marking a row ``FAILED``.
    base_backoff:
        Base delay (seconds) for exponential back-off between retries.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        messaging,
        *,
        subject_prefix: str,
        table_name: str = DEFAULT_TABLE_NAME,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_backoff: int = DEFAULT_BASE_BACKOFF,
    ) -> None:
        self._session_factory = session_factory
        self._messaging = messaging
        self._subject_prefix = subject_prefix
        self._max_retries = max_retries
        self._base_backoff = base_backoff

        self._lock_sql = text(_LOCK_SQL_TEMPLATE.format(table=table_name))
        self._fetch_sql = text(_FETCH_SQL_TEMPLATE.format(table=table_name))
        self._mark_published_sql = text(_MARK_PUBLISHED_TEMPLATE.format(table=table_name))
        self._mark_retry_sql = text(_MARK_RETRY_TEMPLATE.format(table=table_name))

    def _build_subject(self, aggregate_type: str) -> str:
        return f"{self._subject_prefix}.{aggregate_type}"

    async def process_batch(self, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
        """Process up to *batch_size* pending outbox rows.

        Returns the number of rows successfully published.
        """
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(self._lock_sql, {"batch": batch_size})
                ids = [row[0] for row in result.fetchall()]
                if not ids:
                    return 0

                rows_result = await session.execute(self._fetch_sql, {"ids": ids})
                rows = rows_result.mappings().all()

                published = 0
                for r in rows:
                    try:
                        subject = self._build_subject(r["aggregate_type"])
                        data = json.dumps(r["payload"]).encode()
                        await self._messaging.publish(subject, data)

                        await session.execute(
                            self._mark_published_sql, {"id": r["id"]}
                        )
                        published += 1
                    except Exception as ex:
                        retries = r["retries"] + 1
                        delay = self._base_backoff * min(2 ** (retries - 1), 64)
                        next_time = datetime.now(timezone.utc) + timedelta(seconds=delay)

                        new_status = (
                            "FAILED" if retries >= self._max_retries else "PENDING"
                        )
                        await session.execute(
                            self._mark_retry_sql,
                            {
                                "status": new_status,
                                "retries": retries,
                                "available_at": next_time,
                                "err": str(ex),
                                "id": r["id"],
                            },
                        )
                        logger.warning(
                            "Outbox row %s publish failed (retry %d): %s",
                            r["id"], retries, ex,
                        )

                return published


async def relay_loop(
    relay: OutboxRelay,
    *,
    poll_interval: float = 0.5,
    on_tick: Optional[Callable[[], object]] = None,
) -> None:
    """Run the relay in an infinite loop, sleeping when idle.

    Designed to be run as a background ``asyncio.Task``.  Cancel the task
    to stop the loop gracefully.

    *on_tick*, when given, is called once at the start of every iteration,
    idle or not (it may return an awaitable).  The standalone runner uses it
    to touch a liveness heartbeat.
    """
    while True:
        if on_tick is not None:
            # A failing tick must not stop publishing; a heartbeat that cannot
            # be written goes stale, and the probe reports that on its own.
            try:
                result = on_tick()
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox relay on_tick callback failed")
        try:
            count = await relay.process_batch()
            if count == 0:
                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Outbox relay unexpected error")
            await asyncio.sleep(poll_interval)
