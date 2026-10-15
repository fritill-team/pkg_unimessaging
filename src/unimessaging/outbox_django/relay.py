"""Django outbox relay: polls the outbox table and publishes rows via messaging.

Uses raw SQL with ``connection.cursor()`` for ``FOR UPDATE SKIP LOCKED``
support, and bridges sync DB operations with async NATS publishing.

Run as a management command::

    python manage.py outbox_relay --subject-prefix reservations
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

try:
    from django.db import connection
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "Django is required for the outbox_django relay. "
        "Install it with: pip install unimessaging[django]"
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
LIMIT %s
FOR UPDATE SKIP LOCKED
"""

_FETCH_SQL_TEMPLATE = "SELECT * FROM {table} WHERE id = ANY(%s)"

_MARK_PUBLISHED_TEMPLATE = """
UPDATE {table}
SET status='PUBLISHED', published_at=now(), last_error=NULL
WHERE id=%s
"""

_MARK_RETRY_TEMPLATE = """
UPDATE {table}
SET status=%s, retries=%s, available_at=%s, last_error=%s
WHERE id=%s
"""


class DjangoOutboxRelay:
    """Polls outbox rows and publishes them to a messaging backend.

    Parameters
    ----------
    messaging:
        Any object with an async ``publish(subject, data, headers=...)`` method
        (e.g. ``UnifiedMessaging``).
    subject_prefix:
        Prefix prepended to ``aggregate_type`` to form the NATS subject.
        E.g. ``"reservations"`` -> ``"reservations.slot"``.
    table_name:
        Name of the outbox table.  Defaults to ``"outbox"``.
    max_retries:
        Max publish attempts before marking a row ``FAILED``.
    base_backoff:
        Base delay (seconds) for exponential back-off between retries.
    """

    def __init__(
        self,
        messaging,
        *,
        subject_prefix: str,
        table_name: str = DEFAULT_TABLE_NAME,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_backoff: int = DEFAULT_BASE_BACKOFF,
    ) -> None:
        self._messaging = messaging
        self._subject_prefix = subject_prefix
        self._max_retries = max_retries
        self._base_backoff = base_backoff

        self._lock_sql = _LOCK_SQL_TEMPLATE.format(table=table_name)
        self._fetch_sql = _FETCH_SQL_TEMPLATE.format(table=table_name)
        self._mark_published_sql = _MARK_PUBLISHED_TEMPLATE.format(table=table_name)
        self._mark_retry_sql = _MARK_RETRY_TEMPLATE.format(table=table_name)

    def _build_subject(self, aggregate_type: str) -> str:
        return f"{self._subject_prefix}.{aggregate_type}"

    def _build_headers(self, row) -> dict[str, str]:
        # The outbox row id is the JetStream de-duplication key (estate:AD-22).
        return {"Nats-Msg-Id": str(row["id"])}

    def process_batch(self, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
        """Process up to *batch_size* pending outbox rows.

        Sync DB queries with async NATS publish (bridged via the event loop).
        Returns the number of rows successfully published.
        """
        loop = asyncio.get_event_loop()

        with connection.cursor() as cursor:
            cursor.execute(self._lock_sql, [batch_size])
            ids = [row[0] for row in cursor.fetchall()]
            if not ids:
                return 0

            cursor.execute(self._fetch_sql, [ids])
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

            published = 0
            for r in rows:
                try:
                    subject = self._build_subject(r["aggregate_type"])
                    data = json.dumps(r["payload"]).encode()
                    loop.run_until_complete(
                        self._messaging.publish(
                            subject, data, headers=self._build_headers(r)
                        )
                    )
                    cursor.execute(self._mark_published_sql, [r["id"]])
                    published += 1
                except Exception as ex:
                    retries = r["retries"] + 1
                    delay = self._base_backoff * min(2 ** (retries - 1), 64)
                    next_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
                    new_status = (
                        "FAILED" if retries >= self._max_retries else "PENDING"
                    )
                    cursor.execute(
                        self._mark_retry_sql,
                        [new_status, retries, next_time, str(ex), r["id"]],
                    )
                    logger.warning(
                        "Outbox row %s publish failed (retry %d): %s",
                        r["id"], retries, ex,
                    )

            return published
