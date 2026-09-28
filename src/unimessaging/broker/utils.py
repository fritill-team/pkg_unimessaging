from __future__ import annotations

import os
import uuid
from typing import Dict, List, Optional

from .config import JetStreamConsumer, MessagingConfig


def create_messaging_client(
    service_name: str,
    *,
    url: Optional[str] = None,
    enable_durable: bool = False,
    stream_name: Optional[str] = None,
    stream_subjects: Optional[List[str]] = None,
    consumers: Optional[List[JetStreamConsumer]] = None,
    pull_batch: int = 10,
    pull_timeout: float = 1.0,
    user: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    creds_file: Optional[str] = None,
) -> "UnifiedMessaging":
    from .client import UnifiedMessaging

    cfg = MessagingConfig(
        backend="nats",
        url=url or os.getenv("NATS_URL", "nats://localhost:4222"),
        name=service_name,
        enable_durable=enable_durable,
        stream_name=stream_name,
        stream_subjects=list(stream_subjects or []),
        consumers=consumers or [],
        pull_batch=pull_batch,
        pull_timeout=pull_timeout,
        default_headers={"service": service_name},
        user=_arg_or_env(user, "NATS_USER"),
        password=_arg_or_env(password, "NATS_PASSWORD"),
        token=_arg_or_env(token, "NATS_TOKEN"),
        creds_file=_arg_or_env(creds_file, "NATS_CREDS_FILE"),
    )
    return UnifiedMessaging(cfg)


def _arg_or_env(value: Optional[str], env_name: str) -> Optional[str]:
    # An empty env var counts as unset, so a blank Secret key cannot send an
    # empty password.
    if value is not None:
        return value
    return os.getenv(env_name) or None


def prepare_notification_payload(event_type: str, payload: dict) -> dict:
    return {"event": event_type, "payload": payload}


def build_notification_headers(service_name: str) -> Dict[str, str]:
    return {"trace_id": str(uuid.uuid4()), "service": service_name}