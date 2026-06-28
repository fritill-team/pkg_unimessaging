from __future__ import annotations

import re
from typing import Callable, Dict, Optional, Pattern


def _nats_to_regex(pattern: str) -> Pattern[str]:
    """Compile a NATS subject pattern into a regex with NATS wildcard semantics.

    The registry matches NATS subjects, so it must use NATS rules — not fnmatch globbing,
    where `>` is a literal and `*` crosses token boundaries:

    * ``*`` matches exactly one token (no dots).
    * ``>`` is a tail wildcard (valid only as the final token); matches one or more tokens.
    * every other token matches literally.
    """
    parts: list[str] = []
    for token in pattern.split("."):
        if token == ">":
            parts.append(r".+")
            break  # `>` must be terminal in NATS; ignore anything after it.
        if token == "*":
            parts.append(r"[^.]+")
        else:
            parts.append(re.escape(token))
    return re.compile("^" + r"\.".join(parts) + "$")


class HandlerRegistry:
    """Instance-based handler registry for test isolation."""

    def __init__(self) -> None:
        self._handlers: Dict[str, Callable] = {}
        self._compiled: Dict[str, Pattern[str]] = {}
        self._rpc_handlers: Dict[str, Callable] = {}

    def register_handler(self, pattern: str, handler: Callable) -> None:
        self._handlers[pattern] = handler
        self._compiled[pattern] = _nats_to_regex(pattern)

    def register_rpc(self, subject: str, handler: Callable) -> None:
        self._rpc_handlers[subject] = handler

    def resolve_handler(self, subject: str) -> Optional[Callable]:
        for pattern, h in self._handlers.items():
            if self._compiled[pattern].match(subject):
                return h
        return None

    def resolve_rpc_handler(self, subject: str) -> Optional[Callable]:
        return self._rpc_handlers.get(subject)


# Module-level convenience functions (delegate to a default instance).
_default_registry = HandlerRegistry()
register_handler = _default_registry.register_handler
register_rpc = _default_registry.register_rpc
resolve_handler = _default_registry.resolve_handler
resolve_rpc_handler = _default_registry.resolve_rpc_handler