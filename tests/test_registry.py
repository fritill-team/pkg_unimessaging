"""HandlerRegistry matches NATS subjects with NATS wildcard semantics.

Regression: dispatch previously used fnmatch, where `>` is a literal and `*` crosses token
boundaries, so a consumer bound on `notifications.trigger.>` never resolved a handler for a
delivered subject like `notifications.trigger.cohort_provider_assigned` (message dropped).
"""

from __future__ import annotations

import pytest

from unimessaging.broker.registry import HandlerRegistry


@pytest.mark.parametrize(
    "pattern,subject,should_match",
    [
        # `>` — tail wildcard, one or more tokens.
        ("notifications.trigger.>", "notifications.trigger.cohort_provider_assigned", True),
        ("notifications.trigger.>", "notifications.trigger.a.b.c", True),
        ("notifications.trigger.>", "notifications.trigger", False),  # `>` needs >=1 token
        ("notifications.cmd.email.>", "notifications.cmd.email.welcome", True),
        ("notifications.cmd.email.>", "notifications.cmd.sms.welcome", False),
        # `*` — exactly one token, does not cross dots.
        ("notifications.registry.*", "notifications.registry.courses", True),
        ("notifications.registry.*", "notifications.registry.result.courses", False),
        # literal subjects match exactly.
        ("auth.UserRegistered", "auth.UserRegistered", True),
        ("auth.UserRegistered", "auth.UserRegisteredX", False),
    ],
)
def test_nats_wildcard_matching(pattern, subject, should_match):
    registry = HandlerRegistry()
    registry.register_handler(pattern, lambda *_: "hit")
    resolved = registry.resolve_handler(subject)
    assert (resolved is not None) is should_match


def test_first_registered_match_wins():
    registry = HandlerRegistry()
    registry.register_handler("notifications.trigger.>", lambda *_: "broad")
    handler = registry.resolve_handler("notifications.trigger.welcome")
    assert handler is not None and handler() == "broad"
