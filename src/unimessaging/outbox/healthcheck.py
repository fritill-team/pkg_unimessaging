"""Exec liveness probe for a standalone outbox relay.

A standalone relay has no HTTP port, so its Deployment probes the heartbeat
file that :func:`run_standalone_relay` touches once per loop iteration::

    python -m unimessaging.outbox.healthcheck --path /tmp/outbox-relay.heartbeat --max-age 30

Exits 0 when the file was touched within ``--max-age`` seconds, 1 when it is
missing or older.  Standard library only.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Optional, Sequence


def heartbeat_age(path: str, now: Optional[float] = None) -> Optional[float]:
    """Seconds since *path* was last touched, or ``None`` if it does not exist."""
    try:
        mtime = os.stat(path).st_mtime
    except FileNotFoundError:
        return None
    return (time.time() if now is None else now) - mtime


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m unimessaging.outbox.healthcheck",
        description="Fail when the outbox relay heartbeat is missing or stale.",
    )
    parser.add_argument("--path", required=True, help="heartbeat file")
    parser.add_argument(
        "--max-age", required=True, type=float, help="maximum age in seconds"
    )
    args = parser.parse_args(argv)
    if args.max_age <= 0:
        parser.error("--max-age must be positive")

    age = heartbeat_age(args.path)
    if age is None:
        print(f"outbox relay heartbeat missing: {args.path}", file=sys.stderr)
        return 1
    if age > args.max_age:
        print(
            f"outbox relay heartbeat stale: {args.path} is {age:.1f}s old "
            f"(max {args.max_age:g}s)",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
