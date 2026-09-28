"""Mask credentials embedded in a NATS URL before it reaches a log line."""

from __future__ import annotations

from urllib.parse import urlsplit

MASK = "***"


def _mask_entry(entry: str) -> str:
    # A scheme-less entry ("u:p@host:4222") would otherwise parse "u" as the
    # scheme and hide the userinfo in the path.
    candidate = entry if "://" in entry else "//" + entry
    try:
        netloc = urlsplit(candidate).netloc
    except ValueError:
        # Malformed (e.g. an unbalanced IPv6 bracket): never let the log line
        # raise, and never let anything before the last "@" through.
        head, sep, tail = entry.rpartition("@")
        if not sep:
            return entry
        scheme, sep_s, _ = head.partition("://")
        return f"{scheme}://{MASK}@{tail}" if sep_s else f"{MASK}@{tail}"
    userinfo, sep, hostport = netloc.rpartition("@")
    if not sep:
        return entry
    user, colon, _ = userinfo.partition(":")
    # No colon means the whole userinfo is a token, i.e. the secret itself.
    masked = f"{user}:{MASK}" if colon else MASK
    result = candidate.replace(f"//{netloc}", f"//{masked}@{hostport}", 1)
    return result if candidate is entry else result[2:]


def mask_url(url: str) -> str:
    """Return ``url`` with every password or token replaced by ``***``.

    Comma-separated server lists are masked entry by entry, because
    ``urllib.parse`` treats the whole list as one URL and would leave every
    entry after the first untouched.
    """
    if not url or "@" not in url:
        return url
    return ",".join(_mask_entry(entry) for entry in url.split(","))
