"""
FiniexDataCollector - Redaction
Removes credentials from anything the API is about to answer with.

The configuration route exists to answer "what settings is it actually running",
and the honest answer contains a Telegram bot token and every consumer token this
API accepts. Serving those would turn a diagnostic into a credential leak, and the
route that leaks is the one nobody reviews again after it works.

Two nets, and they catch different things.

**By key name**, recursively, rather than by a list of paths to hide. A path list is
a promise about today's configuration shape: add a section with a secret in it and
the list is silently wrong. A name rule covers a key that did not exist when it was
written, which is the case that matters. The cost is the opposite error - a harmless
key called `market_key` is redacted for nothing. An over-redacted diagnostic is an
annoyance; an under-redacted one is an incident.

**By shape**, using `finiex_auth.redaction` - the vocabulary shared with the sister
projects, which recognises a bearer token, a DSN password or a bot token wherever it
sits inside a string. That is deliberately not reimplemented here: a second copy of a
security vocabulary is worse than none, because the copy nobody updates is the one
that leaks and nothing fails when they drift.

The key rule cannot see a secret that reached a value by accident, and the shape rule
cannot know that `chat_id` is private. Hence both.

Location: python/api/redaction.py
"""

from typing import Any, Dict, Tuple

from finiex_auth.redaction import redact as redact_text

REDACTED = "<redacted>"

# Substrings that make a key a secret, matched case-insensitively against the key
# name alone - never against the value, which would make redaction depend on what
# happens to be stored.
SECRET_MARKERS = ("token", "secret", "password", "passwd", "credential", "apikey")

# Not a secret, but it identifies a person's chat and has no diagnostic use.
ALSO_REDACT = ("chat_id",)


def is_secret_key(key: str) -> bool:
    """
    Decide whether a configuration key must not be served.

    Args:
        key: The key name

    Returns:
        True when its value has to be replaced
    """
    lowered = key.lower()
    return (any(marker in lowered for marker in SECRET_MARKERS)
            or lowered in ALSO_REDACT)


def redact(value: Any) -> Any:
    """
    Copy a structure with every secret value replaced.

    A dict whose KEY is a secret is replaced whole rather than walked: `tokens`
    maps consumer names to objects that carry the secret one level down, and
    walking it would serve the names of everyone holding a credential.

    Args:
        value: Any part of the configuration tree

    Returns:
        The same structure, without secrets
    """
    if isinstance(value, dict):
        return {
            key: (REDACTED if is_secret_key(key) else redact(inner))
            for key, inner in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [redact(inner) for inner in value]

    if isinstance(value, str):
        masked, _ = redact_text(value)
        return masked

    return value


def redact_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare a configuration document for the API.

    Args:
        config: The effective configuration

    Returns:
        A copy safe to serve
    """
    return redact(config)


def redact_line(text: str) -> Tuple[str, bool]:
    """
    Mask anything credential-shaped in one line of free text.

    Log lines are not structured, so the key rule has nothing to work with - what
    reaches them is a bearer token in a traceback or a bot token inside a URL.

    The boolean is returned rather than swallowed because the package asks every
    caller to surface it: a reader trusts a diagnostic surface, so a line that was
    altered without saying so is worse than one that was withheld.

    Args:
        text: One log line

    Returns:
        Tuple of the masked line and whether anything was masked
    """
    return redact_text(text)
