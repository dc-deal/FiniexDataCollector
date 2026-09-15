"""
FiniexDataCollector - Redaction
Removes credentials from anything the API is about to answer with.

The configuration route exists to answer "what settings is it actually running",
and the honest answer contains a Telegram bot token and every consumer token this
API accepts. Serving those would turn a diagnostic into a credential leak, and the
route that leaks is the one nobody reviews again after it works.

Redaction is by key name, applied recursively, rather than by a list of paths to
hide. A path list is a promise about today's configuration shape: add a section
with a secret in it and the list is silently wrong. A name rule covers a key that
did not exist when it was written, which is the case that matters.

The cost is the opposite error - a harmless key called `market_key` would be
redacted for nothing. That trade is deliberate: an over-redacted diagnostic is an
annoyance, an under-redacted one is an incident.

Location: python/api/redaction.py
"""

from typing import Any, Dict

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
