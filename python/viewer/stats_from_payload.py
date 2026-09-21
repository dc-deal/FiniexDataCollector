"""
FiniexDataCollector - Rebuilding the statistics a viewer draws
Turns a `/v1/status` payload back into the CollectorStats object the renderer reads.

This is the counterpart of `python/api/stats_serializer.py`, and the pair is only
worth having if both halves stay structural rather than enumerated: a field added
to the statistics has to reach a remote screen without anyone editing a converter.
A converter that lists its fields is how a status screen ends up describing last
month's collector while looking perfectly current.

Everything is rebuilt from the type of the value `CollectorStats.__init__` put
there. That fails for exactly one group - the fields a fresh object leaves empty
or `None`, which therefore cannot say what belongs inside them - and those are
named in `CONTENT_TYPES`. A test fails when a new field joins that group without
an entry here.

A payload this file cannot read raises `PayloadMismatch` rather than returning a
half-filled object. The viewer then says so on screen. Drawing whatever happened
to parse would be the same defect this project refuses in its files: a value
asserting more than it knows.

Location: python/viewer/stats_from_payload.py
"""

from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from typing import (Any, Dict, List, Type, Union, get_args, get_origin,
                    get_type_hints)

from python.types.collector_stats import (CollectorStats, FileInfo,
                                          FolderStats, LogEntry,
                                          ReconnectEvent, StallEvent,
                                          SymbolStats)

# The fields a freshly constructed CollectorStats leaves as `[]`, `{}` or `None`.
# The object carries no type there, so the mapping has to be stated. Everything
# else - the nested dataclasses and the scalars - is inferred from what the
# constructor put in place, which is what lets a new measurement appear on a
# remote screen with no edit to this file.
CONTENT_TYPES: Dict[str, type] = {
    "symbols": SymbolStats,
    "folders": FolderStats,
    "recent_logs": LogEntry,
    "reconnect_events": ReconnectEvent,
    "stalls": StallEvent,
    "last_file": FileInfo,
    "last_reconnect": ReconnectEvent,
    "streams": str,
}


class PayloadMismatch(ValueError):
    """The payload does not describe the statistics this build knows."""


def stats_from_payload(payload: Dict[str, Any]) -> CollectorStats:
    """
    Rebuild the statistics object a renderer reads.

    Keys the payload carries and this build does not know are ignored, so a newer
    collector never breaks an older viewer. The reverse - a field this build needs
    and the payload does not carry - raises, because that one cannot be drawn.

    Args:
        payload: The decoded body of `/v1/status`

    Returns:
        A CollectorStats object holding what the collector reported

    Raises:
        PayloadMismatch: The payload could not be read into this build's shape
    """
    if not isinstance(payload, dict):
        raise PayloadMismatch(
            f"expected a JSON object, got {type(payload).__name__}")

    stats = CollectorStats()

    for name, current in vars(stats).items():
        if name.startswith("_") or name not in payload:
            continue
        try:
            setattr(stats, name, _revive_member(name, current, payload[name]))
        except PayloadMismatch:
            raise
        except Exception as error:
            raise PayloadMismatch(
                f"{name}: {type(error).__name__}: {error}") from error

    return stats


def _revive_member(name: str, current: Any, value: Any) -> Any:
    """
    Rebuild one top-level member of the statistics.

    Args:
        name: Attribute name, used to look up a content type when needed
        current: What the constructor put there, which carries the type
        value: The value the payload holds for it

    Returns:
        The member in the shape the renderer expects
    """
    if isinstance(current, list):
        return [_revive(item, _content_type(name)) for item in value or []]

    if isinstance(current, dict):
        return {str(key): _revive(item, _content_type(name))
                for key, item in (value or {}).items()}

    if current is None:
        return _revive(value, _content_type(name)) if value is not None else None

    if is_dataclass(current):
        return _revive(value, type(current))

    if isinstance(current, datetime):
        return _parse_time(value)

    return value


def _content_type(name: str) -> type:
    """
    The type that belongs inside a container the constructor left empty.

    Args:
        name: Attribute name on CollectorStats

    Returns:
        The element type

    Raises:
        PayloadMismatch: No content type is registered for that field
    """
    try:
        return CONTENT_TYPES[name]
    except KeyError:
        raise PayloadMismatch(
            f"no content type registered for {name} - add it to CONTENT_TYPES")


def _revive(value: Any, hint: Any) -> Any:
    """
    Turn one JSON value back into what its annotation says it is.

    Args:
        value: JSON-native value
        hint: The declared type it should become

    Returns:
        The reconstructed value
    """
    if value is None:
        return None

    hint = _without_none(hint)

    if hint is datetime:
        return _parse_time(value)

    origin = get_origin(hint)
    if origin in (list, tuple):
        inner = get_args(hint)
        return [_revive(item, inner[0] if inner else Any) for item in value]

    if origin is dict:
        inner = get_args(hint)
        return {str(key): _revive(item, inner[1] if len(inner) > 1 else Any)
                for key, item in value.items()}

    if is_dataclass(hint) and isinstance(hint, type):
        return _revive_dataclass(hint, value)

    return value


def _revive_dataclass(cls: Type[Any], raw: Any) -> Any:
    """
    Rebuild one dataclass from its dict.

    Keys the class does not declare are dropped rather than passed on: the
    serializer deliberately adds computed values next to the stored ones
    (`disk_space.free_gb` beside `free_bytes`), and those are properties here.

    Args:
        cls: The dataclass to build
        raw: Its dict from the payload

    Returns:
        An instance of cls

    Raises:
        PayloadMismatch: A field the class requires is missing
    """
    if not isinstance(raw, dict):
        raise PayloadMismatch(
            f"{cls.__name__}: expected an object, got {type(raw).__name__}")

    hints = get_type_hints(cls)
    known = {member.name: raw[member.name]
             for member in fields(cls) if member.name in raw}

    try:
        return cls(**{name: _revive(value, hints.get(name, Any))
                      for name, value in known.items()})
    except TypeError as error:
        # A required field the payload does not carry. Naming the class is what
        # turns "the viewer is broken" into "that collector is older than this
        # viewer, and here is the field it no longer sends".
        raise PayloadMismatch(f"{cls.__name__}: {error}") from error


def _without_none(hint: Any) -> Any:
    """
    Strip the `None` out of an `Optional[...]`.

    Args:
        hint: A type annotation

    Returns:
        The annotation without NoneType, unchanged when it is not a Union
    """
    if get_origin(hint) is Union:
        remaining = [arg for arg in get_args(hint) if arg is not type(None)]
        if len(remaining) == 1:
            return remaining[0]
    return hint


def _parse_time(value: Any) -> datetime:
    """
    Read a timestamp the serializer wrote.

    It writes UTC with an offset, always. A value without one would read as local
    time on the viewer's machine, which is the failure this project has already
    paid for once - so an absent offset is called UTC rather than guessed at.

    Args:
        value: ISO-8601 string

    Returns:
        A timezone-aware datetime in UTC

    Raises:
        PayloadMismatch: The value is not a timestamp
    """
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as error:
        raise PayloadMismatch(f"not a timestamp: {value!r}") from error

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def untyped_fields() -> List[str]:
    """
    The CollectorStats members whose content type a fresh object cannot state.

    Used by the test that keeps `CONTENT_TYPES` complete: every name listed here
    needs an entry, so a field added to the statistics fails a test rather than a
    screen in front of the operator.

    Returns:
        Attribute names that are empty or None on a freshly built object
    """
    empty = ([], {}, None)
    return [name for name, value in vars(CollectorStats()).items()
            if not name.startswith("_")
            and any(value is candidate or value == candidate
                    for candidate in empty if type(value) is type(candidate))]
