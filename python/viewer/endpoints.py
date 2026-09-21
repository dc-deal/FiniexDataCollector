"""
FiniexDataCollector - Which collector a viewer points at
Reads the base URL and credential for a named instance out of the overlay.

`user_configs/remote_endpoints.json` already held these so that a session did not
have to be told the URL again. The viewer is the first program to read it, which
turns a note into configuration - so the failure modes get named here rather than
surfacing as a traceback in front of somebody who just wanted a screen.

One token per consumer is the rule this file serves. The viewer's entry should
carry `status:detail` and nothing else: it runs on a desk all day, where a token
that can also fetch archive files is a credential left lying in a window.

Never print a token. Every error here names the file, the entry and the missing
key, and never the value.

Location: python/viewer/endpoints.py
"""

import json
from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse

DEFAULT_CONFIG = Path("user_configs/remote_endpoints.json")

# Loopback is free; a request per second through a TLS proxy from a laptop is a
# request per second somebody pays for. Both are small, and the difference is
# worth not having to think about again.
LOOPBACK_INTERVAL_SECONDS = 1.0
REMOTE_INTERVAL_SECONDS = 2.0

LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1", "[::1]")


class EndpointError(ValueError):
    """The endpoint a viewer was pointed at cannot be used."""


def load_endpoint(name: str, path: Path = DEFAULT_CONFIG) -> Tuple[str, str]:
    """
    Read one named endpoint.

    Args:
        name: Key under `endpoints`, for instance `live` or `local`
        path: The overlay file to read

    Returns:
        (base_url, token)

    Raises:
        EndpointError: The file, the entry or a required key is missing
    """
    if not path.exists():
        raise EndpointError(
            f"{path} does not exist - copy remote_endpoints.example.json and "
            f"fill in the instance this viewer should watch")

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise EndpointError(f"{path} is not valid JSON: {error}") from error

    endpoints = document.get("endpoints")
    if not isinstance(endpoints, dict):
        raise EndpointError(f"{path} carries no 'endpoints' object")

    entry = endpoints.get(name)
    if entry is None:
        known = ", ".join(sorted(endpoints)) or "none"
        raise EndpointError(
            f"{path} has no endpoint '{name}' - it knows: {known}")

    base_url = entry.get("base_url")
    token = entry.get("token")
    if not base_url:
        raise EndpointError(f"endpoint '{name}' has no base_url")
    if not token:
        raise EndpointError(f"endpoint '{name}' has no token")

    return str(base_url), str(token)


def default_interval(base_url: str) -> float:
    """
    How often to read, decided by where the collector is.

    Args:
        base_url: The collector's base URL

    Returns:
        Seconds between readings
    """
    host = (urlparse(base_url).hostname or "").lower()
    return (LOOPBACK_INTERVAL_SECONDS if host in LOOPBACK_HOSTS
            else REMOTE_INTERVAL_SECONDS)
