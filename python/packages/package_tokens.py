"""
FiniexDataCollector - Package Tokens
The package service's own grant vocabulary.

Separate from the collector's on purpose. The collector validates every grant
against its own surfaces at boot, so a `packages:*` written into the collector's
overlay would make a collector that restarts - after a host reboot, inside a freeze
- refuse to start. The package service keeps its tokens in its own overlay, and the
two vocabularies never meet.

Location: python/packages/package_tokens.py
"""

from typing import Any, Dict, Optional

from finiex_auth.consumer_token_base import ConsumerTokenBase
from finiex_auth.token_registry import TokenRegistry


class PackageToken(ConsumerTokenBase):
    """
    One consumer's credential for the package service.

    `packages` reads: the list, a manifest, a zip. `receipts` writes the one thing
    this service accepts from outside, an acknowledgement - a separate surface, so
    a token can fetch packages without being able to mark them received.
    """
    GRANT_SURFACES = ("packages", "receipts")


def load_package_tokens(configured: Optional[Dict[str, Any]] = None) -> TokenRegistry:
    """
    Build the registry of package consumers.

    No environment variable: the collector's `FINIEX_COLLECTOR_TOKENS` grants
    `status:detail`, which means nothing here, and a second variable would be a
    second place a token could hide.

    Args:
        configured: `name -> {token, grants, active, note}` from `packages.tokens`

    Returns:
        TokenRegistry, empty when nothing is configured - every gated route refuses

    Raises:
        ValidationError: If an entry names a surface this service does not have
    """
    if configured:
        return TokenRegistry(
            {name: PackageToken(**entry)
             for name, entry in configured.items()},
            source="user_configs")
    return TokenRegistry({}, source="none")
