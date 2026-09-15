"""
FiniexDataCollector - Consumer Tokens
Where this project keeps its API tokens.

The part that stays here rather than in `finiex_auth`: which file holds the
credentials and which environment variable names them. The package never learns
either on purpose - a shared variable name would let one service's token
authenticate against another.

Two sources, in this order:
  1. `FINIEX_COLLECTOR_TOKENS` - `name:token` pairs, comma separated
  2. `api.tokens` in the configuration overlay

Empty is a valid state and not an error: with no token configured, every gated
route refuses. Access is granted by writing a name down, never by a default
nobody chose.

Location: python/api/token_loader.py
"""

import os
from typing import Any, Dict, Optional

from finiex_auth.consumer_token_base import ConsumerTokenBase
from finiex_auth.token_registry import TokenRegistry

ENV_VAR = "FINIEX_COLLECTOR_TOKENS"


class ConsumerToken(ConsumerTokenBase):
    """
    One consumer's credential, with this project's closed grant vocabulary.

    What the vocabulary catches, measured rather than assumed: the **surface** is
    validated at parse time, so `statsu:detail` is refused at boot. The name after
    the colon is not - `status:detial` parses and then denies at request time. So
    a surface typo cannot reach production; a name typo can, and reads as a
    permission problem.

    A bare `*` also parses. It is never written into this project's configuration.
    """
    GRANT_SURFACES = ("status", "config", "logs", "archive")


def load_token_registry(
    configured: Optional[Dict[str, Any]] = None
) -> TokenRegistry:
    """
    Build the registry of consumer tokens.

    Every token carries explicit grants. The package also accepts a bare
    `name -> token` string mapping, and that form grants `*` - every surface, for
    every consumer. Convenient and wrong here: a status token would silently hold
    whatever surface is added next. So the string form is not used.

    Args:
        configured: `name -> {token, grants, active, note}` from the overlay

    Returns:
        TokenRegistry, empty when nothing is configured

    Raises:
        ValidationError: If an entry names a surface this project does not have
    """
    raw = os.environ.get(ENV_VAR, "").strip()

    if raw:
        # `name:token` pairs, comma separated. A token supplied this way reaches
        # only the status surface - the environment carries no grant vocabulary,
        # and inventing a wider default here is how `*` gets in through a side door.
        tokens = {}
        for pair in raw.split(","):
            name, _, secret = pair.strip().partition(":")
            if name and secret:
                tokens[name] = ConsumerToken(
                    token=secret, grants=["status:detail"], note=f"from {ENV_VAR}")
        return TokenRegistry(tokens, source=ENV_VAR)

    if configured:
        return TokenRegistry(
            {name: ConsumerToken(**entry) for name, entry in configured.items()},
            source="user_configs")

    return TokenRegistry({}, source="none")
