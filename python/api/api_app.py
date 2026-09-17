"""
FiniexDataCollector - Status API
Three routes: is it alive, what code is it running, and what is it collecting.

The collector produces files somebody else consumes, and until now the only way to
learn whether it was running - and which version - was to look at the machine. On
2026-09-15 that inference had to be made from process uptime against commit
timestamps, which is the kind of answer that is right until it quietly is not.

The process binds loopback and never speaks to the internet directly; a reverse
proxy terminates TLS and forwards to it. The port gets no firewall rule.

Which routes are open, and why each one is:
  - `/v1/health`  - an uptime probe needs it without a credential. It answers
    liveness and uptime only; what is being collected is not operational trivia.
  - `/v1/build`   - the repository is public, so a commit hash discloses nothing
    that is not already readable on GitHub. Behind a private repository the same
    field would fingerprint the exact version and its known defects.
  - `/v1/status`  - gated on `status:detail`. Symbol names, tick rates and buffer
    depths describe what is being traded and how much of it, which is not a thing
    an uptime probe needs.

Location: python/api/api_app.py
"""

from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import FileResponse
from starlette.middleware.gzip import GZipMiddleware
from finiex_auth.bearer_auth import build_bearer_dependency
from finiex_auth.grant_auth import build_grant_dependency
from finiex_auth.token_registry import TokenRegistry

from python.api.archive_reader import read_archive
from python.api.build_info import BuildInfo
from python.api.file_server import resolve_archive_file
from python.api.log_reader import MAX_LINES, available_days, read_log
from python.api.redaction import redact_config
from python.types.tick_types import OriginBlock

SURFACE = "status"

# Tick JSON is the same keys on every line, so it compresses extraordinarily well:
# measured 22x on a real 433 KB file, in 3 ms. Level 9 reaches 24.8x for 8 ms and
# lzma 30.7x for 41 ms - neither is worth the CPU on a four-core box shared with
# two other services, so the default level is lowered rather than kept.
COMPRESS_LEVEL = 6

# Below this, the gzip header and the round trip cost more than they save. /v1/health
# is roughly 110 bytes and has nothing to gain.
COMPRESS_MIN_BYTES = 1024


def create_api(
    build: BuildInfo,
    health_provider: Callable[[], Dict[str, Any]],
    detail_provider: Callable[[], Dict[str, Any]],
    registry: TokenRegistry,
    origin: OriginBlock,
    config_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    raw_data_dir: Optional[Path] = None,
    log_dir: Optional[Path] = None,
    data_collector: str = "kraken"
) -> FastAPI:
    """
    Build the status API.

    State is read through providers rather than from a collector reference, so the
    routes cannot reach into the collector and change anything: the surface is
    read-only by construction, not by discipline.

    Args:
        build: Identity sampled once at startup
        health_provider: Returns liveness and uptime
        detail_provider: Returns per-symbol collection state
        registry: Consumer tokens; an empty one refuses every gated route
        origin: The identity every file this process writes will carry. Required,
            not optional: a collector without one refuses to start, so a running
            API always has it, and an optional parameter would only create a way
            to serve a status nobody can attribute
        config_provider: Returns the effective configuration; the route is not
            mounted without it
        raw_data_dir: Archive root, for the inventory route
        log_dir: Where the log files are, for the excerpt route
        data_collector: Collector subdirectory inside raw_data_dir

    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="FiniexDataCollector Status API",
        version=build.version,
        docs_url=None,
        redoc_url=None
    )

    # Transparent to a caller: it is negotiated through Accept-Encoding, every
    # HTTP client decompresses on its own, and one that does not ask still gets
    # plain JSON. The SHA-256 in the archive register stays a hash of the
    # UNCOMPRESSED file, which is what a client sees after decoding.
    app.add_middleware(
        GZipMiddleware,
        minimum_size=COMPRESS_MIN_BYTES,
        compresslevel=COMPRESS_LEVEL)

    verify_bearer = build_bearer_dependency(registry)
    verify_grant = build_grant_dependency(registry)

    @app.get("/v1/health")
    def health() -> Dict[str, Any]:
        """Liveness and uptime. Open on purpose - a probe carries no credential."""
        return health_provider()

    @app.get("/v1/build")
    def build_identity() -> Dict[str, Any]:
        """What code this process is running, as sampled at startup."""
        return {
            "version": build.version,
            "data_format_version": build.data_format_version,
            "commit": build.commit,
            "dirty": build.dirty,
            "started_at": build.started_at
        }

    @app.get(
        "/v1/status",
        dependencies=[
            Depends(verify_bearer),
            Security(verify_grant, scopes=[SURFACE])
        ]
    )
    def status_detail() -> Dict[str, Any]:
        """
        Per-symbol collection state. Requires a token granting `status:detail`.

        Carries `origin` in the same shape a tick file carries it, so a consumer
        parses one structure rather than two. Without it an identity can only be
        learned by receiving a file - and the consumer needs it BEFORE the first
        file, because an unregistered identity refuses a measurement run at
        admission. A freshly deployed collector would otherwise have to be read
        off its own disk over a shell.

        It sits here and not on `/v1/build`, which is open: the build route
        discloses only what the public repository already shows, while an
        identity is a name for one machine's data directory.
        """
        return {"origin": asdict(origin), **detail_provider()}

    if config_provider is not None:
        @app.get(
            "/v1/configs",
            dependencies=[
                Depends(verify_bearer),
                Security(verify_grant, scopes=["config"])
            ]
        )
        def effective_config() -> Dict[str, Any]:
            """
            The configuration actually in force, after the user overlay is merged.

            `/v1/build` says which code runs; this says with which settings. Every
            secret is removed before it leaves - see redaction.py for why the rule
            is by key name rather than by a list of paths.
            """
            return redact_config(config_provider())

    if raw_data_dir is not None:
        @app.get(
            "/v1/archive",
            dependencies=[
                Depends(verify_bearer),
                Security(verify_grant, scopes=["archive"])
            ]
        )
        def archive_inventory(
            symbol: Optional[str] = Query(None),
            only_corrected: bool = Query(
                False,
                description="Only files whose anchor counters grew while open"),
            with_checksum: bool = Query(
                False, description="Include a SHA-256 per file")
        ) -> Dict[str, Any]:
            """
            What has been written: per file, its symbol, tick count, event and
            arrival bounds, and whether it absorbed a clock correction.

            Metadata only - the tick arrays are the bulk of a file and no
            inventory question needs them.
            """
            return read_archive(
                raw_data_dir, data_collector, symbol=symbol,
                only_corrected=only_corrected, with_checksum=with_checksum)

        @app.get(
            "/v1/files/{name}",
            dependencies=[
                Depends(verify_bearer),
                Security(verify_grant, scopes=["files"])
            ],
            response_class=FileResponse
        )
        def archive_file(name: str) -> FileResponse:
            """
            Hand out one finished archive file.

            **Only finished files can be reached, and not because of a check
            here.** A `*_ticks.json` is written to a temporary file and renamed
            into place, so the name never exists before the content is complete.
            What is still being collected has no `.json` yet - it lives in memory
            and in a `.jsonl.part`, which this route's name pattern does not
            match.

            The name is a path parameter, so the grant is `files:<name>`. A
            consumer entitled to the archive holds `files:*`; a per-file grant is
            possible and is what the model is for, not what it expects.
            """
            path = resolve_archive_file(raw_data_dir, data_collector, name)

            if path is None:
                # 404 whether the name was malformed, pointed outside the
                # archive, or simply is not there. Distinguishing them would
                # turn the route into a probe for what exists on the disk.
                raise HTTPException(status_code=404, detail=f"no such file: {name}")

            return FileResponse(
                path, media_type="application/json", filename=path.name)

    if log_dir is not None:
        @app.get(
            "/v1/logs",
            dependencies=[
                Depends(verify_bearer),
                Security(verify_grant, scopes=["logs"])
            ]
        )
        def log_excerpt(
            day: Optional[str] = Query(
                None,
                description="UTC date, YYYY-MM-DD. Omitted: the newest day present"),
            min_level: str = Query("INFO"),
            since: Optional[datetime] = Query(None),
            until: Optional[datetime] = Query(None),
            contains: Optional[str] = Query(None),
            limit: int = Query(MAX_LINES, ge=1, le=MAX_LINES)
        ) -> Dict[str, Any]:
            """
            One UTC day of the log, filtered.

            `day`, `since` and `until` are all UTC, and so is every line in the
            file - there is no offset to apply at either end.

            The day is a QUERY parameter on purpose. A path parameter becomes the
            grant name (`logs:<value>`), so a date there would demand a grant per
            calendar day. Without one, the surface itself is the permission.

            **Omitting it means the newest day present**, not today. From a remote
            session the box's own date boundary is not known - asking for "today"
            a few minutes after midnight UTC returns an almost empty file, and
            asking for yesterday returns a finished one. The newest file is what
            somebody means by "the log" in both cases.
            """
            if day is None:
                days = available_days(log_dir)
                if not days:
                    return {"day": None, "exists": False, "line_count": 0,
                            "available_days": []}
                parsed = date.fromisoformat(days[-1])
            else:
                try:
                    parsed = date.fromisoformat(day)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"day must be YYYY-MM-DD, got {day!r}")

            return read_log(
                log_dir, parsed, min_level=min_level,
                since=_as_utc(since), until=_as_utc(until),
                contains=contains, limit=limit)

    return app


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """
    Read a query timestamp as UTC.

    A caller who omits the offset means UTC here; assuming the server's local
    zone instead is the mistake this project keeps paying for elsewhere.

    Args:
        value: Parsed query parameter, possibly naive

    Returns:
        Timezone-aware UTC datetime, or None
    """
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)
