"""
FiniexDataCollector - Package API
List, describe, hand out and acknowledge packages.

Behind the edge at `/v1/packages*` on the collector's name; the process binds
loopback. Every route reads files that already exist - a manifest, a receipt, a
zip - and none parses a tick file. The parsing happened once, when the package was
built, in a run of its own.

  GET  /v1/packages                       packages   the list, with each package's receipt
  GET  /v1/packages/ledger                packages   the book: packed, served, received
  GET  /v1/packages/{package_id}          packages   its manifest
  GET  /v1/packages/{package_id}/zip      packages   the zip; `Range` resumes a download
  POST /v1/packages/{package_id}/receipt  receipts   "received and imported"
  GET  /v1/packages/purge                 purge      what a purge would delete
  POST /v1/packages/purge                 purge      delete confirmed originals to the target
  GET  /v1/packages/health                open       verdict and reasons, as codes
  GET  /v1/packages/schedule              open       the weekly runs and the special runs to come
  POST /v1/packages/schedule              schedule   plan a special run; 409 names the rule it breaks
  DELETE /v1/packages/schedule/{id}       schedule   cancel a planned special run
  POST /v1/packages/schedule/weekly       schedule   set the weekly run aside, or resume it

No route packs. A package comes from the schedule, and only the `schedule` grant -
the operator's - changes the schedule, within limits that keep packages from piling
up (schedule.py).

Location: python/packages/package_api.py
"""

import re
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Optional

import anyio
from fastapi import (Body, Depends, FastAPI, HTTPException, Query, Request,
                     Security)
from fastapi.responses import FileResponse, JSONResponse
from finiex_auth.bearer_auth import build_bearer_dependency
from finiex_auth.grant_auth import build_grant_dependency
from finiex_auth.token_registry import TokenRegistry
from starlette.types import Message, Receive, Scope, Send

from python.packages import lifecycle
from python.packages.ledger import Ledger, downloaded_whole
from python.packages.package_store import PackageStore
from python.packages.schedule import Schedule, ScheduleRefused
from python.utils.config_loader import PackagesConfig
from python.utils.logging_setup import describe_exception, get_logger

NOTE_MAX_CHARS = 500
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class LedgeredFileResponse(FileResponse):
    """
    A file response that reports, when it ends, how much of the file it handed out.

    Starlette sends the file and says nothing about how far it got. This wrapper
    counts the body bytes passed to the server, watches for the client going away,
    and calls `on_finish` once with the outcome - after the last byte, after an
    abort, or after an error alike.

    **What `complete` can and cannot mean.** The server accepts bytes into its
    buffers; uvicorn drops body messages silently once a client is gone. So the
    count alone cannot tell a finished transfer from an abandoned one - hence the
    watch on `receive()` for `http.disconnect`, which is what turns "handed over"
    into "handed over while the client was still there". Arrival is beyond it.
    """

    def __init__(self, *args: Any, on_finish: Callable[[Dict[str, Any]], None], **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._on_finish = on_finish

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # A server offering `pathsend` would get the path instead of the bytes, and
        # nothing would be counted. uvicorn offers none; the line makes it a rule.
        extensions = {k: v for k, v in (scope.get("extensions") or {}).items()
                      if k != "http.response.pathsend"}
        scope = {**scope, "extensions": extensions}
        outcome: Dict[str, Any] = {"status": None, "content_length": None,
                                   "bytes_sent": 0, "complete": False, "disconnected": False}

        async def counting_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                outcome["status"] = message["status"]
                for key, value in message.get("headers", []):
                    if key.lower() == b"content-length":
                        outcome["content_length"] = int(value)
            elif message["type"] == "http.response.body":
                outcome["bytes_sent"] += len(message.get("body", b""))
                if not message.get("more_body", False):
                    outcome["complete"] = True
            await send(message)

        try:
            async with anyio.create_task_group() as group:
                async def watch_for_disconnect() -> None:
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            # After the last byte a disconnect is the normal end of
                            # the exchange, not an abort.
                            if not outcome["complete"]:
                                outcome["disconnected"] = True
                                group.cancel_scope.cancel()
                            return

                group.start_soon(watch_for_disconnect)
                await super().__call__(scope, receive, counting_send)
                group.cancel_scope.cancel()
        finally:
            # `complete` is set as the last body leaves, and a disconnect after that
            # is ignored - so a disconnect and `complete` never both hold.
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(self._on_finish, outcome)


def _summary(manifest: Dict[str, Any], receipt: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """One package as the list shows it."""
    return {
        "package_id": manifest["package_id"],
        "collector": manifest["collector"],
        "instance_id": manifest["instance_id"],
        "cutoff": manifest["cutoff"],
        "previous_package_id": manifest["previous_package_id"],
        "created_at": manifest["created_at"],
        "member_count": manifest["member_count"],
        "tick_count": manifest["tick_count"],
        "size_bytes": manifest["zip"]["size_bytes"],
        "sha256": manifest["zip"]["sha256"],
        "received_at": receipt["received_at"] if receipt else None,
    }


def create_package_api(
    store: PackageStore,
    registry: TokenRegistry,
    status_provider: Callable[[], Dict[str, Any]],
    ledger: Ledger,
    config: PackagesConfig,
    on_receipt: Optional[Callable[[str], None]] = None,
    version: str = "",
    schedule: Optional[Schedule] = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
) -> FastAPI:
    """
    Build the package API.

    Args:
        store: The package store
        registry: Consumer tokens; an empty one refuses every gated route
        status_provider: Returns the service's own state - start time, next run
        ledger: Where downloads and receipts are booked
        config: The packages block - folders, thresholds, the purge target
        on_receipt: Called with the collector's name after a new receipt, so the
            service can move the package and its originals on without waiting
        version: The program's version, for the title
        schedule: The schedule the service runs by; default one over the store's root
        clock: The time the schedule routes judge by; tests drive it

    Returns:
        Configured FastAPI application
    """
    logger = get_logger("FiniexPackages")
    schedule = schedule or Schedule(store.root, config)

    def book(event: str, **fields: Any) -> None:
        """A record, not a gate: a ledger that cannot be written stops nothing."""
        try:
            ledger.append(event, **fields)
        except OSError as e:
            logger.error(f"[LEDGER] could not book {event} for "
                         f"{fields.get('package_id')}: {describe_exception(e)}")

    # FastAPI refuses an empty version; "unversioned" says what it is.
    app = FastAPI(title="FiniexDataCollector Package API", version=version or "unversioned",
                  docs_url=None, redoc_url=None)

    # No compression middleware on purpose: a zip does not shrink, and compressing
    # a ranged response would break the resumed download it is meant to serve.
    verify_bearer = build_bearer_dependency(registry)
    verify_grant = build_grant_dependency(registry)

    @app.get("/v1/packages/health")
    def health() -> Dict[str, Any]:
        """
        The verdict, with reasons as codes, and what each collector's last run
        came to. Open, like the collector's `/v1/health`: it names no file, no
        path and no package content - the reasons' detail is behind the token.
        """
        verdict = lifecycle.health(config, store)
        runs = {name: {"result": run.get("result"), "at": run.get("at"),
                       "duration_seconds": run.get("duration_seconds")}
                for name, run in store.last_runs().items()}
        return {"status": verdict["status"], "reasons": verdict["reasons"],
                **status_provider(), "last_runs": runs}

    @app.get(
        "/v1/packages",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["packages"])]
    )
    def list_packages(
        request: Request,
        collector: Optional[str] = Query(None),
        unacknowledged: bool = Query(
            False, description="Only packages without a receipt")
    ) -> Dict[str, Any]:
        """
        Every package, oldest first, with its receipt. A package nobody has
        acknowledged stays listed; an acknowledged one stays too - the receipt
        deletes nothing.
        """
        consumer = getattr(request.state, "consumer", None)
        packages = []
        for manifest in store.manifests(collector):
            if consumer is not None and not registry.may(
                    consumer, f"packages:{manifest['package_id']}"):
                continue
            receipt = store.receipt(manifest["package_id"])
            if unacknowledged and receipt is not None:
                continue
            paths = store.find(manifest["package_id"])
            packages.append({**_summary(manifest, receipt),
                             "state": paths["state"] if paths else None})
        return {"count": len(packages), "packages": packages,
                "health": lifecycle.health(config, store),
                "storage": lifecycle.storage(config),
                "last_runs": store.last_runs(), **status_provider()}

    @app.get(
        "/v1/packages/purge",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["purge"])]
    )
    def purge_preview(
        target_free_percent: Optional[float] = Query(None, ge=1.0, le=95.0)
    ) -> Dict[str, Any]:
        """
        What a purge would delete now, and why each package would or would not go.
        Deletes nothing.
        """
        return lifecycle.purge(config, store, ledger, apply=False,
                               target_free_percent=target_free_percent)

    @app.post(
        "/v1/packages/purge",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["purge"])]
    )
    def purge_apply(
        request: Request,
        body: Optional[Dict[str, Any]] = Body(None)
    ) -> Dict[str, Any]:
        """
        Delete confirmed originals, oldest receipt first, until the disk has
        `target_free_percent` free (default from the configuration, 50). Every
        package is proven again before it goes. A full purge is not offered here;
        that stays a command on the machine.
        """
        target = (body or {}).get("target_free_percent")
        if target is not None and (not isinstance(target, (int, float)) or not 1 <= target <= 95):
            raise HTTPException(
                status_code=400, detail="target_free_percent must be a number from 1 to 95")
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"
        return lifecycle.purge(config, store, ledger, apply=True,
                               target_free_percent=target, by=consumer)

    @app.get("/v1/packages/schedule")
    def read_schedule() -> Dict[str, Any]:
        """
        When packages will come: the next weekly runs, whether one is set aside,
        and the special runs planned or recently run, each with its cutoff. Open,
        so a consumer can plan its pickups; it names no token and no file.
        """
        return schedule.view(clock())

    def _day(value: Any, name: str) -> date:
        try:
            return date.fromisoformat(value)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail=f"{name} must be a date, YYYY-MM-DD")

    @app.post(
        "/v1/packages/schedule",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["schedule"])]
    )
    def add_special(
        request: Request,
        body: Optional[Dict[str, Any]] = Body(None)
    ) -> JSONResponse:
        """
        Plan a special run: `collector` (a configured one, or `all`), `cutoff` (a
        day; its 00:00 UTC is the cutoff), optionally `run_at` (ISO, UTC). Refused
        with 409 and the rule when it would break one.
        """
        body = body or {}
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"
        collector = body.get("collector")
        if not isinstance(collector, str):
            raise HTTPException(
                status_code=400, detail="collector is required")
        cutoff = _day(body.get("cutoff"), "cutoff")
        run_at = None
        if body.get("run_at") is not None:
            try:
                run_at = datetime.fromisoformat(body["run_at"])
            except (TypeError, ValueError):
                raise HTTPException(
                    status_code=400, detail="run_at must be an ISO moment")
            if run_at.tzinfo is None:
                raise HTTPException(
                    status_code=400, detail="run_at must carry its offset")
        try:
            special = schedule.add(
                collector, cutoff, clock(), consumer, run_at=run_at)
        except ScheduleRefused as e:
            book("schedule_refused", consumer=consumer, collector=collector,
                 cutoff=cutoff.isoformat(), reason=str(e))
            raise HTTPException(status_code=409, detail=str(e))
        book("schedule_added", consumer=consumer, special_id=special["id"],
             collector=collector, cutoff=special["cutoff"], run_at=special["run_at"])
        return JSONResponse(special, status_code=201)

    @app.post(
        "/v1/packages/schedule/weekly",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["schedule"])]
    )
    def change_weekly(
        request: Request,
        body: Optional[Dict[str, Any]] = Body(None)
    ) -> Dict[str, Any]:
        """
        `{"action": "set_aside"}` sets the next weekly run aside; with `until` (a
        day), every one before it. `{"action": "resume"}` lifts both. The weekly
        rule itself stays in the configuration.
        """
        body = body or {}
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"
        action = body.get("action")
        now = clock()
        if action == "set_aside":
            until = _day(body["until"], "until") if body.get(
                "until") is not None else None
            try:
                weekly = schedule.set_aside(now, until)
            except ScheduleRefused as e:
                raise HTTPException(status_code=409, detail=str(e))
            book("schedule_weekly_set_aside", consumer=consumer, **weekly)
        elif action == "resume":
            weekly = schedule.resume()
            book("schedule_weekly_resumed", consumer=consumer)
        else:
            raise HTTPException(
                status_code=400, detail="action must be set_aside or resume")
        return schedule.view(now)

    @app.delete(
        "/v1/packages/schedule/{special_id}",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["schedule"])]
    )
    def cancel_special(special_id: str, request: Request) -> Dict[str, Any]:
        """Cancel a planned special run. One that ran or was cancelled stays as it is."""
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"
        special = schedule.cancel(special_id)
        if special is None:
            raise HTTPException(
                status_code=404, detail=f"no planned special run {special_id}")
        book("schedule_cancelled", consumer=consumer, special_id=special_id,
             collector=special["collector"], cutoff=special["cutoff"])
        return special

    @app.get(
        "/v1/packages/ledger",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["packages"])]
    )
    def read_ledger(
        request: Request,
        package_id: Optional[str] = Query(None),
        event: Optional[str] = Query(
            None, description="packed, served, received or receipt_refused")
    ) -> Dict[str, Any]:
        """
        The book, oldest first: what was packed, what went out, what came back
        confirmed. A caller sees the events of the packages it may fetch.
        """
        consumer = getattr(request.state, "consumer", None)
        book_ = ledger.read(package_id=package_id, event=event)
        if consumer is not None:
            book_["events"] = [e for e in book_["events"] if registry.may(
                consumer, f"packages:{e.get('package_id')}")]
            book_["count"] = len(book_["events"])
        return book_

    @app.get(
        "/v1/packages/{package_id}",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["packages"])]
    )
    def package_manifest(package_id: str) -> Dict[str, Any]:
        """The manifest, exactly as written beside the zip, and the receipt."""
        manifest = store.manifest(package_id)
        if manifest is None:
            raise HTTPException(
                status_code=404, detail=f"no such package: {package_id}")
        return {**manifest, "receipt": store.receipt(package_id)}

    @app.get(
        "/v1/packages/{package_id}/zip",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["packages"])],
        response_class=FileResponse
    )
    def package_zip(package_id: str, request: Request) -> FileResponse:
        """
        The zip. Its bytes never change once listed, so a download can be repeated,
        and resumed with `Range` - the manifest's SHA-256 is the check either way.
        Every download is booked when it ends, finished or not.
        """
        paths = store.find(package_id)
        if paths is None:
            raise HTTPException(
                status_code=404, detail=f"no such package: {package_id}")
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"
        requested = request.headers.get("range")

        def served(outcome: Dict[str, Any]) -> None:
            book("served", package_id=package_id, consumer=consumer, range=requested,
                 status=outcome["status"], content_length=outcome["content_length"],
                 bytes_sent=outcome["bytes_sent"], complete=outcome["complete"],
                 disconnected=outcome["disconnected"])

        return LedgeredFileResponse(paths["zip"], media_type="application/zip",
                                    filename=paths["zip"].name, on_finish=served)

    @app.post(
        "/v1/packages/{package_id}/receipt",
        dependencies=[Depends(verify_bearer), Security(
            verify_grant, scopes=["receipts"])]
    )
    def acknowledge(
        package_id: str,
        request: Request,
        body: Optional[Dict[str, Any]] = Body(None)
    ) -> JSONResponse:
        """
        Mark a package received and imported. The first acknowledgement stands; a
        repeated one answers 200 with the original, a new one 201. Deletes nothing.

        `zip_sha256`, optional: the hash the consumer computed over what it
        received. Given, it must be the package's, or the acknowledgement is
        refused with 409 - and then the receipt says "these exact bytes", not only
        "a package of this name".
        """
        body = body or {}
        note = body.get("note", "")
        if not isinstance(note, str) or len(note) > NOTE_MAX_CHARS:
            raise HTTPException(
                status_code=400, detail=f"note must be text of at most {NOTE_MAX_CHARS} characters")
        reported = body.get("zip_sha256")
        if reported is not None and (not isinstance(reported, str)
                                     or not SHA256.match(reported.lower())):
            raise HTTPException(
                status_code=400, detail="zip_sha256 must be 64 hexadecimal characters")
        reported = reported.lower() if reported else None
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"

        manifest = store.manifest(package_id)
        if manifest is None:
            raise HTTPException(
                status_code=404, detail=f"no such package: {package_id}")
        expected = manifest["zip"]["sha256"]
        if reported is not None and reported != expected:
            book("receipt_refused", package_id=package_id, consumer=consumer,
                 reported_sha256=reported, expected_sha256=expected)
            raise HTTPException(
                status_code=409,
                detail=f"zip_sha256 {reported} is not {package_id}'s {expected}: "
                f"what you hold is not this package. Fetch it again.")

        # The hash alone proves knowledge, not possession: the list publishes it.
        # So a receipt also needs the ledger to show that this token was handed
        # every byte of the zip. A repeated acknowledgement passes on its original;
        # a rehearsal is always checked, it never stands on anything.
        dev = consumer in config.dev_consumers
        if (dev or store.receipt(package_id) is None) and not downloaded_whole(
                ledger.read(package_id=package_id, event="served")["events"],
                consumer, manifest["zip"]["size_bytes"]):
            book("receipt_refused", package_id=package_id, consumer=consumer,
                 reported_sha256=reported, reason="no complete download by this token")
            raise HTTPException(
                status_code=409,
                detail=f"no complete download of {package_id} by your token is on record. "
                f"Fetch the whole zip, check it, then acknowledge.")

        if dev:
            # Every check above ran; nothing below happens. Our own pickup client
            # rehearses the consumer's whole path without marking the package or
            # moving a single file, and the consumer's real receipt stays open.
            book("dev_received", package_id=package_id, consumer=consumer, note=note,
                 zip_sha256=reported, verified=reported is not None)
            return JSONResponse({"package_id": package_id, "consumer": consumer, "note": note,
                                 "zip_sha256": reported, "dev": True, "created": False,
                                 "detail": "dev receipt: every check passed, nothing was marked"},
                                status_code=200)

        receipt, created = store.write_receipt(
            package_id, consumer, note, zip_sha256=reported)
        if receipt is None:
            raise HTTPException(
                status_code=404, detail=f"no such package: {package_id}")
        if created:
            book("received", package_id=package_id, consumer=consumer, note=note,
                 zip_sha256=reported, verified=reported is not None)
            if on_receipt is not None and reported is not None:
                on_receipt(manifest["collector"])
        return JSONResponse({**receipt, "created": created}, status_code=201 if created else 200)

    return app
