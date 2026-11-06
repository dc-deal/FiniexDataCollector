"""
FiniexDataCollector - Package API
List, describe, hand out and acknowledge packages.

Behind the edge at `/v1/packages*` on the collector's name; the process binds
loopback. Every route reads files that already exist - a manifest, a receipt, a
zip - and none parses a tick file. The parsing happened once, when the package was
built, in a run of its own.

  GET  /v1/packages                       packages   the list, with each package's receipt
  GET  /v1/packages/{package_id}          packages   its manifest
  GET  /v1/packages/{package_id}/zip      packages   the zip; `Range` resumes a download
  POST /v1/packages/{package_id}/receipt  receipts   "received and imported"
  GET  /v1/packages/health                open       alive, and what the last runs did

Location: python/packages/package_api.py
"""

from typing import Any, Callable, Dict, Optional

from fastapi import (Body, Depends, FastAPI, HTTPException, Query, Request,
                     Security)
from fastapi.responses import FileResponse, JSONResponse
from finiex_auth.bearer_auth import build_bearer_dependency
from finiex_auth.grant_auth import build_grant_dependency
from finiex_auth.token_registry import TokenRegistry

from python.packages.package_store import PackageStore

NOTE_MAX_CHARS = 500


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
    version: str = ""
) -> FastAPI:
    """
    Build the package API.

    Args:
        store: The package store
        registry: Consumer tokens; an empty one refuses every gated route
        status_provider: Returns the service's own state - start time, next run
        version: The program's version, for the title

    Returns:
        Configured FastAPI application
    """
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
        Alive, and what each collector's last run did. Open, like the collector's
        `/v1/health`: it names no file, no symbol and no package content.
        """
        runs = {name: {"result": run.get("result"), "at": run.get("at"),
                       "reason": run.get("reason", "")}
                for name, run in store.last_runs().items()}
        return {"status": "ok", **status_provider(), "last_runs": runs}

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
            packages.append(_summary(manifest, receipt))
        return {"count": len(packages), "packages": packages,
                "last_runs": store.last_runs(), **status_provider()}

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
    def package_zip(package_id: str) -> FileResponse:
        """
        The zip. Its bytes never change once listed, so a download can be repeated,
        and resumed with `Range` - the manifest's SHA-256 is the check either way.
        """
        paths = store.find(package_id)
        if paths is None:
            raise HTTPException(
                status_code=404, detail=f"no such package: {package_id}")
        return FileResponse(paths["zip"], media_type="application/zip",
                            filename=paths["zip"].name)

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
        """
        note = (body or {}).get("note", "")
        if not isinstance(note, str) or len(note) > NOTE_MAX_CHARS:
            raise HTTPException(
                status_code=400, detail=f"note must be text of at most {NOTE_MAX_CHARS} characters")
        consumer = getattr(request.state, "consumer",
                           None) or "unauthenticated"
        receipt, created = store.write_receipt(package_id, consumer, note)
        if receipt is None:
            raise HTTPException(
                status_code=404, detail=f"no such package: {package_id}")
        return JSONResponse({**receipt, "created": created}, status_code=201 if created else 200)

    return app
