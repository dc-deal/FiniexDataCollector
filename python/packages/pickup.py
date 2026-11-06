"""
FiniexDataCollector - Pickup Client
What a consumer does to take a package, done from our side.

    list the packages waiting -> fetch the manifest -> download the zip, resuming a
    cut download -> check the zip's SHA-256 -> unpack into a temporary folder ->
    check every member's name, size and SHA-256 against the manifest -> acknowledge
    with the hash just computed

Two uses. It is the reference for FiniexTestingIDE's importer: the steps above are
the ones its side has to take, in this order. And it is our test rig: run with a
token listed in `packages.dev_consumers`, its receipt is a rehearsal - every check
on the server runs, nothing is marked, nothing moves, and the consumer's real
receipt stays open. A consumer's token is never in that list.

The downloaded zip is kept in the output folder, as a consumer files it; a cut
download stays as `<zip>.part` and the next run resumes it with `Range`.

Location: python/packages/pickup.py
"""

import hashlib
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from python.utils.logging_setup import describe_exception

HASH_BLOCK = 1024 * 1024
TIMEOUT_S = 120


class PickupError(Exception):
    """A step that failed; the package is not acknowledged."""


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(HASH_BLOCK), b""):
            digest.update(block)
    return digest.hexdigest()


def _get_json(session: Any, url: str, headers: Dict[str, str],
              params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    response = session.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise PickupError(
            f"GET {url} answered {response.status_code}: {response.text[:200]}")
    return response.json()


def download(session: Any, url: str, headers: Dict[str, str], target: Path, size: int) -> int:
    """
    Fetch a file to `target`, resuming a `.part` left by a cut download.

    Works with a `requests.Session` and with an `httpx` client alike.

    Returns:
        The byte offset the download resumed from (0 for a fresh one)
    """
    part = target.with_name(target.name + ".part")
    have = part.stat().st_size if part.exists() else 0
    if have > size:
        part.unlink()
        have = 0
    request_headers = dict(headers)
    if have:
        request_headers["Range"] = f"bytes={have}-"

    def write(status: int, chunks: Any) -> None:
        if status not in (200, 206):
            raise PickupError(f"GET {url} answered {status}")
        # A server that ignored the range sends the whole file again: start over
        # rather than append it to what is there.
        with part.open("ab" if status == 206 else "wb") as handle:
            for chunk in chunks:
                handle.write(chunk)

    if have < size:
        if hasattr(session, "stream"):
            with session.stream("GET", url, headers=request_headers) as response:
                write(response.status_code, response.iter_bytes())
        else:
            with session.get(url, headers=request_headers, stream=True,
                             timeout=TIMEOUT_S) as response:
                write(response.status_code, response.iter_content(HASH_BLOCK))
    got = part.stat().st_size if part.exists() else 0
    if got != size:
        raise PickupError(
            f"download stopped at {got} of {size} bytes; run again to resume")
    part.replace(target)
    return have


def verify(zip_path: Path, manifest: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check a downloaded zip against its manifest: the zip's hash, then every member
    unpacked into a temporary folder.

    Returns:
        The zip's SHA-256 as computed here, the file count and the bytes checked

    Raises:
        PickupError: At the first thing that does not match
    """
    zip_sha256 = _sha256_of(zip_path)
    if zip_sha256 != manifest["zip"]["sha256"]:
        raise PickupError(f"the zip's SHA-256 {zip_sha256} is not the manifest's "
                          f"{manifest['zip']['sha256']}")
    members = {m["name"]: m for m in manifest["members"]}
    total = 0
    with zipfile.ZipFile(zip_path) as archive, tempfile.TemporaryDirectory() as folder:
        names = archive.namelist()
        if sorted(names) != sorted(members):
            raise PickupError(
                "the zip does not hold exactly the manifest's files")
        for name in names:
            if Path(name).name != name:
                raise PickupError(f"a member with a path, not flat: {name}")
            archive.extract(name, folder)
            path = Path(folder) / name
            if path.stat().st_size != members[name]["size_bytes"]:
                raise PickupError(f"{name}: not the manifest's size")
            if _sha256_of(path) != members[name]["sha256"]:
                raise PickupError(f"{name}: not the manifest's SHA-256")
            total += path.stat().st_size
    return {"zip_sha256": zip_sha256, "files": len(names), "bytes": total}


def pickup(base_url: str, token: str, out_dir: Path, package_id: Optional[str] = None,
           acknowledge: bool = True, session: Any = None,
           log: Callable[[str], None] = print) -> List[Dict[str, Any]]:
    """
    Take the waiting packages, or one named package, the way a consumer does.

    Args:
        base_url: The collector's address, e.g. https://collector.example
        token: The bearer token
        out_dir: Where the zips are kept
        package_id: Only this package; default: every one without a receipt
        acknowledge: Send the receipt after every check passed
        session: A `requests.Session` or an `httpx` client; default a new session
        log: Where progress lines go

    Returns:
        One record per package: what was checked, the receipt's answer, or the
        step that failed
    """
    if session is None:
        import requests
        session = requests.Session()
    headers = {"Authorization": f"Bearer {token}"}
    base = base_url.rstrip("/") + "/v1/packages"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if package_id:
        ids = [package_id]
    else:
        listing = _get_json(session, base, headers, {"unacknowledged": "true"})
        ids = [p["package_id"] for p in listing["packages"]]
        log(f"{len(ids)} package(s) waiting: {', '.join(ids) or '-'}")

    results = []
    for pid in ids:
        result: Dict[str, Any] = {"package_id": pid, "ok": False}
        try:
            manifest = _get_json(session, f"{base}/{pid}", headers)
            target = out_dir / manifest["zip"]["name"]
            resumed = download(session, f"{base}/{pid}/zip", headers, target,
                               manifest["zip"]["size_bytes"])
            result["resumed_from"] = resumed
            log(f"{pid}: downloaded {manifest['zip']['size_bytes']} bytes"
                + (f", resumed at {resumed}" if resumed else ""))
            result.update(verify(target, manifest))
            log(f"{pid}: zip and {result['files']} files match the manifest")
            if acknowledge:
                response = session.post(
                    f"{base}/{pid}/receipt", headers=headers,
                    json={"zip_sha256": result["zip_sha256"],
                          "note": f"pickup client: {result['files']} files verified"})
                result["receipt_status"] = response.status_code
                result["receipt"] = response.json()
                if response.status_code not in (200, 201):
                    raise PickupError(f"the receipt was refused: {response.status_code} "
                                      f"{result['receipt'].get('detail', '')}")
                log(f"{pid}: acknowledged ({response.status_code}"
                    + (", dev - nothing marked" if result["receipt"].get("dev") else "") + ")")
            result["ok"] = True
        except PickupError as e:
            result["error"] = describe_exception(e)
            log(f"{pid}: {result['error']}")
        results.append(result)
    return results
