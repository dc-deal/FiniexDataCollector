"""
FiniexDataCollector - Package Service Entry Point

    python -m python.packages check            prove the access, pack nothing
    python -m python.packages pack [--source mt5] [--cutoff 2026-11-15]
    python -m python.packages settle           bring the folders in line, now
    python -m python.packages purge [--apply] [--full] [--target 50]
    python -m python.packages serve            the API, the schedule, the housekeeping
    python -m python.packages pickup [--endpoint live] [--out DIR] [--package ID] [--no-ack]
                                               take packages as a consumer does - from anywhere

A program of its own: it loads the same configuration files as the collector but
reads only the `packages` and `telegram` blocks, and it writes its log under its
own `paths.logs_dir`. Run it from a checkout of its own, never from the directory a
running collector was started from. docs/operations/package_lifecycle.md walks
through what each step does to the folders.

Location: python/packages/__main__.py
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from python.api.build_info import sample_build_info
from python.exceptions.collector_exceptions import ConfigurationError
from python.packages import lifecycle
from python.packages.ledger import Ledger
from python.packages.package_service import (PackageService, lower_priority,
                                             pack_all)
from python.packages.package_store import PackageStore
from python.packages.preflight import check_source
from python.types.tick_types import DATA_FORMAT_VERSION
from python.utils.config_loader import AppConfig, ConfigLoader
from python.utils.logging_setup import (describe_exception, get_logger,
                                        setup_logging)

# Matches the collector's: a bad configuration is not fixed by a restart, so a
# service manager maps this code to "stay stopped".
EXIT_CONFIGURATION = 2


def _packer_identity(config: AppConfig) -> Dict[str, Any]:
    """What built a package, for its manifest."""
    build = sample_build_info(config.version, DATA_FORMAT_VERSION)
    return {"program": "finiex-packages", "version": build.version,
            "commit": build.commit, "dirty": build.dirty}


def _require_sources(config: AppConfig) -> None:
    """Refuse a run with nowhere to write or nothing to read."""
    if not config.packages.output_dir or not config.packages.originals_dir:
        raise ConfigurationError(
            "packages.output_dir and packages.originals_dir must be set")
    if not config.packages.sources:
        raise ConfigurationError("packages.sources names no collector")


def _store(config: AppConfig) -> PackageStore:
    return PackageStore(Path(config.packages.output_dir))


def cmd_check(config: AppConfig) -> int:
    """Preflight every source and print what it found. Packs nothing."""
    store = _store(config)
    failed = 0
    for name, source in config.packages.sources.items():
        check = check_source(name, source, store, config.packages.min_free_percent,
                             Path(config.packages.originals_dir))
        print(json.dumps(check.summary(), indent=2))
        failed += 0 if check.ok else 1
    return 1 if failed else 0


def cmd_pack(config: AppConfig, only: str, cutoff: str) -> int:
    """One packing run now, with the same lock the service takes."""
    print(f"priority {lower_priority()}")
    parsed = None
    if cutoff:
        parsed = datetime.strptime(
            cutoff, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    logger = get_logger("FiniexPackages")
    results = pack_all(config.packages, _store(config), _packer_identity(config),
                       logger.info, only=only or None, cutoff=parsed)
    for result in results:
        print(json.dumps(result, indent=2))
    return 0 if all(r["result"] in ("packed", "nothing_new") for r in results) else 1


def cmd_settle(config: AppConfig) -> int:
    """Bring the folders in line with the manifests and receipts, now."""
    store = _store(config)
    done = lifecycle.settle(config.packages, store, Ledger(store.root),
                            get_logger("FiniexPackages").info)
    print(json.dumps(done, indent=2))
    print(json.dumps(lifecycle.storage(config.packages), indent=2))
    return 0


def cmd_purge(config: AppConfig, apply: bool, full: bool, target: float) -> int:
    """Delete confirmed originals - or, without --apply, say what would go."""
    store = _store(config)
    report = lifecycle.purge(config.packages, store, Ledger(store.root), apply=apply,
                             full=full, target_free_percent=target, by="operator (command)")
    print(json.dumps(report, indent=2))
    if not apply:
        print("\nNothing was deleted. Add --apply to delete what is marked ok.")
    return 0 if all(p["ok"] for p in report["packages"]) else 1


def cmd_pickup(endpoint: str, out_dir: Path, package_id: str, acknowledge: bool) -> int:
    """Take packages the way a consumer does, from the address and token of an endpoint."""
    from python.packages.pickup import pickup
    from python.viewer.endpoints import EndpointError, load_endpoint

    try:
        base_url, token = load_endpoint(endpoint)
    except EndpointError as e:
        print(f"Endpoint {endpoint!r}: {describe_exception(e)}")
        return EXIT_CONFIGURATION
    results = pickup(base_url, token, out_dir, package_id=package_id or None,
                     acknowledge=acknowledge)
    print(json.dumps(results, indent=2))
    return 0 if all(r["ok"] for r in results) else 1


def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="FiniexDataCollector - tick packages")
    parser.add_argument("command", choices=[
                        "check", "pack", "settle", "purge", "serve", "pickup"])
    parser.add_argument("--endpoint", default="live",
                        help="pickup: entry of user_configs/remote_endpoints.json")
    parser.add_argument("--out", default="./data/pickup",
                        help="pickup: where the zips are kept")
    parser.add_argument("--package", default="",
                        help="pickup: only this package id")
    parser.add_argument("--no-ack", action="store_true",
                        help="pickup: download and check, do not acknowledge")
    parser.add_argument("--config", default="./configs/app_config.json")
    parser.add_argument("--source", default="", help="pack: only this source")
    parser.add_argument("--cutoff", default="",
                        help="pack: UTC date, YYYY-MM-DD; default today 00:00 UTC")
    parser.add_argument("--apply", action="store_true",
                        help="purge: delete; without it nothing is deleted")
    parser.add_argument("--full", action="store_true",
                        help="purge: every confirmed package, not only until the target")
    parser.add_argument("--target", type=float, default=None,
                        help="purge: free disk percent to reach; default from the configuration")
    args = parser.parse_args()

    # The pickup client is the consumer's side, run from anywhere - the laptop
    # included, where there are no package sources and nothing to configure.
    if args.command == "pickup":
        sys.exit(cmd_pickup(args.endpoint, Path(
            args.out), args.package, not args.no_ack))

    try:
        config = ConfigLoader(Path(args.config)).load()
    except Exception as e:
        print(f"Failed to load config: {describe_exception(e)}")
        sys.exit(EXIT_CONFIGURATION)

    setup_logging(console_level=config.logging.console_level,
                  file_level=config.logging.file_level,
                  log_dir=Path(config.paths.logs_dir))

    try:
        _require_sources(config)
        if args.command == "check":
            sys.exit(cmd_check(config))
        if args.command == "pack":
            sys.exit(cmd_pack(config, args.source, args.cutoff))
        if args.command == "settle":
            sys.exit(cmd_settle(config))
        if args.command == "purge":
            sys.exit(cmd_purge(config, args.apply, args.full, args.target))
        asyncio.run(PackageService(config.packages, _packer_identity(config),
                                   telegram=config.telegram).serve())
    except ConfigurationError as e:
        get_logger("FiniexPackages").error(
            f"Configuration error: {describe_exception(e)}")
        sys.exit(EXIT_CONFIGURATION)


if __name__ == "__main__":
    main()
