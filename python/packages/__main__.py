"""
FiniexDataCollector - Package Service Entry Point

    python -m python.packages check            prove the access, pack nothing
    python -m python.packages pack [--source kraken] [--cutoff 2026-11-09]
    python -m python.packages serve            the API and the weekly schedule

A program of its own: it loads the same configuration files as the collector but
reads only the `packages` block, and it writes its log under its own
`paths.logs_dir`. Run it from a checkout of its own, never from the directory a
running collector was started from.

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
    if not config.packages.output_dir:
        raise ConfigurationError("packages.output_dir is not set")
    if not config.packages.sources:
        raise ConfigurationError("packages.sources names no collector")


def cmd_check(config: AppConfig) -> int:
    """Preflight every source and print what it found. Packs nothing."""
    store = PackageStore(Path(config.packages.output_dir))
    failed = 0
    for name, source in config.packages.sources.items():
        check = check_source(name, source, store,
                             config.packages.min_free_percent)
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
    results = pack_all(config.packages, PackageStore(Path(config.packages.output_dir)),
                       _packer_identity(config), logger.info, only=only or None, cutoff=parsed)
    for result in results:
        print(json.dumps(result, indent=2))
    return 0 if all(r["result"] in ("packed", "nothing_new") for r in results) else 1


def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="FiniexDataCollector - tick packages")
    parser.add_argument("command", choices=["check", "pack", "serve"])
    parser.add_argument("--config", default="./configs/app_config.json")
    parser.add_argument("--source", default="", help="pack: only this source")
    parser.add_argument("--cutoff", default="",
                        help="pack: UTC date, YYYY-MM-DD; default today 00:00 UTC")
    args = parser.parse_args()

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
        asyncio.run(PackageService(config.packages,
                    _packer_identity(config)).serve())
    except ConfigurationError as e:
        get_logger("FiniexPackages").error(
            f"Configuration error: {describe_exception(e)}")
        sys.exit(EXIT_CONFIGURATION)


if __name__ == "__main__":
    main()
