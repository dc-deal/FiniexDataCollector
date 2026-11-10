"""
FiniexDataCollector - A named overlay for a second instance

A second collector on the same machine needs symbols and folders of its own, and
must never fall back on the first one's overlay: that overlay enables the Telegram
bot, and a second poller takes the first one's commands. `--user-config` names the
overlay; named, it must exist. And a configuration that cannot be loaded exits 2,
which the service manager maps to "stay stopped" - it exited 1 until 2026-11-10,
so NSSM retried a broken file at its throttle interval.

Location: tests/utils/test_user_config.py
"""

import json
import shutil
import sys
from pathlib import Path
from typing import List

import pytest

from python import main as main_module
from python.exceptions.collector_exceptions import ConfigurationError
from python.main import EXIT_CONFIGURATION, FiniexDataCollector
from python.utils.config_loader import ConfigLoader
from python.utils.logging_setup import remove_log_listener
from tests.conftest import build_ticks

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A checkout's shape: the tracked base, and the default overlay enabling Telegram."""
    (tmp_path / "configs").mkdir()
    shutil.copy(REPO / "configs" / "app_config.json",
                tmp_path / "configs" / "app_config.json")
    (tmp_path / "user_configs").mkdir()
    (tmp_path / "user_configs" / "app_config.json").write_text(json.dumps({
        "telegram": {"enabled": True, "bot_token": "123:abc", "chat_id": "42"},
        "kraken": {"symbols": ["BTC/USD"]}}), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_a_named_overlay_replaces_the_default_one(workdir: Path) -> None:
    (workdir / "user_configs" / "devline.json").write_text(json.dumps({
        "kraken": {"symbols": ["EUR/USD", "PAXG/USD"]},
        "paths": {"raw_data_dir": "./data/devline/raw"}}), encoding="utf-8")

    config = ConfigLoader(Path("configs/app_config.json"),
                          Path("user_configs/devline.json")).load()

    assert config.kraken.symbols == ["EUR/USD", "PAXG/USD"]
    assert config.paths.raw_data_dir == "./data/devline/raw"
    assert config.telegram.enabled is False, "the default overlay's bot leaked in"


def test_without_a_name_the_default_overlay_applies(workdir: Path) -> None:
    config = ConfigLoader(Path("configs/app_config.json")).load()

    assert config.telegram.enabled is True
    assert config.kraken.symbols == ["BTC/USD"]


def test_a_named_overlay_that_is_missing_refuses(workdir: Path) -> None:
    with pytest.raises(ConfigurationError) as caught:
        ConfigLoader(Path("configs/app_config.json"),
                     Path("user_configs/devlien.json")).load()

    assert "devlien.json" in str(caught.value)


def test_an_absent_default_overlay_is_no_error(workdir: Path) -> None:
    (workdir / "user_configs" / "app_config.json").unlink()

    assert ConfigLoader(Path("configs/app_config.json")
                        ).load().telegram.enabled is False


def run_main(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    """
    The entry point up to the refusal. Should the configuration load after all,
    collecting fails at once rather than connecting to the exchange - a broken
    guard must fail this test, not hang it (it did, on 2026-11-10).
    """
    async def must_not_collect(*args, **kwargs) -> None:
        raise AssertionError(
            "the configuration loaded - a collector would have started")

    monkeypatch.setattr(main_module, "cmd_collect", must_not_collect)
    monkeypatch.setattr(sys, "argv", ["main.py", *argv])
    with pytest.raises(SystemExit) as caught:
        main_module.main()
    return caught.value.code


def test_a_mistyped_overlay_stops_the_service_instead_of_restarting(
        workdir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_main(monkeypatch, "collect", "--user-config",
                    "user_configs/devlien.json") == EXIT_CONFIGURATION


def test_a_broken_configuration_exits_two(workdir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (workdir / "user_configs" /
     "app_config.json").write_text("{", encoding="utf-8")
    assert run_main(monkeypatch, "collect") == EXIT_CONFIGURATION

    (workdir / "user_configs" / "app_config.json").write_text(
        json.dumps({"kraken": {"broker_type": ""}}), encoding="utf-8")
    assert run_main(monkeypatch, "collect") == EXIT_CONFIGURATION

    assert run_main(monkeypatch, "collect", "--config",
                    "configs/nope.json") == EXIT_CONFIGURATION


def test_the_first_tick_line_shows_the_price_as_written() -> None:
    """A forex price has five decimals; the line said bid=1.12 for 1.12109."""
    collector = FiniexDataCollector(ConfigLoader().load(), show_display=False)
    lines: List[str] = []

    class Logger:
        def info(self, message: str) -> None:
            lines.append(message)

        def __getattr__(self, name: str):
            return lambda *args, **kwargs: None

    try:
        collector._logger = Logger()
        tick = build_ticks(count=1, symbol="EURUSD")[0]
        tick.bid, tick.ask = 1.12109, 1.12112
        collector._handle_tick(tick)
    finally:
        remove_log_listener(collector._stats.record_logged)

    first = [line for line in lines if line.startswith("First tick: EURUSD")]
    assert first and "bid=1.12109 ask=1.12112" in first[0]
