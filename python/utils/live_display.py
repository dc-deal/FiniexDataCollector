"""
FiniexDataCollector - Live Display
Real-time terminal display for collection monitoring using rich.

Features:
- Live updating table with per-symbol stats
- File progress percentage (current/max)
- WebSocket connection status
- Disk space monitoring
- Folder statistics (Kraken, MT5, Logs)
- Recent errors/warnings
- Last file info

Location: python/utils/live_display.py
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional, List

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.markup import escape
from rich.text import Text
from rich import box

from python.types.collector_stats import CollectorStats
from python.types.feed_state import FeedState


class LiveDisplay:
    """
    Live terminal display for FiniexDataCollector.

    Uses rich.Live for flicker-free updates.
    Runs in async loop alongside collector.
    """

    def __init__(
        self,
        stats: CollectorStats,
        update_interval: float = 1.0,
        max_log_lines: int = 5,
        feed: Optional[FeedState] = None
    ):
        """
        Initialize live display.

        Everything it draws comes from the stats object and nothing else. That
        is what makes the same renderer usable from another process, which is
        the point of issue #15: the streams, the clock counters and each
        symbol's decimal places used to be read off live objects only a program
        inside the collector can reach.

        Args:
            stats: Shared CollectorStats object
            update_interval: Display update interval in seconds
            max_log_lines: Maximum log lines to show
            feed: Where the statistics come from, when they arrive over a wire.
                None means this display runs inside the collector, where every
                number is current by construction
        """
        self._stats = stats
        self._update_interval = update_interval
        self._max_log_lines = max_log_lines
        self._feed = feed
        self._running = False
        self._console = Console()
        self._live: Optional[Live] = None
        self._task: Optional[asyncio.Task] = None

    @property
    def stats(self) -> CollectorStats:
        """The statistics being drawn."""
        return self._stats

    @stats.setter
    def stats(self, stats: CollectorStats) -> None:
        """
        Draw a different statistics object from the next frame onwards.

        A viewer replaces the whole object per reading rather than merging into
        the previous one: a merge would leave a field the collector stopped
        sending standing at its last value, which is precisely what a remote
        screen must not do.

        Args:
            stats: The statistics of the most recent reading
        """
        self._stats = stats

    @property
    def _streams(self) -> List[str]:
        """The subscribed streams, as the collector reported them."""
        return self._stats.streams

    async def start(self) -> None:
        """Start the live display."""
        self._running = True
        self._task = asyncio.create_task(self._update_loop())

    async def stop(self) -> None:
        """Stop the live display gracefully."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._live:
            self._live.stop()

    async def _update_loop(self) -> None:
        """Main update loop."""
        with Live(
            self._render(),
            console=self._console,
            refresh_per_second=1,
            screen=False
        ) as live:
            self._live = live

            while self._running:
                try:
                    # Render, and time it. This runs on the collector's only
                    # event loop, so a slow console is a slow tick stamp - and
                    # on Windows the legacy renderer is slow. Measured rather
                    # than assumed, because a stall gets attributed from these
                    # numbers.
                    started = time.monotonic()
                    live.update(self._render())
                    if self._feed is None:
                        # Only in-process: a viewer writing its own render cost
                        # into statistics that describe the collector would put
                        # a measurement from this machine under that machine's
                        # name, which is exactly the kind of claim this project
                        # refuses in its files.
                        self._stats.record_render(
                            (time.monotonic() - started) * 1000)

                    # Wait
                    await asyncio.sleep(self._update_interval)

                except asyncio.CancelledError:
                    break
                except Exception:
                    # Don't crash on render errors
                    pass

    def _render(self) -> Panel:
        """
        Render the complete display.

        Returns:
            Rich Panel with all components
        """
        layout = Layout()

        # Build sections
        header = self._build_header()
        monitoring = self._build_monitoring_status()
        symbol_table = self._build_symbol_table()
        storage_summary = self._build_storage_summary()
        footer = self._build_footer()

        # Combine
        layout.split_column(
            Layout(header, name="header", size=3),
            Layout(monitoring, name="monitoring", size=3),
            Layout(symbol_table, name="symbols"),
            Layout(storage_summary, name="storage", size=3),
            Layout(footer, name="footer", size=8)
        )

        title, border = self._build_frame()
        return Panel(
            layout,
            title=title,
            border_style=border,
            box=box.ROUNDED
        )

    def _build_frame(self) -> tuple:
        """
        The panel's title and border colour.

        The whole frame turns red when the collector stops answering, rather than
        one line somewhere inside it. A screen full of numbers that are quietly
        half a minute old is read as current by anyone glancing at it, so the part
        that changes has to be the part nobody can miss.

        Which is why the first second must NOT be red. A viewer that has just
        started has asked nothing and been told nothing, and painting that as an
        outage spends the one signal the screen has on a state that resolves
        itself - after which a red frame is something the eye has learned to wait
        out.

        Returns:
            (title markup, border style)
        """
        if self._feed is None:
            return "[bold cyan]📡 FiniexDataCollector Live[/bold cyan]", "cyan"

        if self._feed.connected:
            return (f"[bold cyan]📡 FiniexDataCollector[/bold cyan] "
                    f"[dim]← {self._feed.source}[/dim]", "cyan")

        if self._feed.awaiting_first_answer:
            waited = self._format_age(self._feed.waiting_seconds)
            return (f"[bold yellow]⏳ waiting for the first answer[/bold yellow] "
                    f"[dim]← {self._feed.source} ({waited})[/dim]", "yellow")

        age = self._feed.age_seconds
        since = ("never answered" if self._feed.last_success is None else
                 f"no answer since "
                 f"{self._feed.last_success.strftime('%H:%M:%S')} UTC "
                 f"({self._format_age(age)} ago)")
        return f"[bold red]⛔ {since}[/bold red]", "red"

    @staticmethod
    def _format_age(seconds: Optional[float]) -> str:
        """
        A duration a glance can read.

        Args:
            seconds: Age in seconds, or None

        Returns:
            Short human form
        """
        if seconds is None:
            return "—"
        if seconds < 60:
            return f"{int(seconds)}s"
        if seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"

    def _get_local_tz_label(self) -> str:
        """Get local timezone label like 'GMT+1' or 'GMT-5'."""
        offset = datetime.now().astimezone().utcoffset()
        total_seconds = int(offset.total_seconds())
        hours = total_seconds // 3600
        minutes = abs(total_seconds) % 3600 // 60
        if minutes:
            return f"GMT{hours:+d}:{minutes:02d}"
        return f"GMT{hours:+d}"

    def _build_header(self) -> Text:
        """Build header with uptime, files, WebSocket status, and clocks."""
        uptime = self._format_uptime(self._stats.get_uptime_seconds())

        # WebSocket status with color
        ws_status = self._stats.websocket_status
        if ws_status == "connected":
            ws_display = "[green]● connected[/green]"
        elif ws_status == "reconnecting":
            ws_display = "[yellow]● reconnecting[/yellow]"
        else:
            ws_display = "[red]○ disconnected[/red]"

        # Streams display
        streams_str = ", ".join(self._streams) or "none yet"

        # Time displays
        now_utc = datetime.now(timezone.utc)
        now_local = datetime.now()
        tz_label = self._get_local_tz_label()

        line1 = (
            f"[bold]📋 Streams:[/bold] [magenta]{streams_str}[/magenta] │ "
            f"[bold]⏱️ Uptime:[/bold] {uptime} │ "
            f"[bold]📁 Files:[/bold] {self._stats.total_files} │ "
            f"[bold]🔌 WS:[/bold] {ws_display} │ "
            f"[bold]⚠️ Errors:[/bold] [red]{self._stats.total_errors}[/red]"
        )

        line2 = (
            f"[bold]🕐 Broker:[/bold] [cyan]{now_utc.strftime('%H:%M:%S')}[/cyan] [dim](UTC)[/dim] │ "
            f"[bold]🏠 Local:[/bold] [green]{now_local.strftime('%H:%M:%S')}[/green] [dim]({tz_label})[/dim]"
        )

        lines = [line1, line2]
        feed_line = self._build_feed_line()
        if feed_line:
            lines.append(feed_line)

        return Text.from_markup("\n".join(lines))

    def _build_feed_line(self) -> str:
        """
        What the viewer knows about its own reading, or nothing in-process.

        Two things belong here that nowhere else can state: how old the numbers
        above are, and whether the two machines agree about the time. The second
        is not cosmetic - uptime is computed from the collector's start time
        against this machine's clock, so a skewed viewer prints an uptime that
        never happened.

        Returns:
            Markup for one line, or an empty string when there is no feed
        """
        if self._feed is None:
            return ""

        if self._feed.awaiting_first_answer:
            # Not "the numbers below are not current": there are no numbers
            # below, only the defaults of an empty statistics object. And not a
            # reason either - `last_error` is None here, which printed the word
            # "unknown" and read as a failure nobody could diagnose.
            return (f"[yellow]⏳ no reading yet - first request in flight"
                    f"[/yellow] │ "
                    f"[dim]{escape(self._feed.source)}, "
                    f"every {self._feed.interval_seconds:.0f}s[/dim]")

        if not self._feed.connected:
            reason = self._feed.last_error or "unknown"
            return (f"[bold red]⛔ the numbers below are not current[/bold red] │ "
                    f"[red]{escape(reason)}[/red] │ "
                    f"[dim]{escape(self._feed.source)}[/dim]")

        parts = [
            f"[dim]📡 {escape(self._feed.source)} │ "
            f"read {self._format_age(self._feed.age_seconds)} ago, "
            f"every {self._feed.interval_seconds:.0f}s[/dim]"
        ]
        if self._feed.clock_disagrees:
            parts.append(
                f"[yellow]⚠️ clock skew {self._feed.skew_seconds:+.0f}s - "
                f"durations here are off by that much[/yellow]")

        return " │ ".join(parts)

    def _build_monitoring_status(self) -> Text:
        """Build monitoring status line with disk space and last check."""
        lines = []

        # Disk space
        disk = self._stats.disk_space
        if disk.last_checked:
            if disk.status == "OK":
                status_color = "green"
                status_icon = "✅"
            elif disk.status == "WARNING":
                status_color = "yellow"
                status_icon = "⚠️"
            elif disk.status == "CRITICAL":
                status_color = "red"
                status_icon = "🚨"
            else:
                status_color = "bright_red"
                status_icon = "🔴"

            disk_line = (
                f"[bold]💾 Disk:[/bold] "
                f"[{status_color}]{disk.free_gb:.1f} GB free ({disk.percent_free:.0f}%) {status_icon}[/{status_color}]"
            )
        else:
            disk_line = "[dim]💾 Disk: checking...[/dim]"

        lines.append(disk_line)

        # The collection clock. Silent while the OS behaves, loud when it does
        # not: a clamped stamp is invisible in the data by design, so if it is
        # not shown here nobody learns the machine's clock is stepping. It fired
        # 14 times in one night on this host without anyone noticing.
        clock = self._stats.clock
        if clock.resyncs == 0:
            lines.append("[dim]🕐 Clock: steady[/dim]")
        else:
            lines.append(
                f"[bold]🕐 Clock:[/bold] [yellow]{clock.resyncs} "
                f"correction(s), max {clock.max_correction_ms} ms[/yellow]"
            )

        # Last check time
        if disk.last_checked:
            last_check = disk.last_checked.strftime("%a %d.%m %H:%M")
            lines.append(f"[dim]Last Check: {last_check}[/dim]")
        else:
            lines.append("[dim]Last Check: N/A[/dim]")

        return Text.from_markup(" │ ".join(lines))

    def _build_symbol_table(self) -> Table:
        """Build per-symbol statistics table."""
        table = Table(
            show_header=True,
            header_style="bold cyan",
            box=box.SIMPLE,
            padding=(0, 1)
        )

        # Columns
        table.add_column("Symbol", width=10)
        table.add_column("Start (UTC)", width=12)
        table.add_column("Current File", justify="right", width=22)
        table.add_column("Files", justify="right", width=12)

        # For trade streams, show Price instead of Bid/Ask
        if "trade" in self._streams and "ticker" not in self._streams:
            table.add_column("Last Price", justify="right", width=14)
            table.add_column("Volume", justify="right", width=12)
        else:
            table.add_column("Bid", justify="right", width=12)
            table.add_column("Ask", justify="right", width=12)
            table.add_column("Spread %", justify="right", width=10)
            # A spread without the age of the quote it came from cannot be
            # judged: 0.05 % off a 40 ms quote and off a 7 s quote look the same.
            table.add_column("Quote age", justify="right", width=11)

        table.add_column("Status", width=12)

        # No symbols yet
        if not self._stats.symbols:
            table.add_row(
                "[dim]Waiting...[/dim]", "", "", "", "", "", "", ""
            )
            return table

        # Add rows for each symbol
        for symbol, stats in sorted(self._stats.symbols.items()):
            # Status carries how long ago the last tick arrived, not just
            # whether it was recent. A thin symbol can hold "Active" for hours
            # while filling nothing - DASHUSD took 4.5 h for one file and never
            # looked different from a busy one.
            status = self._format_last_tick(stats.last_tick_time)

            # File progress, against the limit the COLLECTOR was configured
            # with. Reading it from a local config file was wrong twice over:
            # once per symbol per frame off the disk, and - in a viewer - off
            # the wrong machine, which rendered a production file of 12,737
            # ticks as 1274 % of a limit that instance does not have.
            max_ticks = self._stats.max_ticks_per_file
            if max_ticks > 0:
                percent = stats.current_file_ticks / max_ticks * 100
                file_progress = (f"{stats.current_file_ticks:,} / "
                                 f"{max_ticks:,} ({percent:.0f}%)")
            else:
                # An older collector does not report it. The count is a fact;
                # a denominator would be invented.
                file_progress = f"{stats.current_file_ticks:,}"

            # Folder files
            if stats.folder_file_count > 0:
                files_display = f"{stats.folder_file_count} total"
            else:
                files_display = f"{stats.file_count} created"

            # Start time display
            start_str = (
                f"[cyan]{stats.start_time.strftime('%H:%M:%S')}[/cyan]"
                if stats.start_time else "[dim]-[/dim]"
            )

            # Format based on stream type
            if "trade" in self._streams and "ticker" not in self._streams:
                # Trade stream: show last price and volume
                price_str = self._format_price(
                    stats.last_bid, self._digits_for(symbol))
                vol_str = f"{stats.last_volume:.4f}" if stats.last_volume > 0 else "-"

                table.add_row(
                    f"[bold]{symbol}[/bold]",
                    start_str,
                    file_progress,
                    files_display,
                    price_str,
                    vol_str,
                    status
                )
            else:
                # Ticker stream: show bid/ask/spread
                digits = self._digits_for(symbol)
                bid_str = self._format_price(stats.last_bid, digits)
                ask_str = self._format_price(stats.last_ask, digits)
                spread_str = f"{stats.last_spread_pct:.4f}" if stats.last_spread_pct > 0 else "-"
                age_str = self._format_quote_age(stats.last_quote_age_ms)

                table.add_row(
                    f"[bold]{symbol}[/bold]",
                    start_str,
                    file_progress,
                    files_display,
                    bid_str,
                    ask_str,
                    spread_str,
                    age_str,
                    status
                )

        return table

    def _build_storage_summary(self) -> Text:
        """Build storage summary with folder stats and reconnects."""
        parts = []

        # Folder stats
        kraken = self._stats.folders.get("kraken")
        mt5 = self._stats.folders.get("mt5")
        logs = self._stats.folders.get("logs")

        if kraken:
            parts.append(f"Kraken: {kraken.file_count} files")
        else:
            parts.append("Kraken: -")

        if mt5:
            parts.append(f"MT5: {mt5.file_count} files")
        else:
            parts.append("MT5: -")

        if logs:
            parts.append(f"Logs: {logs.file_count} files")
        else:
            parts.append("Logs: -")

        # Reconnects
        reconnect_count = len(self._stats.reconnect_events)
        if self._stats.last_reconnect:
            last = self._stats.last_reconnect
            # Seconds below three minutes: whole minutes rendered a 105 s
            # outage as "1m" and a 45 s one as "0m", which is the range these
            # actually fall in.
            secs = last.duration_seconds
            down = f"{secs:.0f}s" if secs < 180 else f"{secs / 60:.0f}m"
            time_str = last.reconnected_at.strftime(
                "%a %d.%m %H:%M") if last.reconnected_at else "unknown"
            parts.append(
                f"Reconnects: {reconnect_count} (Last: {time_str}, {down} down)")
        else:
            parts.append(f"Reconnects: {reconnect_count}")

        # The archive writers, and how late this loop has been running. Both
        # describe the one thing a tick file cannot show: a blocked loop stamps
        # collected_msc late, and the importer refuses a file beyond 30 s of it.
        exports = self._stats.exports
        if exports.started:
            owed = exports.started - exports.finished
            parts.append(
                f"Exports: {exports.finished} written"
                + (f", {owed} owed" if owed else "")
                + (f", {exports.failed} failed" if exports.failed else ""))

        lag = self._stats.loop_lag
        if lag.samples:
            parts.append(f"Loop max: {lag.max_ms:.0f} ms")

        summary = "[bold]📁 Storage:[/bold] " + " │ ".join(parts)

        return Text.from_markup(summary)

    def _build_footer(self) -> Text:
        """Build footer with last file and recent logs."""
        lines = []

        # Last file info
        if self._stats.last_file:
            lf = self._stats.last_file
            lines.append(
                f"[bold]📄 Last file:[/bold] {lf.filename} "
                f"([cyan]{lf.tick_count:,} ticks[/cyan])"
            )
        else:
            lines.append("[dim]📄 No files created yet[/dim]")

        lines.append("")  # Spacer

        # Recent logs section
        total_logs = len(self._stats.recent_logs)
        if total_logs > 0:
            # Show last N logs
            show_logs = self._stats.recent_logs[-self._max_log_lines:]
            hidden_count = total_logs - len(show_logs)

            if hidden_count > 0:
                lines.append(f"[dim]... ({hidden_count} more)[/dim]")

            for entry in show_logs:
                time_str = entry.timestamp.strftime("%H:%M:%S")
                if entry.level == "ERROR":
                    level_color = "red"
                elif entry.level == "WARNING":
                    level_color = "yellow"
                else:
                    level_color = "white"

                # Truncate long messages
                msg = entry.message
                if len(msg) > 60:
                    msg = msg[:57] + "..."
                # A log line is text, not markup. Unescaped, a message holding
                # a closing-tag shape like "[/x]" raises MarkupError inside the
                # render - and until 2026-09-19 no entry had ever reached this
                # code, because nothing filled recent_logs.
                msg = escape(msg)

                lines.append(
                    f"[dim]{time_str}[/dim] "
                    f"[{level_color}]{entry.level}[/{level_color}] "
                    f"{msg}"
                )
        else:
            lines.append("[dim]No errors or warnings[/dim]")

        return Text.from_markup("\n".join(lines))

    def _format_last_tick(self, last: Optional[datetime]) -> str:
        """
        Render how long ago a symbol last produced a tick.

        Replaces a boolean "active within 30 s" with the measurement behind it:
        a quiet symbol and a dead one are both "not recent", and only the
        elapsed time tells them apart.

        Args:
            last: Timestamp of the last tick, or None

        Returns:
            Rich-markup string
        """
        if last is None:
            return "[dim]⏳ waiting[/dim]"

        secs = (datetime.now(timezone.utc) - last).total_seconds()

        if secs < 30:
            return f"[green]✅ {secs:.0f}s[/green]"
        if secs < 300:
            return f"[yellow]⸻ {secs:.0f}s[/yellow]"
        if secs < 3600:
            return f"[yellow]⸻ {secs / 60:.0f}m[/yellow]"
        return f"[red]⚠ {secs / 3600:.1f}h[/red]"

    def _digits_for(self, symbol: str) -> Optional[int]:
        """
        Decimal places for a symbol, as the collector reported them.

        Two fixed places rendered ADAUSD's 0.2103 / 0.2104 as 0.21 and 0.21 -
        a display asserting bid == ask where the book has a spread. The same
        mistake the data had in spread_points, one layer up. So an unknown
        precision is `None` and not a default: rounding to a number nobody
        stated is how the first version of this printed a spread of zero.

        Args:
            symbol: Normalized symbol

        Returns:
            Digit count, or None when the collector did not state one
        """
        stats = self._stats.symbols.get(symbol)
        return stats.digits if stats is not None else None

    @staticmethod
    def _format_price(value: float, digits: Optional[int]) -> str:
        """
        Render a price at the instrument's precision, or exactly as measured.

        Args:
            value: The price
            digits: Decimal places, or None when none was stated

        Returns:
            The formatted price, "-" when there is no price yet
        """
        if value <= 0:
            return "-"
        if digits is None:
            return f"{value:,}"
        return f"{value:,.{digits}f}"

    def _format_quote_age(self, age_ms: Optional[int]) -> str:
        """
        Render the age of the quote a spread was taken from.

        None means no quote had been observed - distinct from a fresh one, and
        the reason the field is nullable at all. Colour marks the threshold at
        which a quoted spread stops describing the current book.

        Args:
            age_ms: Age in milliseconds, or None

        Returns:
            Rich-markup string
        """
        if age_ms is None:
            return "[dim]no quote[/dim]"
        if age_ms < 250:
            return f"[green]{age_ms} ms[/green]"
        if age_ms < 1000:
            return f"[yellow]{age_ms} ms[/yellow]"
        return f"[red]{age_ms / 1000:.1f} s[/red]"

    def _format_uptime(self, seconds: float) -> str:
        """
        Format uptime as HH:MM:SS.

        Args:
            seconds: Uptime in seconds

        Returns:
            Formatted string
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
