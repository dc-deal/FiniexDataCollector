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
from datetime import datetime, timezone
from typing import Optional, List

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box

from python.types.collector_stats import CollectorStats


class LiveDisplay:
    """
    Live terminal display for FiniexDataCollector.

    Uses rich.Live for flicker-free updates.
    Runs in async loop alongside collector.
    """

    def __init__(
        self,
        stats: CollectorStats,
        streams: List[str] = None,
        update_interval: float = 1.0,
        max_log_lines: int = 5
    ):
        """
        Initialize live display.

        Args:
            stats: Shared CollectorStats object
            streams: List of active streams (e.g., ["ticker"], ["trade"])
            update_interval: Display update interval in seconds
            max_log_lines: Maximum log lines to show
        """
        self._stats = stats
        self._streams = streams if streams else ["ticker"]
        self._update_interval = update_interval
        self._max_log_lines = max_log_lines
        self._running = False
        self._console = Console()
        self._live: Optional[Live] = None
        self._task: Optional[asyncio.Task] = None

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
                    # Render
                    live.update(self._render())

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

        return Panel(
            layout,
            title="[bold cyan]📡 FiniexDataCollector Live[/bold cyan]",
            border_style="cyan",
            box=box.ROUNDED
        )

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
        streams_str = ", ".join(self._streams)

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

        return Text.from_markup(f"{line1}\n{line2}")

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

        table.add_column("Status", width=12)

        # No symbols yet
        if not self._stats.symbols:
            table.add_row(
                "[dim]Waiting...[/dim]", "", "", "", "", "", "", ""
            )
            return table

        # Add rows for each symbol
        for symbol, stats in sorted(self._stats.symbols.items()):
            # Status indicator
            if stats.is_active:
                status = "[green]✅ Active[/green]"
            elif stats.current_file_ticks > 0:
                status = "[yellow]⸻ Idle[/yellow]"
            else:
                status = "[dim]⏳ Waiting[/dim]"

            # File progress
            from python.utils.config_loader import load_config
            try:
                config = load_config()
                max_ticks = config.kraken.max_ticks_per_file
            except:
                max_ticks = 50000

            percent = (stats.current_file_ticks /
                       max_ticks * 100) if max_ticks > 0 else 0
            file_progress = f"{stats.current_file_ticks:,} / {max_ticks:,} ({percent:.0f}%)"

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
                price_str = f"{stats.last_bid:,.2f}" if stats.last_bid > 0 else "-"
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
                bid_str = f"{stats.last_bid:,.2f}" if stats.last_bid > 0 else "-"
                ask_str = f"{stats.last_ask:,.2f}" if stats.last_ask > 0 else "-"
                spread_str = f"{stats.last_spread_pct:.4f}" if stats.last_spread_pct > 0 else "-"

                table.add_row(
                    f"[bold]{symbol}[/bold]",
                    start_str,
                    file_progress,
                    files_display,
                    bid_str,
                    ask_str,
                    spread_str,
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
            duration = int(last.duration_seconds / 60)
            time_str = last.reconnected_at.strftime(
                "%a %d.%m %H:%M") if last.reconnected_at else "unknown"
            parts.append(
                f"Reconnects: {reconnect_count} (Last: {time_str}, {duration}m down)")
        else:
            parts.append(f"Reconnects: {reconnect_count}")

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

                lines.append(
                    f"[dim]{time_str}[/dim] "
                    f"[{level_color}]{entry.level}[/{level_color}] "
                    f"{msg}"
                )
        else:
            lines.append("[dim]No errors or warnings[/dim]")

        return Text.from_markup("\n".join(lines))

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
