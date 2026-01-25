"""
FiniexDataCollector - Live Display
Real-time terminal display for collection monitoring using rich.

Features:
- Live updating table with per-symbol stats
- WebSocket connection status
- Stream configuration display
- Recent errors/warnings with count
- Last file info
- Uptime and totals

Location: python/utils/live_display.py
"""

import asyncio
from datetime import datetime
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
            max_log_lines: Maximum log lines to show (rest shown as "...")
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
                    # Update tick rates
                    self._stats.calculate_ticks_per_minute()

                    # Render
                    live.update(self._render())

                    # Wait
                    await asyncio.sleep(self._update_interval)

                except asyncio.CancelledError:
                    break
                except Exception as e:
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
        symbol_table = self._build_symbol_table()
        footer = self._build_footer()

        # Combine
        layout.split_column(
            Layout(header, name="header", size=3),
            Layout(symbol_table, name="symbols"),
            Layout(footer, name="footer", size=8)
        )

        return Panel(
            layout,
            title="[bold cyan]📡 FiniexDataCollector Live[/bold cyan]",
            border_style="cyan",
            box=box.ROUNDED
        )

    def _build_header(self) -> Text:
        """Build header with uptime, totals, and stream info."""
        uptime = self._format_uptime(self._stats.get_uptime_seconds())

        # WebSocket status with color
        ws_status = self._stats.websocket_status
        if ws_status == "connected":
            ws_display = "[green]● connected[/green]"
        elif ws_status == "reconnecting":
            ws_display = "[yellow]◐ reconnecting[/yellow]"
        else:
            ws_display = "[red]○ disconnected[/red]"

        # Streams display
        streams_str = ", ".join(self._streams)

        header = (
            f"[bold]📋 Streams:[/bold] [magenta]{streams_str}[/magenta] │ "
            f"[bold]⏱️ Uptime:[/bold] {uptime} │ "
            f"[bold]📊 Ticks:[/bold] {self._stats.total_ticks:,} │ "
            f"[bold]📁 Files:[/bold] {self._stats.total_files} │ "
            f"[bold]🔌 WS:[/bold] {ws_display} │ "
            f"[bold]⚠️ Errors:[/bold] [red]{self._stats.total_errors}[/red]"
        )

        return Text.from_markup(header)

    def _build_symbol_table(self) -> Table:
        """Build per-symbol statistics table."""
        table = Table(
            show_header=True,
            header_style="bold cyan",
            box=box.SIMPLE,
            padding=(0, 1)
        )

        # Columns - adapt based on stream type
        table.add_column("Symbol", width=10)
        table.add_column("Ticks", justify="right", width=10)
        table.add_column("Ticks/min", justify="right", width=10)

        # For trade streams, show Price instead of Bid/Ask
        if "trade" in self._streams and "ticker" not in self._streams:
            table.add_column("Last Price", justify="right", width=14)
            table.add_column("Volume", justify="right", width=12)
        else:
            table.add_column("Bid", justify="right", width=12)
            table.add_column("Ask", justify="right", width=12)
            table.add_column("Spread %", justify="right", width=10)

        table.add_column("Files", justify="right", width=6)
        table.add_column("Status", width=12)

        # No symbols yet
        if not self._stats.symbols:
            table.add_row(
                "[dim]Waiting...[/dim]", "", "", "", "", "", ""
            )
            return table

        # Add rows for each symbol
        for symbol, stats in sorted(self._stats.symbols.items()):
            # Status indicator
            if stats.is_active:
                status = "[green]✅ Active[/green]"
            elif stats.ticks_count > 0:
                status = "[yellow]⏸️ Idle[/yellow]"
            else:
                status = "[dim]⏳ Waiting[/dim]"

            # Format based on stream type
            if "trade" in self._streams and "ticker" not in self._streams:
                # Trade stream: show last price and volume
                price_str = f"{stats.last_bid:,.2f}" if stats.last_bid > 0 else "-"
                # For trades, last_ask is same as last_bid, use spread_pct field for volume display
                vol_str = f"{stats.last_volume:.4f}" if stats.last_volume > 0 else "-"

                table.add_row(
                    f"[bold]{symbol}[/bold]",
                    f"{stats.ticks_count:,}",
                    f"{stats.ticks_per_minute:.1f}",
                    price_str,
                    vol_str,
                    str(stats.file_count),
                    status
                )
            else:
                # Ticker stream: show bid/ask/spread
                bid_str = f"{stats.last_bid:,.2f}" if stats.last_bid > 0 else "-"
                ask_str = f"{stats.last_ask:,.2f}" if stats.last_ask > 0 else "-"
                spread_str = f"{stats.last_spread_pct:.4f}" if stats.last_spread_pct > 0 else "-"

                table.add_row(
                    f"[bold]{symbol}[/bold]",
                    f"{stats.ticks_count:,}",
                    f"{stats.ticks_per_minute:.1f}",
                    bid_str,
                    ask_str,
                    spread_str,
                    str(stats.file_count),
                    status
                )

        return table

    def _build_footer(self) -> Text:
        """Build footer with last file and recent logs."""
        lines = []

        # Last file info
        if self._stats.last_file:
            lf = self._stats.last_file
            lines.append(
                f"[bold]📄 Last file:[/bold] {lf.filename} "
                f"([cyan]{lf.tick_count+1:,} ticks[/cyan])"
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
