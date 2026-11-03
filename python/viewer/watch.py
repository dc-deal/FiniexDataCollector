"""
FiniexDataCollector - The viewer's main loop
Polls one collector and draws it with the renderer the collector used to run itself.

Two loops, deliberately. The feed reads at its own pace, which is set by where the
collector is; the screen redraws once a second regardless, so the age of the last
reading keeps counting up in front of the operator instead of freezing along with
everything else. A frozen screen that looks frozen is the whole point.

It holds no state the collector can be affected by, issues nothing but GETs, and
exits on Ctrl+C. Killing it at any moment costs nothing, which is what makes it
safe to leave open on a desk.

Location: python/viewer/watch.py
"""

import asyncio
from typing import Optional

from python.types.collector_stats import CollectorStats
from python.utils.live_display import LiveDisplay
from python.viewer.status_feed import StatusFeed

# The screen redraws at this rate whatever the feed does. It is what keeps "no
# answer since ..." counting while nothing is arriving.
REDRAW_INTERVAL_SECONDS = 1.0


async def run_viewer(base_url: str, token: str, interval_seconds: float,
                     max_log_lines: int = 5) -> None:
    """
    Watch one collector until interrupted.

    Args:
        base_url: The collector's base URL
        token: Bearer token carrying `status:detail`
        interval_seconds: Seconds between readings
        max_log_lines: Log lines to show in the footer
    """
    feed = StatusFeed(base_url, token, interval_seconds)

    # An empty object, so the screen exists before the first answer does. It
    # renders as a collector that has seen nothing, under a red frame that says
    # no reading has arrived - which is exactly the truth at that moment.
    display = LiveDisplay(CollectorStats(),
                          update_interval=REDRAW_INTERVAL_SECONDS,
                          max_log_lines=max_log_lines,
                          feed=feed.state)

    def show(stats: CollectorStats) -> None:
        display.stats = stats

    await display.start()
    poller: Optional[asyncio.Task] = asyncio.create_task(feed.run(show))

    try:
        await poller
    except asyncio.CancelledError:
        pass
    finally:
        poller.cancel()
        await display.stop()
