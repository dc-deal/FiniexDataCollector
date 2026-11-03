"""
FiniexDataCollector - Kraken WebSocket Client
WebSocket client for Kraken v2 API with automatic reconnection.

A drop costs the trades Kraken makes while this process is not listening, so
the client's first job after a drop is to notice it. Measured 2026-10-08 over
19 days of production logs: in 136 of 137 forced reconnects the socket was
already closing when the 10 s silence watchdog found it. The end of the receive
loop was handed back by gather(..., return_exceptions=True) and ignored, and a
close frame that leaves the TCP connection open does not end recv() at all until
the next keepalive ping, 20 s later. So the connection now ends at whichever
comes first - the receive loop ending, a close frame on the connection's state,
or silence - and each drop is followed through an OutageTracker into one record
of what it cost.

Location: python/collectors/kraken/websocket_client.py
"""

import asyncio
import json
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import certifi
import websockets
from websockets.exceptions import ConnectionClosed

from python.collectors.base import AbstractCollector
from python.collectors.kraken.message_parser import KrakenMessageParser
from python.collectors.kraken.outage_tracker import (Detection, OutageCallback,
                                                     OutageTracker,
                                                     classify_close,
                                                     close_code_of)
from python.collectors.kraken.quote_cache import QuoteCache
from python.exceptions.collector_exceptions import (WebSocketConnectionError,
                                                    WebSocketSubscriptionError)
from python.types.broker_config_types import normalize_symbol, to_kraken_format
from python.utils.collection_clock import CollectionClock
from python.utils.logging_setup import describe_exception, get_collector_logger

# What makes Kraken push a ticker update. The API default is "trades", which
# ties the quote to the trade stream: the cache then only refreshes when someone
# trades, and a trade tick reads a quote as old as the gap since the last one -
# measured at a median of 464 ms and over a second for a third of all ticks.
# "bbo" pushes whenever the best bid or offer moves, which is what the quote on a
# trade tick is supposed to describe. quote_age_ms is the instrument that shows
# the difference, so this is a measured setting, not a guessed one.
TICKER_EVENT_TRIGGER = "bbo"

# How often the feed's silence is judged. Kraken sends a heartbeat every second
# on a subscribed connection - measured 2026-09-19 on a quiet pair, the largest
# gap between any two messages 1.03 s over 45 s - so a live feed is never quiet
# for long, and checking every second costs nothing. Checking only every
# `stale_after` seconds, as before, added up to a whole interval to detection.
STALE_CHECK_INTERVAL = 1.0

# A check that wakes this much later than it asked to was not sleeping: the
# event loop was blocked - a file closing, the midnight cascade, 17.85 s
# measured on production 2026-09-19. The silence it would see is then ours, not
# the feed's, because the receive loop has not had its turn to read what
# arrived. Judged anyway, it would force a reconnect at every day cut.
LATE_WAKE_TOLERANCE = 1.0

# How long a subscription may go unanswered before the client acts. Acting
# means listing the pairs and carrying on - their data counts as the answer
# whenever it comes - and reconnecting only when a whole stream produced
# nothing at all. Kraken answers in tens of milliseconds (probe 2026-10-08), so
# a slow answer is a degraded Kraken, and killing every attempt over it would
# turn a working feed into an outage.
SUBSCRIBE_DEADLINE_SECONDS = 10.0

# A restored connection must stay up this long before the backoff starts over.
# Resetting at the handshake, as until 2026-10-08, kept the delay at 1 s for a
# far side that accepts and closes again; with the drop now noticed at once that
# is about 150 attempts in ten minutes - simulated 2026-10-09 with the 2.3 s
# handshake measured against Kraken - at the Cloudflare limit Kraken documents,
# with a ten-minute ban beyond it. The 10 s silence watchdog used to hide this
# by accident. Ten seconds keeps even a far side that closes just past the
# window at 45 attempts. Much longer costs data instead: when a degraded Kraken
# drops connections every 20-50 s, a 60 s window climbs to the 60 s maximum
# delay and stays there - 62 % offline in the same simulation, against 9 %.
STABLE_CONNECTION_SECONDS = 10.0


def _received_close(websocket: Any) -> Any:
    """
    The close frame the far side sent on a connection, if any.

    Read straight off the protocol state: `close_code` would answer 1006 for a
    connection that ended without any frame - a code nobody sent - and is None
    in the legacy implementation until the connection is fully closed.

    Args:
        websocket: A connection from either websockets implementation

    Returns:
        The received Close frame, or None
    """
    protocol = getattr(websocket, "protocol", None)
    if protocol is not None and hasattr(protocol, "close_rcvd"):
        return protocol.close_rcvd
    return getattr(websocket, "close_rcvd", None)


def _abort(websocket: Any) -> None:
    """
    Drop a connection without waiting for a closing handshake.

    A connection being given up on has either been closed by the far side
    already or is not answering; a polite close() waits out its close timeout
    in both cases - up to 5 s of trades, measured on production on a dead link.

    Args:
        websocket: A connection from either websockets implementation
    """
    transport = getattr(websocket, "transport", None)
    if transport is None:
        return
    try:
        transport.abort()
    except Exception:
        pass  # Already gone; nothing left to drop.


class KrakenWebSocketClient(AbstractCollector):
    """
    Kraken WebSocket v2 ticker/trade collector.

    Features:
    - Automatic reconnection with exponential backoff
    - Drop detection by far-side close, close frame and silence
    - One outage record per interruption, from last message to restored feed
    - Multi-symbol subscription
    - Configurable streams (ticker, trade)
    - Graceful shutdown
    """

    DEFAULT_URL = "wss://ws.kraken.com/v2"
    VALID_STREAMS = {"ticker", "trade"}

    def __init__(
        self,
        symbols: List[str],
        clock: CollectionClock,
        quote_cache: QuoteCache,
        streams: List[str] = None,
        url: str = DEFAULT_URL,
        reconnect_initial_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        stale_after: float = 10.0
    ):
        """
        Initialize Kraken WebSocket client.

        Args:
            symbols: List of symbols to subscribe (e.g., ["BTC/USD", "ETH/USD"])
            clock: Session clock handed to the parser, which stamps collected_msc
            quote_cache: Last bid/ask per symbol, filled from the ticker channel
                and stated on every trade tick
            streams: List of streams to subscribe (e.g., ["ticker"], ["trade"], ["ticker", "trade"])
            url: WebSocket URL
            reconnect_initial_delay: Initial reconnect delay in seconds
            reconnect_max_delay: Maximum reconnect delay in seconds
            stale_after: Seconds without any message, heartbeats included,
                after which the connection is closed and reopened
        """
        super().__init__(name="kraken", symbols=symbols)

        self._url = url
        self._streams = streams if streams else ["ticker"]
        self._reconnect_initial_delay = reconnect_initial_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._stale_after = stale_after

        # The time sources, the sleep and the opener are attributes so a test
        # can drive all of them without patching the module-wide ones the event
        # loop itself runs on.
        self._monotonic: Callable[[], float] = time.monotonic
        self._sleep: Callable[[float], Any] = asyncio.sleep
        self._wall: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
        self._open: Callable[..., Any] = websockets.connect

        # Validate streams
        for stream in self._streams:
            if stream not in self.VALID_STREAMS:
                raise ValueError(
                    f"Invalid stream '{stream}'. Valid: {self.VALID_STREAMS}")

        self._websocket: Optional[Any] = None
        self._parser = KrakenMessageParser(clock, quote_cache)
        self._logger = get_collector_logger("kraken")
        self._tracker = OutageTracker(
            [normalize_symbol(symbol) for symbol in symbols], self._streams,
            wall=lambda: self._wall())

        self._connection_status = "disconnected"
        # Wall clock for reporting when, monotonic for measuring how long: a
        # silence derived from two wall-clock readings turns every forward NTP
        # step into a dead connection.
        self._last_message_time: Optional[datetime] = None
        self._last_message_monotonic: Optional[float] = None
        self._reconnect_attempt = 0
        self._should_reconnect = True

        # Kraken echoes req_id on every answer, which is how an answer is
        # matched to the stream it belongs to - its result names the channel
        # only on success.
        self._req_streams: Dict[int, str] = {}
        self._req_counter = 0
        self._deadline_handled = False
        self._diagnostic_errors: Set[str] = set()

        # SSL context with certifi certificates (cross-platform)
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())

        # Callbacks
        self._status_callback: Optional[Callable[[str], None]] = None

        # Tasks
        self._receive_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    def set_status_callback(self, callback: Callable[[str], None]) -> None:
        """
        Set callback for connection status changes.

        Args:
            callback: Function called with status string (connected, disconnected, reconnecting)
        """
        self._status_callback = callback

    def set_outage_callback(self, callback: OutageCallback) -> None:
        """
        Set callback for the stages of each outage record.

        Args:
            callback: Called with ('opened' | 'restored' | 'resolved', event)
        """
        self._tracker.set_callback(callback)

    def _set_status(self, status: str) -> None:
        """
        Update connection status and notify callback.

        Args:
            status: New status string
        """
        self._connection_status = status
        if self._status_callback:
            try:
                self._status_callback(status)
            except Exception:
                pass  # Don't crash on callback errors

    def _guard(self, function: Callable[..., Any], *args: Any) -> Any:
        """
        Run one diagnostic step so that its failure can never end the feed.

        Each distinct failure is logged once: a broken diagnostic on the hot
        path would otherwise write one line per message.

        Args:
            function: The step to run
            *args: Its arguments

        Returns:
            Its result, or None when it raised
        """
        try:
            return function(*args)
        except Exception as error:
            text = describe_exception(error)
            if text not in self._diagnostic_errors and \
                    len(self._diagnostic_errors) < 100:
                self._diagnostic_errors.add(text)
                self._logger.error(
                    f"Outage diagnostics failed in "
                    f"{getattr(function, '__name__', 'a step')}: {text} - "
                    f"collection continues")
            return None

    async def connect(self) -> bool:
        """
        Establish WebSocket connection.

        Returns:
            True if connection successful, False when a stop arrived while the
            handshake was pending

        Raises:
            WebSocketConnectionError: The connection could not be opened; the
                original error is chained as its cause
        """
        self._logger.info(f"Connecting to {self._url}...")
        try:
            websocket = await self._open(
                self._url,
                ssl=self._ssl_context,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5
            )
        except Exception as e:
            # A handshake still pending when stop() ran fails after it; 'failed'
            # would then overwrite the 'disconnected' the stop already set, and
            # an ERROR would count a failure of a collector that had stopped.
            if self._should_reconnect:
                self._set_status("failed")
                self._logger.error(
                    f"Connection failed: {describe_exception(e)}")
            else:
                self._logger.info(
                    f"Connection attempt ended after the stop: "
                    f"{describe_exception(e)}")
            raise WebSocketConnectionError(
                message=str(e),
                url=self._url,
                attempt=self._reconnect_attempt
            ) from e

        if not self._should_reconnect:
            # stop() ran while the handshake was pending. This socket belongs to
            # a collector that no longer runs; leaving it open would also leave
            # the status at 'connected' after shutdown.
            _abort(websocket)
            return False

        self._websocket = websocket
        mono = self._monotonic()
        self._last_message_time = self._wall()
        self._last_message_monotonic = mono
        self._guard(self._tracker.handshake, mono)

        self._set_status("connected")
        self._logger.info("WebSocket connected successfully")
        return True

    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self._should_reconnect = False
        self._set_status("disconnecting")

        # Cancel tasks
        if self._receive_task:
            self._receive_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        # Close WebSocket
        if self._websocket:
            try:
                await self._websocket.close()
            except Exception as e:
                self._logger.warning(
                    f"Error closing WebSocket: {describe_exception(e)}")

        self._websocket = None
        self._set_status("disconnected")
        self._is_running = False
        self._logger.info("Disconnected from Kraken WebSocket")

    async def subscribe(self) -> bool:
        """
        Send the subscription requests for every configured stream.

        The answers are NOT read here. Until 2026-10-08 this read one message per
        stream and kept it only if it looked like an acknowledgement: Kraken's
        first message on a connection is a status message, so the first read
        discarded it unseen and the second labelled the first TRADE answer as
        the ticker's - 'Subscription confirmed: ticker' in 169 of 169
        subscription cycles on production, 'trade' in none. The receive loop
        reads every answer now and matches it by req_id.

        Returns:
            True when every request was sent

        Raises:
            WebSocketSubscriptionError: A request could not be sent; its
                `detection` attribute says how the connection ended
        """
        if not self._websocket:
            return False

        # Convert symbols to Kraken format
        kraken_symbols = [to_kraken_format(s) for s in self._symbols]
        self._req_streams = {}

        for stream in self._streams:
            self._req_counter += 1
            req_id = self._req_counter
            self._req_streams[req_id] = stream

            params: Dict[str, Any] = {
                "channel": stream,
                "symbol": kraken_symbols
            }
            if stream == "ticker":
                params["event_trigger"] = TICKER_EVENT_TRIGGER

            subscribe_msg = {
                "method": "subscribe",
                "params": params,
                "req_id": req_id
            }

            try:
                await self._websocket.send(json.dumps(subscribe_msg))
            except Exception as e:
                # A send refused because the connection ended is a drop, which
                # the 'Connection ended' line reports with its code; only a send
                # that failed on a live connection is an error of its own.
                if not isinstance(e, ConnectionClosed):
                    self._logger.error(
                        f"Subscription failed for {stream}: "
                        f"{describe_exception(e)}")
                error = WebSocketSubscriptionError(
                    message=str(e),
                    channel=stream,
                    symbols=kraken_symbols
                )
                # A send refused because the far side closed is that close,
                # with its code - not a failed send.
                error.detection = (
                    classify_close(e) if isinstance(e, ConnectionClosed)
                    else Detection("subscribe_send_failed",
                                   exception=describe_exception(e)))
                raise error from e

            self._logger.info(
                f"Subscription request sent: {stream} for {kraken_symbols}")

        self._guard(self._tracker.subscriptions_sent, self._monotonic())
        return True

    async def start(self) -> None:
        """
        Start tick collection.

        One iteration is one connection: open it, subscribe, read until it ends,
        tear it down. Everything inside an iteration is under the catch-all -
        the diagnostics included - so that no failure of anything but a stop
        can end collection; the next attempt follows as it would after a drop.
        """
        self._is_running = True
        self._should_reconnect = True
        self._start_time = self._wall()

        self._logger.info(
            f"Starting Kraken collector for {len(self._symbols)} symbols, "
            f"streams: {self._streams}"
        )

        while self._should_reconnect:
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._logger.error(
                    f"Unexpected error: {describe_exception(e)}")
                self._guard(self._tracker.attempt_failed,
                            f"unexpected: {describe_exception(e)}")
                self._drop_socket()

            if not self._should_reconnect:
                break

            try:
                await self._backoff()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Never a reason to stop collecting; wait the longest delay
                # rather than spin.
                self._logger.error(
                    f"Unexpected error: {describe_exception(e)}")
                await asyncio.sleep(self._reconnect_max_delay)

    async def _run_connection(self) -> None:
        """Open one connection, read it until it ends, and tear it down."""
        self._guard(self._tracker.attempt_started)

        try:
            connected = await self.connect()
        except WebSocketConnectionError as e:
            # connect() has logged the failure; a second ERROR line for the same
            # attempt would count every failed attempt twice in total_errors.
            reason = e.__cause__ if e.__cause__ is not None else e
            self._guard(self._tracker.attempt_failed,
                        f"connect failed: {describe_exception(reason)}")
            return
        if not connected:
            return

        try:
            await self.subscribe()
        except WebSocketSubscriptionError as e:
            if not self._should_reconnect:
                # stop() ran while a send was waiting on a closing connection.
                # The open record was handed over as 'shutdown'; reporting this
                # end would open a new one that nothing will ever close.
                self._drop_socket()
                return
            detection = getattr(e, "detection", None) or Detection(
                "subscribe_send_failed", exception=describe_exception(e))
            self._end_connection(detection, self._monotonic())
            return

        self._deadline_handled = False
        self._receive_task = asyncio.create_task(self._receive_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        detection, detected_mono = await self._wait_for_end()
        if detection is None or not self._should_reconnect:
            return
        self._end_connection(detection, detected_mono)

    async def _wait_for_end(self) -> Tuple[Optional[Detection], float]:
        """
        Wait until the receive loop or the watchdog reports the end.

        Returns:
            (how the connection ended, monotonic reading at detection); the
            detection is None when a stop ended it
        """
        receive, watchdog = self._receive_task, self._heartbeat_task
        tasks = {receive, watchdog}
        detected_mono = self._monotonic()
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            detected_mono = self._monotonic()
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            # asyncio.wait never raises a child's CancelledError here, and it
            # lets this task's own cancellation through - suppressing the
            # child's would swallow ours, and shutdown would hang on it.
            await asyncio.wait(tasks)

        outcomes: Dict[asyncio.Task, Detection] = {}
        for task in tasks:
            if task.cancelled():
                continue
            # Retrieved for every finished task, so that nothing is ever logged
            # as 'Task exception was never retrieved'.
            error = task.exception()
            if error is not None:
                outcomes[task] = Detection(
                    "unexpected", exception=describe_exception(error))
            elif task.result() is not None:
                outcomes[task] = task.result()

        if not self._should_reconnect:
            return None, detected_mono

        # A tie goes to the receive loop: it carries the close frame.
        detection = outcomes.get(receive) or outcomes.get(watchdog)
        if detection is None:
            detection = Detection("unexpected",
                                  exception="neither detector reported an end")
        return detection, detected_mono

    def _end_connection(self, detection: Detection, detected_mono: float) -> None:
        """
        Record how a connection ended and drop it.

        Args:
            detection: How it ended
            detected_mono: Monotonic reading at detection
        """
        websocket = self._websocket
        # Decided before the frame renames the end: a detector that raised is a
        # fault of this code whatever Kraken sent meanwhile.
        own_failure = detection.kind == "unexpected"
        fault = detection.exception

        # A detector without the frame - the silence watchdog, a failed send -
        # can fire after a close frame did arrive. Named from the frame, as
        # whatever was received before the connection is dropped here.
        if detection.kind != "far_side_close" and websocket is not None:
            frame = _received_close(websocket)
            if frame is not None:
                detection = Detection("far_side_close", close_code_of(frame),
                                      frame.reason, detection.exception,
                                      detection.cause)

        self._guard(self._tracker.connection_ended, detection, detected_mono)
        self._set_status("reconnecting")

        if detection.kind != "silence_watchdog":
            # The watchdog writes its own ERROR line; every other end gets one
            # line here, so the log names how each connection ended. A drop is
            # a warning; an end the code itself caused - a detector that
            # raised - is an error, and counts as one.
            since = ""
            if self._last_message_monotonic is not None:
                since = (f", {detected_mono - self._last_message_monotonic:.1f}s"
                         f" after the last message")
            what = detection.describe()
            if own_failure and detection.kind != "unexpected":
                what += f", while: {fault}"
            log = self._logger.error if own_failure else self._logger.warning
            log(f"Connection ended: {what}{since} - reconnecting")

        started = self._monotonic()
        self._drop_socket()
        self._guard(self._tracker.teardown,
                    (self._monotonic() - started) * 1000)

    def _drop_socket(self) -> None:
        """Forget the current connection, aborting it if it is still open."""
        websocket, self._websocket = self._websocket, None
        if websocket is not None:
            _abort(websocket)

    async def _backoff(self) -> None:
        """Wait before the next attempt, doubling while connections stay short."""
        delay = self._get_reconnect_delay()
        self._logger.info(f"Reconnecting in {delay:.1f}s...")
        started = self._monotonic()
        await self._sleep(delay)
        self._guard(self._tracker.backoff, self._monotonic() - started)
        self._reconnect_attempt += 1

    async def stop(self) -> None:
        """Stop tick collection gracefully."""
        self._logger.info("Stopping Kraken collector...")
        self._should_reconnect = False
        # Before anything is cancelled: start() is not awaited at shutdown, so a
        # record handed over on its exit path would never be written.
        self._guard(self._tracker.stop)
        await self.disconnect()

    async def _receive_loop(self) -> Detection:
        """
        Read every message of one connection until it ends.

        Returns:
            How the connection ended
        """
        websocket = self._websocket
        if websocket is None:
            return Detection("unexpected", exception="no connection to read")

        while True:
            try:
                raw = await websocket.recv()
            except ConnectionClosed as error:
                return classify_close(error)

            mono = self._monotonic()
            self._last_message_time = self._wall()
            self._last_message_monotonic = mono
            self._guard(self._tracker.message, mono)
            try:
                self._dispatch(raw, mono)
            except Exception as e:
                # A message that cannot be handled costs that message, as it
                # always did - not the connection. Uncontained, a format Kraken
                # changed in its status message would end every connection on
                # its first message and hold the backoff at its maximum.
                self._logger.warning(
                    f"Message handling failed: {describe_exception(e)} - "
                    f"message skipped")

    def _dispatch(self, raw: Any, mono: float) -> None:
        """
        Decode one message once and hand it to whoever it is for.

        Args:
            raw: The message as received
            mono: Monotonic reading when it was read
        """
        try:
            data = json.loads(raw)
        except (ValueError, TypeError) as e:
            self._logger.warning(
                f"Message parse error: {describe_exception(e)}")
            return

        if not isinstance(data, dict):
            return

        channel = data.get("channel")
        if channel == "heartbeat":
            return
        if channel == "status":
            self._on_status_message(data)
            return

        method = data.get("method")
        if method == "subscribe":
            self._on_subscribe_answer(data, mono)
            return
        if method is not None or channel not in ("trade", "ticker"):
            return

        symbols = [normalize_symbol(item["symbol"])
                   for item in data.get("data") or []
                   if isinstance(item, dict) and item.get("symbol")]
        self._announce_restoration(
            self._guard(self._tracker.data_seen, channel, symbols, mono))

        try:
            ticks = self._parser.parse_decoded(data)
        except Exception as e:
            self._logger.warning(
                f"Message parse error: {describe_exception(e)}")
            return

        for tick in ticks or []:
            warning = self._guard(self._tracker.trade, tick.symbol,
                                  tick.trade_id, tick.time_msc, mono)
            if warning:
                self._logger.warning(warning)
            try:
                self._emit_tick(tick)
            except Exception as e:
                # Its own line: until 2026-10-08 a failing tick handler was
                # reported as a 'Message parse error' and took the rest of the
                # message with it.
                self._logger.error(
                    f"Tick handler failed for {tick.symbol}: "
                    f"{describe_exception(e)}")

    def _on_status_message(self, data: Dict[str, Any]) -> None:
        """
        Keep what Kraken announces about the connection.

        Args:
            data: The decoded status message
        """
        for item in data.get("data") or []:
            if not isinstance(item, dict):
                continue
            system = str(item.get("system") or "")
            connection_id = item.get("connection_id")
            self._guard(self._tracker.status,
                        str(connection_id) if connection_id is not None else "",
                        system)
            if system and system != "online":
                self._logger.warning(
                    f"Kraken reports system={system} on this connection")

    def _on_subscribe_answer(self, data: Dict[str, Any], mono: float) -> None:
        """
        Match one subscription answer to its stream and pair.

        Args:
            data: The decoded answer
            mono: Monotonic reading when it was read
        """
        result = data.get("result") if isinstance(
            data.get("result"), dict) else {}
        stream = self._req_streams.get(
            data.get("req_id")) or result.get("channel")
        if stream not in self._streams:
            return

        symbol = result.get("symbol") or data.get("symbol")
        error = None
        if not data.get("success"):
            error = str(data.get("error") or "refused")
            self._logger.error(
                f"Kraken refused the {stream} subscription for "
                f"{symbol or 'every symbol'}: {error} - the other symbols "
                f"continue")

        self._announce_restoration(self._guard(
            self._tracker.subscription_answered, stream,
            normalize_symbol(symbol) if symbol else None, error, mono))

    def _announce_restoration(self, completed: Optional[str]) -> None:
        """
        Write the line that says the feed is fully subscribed.

        Args:
            completed: 'startup' or 'restored' from the tracker; anything else
                writes nothing
        """
        if completed not in ("startup", "restored"):
            return
        text = self._guard(self._tracker.confirmation_text) or ""
        if completed == "startup":
            self._logger.info(f"Subscriptions confirmed: {text}")
            return

        event = self._tracker.last_restored
        if event is None or event.duration_seconds is None:
            return
        attempts = event.attempts
        self._logger.info(
            f"Feed restored {event.duration_seconds:.1f}s after the last "
            f"message: {text} ({event.reason}, {attempts} attempt"
            f"{'' if attempts == 1 else 's'})")

    async def _heartbeat_loop(self) -> Optional[Detection]:
        """
        Watch one connection for the ends the receive loop does not report.

        Detection is what a drop costs: the trades Kraken makes while a dead
        socket is still believed alive are never received. Measured on
        production 2026-09-18, four drops lost 537 trades at 41-51 s of silence
        each on the liquid pairs, of which the old check - every 10 s,
        reconnect at three times that - accounted for 30-40 s.

        Returns:
            How the connection ended, or None when the collector is stopping
        """
        while self._is_running and self._websocket:
            asleep_since = self._monotonic()
            await self._sleep(STALE_CHECK_INTERVAL)
            now = self._monotonic()

            websocket = self._websocket
            if websocket is None:
                return None

            # Kraken can send its close frame and leave the TCP connection open.
            # recv() then waits for the transport - until the next keepalive
            # ping, 20 s later, measured 2026-10-08 - while the frame already
            # says the far side is gone. A fact about the connection, not a
            # timing judgement, so it is read even after a blocked loop.
            frame = _received_close(websocket)
            if frame is not None:
                return Detection(
                    "far_side_close", close_code_of(frame), frame.reason,
                    "close frame received while the connection stayed open")

            if now - asleep_since > STALE_CHECK_INTERVAL + LATE_WAKE_TOLERANCE:
                # The loop was blocked, so this silence is ours; the next round
                # sees what the receive loop reads in the meantime.
                continue

            self._guard(self._tracker.tick, now)
            self._reset_backoff_if_stable(now)

            detection = self._check_subscriptions(now)
            if detection is not None:
                return detection

            if self._last_message_monotonic is None:
                continue

            silence = now - self._last_message_monotonic
            if silence > self._stale_after:
                self._logger.error(
                    f"No messages for {silence:.0f}s - connection appears "
                    f"dead, forcing reconnect")
                return Detection("silence_watchdog",
                                 exception=f"no message for {silence:.1f}s")
        return None

    def _check_subscriptions(self, now: float) -> Optional[Detection]:
        """
        Act once on subscriptions still unanswered past the deadline.

        Args:
            now: Current monotonic reading

        Returns:
            A detection when a whole stream produced nothing, else None
        """
        if self._deadline_handled:
            return None
        overdue = self._guard(self._tracker.overdue_subscriptions, now,
                              SUBSCRIBE_DEADLINE_SECONDS) or []
        if not overdue:
            return None
        self._deadline_handled = True

        dead = self._guard(self._tracker.dead_streams) or []
        if dead:
            streams = ", ".join(dead)
            self._logger.error(
                f"No answer and no data on the {streams} subscription within "
                f"{SUBSCRIBE_DEADLINE_SECONDS:.0f}s - reconnecting")
            return Detection(
                "subscribe_unanswered",
                exception=f"nothing on {streams} within "
                f"{SUBSCRIBE_DEADLINE_SECONDS:.0f}s")

        self._logger.error(
            f"No answer within {SUBSCRIBE_DEADLINE_SECONDS:.0f}s for "
            f"{len(overdue)} pair(s): {', '.join(overdue)} - continuing, their "
            f"data counts when it arrives")
        self._announce_restoration(
            self._guard(self._tracker.give_up_waiting, now))
        return None

    def _reset_backoff_if_stable(self, now: float) -> None:
        """
        Start the backoff over once a restored connection has proved stable.

        Args:
            now: Current monotonic reading
        """
        stable_for = self._guard(self._tracker.restored_for, now)
        if (stable_for is not None and stable_for >= STABLE_CONNECTION_SECONDS
                and self._reconnect_attempt):
            self._reconnect_attempt = 0

    def _get_reconnect_delay(self) -> float:
        """
        Calculate reconnect delay with exponential backoff.

        Returns:
            Delay in seconds
        """
        delay = self._reconnect_initial_delay * (2 ** self._reconnect_attempt)
        return min(delay, self._reconnect_max_delay)
