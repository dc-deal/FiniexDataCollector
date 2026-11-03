"""
FiniexDataCollector - A Kraken-like websocket endpoint for drop tests

Written frame by frame on a raw TCP server rather than with a websocket
library, because the drops these tests are about live below a library's API: a
close frame sent while the TCP connection stays open, a FIN without any frame,
a reset, a peer that stops answering pings. A server built on websockets can
produce only the endings websockets itself would.

It behaves like Kraken where the collector depends on it: a status message
first, one subscription answer per symbol echoing req_id, and trade ids that
keep counting while nobody is listening - the venue does not stop trading
because a socket dropped - with no replay of the backlog on a new connection
(Kraken's trade answer says snapshot: false).

Location: tests/collectors/far_side.py
"""

import asyncio
import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _frame(opcode: int, payload: bytes) -> bytes:
    """One unmasked server-to-client frame."""
    size = len(payload)
    if size < 126:
        header = bytes([0x80 | opcode, size])
    elif size < 65536:
        header = bytes([0x80 | opcode, 126]) + size.to_bytes(2, "big")
    else:
        header = bytes([0x80 | opcode, 127]) + size.to_bytes(8, "big")
    return header + payload


def _close_frame(code: int, reason: str) -> bytes:
    return _frame(8, code.to_bytes(2, "big") + reason.encode())


async def _read_frame(reader: asyncio.StreamReader) -> Tuple[int, bytes]:
    """One client-to-server frame, unmasked."""
    first, second = await reader.readexactly(2)
    opcode = first & 0x0F
    size = second & 0x7F
    if size == 126:
        size = int.from_bytes(await reader.readexactly(2), "big")
    elif size == 127:
        size = int.from_bytes(await reader.readexactly(8), "big")
    mask = await reader.readexactly(4) if second & 0x80 else b""
    data = await reader.readexactly(size)
    if mask:
        data = bytes(byte ^ mask[i % 4] for i, byte in enumerate(data))
    return opcode, data


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class FarSide:
    """
    The other end of the collector's connection, scriptable per connection.

    Args:
        ending: How connection 1 ends after `end_after` seconds of data -
            clean_close, empty_close (a close frame without a code), abort,
            fin, lingering, empty_lingering (a close frame without a code, the
            TCP connection left open), silent, or none
        end_after: Seconds of data before connection 1 ends
        trade_every: Seconds between trades per symbol
        fail_subscribe_on: Connection numbers closed (1011) while subscribing
        refuse: (connection, stream, 'BTC/USD') answers sent as refusals
        withhold_answers: Streams whose subscription answers are never sent
        withhold_data: Streams for which no data is ever sent
        silent_pairs: (stream, 'BTC/USD') pairs that get neither an answer nor
            data - one thin pair while the rest of its stream is alive
        close_every_after: Close every connection this many seconds after it
            was subscribed (a far side that keeps closing)
        delay_handshake_on: Connection number -> seconds to delay its handshake
        close_at_handshake_on: Connection numbers that get a close frame right
            after the handshake while the TCP connection stays open - a send
            then waits on the closing connection until the client's close
            timeout
        after_status: Messages sent on every connection right after the status
            message, before anything is subscribed
        drop_handshake_on: Connection number -> seconds after which its
            handshake is dropped unanswered, so the client's connect fails
    """

    def __init__(self, ending: str = "none", end_after: float = 0.3,
                 trade_every: float = 0.02,
                 symbols: Iterable[str] = ("BTC/USD", "ETH/USD"),
                 fail_subscribe_on: Iterable[int] = (),
                 refuse: Iterable[Tuple[int, str, str]] = (),
                 withhold_answers: Iterable[str] = (),
                 withhold_data: Iterable[str] = (),
                 silent_pairs: Iterable[Tuple[str, str]] = (),
                 close_every_after: Optional[float] = None,
                 delay_handshake_on: Optional[Dict[int, float]] = None,
                 close_at_handshake_on: Iterable[int] = (),
                 after_status: Iterable[dict] = (),
                 drop_handshake_on: Optional[Dict[int, float]] = None):
        self.ending = ending
        self.end_after = end_after
        self.trade_every = trade_every
        self.symbols = list(symbols)
        self.fail_subscribe_on = set(fail_subscribe_on)
        self.refuse = set(refuse)
        self.withhold_answers = set(withhold_answers)
        self.withhold_data = set(withhold_data)
        self.silent_pairs = set(silent_pairs)
        self.close_every_after = close_every_after
        self.delay_handshake_on = dict(delay_handshake_on or {})
        self.close_at_handshake_on = set(close_at_handshake_on)
        self.after_status = list(after_status)
        self.drop_handshake_on = dict(drop_handshake_on or {})

        self.connections = 0
        self.closed_by_client: Set[int] = set()
        # Trade ids the venue has produced, and the ones sent, per connection.
        self.produced = {symbol: 1000 * (i + 1)
                         for i, symbol in enumerate(self.symbols)}
        self.sent: Dict[str, List[Tuple[int, int]]] = {
            s: [] for s in self.symbols}
        self._server: Optional[asyncio.AbstractServer] = None
        self._tasks: List[asyncio.Task] = []
        self.port = 0

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    async def __aenter__(self) -> "FarSide":
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        self._tasks.append(asyncio.create_task(self._trade()))
        return self

    async def __aexit__(self, *exc) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._server.close()

    async def _trade(self) -> None:
        """The venue keeps trading whoever is connected."""
        while True:
            await asyncio.sleep(self.trade_every)
            for symbol in self.symbols:
                self.produced[symbol] += 1

    def last_sent(self, symbol: str, connection: int) -> Optional[int]:
        ids = [tid for tid, conn in self.sent[symbol] if conn == connection]
        return ids[-1] if ids else None

    def first_sent(self, symbol: str, connection: int) -> Optional[int]:
        ids = [tid for tid, conn in self.sent[symbol] if conn == connection]
        return ids[0] if ids else None

    async def _handle(self, reader, writer) -> None:
        self.connections += 1
        conn = self.connections
        self._tasks.append(asyncio.current_task())

        request = await reader.readuntil(b"\r\n\r\n")
        key = [line.split(b":", 1)[1].strip() for line in request.split(b"\r\n")
               if line.lower().startswith(b"sec-websocket-key")][0]
        if conn in self.delay_handshake_on:
            await asyncio.sleep(self.delay_handshake_on[conn])
        if conn in self.drop_handshake_on:
            await asyncio.sleep(self.drop_handshake_on[conn])
            writer.transport.abort()
            return
        accept = base64.b64encode(hashlib.sha1(key + GUID.encode()).digest())
        writer.write(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                     b"Connection: Upgrade\r\nSec-WebSocket-Accept: "
                     + accept + b"\r\n\r\n")

        if conn in self.close_at_handshake_on:
            writer.write(_close_frame(1013, "try again later"))
            await writer.drain()
            try:
                # Held open until the client drops it: no answer to its close.
                await reader.read()
            except (ConnectionError, OSError):
                pass
            return

        def send(message) -> None:
            writer.write(_frame(1, json.dumps(message).encode()))

        send({"channel": "status", "type": "update", "data": [{
            "system": "online", "connection_id": 900_000_000_000 + conn,
            "version": "2.0.0", "api_version": "v2"}]})
        for message in self.after_status:
            send(message)

        subscribed = asyncio.Event()
        quiet = asyncio.Event()
        refused: Set[str] = set()

        async def answer(message) -> None:
            stream = message["params"]["channel"]
            req_id = message.get("req_id")
            if conn in self.fail_subscribe_on and stream == "trade":
                writer.write(_close_frame(1011, "closed during subscribe"))
                await writer.drain()
                writer.close()
                return
            if stream not in self.withhold_answers:
                for symbol in message["params"]["symbol"]:
                    if (stream, symbol) in self.silent_pairs:
                        continue
                    if (conn, stream, symbol) in self.refuse:
                        if stream == "trade":
                            refused.add(symbol)
                        send({"error": f"Currency pair not supported {symbol}",
                              "method": "subscribe", "req_id": req_id,
                              "success": False, "symbol": symbol,
                              "time_in": _now(), "time_out": _now()})
                        continue
                    send({"method": "subscribe", "req_id": req_id,
                          "result": {"channel": stream, "symbol": symbol,
                                     "snapshot": False},
                          "success": True, "time_in": _now(),
                          "time_out": _now()})
                await writer.drain()
            if stream == "trade":
                subscribed.set()

        async def read() -> None:
            try:
                while True:
                    opcode, data = await _read_frame(reader)
                    if opcode == 9 and not quiet.is_set():
                        writer.write(_frame(10, data))
                        await writer.drain()
                    elif opcode == 8:
                        self.closed_by_client.add(conn)
                        if quiet.is_set():
                            continue
                        writer.write(_frame(8, data[:2]))
                        await writer.drain()
                        writer.close()
                        return
                    elif opcode == 1:
                        message = json.loads(data)
                        if message.get("method") == "subscribe":
                            await answer(message)
            except (asyncio.IncompleteReadError, ConnectionError, OSError):
                self.closed_by_client.add(conn)

        reader_task = asyncio.create_task(read())
        self._tasks.append(reader_task)
        try:
            await asyncio.wait_for(subscribed.wait(), timeout=10)
        except asyncio.TimeoutError:
            return

        # No replay: a new connection starts at whatever the venue is at now.
        up_to = dict(self.produced)
        loop = asyncio.get_running_loop()
        since = loop.time()
        ended = False
        try:
            while not writer.is_closing():
                if not quiet.is_set():
                    send({"channel": "heartbeat"})
                    for index, symbol in enumerate(self.symbols):
                        if ("ticker" not in self.withhold_data
                                and ("ticker", symbol) not in self.silent_pairs):
                            send({"channel": "ticker", "type": "update",
                                  "data": [{"symbol": symbol,
                                            "bid": 99.9 + index,
                                            "ask": 100.1 + index}]})
                        if ("trade" in self.withhold_data or symbol in refused
                                or ("trade", symbol) in self.silent_pairs):
                            continue
                        for tid in range(up_to[symbol] + 1,
                                         self.produced[symbol] + 1):
                            self.sent[symbol].append((tid, conn))
                            send({"channel": "trade", "type": "update",
                                  "data": [{"symbol": symbol, "side": "buy",
                                            "price": 100.0 + index, "qty": 0.1,
                                            "ord_type": "market",
                                            "trade_id": tid,
                                            "timestamp": _now()}]})
                        up_to[symbol] = self.produced[symbol]
                    await writer.drain()
                await asyncio.sleep(self.trade_every)

                elapsed = loop.time() - since
                if (self.close_every_after is not None
                        and elapsed > self.close_every_after):
                    writer.write(_close_frame(1001, "going away"))
                    await writer.drain()
                    writer.close()
                    return
                if conn == 1 and not ended and elapsed > self.end_after:
                    ended = True
                    if await self._end(writer, quiet):
                        return
        except (ConnectionError, OSError):
            pass
        # A quiet connection stays open simply because the loop above keeps
        # running without sending, until the client drops it. Never an await
        # in a finally here: a task already cancelled does not get cancelled a
        # second time, and asyncio.run would wait out the sleep at shutdown.

    async def _end(self, writer, quiet: asyncio.Event) -> bool:
        """End connection 1 as scripted; True when the handler should return."""
        if self.ending == "clean_close":
            writer.write(_close_frame(1001, "going away"))
            await writer.drain()
            writer.close()
            return True
        if self.ending == "empty_close":
            writer.write(_frame(8, b""))
            await writer.drain()
            writer.close()
            return True
        if self.ending == "abort":
            writer.transport.abort()
            return True
        if self.ending == "fin":
            writer.close()
            return True
        if self.ending == "lingering":
            writer.write(_close_frame(1001, "going away"))
            await writer.drain()
            quiet.set()
            return False
        if self.ending == "empty_lingering":
            writer.write(_frame(8, b""))
            await writer.drain()
            quiet.set()
            return False
        if self.ending == "silent":
            quiet.set()
            return False
        return False
