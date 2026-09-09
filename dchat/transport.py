"""Length-prefixed JSON framing over asyncio streams, plus a self-healing
peer link.

Frame = 4-byte big-endian length + UTF-8 JSON object.
"""

from __future__ import annotations

import asyncio
import json
import struct

_LEN = struct.Struct(">I")


async def read_frame(reader: asyncio.StreamReader) -> dict | None:
    header = await reader.readexactly(4)
    (n,) = _LEN.unpack(header)
    body = await reader.readexactly(n)
    return json.loads(body.decode())


def encode_frame(msg: dict) -> bytes:
    body = json.dumps(msg, separators=(",", ":")).encode()
    return _LEN.pack(len(body)) + body


class PeerLink:
    """An outbound connection to one peer that reconnects on failure.

    ``send`` never blocks on the network: messages go onto a bounded queue and
    are dropped (like UDP) if the peer is unreachable and the queue fills — Raft
    tolerates message loss.
    """

    def __init__(self, peer_id: str, host: str, port: int, maxq: int = 1000):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self._q: asyncio.Queue[dict] = asyncio.Queue(maxq)
        self._task: asyncio.Task | None = None
        self.connected = False

    def start(self) -> None:
        self._task = asyncio.ensure_future(self._run())

    def send(self, msg: dict) -> None:
        try:
            self._q.put_nowait(msg)
        except asyncio.QueueFull:
            pass  # drop; Raft will retry via heartbeats

    async def _run(self) -> None:
        backoff = 0.1
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                self.connected = True
                backoff = 0.1
                try:
                    while True:
                        msg = await self._q.get()
                        writer.write(encode_frame(msg))
                        await writer.drain()
                finally:
                    self.connected = False
                    writer.close()
            except (OSError, asyncio.IncompleteReadError):
                self.connected = False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
            except asyncio.CancelledError:
                return

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
