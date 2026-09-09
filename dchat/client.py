"""A chat client that finds the leader on its own.

* A single background read loop routes each server line to a control queue
  (``OK`` / ``ERR`` / ``REDIRECT``) or the message stream
  (``MSG seq sender text``). ``MSG`` lines are de-duplicated by sequence number,
  so history replayed after a reconnect is harmless.
* ``send()`` follows ``REDIRECT`` to the leader, and if a node it is talking to
  dies it cycles through the seed addresses until one answers.

``ChatClient`` is async; ``repl`` is a blocking terminal front-end.
"""

from __future__ import annotations

import asyncio

_CTRL_TIMEOUT = 2.0


class ChatClient:
    def __init__(self, name: str, seeds: list[tuple[str, int]] | None = None):
        self.name = name
        self.seeds = list(seeds or [])
        self._seed_idx = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._ctrl: asyncio.Queue[str] = asyncio.Queue()
        self._msgs: asyncio.Queue[tuple[int, str, str]] = asyncio.Queue()
        self._reader_task: asyncio.Task | None = None
        self._last_seq = 0
        self.host = self.port = None

    # -- connection management -------------------------------
    async def connect(self, host: str, port: int) -> None:
        if (host, port) not in self.seeds:
            self.seeds.append((host, port))
        if self._reader_task:
            self._reader_task.cancel()
        self._reader, self._writer = await asyncio.open_connection(host, port)
        self.host, self.port = host, port
        # drain any stale control replies from a previous connection
        while not self._ctrl.empty():
            self._ctrl.get_nowait()
        self._reader_task = asyncio.ensure_future(self._read_loop())
        self._writer.write(f"JOIN {self.name}\n".encode())
        await self._writer.drain()
        await asyncio.wait_for(self._ctrl.get(), _CTRL_TIMEOUT)

    async def _reconnect_next(self) -> None:
        for _ in range(len(self.seeds) or 1):
            self._seed_idx = (self._seed_idx + 1) % max(1, len(self.seeds))
            host, port = self.seeds[self._seed_idx]
            try:
                await self.connect(host, port)
                return
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.2)
        raise RuntimeError("no reachable chat node")

    async def _read_loop(self) -> None:
        try:
            while True:
                line = await self._reader.readline()
                if not line:
                    return
                s = line.decode(errors="replace").rstrip("\r\n")
                if s.startswith("MSG "):
                    _, seq, sender, text = s.split(" ", 3)
                    seq = int(seq)
                    if seq > self._last_seq:
                        self._last_seq = seq
                        await self._msgs.put((seq, sender, text))
                else:
                    await self._ctrl.put(s)
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            return

    # -- API ----------------------------------------------
    async def send(self, text: str, *, _tries: int = 0) -> None:
        if _tries > 12:
            raise RuntimeError("gave up sending after too many retries")
        try:
            self._writer.write(f"SEND {text}\n".encode())
            await self._writer.drain()
            resp = await asyncio.wait_for(self._ctrl.get(), _CTRL_TIMEOUT)
        except (OSError, asyncio.TimeoutError, ConnectionError):
            await self._reconnect_next()
            return await self.send(text, _tries=_tries + 1)

        if resp.startswith("REDIRECT "):
            host, port = resp.split(" ", 1)[1].rsplit(":", 1)
            try:
                await self.connect(host, int(port))
            except (OSError, asyncio.TimeoutError):
                await self._reconnect_next()
            return await self.send(text, _tries=_tries + 1)
        if resp.startswith("ERR"):
            await asyncio.sleep(0.25)
            return await self.send(text, _tries=_tries + 1)

    async def messages(self):
        while True:
            yield await self._msgs.get()

    async def close(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
        if self._writer:
            self._writer.close()


async def _repl(name: str, host: str, port: int) -> None:  # pragma: no cover
    client = ChatClient(name, seeds=[(host, port)])
    await client.connect(host, port)
    print(f"connected to {host}:{port} as {name}. type a message, Ctrl-D to quit.")

    async def printer():
        async for seq, sender, text in client.messages():
            if sender != name:
                print(f"  {seq:>3} <{sender}> {text}")

    asyncio.ensure_future(printer())
    loop = asyncio.get_event_loop()
    while True:
        try:
            text = await loop.run_in_executor(None, input, "")
        except EOFError:
            break
        if text.strip():
            await client.send(text)
    await client.close()


def repl(name: str, host: str, port: int) -> None:  # pragma: no cover
    asyncio.run(_repl(name, host, port))
