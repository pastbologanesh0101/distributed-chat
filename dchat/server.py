"""A chat node: a Raft peer + a client-facing line protocol, over asyncio TCP.

Peer traffic (RequestVote / AppendEntries / replies) is length-prefixed JSON on
the *peer port*. Chat clients connect to the *client port* and speak lines:

    -> JOIN alice
    <- OK joined as alice
    -> SEND hello everyone
    <- OK                       (from the leader; committed messages arrive as MSG)
    <- MSG 1 alice hello everyone
    <- REDIRECT 127.0.0.1:9002  (if you connected to a follower)

Because every node applies the Raft log in the same order, every node streams
the identical MSG sequence to its clients.
"""

from __future__ import annotations

import asyncio
import contextlib

from .raft import RaftNode, Role
from .statemachine import ChatState
from .transport import PeerLink, encode_frame, read_frame

TICK = 0.005


class ChatNode:
    def __init__(self, node_id: str, peers: dict[str, tuple[str, int]],
                 peer_port: int, client_port: int,
                 client_addrs: dict[str, tuple[str, int]] | None = None):
        self.node_id = node_id
        self.peer_addr = ("0.0.0.0", peer_port)
        self.client_addr = ("0.0.0.0", client_port)
        self.client_addrs = client_addrs or {}

        self.state = ChatState()
        self.raft = RaftNode(node_id, [p for p in peers if p != node_id],
                             apply_cb=self.state.apply)
        self.links = {pid: PeerLink(pid, host, port)
                      for pid, (host, port) in peers.items() if pid != node_id}
        self._clients: set[asyncio.StreamWriter] = set()
        self._servers: list[asyncio.AbstractServer] = []
        self._driver: asyncio.Task | None = None
        self._running = False

    # -- lifecycle ------------------------------------
    async def start(self) -> None:
        self._running = True
        for link in self.links.values():
            link.start()
        self._servers.append(await asyncio.start_server(
            self._handle_peer, self.peer_addr[0], self.peer_addr[1]))
        self._servers.append(await asyncio.start_server(
            self._handle_client, self.client_addr[0], self.client_addr[1]))
        self.state.subscribe(self._broadcast_msg)
        self._driver = asyncio.ensure_future(self._drive())

    async def stop(self) -> None:
        self._running = False
        if self._driver:
            self._driver.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._driver
        for link in self.links.values():
            await link.stop()
        for w in list(self._clients):
            with contextlib.suppress(Exception):
                w.close()
        self._clients.clear()
        for srv in self._servers:
            srv.close()
        for srv in self._servers:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(srv.wait_closed(), timeout=1.0)

    # -- the clock --------------------------------
    async def _drive(self) -> None:
        loop = asyncio.get_event_loop()
        try:
            while self._running:
                self.raft.tick(loop.time())
                for dest, msg in self.raft.outbox:
                    link = self.links.get(dest)
                    if link:
                        link.send(msg)
                self.raft.outbox.clear()
                await asyncio.sleep(TICK)
        except asyncio.CancelledError:
            return

    # -- peer side ------------------------------
    async def _handle_peer(self, reader, writer) -> None:
        try:
            while True:
                msg = await read_frame(reader)
                if msg is None:
                    break
                self.raft.receive(msg, asyncio.get_event_loop().time())
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    # -- client side --------------------------
    async def _handle_client(self, reader, writer) -> None:
        self._clients.add(writer)
        peer = writer.get_extra_info("peername")
        name = f"anon-{peer[1] if peer else '?'}"
        try:
            # replay history so a new client sees the full transcript
            for m in list(self.state.messages):
                writer.write(f"MSG {m['seq']} {m['sender']} {m['text']}\n".encode())
            await writer.drain()
            while True:
                line = await reader.readline()
                if not line:
                    break
                parts = line.decode(errors="replace").rstrip("\r\n").split(" ", 1)
                cmd = parts[0].upper()
                arg = parts[1] if len(parts) > 1 else ""
                if cmd == "JOIN":
                    name = arg.strip() or name
                    writer.write(f"OK joined as {name}\n".encode())
                elif cmd == "SEND":
                    await self._client_send(writer, name, arg)
                elif cmd == "WHO":
                    writer.write(f"OK leader={self.raft.leader_id} "
                                 f"role={self.raft.role.value} term={self.raft.current_term}\n".encode())
                elif cmd == "QUIT":
                    break
                else:
                    writer.write(b"ERR unknown command\n")
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self._clients.discard(writer)
            writer.close()

    async def _client_send(self, writer, name: str, text: str) -> None:
        now = asyncio.get_event_loop().time()
        ok, _idx, leader_hint = self.raft.client_append(
            {"type": "msg", "sender": name, "text": text}, now)
        if ok:
            writer.write(b"OK\n")
        elif leader_hint and leader_hint in self.client_addrs:
            host, port = self.client_addrs[leader_hint]
            writer.write(f"REDIRECT {host}:{port}\n".encode())
        else:
            writer.write(b"ERR no leader; retry\n")

    def _broadcast_msg(self, entry: dict) -> None:
        line = f"MSG {entry['seq']} {entry['sender']} {entry['text']}\n".encode()
        for w in list(self._clients):
            try:
                w.write(line)
            except Exception:
                self._clients.discard(w)

    # -- introspection --------------------
    def status(self) -> dict:
        s = self.raft.snapshot()
        s["clients"] = len(self._clients)
        s["messages"] = len(self.state.messages)
        return s
