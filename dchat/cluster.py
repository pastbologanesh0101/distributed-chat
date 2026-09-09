"""A deterministic, in-process simulation of a Raft cluster.

No sockets, no real time — a virtual clock and a message queue with optional
per-link delay, loss and partitions. Used by the tests and the examples to
exercise elections, replication and failure handling reproducibly.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .raft import RaftNode, Role
from .statemachine import ChatState


@dataclass
class _InFlight:
    deliver_at: float
    src: str
    dst: str
    msg: dict


class SimCluster:
    def __init__(self, n: int, *, seed: int = 0, link_delay: float = 0.01,
                 loss: float = 0.0):
        self.rng = random.Random(seed)
        self.now = 0.0
        self.link_delay = link_delay
        self.loss = loss
        self.ids = [f"n{i}" for i in range(n)]
        self.partitions: list[set[str]] = []       # groups that can talk to each other
        self.down: set[str] = set()
        self.queue: list[_InFlight] = []

        self.nodes: dict[str, RaftNode] = {}
        self.states: dict[str, ChatState] = {}
        for nid in self.ids:
            state = ChatState()
            node = RaftNode(
                node_id=nid,
                peers=[x for x in self.ids if x != nid],
                apply_cb=state.apply,
                rng=random.Random(self.rng.random()),
            )
            self.nodes[nid] = node
            self.states[nid] = state

    # -- connectivity ------------------------------------
    def _connected(self, a: str, b: str) -> bool:
        if a in self.down or b in self.down:
            return False
        if not self.partitions:
            return True
        for group in self.partitions:
            if a in group and b in group:
                return True
        return False

    def partition(self, *groups: set[str] | list[str]) -> None:
        self.partitions = [set(g) for g in groups]

    def heal(self) -> None:
        self.partitions = []

    def stop(self, nid: str) -> None:
        self.down.add(nid)

    def start(self, nid: str) -> None:
        self.down.discard(nid)

    # -- driving time ----------------------------------
    def _pump_outboxes(self) -> None:
        for nid, node in self.nodes.items():
            if nid in self.down:
                node.outbox.clear()
                continue
            for dst, msg in node.outbox:
                if self._connected(nid, dst) and self.rng.random() >= self.loss:
                    jitter = self.rng.uniform(0, self.link_delay)
                    self.queue.append(_InFlight(self.now + self.link_delay + jitter,
                                                nid, dst, msg))
            node.outbox.clear()

    def _deliver_due(self) -> None:
        due = [m for m in self.queue if m.deliver_at <= self.now]
        self.queue = [m for m in self.queue if m.deliver_at > self.now]
        for m in due:
            if m.dst not in self.down and self._connected(m.src, m.dst):
                self.nodes[m.dst].receive(m.msg, self.now)

    def step(self, dt: float = 0.01) -> None:
        self.now += dt
        for nid, node in self.nodes.items():
            if nid not in self.down:
                node.tick(self.now)
        self._pump_outboxes()
        self._deliver_due()
        # a second pump so replies generated on delivery start moving promptly
        self._pump_outboxes()

    def run_for(self, duration: float, dt: float = 0.01) -> None:
        end = self.now + duration
        while self.now < end:
            self.step(dt)

    # -- helpers for assertions -----------------------
    def leaders(self) -> list[str]:
        return [nid for nid, n in self.nodes.items()
                if n.role == Role.LEADER and nid not in self.down]

    def leader(self) -> RaftNode | None:
        ls = self.leaders()
        return self.nodes[ls[0]] if len(ls) == 1 else None

    def wait_for_leader(self, timeout: float = 5.0) -> RaftNode:
        while self.now < timeout:
            self.step()
            leader = self.leader()
            if leader and self._quorum_agrees_on_term(leader):
                return leader
        raise AssertionError(f"no stable leader after {timeout}s; leaders={self.leaders()}")

    def _quorum_agrees_on_term(self, leader: RaftNode) -> bool:
        term = leader.current_term
        agree = 1 + sum(1 for p in leader.peers
                        if self.nodes[p].current_term == term
                        and self.nodes[p].leader_id == leader.node_id)
        return agree > len(self.ids) // 2

    def client_send(self, text: str, sender: str = "alice",
                    timeout: float = 5.0) -> int:
        """Append a chat message via the current leader; block (in sim time)
        until it commits everywhere reachable. Returns its sequence number."""
        deadline = self.now + timeout
        while self.now < deadline:
            leader = self.leader()
            if leader:
                ok, idx, _ = leader.client_append(
                    {"type": "msg", "sender": sender, "text": text}, self.now)
                if ok:
                    while self.now < deadline:
                        self.step()
                        if leader.is_committed(idx):
                            return idx
            self.step()
        raise AssertionError("message did not commit in time")

    def all_message_logs(self) -> dict[str, list[str]]:
        return {nid: [m["text"] for m in st.messages]
                for nid, st in self.states.items()}
