"""A compact implementation of the Raft consensus algorithm.

Follows the Raft paper (Ongaro & Ousterhout, 2014): leader election with
randomised timeouts, log replication with the log-matching property, and
commit only of entries from the leader's current term.

The node is **pure logic driven by a clock**. It never touches sockets or
real timers itself:

* ``tick(now)``            — advance time; may start an election or send heartbeats
* ``receive(msg, now)``    — feed in an RPC / RPC reply
* ``client_append(cmd, now)`` — leader-only; append a command to the log
* outgoing messages are appended to ``self.outbox`` for the caller to deliver
* committed commands are handed to ``self.apply_cb`` in log order

This lets a deterministic in-process harness (see ``tests/``) exercise the
same code that runs over TCP in production.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class Entry:
    term: int
    command: Any            # JSON-serialisable

    def as_dict(self) -> dict:
        return {"term": self.term, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "Entry":
        return Entry(d["term"], d["command"])


@dataclass
class RaftNode:
    node_id: str
    peers: list[str]                       # other node ids (not including self)
    apply_cb: Callable[[Any], None] = lambda cmd: None
    heartbeat_interval: float = 0.05
    election_timeout: tuple[float, float] = (0.15, 0.30)
    rng: random.Random = field(default_factory=random.Random)

    # -- persistent state --------------------------------------
    current_term: int = 0
    voted_for: str | None = None
    log: list[Entry] = field(default_factory=list)   # 1-indexed conceptually; log[0] is index 1

    # -- volatile state ---------------------------------------
    role: Role = Role.FOLLOWER
    commit_index: int = 0
    last_applied: int = 0
    leader_id: str | None = None

    # leader-only
    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)
    _votes: set[str] = field(default_factory=set)

    # timers (absolute deadlines, in the caller's clock domain)
    _election_deadline: float = 0.0
    _heartbeat_deadline: float = 0.0
    _initialized: bool = False

    outbox: list[tuple[str, dict]] = field(default_factory=list)

    # -- helpers ---------------------------------------------
    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def last_log_index(self) -> int:
        return len(self.log)

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _term_at(self, index: int) -> int:
        if index <= 0 or index > len(self.log):
            return 0
        return self.log[index - 1].term

    def _reset_election_timer(self, now: float) -> None:
        lo, hi = self.election_timeout
        self._election_deadline = now + self.rng.uniform(lo, hi)

    def _send(self, dest: str, msg: dict) -> None:
        msg.setdefault("term", self.current_term)
        msg["from"] = self.node_id
        self.outbox.append((dest, msg))

    def _maybe_step_down(self, term: int) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.role = Role.FOLLOWER
            self.leader_id = None

    # -- clock -------------------------------------------
    def tick(self, now: float) -> None:
        if not self._initialized:
            # Anchor the election timer to the caller's clock (which may be a
            # large monotonic value in production, or 0.0 in the simulator).
            self._initialized = True
            self._reset_election_timer(now)
            return
        self._apply_committed()
        if self.role == Role.LEADER:
            self._advance_commit()           # covers a single-node cluster
            if now >= self._heartbeat_deadline:
                self._broadcast_append_entries(now)
                self._heartbeat_deadline = now + self.heartbeat_interval
        else:
            if now >= self._election_deadline:
                self._begin_election(now)

    # -- elections --------------------------------------
    def _begin_election(self, now: float) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._votes = {self.node_id}
        self.leader_id = None
        self._reset_election_timer(now)
        for peer in self.peers:
            self._send(peer, {
                "type": "RequestVote",
                "candidate": self.node_id,
                "last_log_index": self.last_log_index,
                "last_log_term": self.last_log_term,
            })
        if self.cluster_size == 1:            # single-node cluster elects itself
            self._become_leader(now)

    def _handle_request_vote(self, msg: dict, now: float) -> None:
        self._maybe_step_down(msg["term"])
        grant = False
        if msg["term"] >= self.current_term and self.voted_for in (None, msg["candidate"]):
            # candidate's log must be at least as up-to-date as ours
            up_to_date = (
                msg["last_log_term"] > self.last_log_term
                or (msg["last_log_term"] == self.last_log_term
                    and msg["last_log_index"] >= self.last_log_index)
            )
            if up_to_date:
                grant = True
                self.voted_for = msg["candidate"]
                self._reset_election_timer(now)
        self._send(msg["from"], {"type": "RequestVoteResp", "granted": grant})

    def _handle_request_vote_resp(self, msg: dict, now: float) -> None:
        self._maybe_step_down(msg["term"])
        if self.role != Role.CANDIDATE or msg["term"] != self.current_term:
            return
        if msg["granted"]:
            self._votes.add(msg["from"])
            if len(self._votes) > self.cluster_size // 2:
                self._become_leader(now)

    def _become_leader(self, now: float) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self.next_index = {p: self.last_log_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self._heartbeat_deadline = now
        # a no-op entry for this term makes commitment of prior entries safe
        self.log.append(Entry(self.current_term, {"type": "noop"}))
        self.tick(now)

    # -- replication ----------------------------------
    def _broadcast_append_entries(self, now: float) -> None:
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        ni = self.next_index.get(peer, self.last_log_index + 1)
        prev_index = ni - 1
        entries = self.log[prev_index:]
        self._send(peer, {
            "type": "AppendEntries",
            "leader": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": self._term_at(prev_index),
            "entries": [e.as_dict() for e in entries],
            "leader_commit": self.commit_index,
        })

    def _handle_append_entries(self, msg: dict, now: float) -> None:
        self._maybe_step_down(msg["term"])
        reply = {"type": "AppendEntriesResp", "success": False,
                 "match_index": 0}
        if msg["term"] < self.current_term:
            self._send(msg["from"], reply)
            return

        # valid leader for this term
        self.role = Role.FOLLOWER
        self.leader_id = msg["leader"]
        self._reset_election_timer(now)

        pli, plt = msg["prev_log_index"], msg["prev_log_term"]
        if pli > 0 and self._term_at(pli) != plt:
            self._send(msg["from"], reply)      # log mismatch -> leader backs off
            return

        # splice in the entries, dropping any conflicting suffix
        new_entries = [Entry.from_dict(e) for e in msg["entries"]]
        idx = pli
        for e in new_entries:
            idx += 1
            if idx <= len(self.log):
                if self.log[idx - 1].term != e.term:
                    del self.log[idx - 1:]
                    self.log.append(e)
            else:
                self.log.append(e)

        if msg["leader_commit"] > self.commit_index:
            self.commit_index = min(msg["leader_commit"], len(self.log))

        reply["success"] = True
        reply["match_index"] = pli + len(new_entries)
        self._send(msg["from"], reply)
        self._apply_committed()

    def _handle_append_entries_resp(self, msg: dict, now: float) -> None:
        self._maybe_step_down(msg["term"])
        if self.role != Role.LEADER or msg["term"] != self.current_term:
            return
        peer = msg["from"]
        if msg["success"]:
            self.match_index[peer] = max(self.match_index.get(peer, 0), msg["match_index"])
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
        else:
            self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)
            self._send_append_entries(peer)

    def _advance_commit(self) -> None:
        for n in range(len(self.log), self.commit_index, -1):
            if self._term_at(n) != self.current_term:
                continue
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if count > self.cluster_size // 2:
                self.commit_index = n
                break
        self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.apply_cb(self.log[self.last_applied - 1].command)

    # -- external API --------------------------------
    def receive(self, msg: dict, now: float) -> None:
        handler = {
            "RequestVote": self._handle_request_vote,
            "RequestVoteResp": self._handle_request_vote_resp,
            "AppendEntries": self._handle_append_entries,
            "AppendEntriesResp": self._handle_append_entries_resp,
        }.get(msg["type"])
        if handler:
            handler(msg, now)

    def client_append(self, command: Any, now: float) -> tuple[bool, int | None, str | None]:
        """Returns ``(accepted, log_index, leader_hint)``."""
        if self.role != Role.LEADER:
            return False, None, self.leader_id
        self.log.append(Entry(self.current_term, command))
        idx = self.last_log_index
        self.match_index[self.node_id] = idx
        self._advance_commit()
        self._broadcast_append_entries(now)
        return True, idx, self.node_id

    def is_committed(self, index: int) -> bool:
        return index <= self.commit_index

    def snapshot(self) -> dict:
        return {
            "id": self.node_id, "role": self.role.value, "term": self.current_term,
            "leader": self.leader_id, "log_len": len(self.log),
            "commit_index": self.commit_index, "last_applied": self.last_applied,
        }
