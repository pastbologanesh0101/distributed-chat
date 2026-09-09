# Distributed Chat Server

A chat server that stays correct when nodes crash or the network splits, because
message ordering is decided by **Raft consensus** — implemented from scratch in
pure Python (no `raft` library, no external deps). Clients connect to *any*
node; every node streams the identical, totally-ordered message history.

```
   client ──TCP──┐        ┌─ node n0 ─┐
   client ──TCP──┼─ any ──┤   node n1  │  Raft: 1 leader, N-1 followers
   client ──TCP──┘  node  └─ node n2 ─┘  replicated log ⇒ same order everywhere
                     │
              follower? -> "REDIRECT host:port"  (client hops to the leader)
```

## Quick start

```bash
git clone https://github.com/pastbologanesh0101/distributed-chat
cd distributed-chat
pip install -e ".[dev]"
pytest -q                       # 10 tests: Raft sim + real-TCP integration
python examples/sim_demo.py     # narrated election / crash / partition run
```

Run a 3-node cluster (three terminals):

```bash
PEERS=n0=127.0.0.1:9000,n1=127.0.0.1:9001,n2=127.0.0.1:9002
CPORTS=n0=8000,n1=8001,n2=8002

dchat node --id n0 --peers $PEERS --client-ports $CPORTS
dchat node --id n1 --peers $PEERS --client-ports $CPORTS
dchat node --id n2 --peers $PEERS --client-ports $CPORTS
```

Chat — connect to any node, it redirects you to the leader:

```bash
dchat client --name alice --connect 127.0.0.1:8000
dchat client --name bob   --connect 127.0.0.1:8002
```

Kill the leader mid-conversation: a new one is elected in a few hundred ms,
clients reconnect automatically, and **no committed message is lost or
reordered**.

## Concepts demonstrated

| Area | Where | What it shows |
|---|---|---|
| **Consensus (Raft)** | `dchat/raft.py` | Leader election with randomised timeouts; log replication with the log-matching property; commit only of entries from the leader's current term (+ the no-op-on-election trick); step-down on a higher term; the up-to-date-log voting restriction. ~250 lines, follows the paper. |
| **Testable design** | `dchat/raft.py` + `dchat/cluster.py` | The Raft node is **pure logic driven by a clock** — `tick(now)` / `receive(msg, now)` / `client_append()`, outgoing messages in a list. A deterministic in-process `SimCluster` (virtual clock, message queue, partition/loss knobs) tests the exact code that later runs over TCP. |
| **Sockets** | `dchat/transport.py`, `dchat/server.py` | asyncio TCP. Peer traffic is length-prefixed JSON over self-healing `PeerLink`s that drop messages when a peer is down (Raft re-sends via heartbeats). Clients speak a line protocol on a separate port. |
| **State-machine replication** | `dchat/statemachine.py` | Applying the committed log in order yields an identical chat transcript on every node; new clients get the whole transcript replayed on connect. |
| **Fault tolerance** | `tests/` | Leader crash → new leader keeps full committed history. Minority partition → can't elect or commit; heals and catches up. 20% packet loss → still converges. An isolated node that inflates its term rejoins without corrupting history. No two leaders in one term (asserted under churn). |
| **Client redirection** | `dchat/client.py` | Follows `REDIRECT` to the leader and cycles through seed addresses when a node dies — `send()` is transparently reliable. |

## The line protocol (client ↔ node)

```
-> JOIN alice
<- OK joined as alice
-> SEND hello everyone
<- OK                         (accepted by the leader)
<- MSG 1 alice hello everyone (every committed message, to every client, in order)
-> SEND ...                   (to a follower)
<- REDIRECT 127.0.0.1:9002    (client reconnects and resends)
-> WHO
<- OK leader=n2 role=follower term=4
```

## What Raft guarantees here

- **Election safety** — at most one leader per term.
- **Log matching** — if two logs agree on an entry's (index, term), they agree
  on everything before it.
- **Leader completeness** — a committed entry is present in every future
  leader's log; committed chat messages are never lost or reordered.
- **State-machine safety** — every node applies the same commands in the same
  order.

Availability needs a majority: a 3-node cluster tolerates 1 failure, a 5-node
cluster tolerates 2.

## Limitations

- **In-memory log** — no on-disk persistence, so a full-cluster restart loses
  history (the log/term/vote would need to be `fsync`-ed to be paper-complete).
- **No snapshotting / log compaction** — the log grows forever.
- **No dynamic membership** — the peer set is fixed at startup.
- **Static leader hint for REDIRECT** — relies on the client-address map passed
  on the command line.
- No auth, no message history queries beyond full replay.

## Project layout

```
dchat/
  raft.py          the consensus algorithm (clock-driven, transport-free)
  cluster.py       SimCluster: deterministic in-process network + virtual clock
  statemachine.py  the replicated chat transcript
  transport.py     length-prefixed JSON framing + self-healing peer links
  server.py        ChatNode: Raft peer + client line-protocol over asyncio TCP
  client.py        leader-following chat client
  cli.py           dchat node / dchat client
examples/
  sim_demo.py      narrated election + crash + partition, fully reproducible
tests/
  test_raft.py         safety & liveness via SimCluster (election, partitions,
                       packet loss, leader crash, term safety under churn)
  test_integration.py  a real 3-node TCP cluster with real chat clients
```

## License

MIT
