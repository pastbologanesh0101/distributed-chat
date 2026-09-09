"""End-to-end over real asyncio TCP: a 3-node cluster, real chat clients."""

import asyncio
import contextlib

from dchat.client import ChatClient
from dchat.raft import Role
from dchat.server import ChatNode

# These tests drive asyncio themselves via run(); no pytest-asyncio needed.


def _free_ports(n):
    import socket
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


async def make_cluster(n=3):
    peer_ports = _free_ports(n)
    client_ports = _free_ports(n)
    ids = [f"n{i}" for i in range(n)]
    peers = {ids[i]: ("127.0.0.1", peer_ports[i]) for i in range(n)}
    caddrs = {ids[i]: ("127.0.0.1", client_ports[i]) for i in range(n)}
    nodes = [ChatNode(ids[i], peers, peer_ports[i], client_ports[i], caddrs)
             for i in range(n)]
    for node in nodes:
        await node.start()
    return nodes, client_ports


async def wait_leader(nodes, timeout=8.0):
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        leaders = [n for n in nodes if n.raft.role == Role.LEADER]
        if len(leaders) == 1 and leaders[0].raft.leader_id == leaders[0].node_id:
            # confirm a follower agrees
            followers = [n for n in nodes if n is not leaders[0]]
            if any(f.raft.leader_id == leaders[0].node_id for f in followers):
                return leaders[0]
        await asyncio.sleep(0.05)
    raise AssertionError("no leader elected")


async def stop_all(nodes):
    for n in nodes:
        with contextlib.suppress(Exception):
            await n.stop()


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        pending = asyncio.all_tasks(loop)
        for t in pending:
            t.cancel()
        with contextlib.suppress(Exception):
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


def test_elects_leader_and_broadcasts_in_order():
    async def scenario():
        nodes, cports = await make_cluster(3)
        try:
            await wait_leader(nodes)

            # two clients on two different nodes
            a = ChatClient("alice", [("127.0.0.1", p) for p in cports]); await a.connect("127.0.0.1", cports[0])
            b = ChatClient("bob", [("127.0.0.1", p) for p in cports]); await b.connect("127.0.0.1", cports[1])

            got_b = []
            async def collect():
                async for seq, sender, text in b.messages():
                    got_b.append((seq, sender, text))
                    if len(got_b) >= 4:
                        return
            task = asyncio.ensure_future(collect())

            await a.send("hello")
            await b.send("hi alice")
            await a.send("how are you")
            await b.send("good thanks")

            await asyncio.wait_for(task, timeout=8)
            assert [t[2] for t in got_b] == ["hello", "hi alice", "how are you", "good thanks"]
            assert [t[0] for t in got_b] == [1, 2, 3, 4]      # global order/seq

            # a late client gets the full transcript replayed
            c = ChatClient("carol", [("127.0.0.1", p) for p in cports]); await c.connect("127.0.0.1", cports[2])
            seen = []
            async for seq, sender, text in c.messages():
                seen.append(text)
                if len(seen) >= 4:
                    break
            assert seen == ["hello", "hi alice", "how are you", "good thanks"]

            await a.close(); await b.close(); await c.close()
        finally:
            await stop_all(nodes)

    run(scenario())


def test_survives_leader_crash():
    async def scenario():
        nodes, cports = await make_cluster(3)
        try:
            leader = await wait_leader(nodes)
            others = [n for n in nodes if n is not leader]

            a = ChatClient("alice", [("127.0.0.1", p) for p in cports])
            await a.connect("127.0.0.1", cports[nodes.index(others[0])])
            await a.send("before-1")
            await a.send("before-2")
            await asyncio.sleep(0.5)

            await leader.stop()                         # kill the leader
            nodes.remove(leader)
            new_leader = await wait_leader(nodes, timeout=10)
            assert new_leader is not leader

            await a.send("after-1")                     # client follows REDIRECT
            await a.send("after-2")
            await asyncio.sleep(1.0)

            for n in nodes:
                texts = [m["text"] for m in n.state.messages]
                assert texts == ["before-1", "before-2", "after-1", "after-2"], n.node_id
            await a.close()
        finally:
            await stop_all(nodes)

    run(scenario())
