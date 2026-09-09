"""``dchat`` command line.

Start a 3-node cluster in three terminals:

    dchat node --id n0 --peers n0=127.0.0.1:9000,n1=127.0.0.1:9001,n2=127.0.0.1:9002 \\
               --client-ports n0=8000,n1=8001,n2=8002
    dchat node --id n1 ... (same --peers / --client-ports)
    dchat node --id n2 ...

Then chat (connect to any node; it redirects you to the leader):

    dchat client --name alice --connect 127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from .server import ChatNode


def _parse_kv(s: str) -> dict[str, str]:
    out = {}
    for pair in s.split(","):
        k, v = pair.split("=", 1)
        out[k.strip()] = v.strip()
    return out


async def _run_node(ns) -> None:
    peers = {pid: (hp.split(":")[0], int(hp.split(":")[1]))
             for pid, hp in _parse_kv(ns.peers).items()}
    client_ports = {pid: int(p) for pid, p in _parse_kv(ns.client_ports).items()}
    client_addrs = {pid: (peers[pid][0], client_ports[pid]) for pid in peers}

    node = ChatNode(ns.id, peers, peers[ns.id][1], client_ports[ns.id], client_addrs)
    await node.start()
    print(f"[{ns.id}] peer={peers[ns.id][1]} client={client_ports[ns.id]}  "
          f"peers={list(peers)}")

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async def status_loop():
        while not stop.is_set():
            await asyncio.sleep(2)
            print(f"[{ns.id}] {node.status()}")

    asyncio.ensure_future(status_loop())
    await stop.wait()
    await node.stop()


def cmd_node(ns) -> int:
    asyncio.run(_run_node(ns))
    return 0


def cmd_client(ns) -> int:
    from .client import repl
    host, port = ns.connect.rsplit(":", 1)
    repl(ns.name, host, int(port))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dchat")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("node")
    s.add_argument("--id", required=True)
    s.add_argument("--peers", required=True,
                   help="id=host:port,... (Raft peer ports, include self)")
    s.add_argument("--client-ports", required=True, help="id=port,...")
    s.set_defaults(func=cmd_node)

    s = sub.add_parser("client")
    s.add_argument("--name", required=True)
    s.add_argument("--connect", required=True, help="host:port of any node")
    s.set_defaults(func=cmd_client)
    return p


def main(argv=None) -> int:
    ns = build_parser().parse_args(argv)
    return ns.func(ns)


if __name__ == "__main__":
    raise SystemExit(main())
