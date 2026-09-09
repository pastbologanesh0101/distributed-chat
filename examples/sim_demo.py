"""A narrated run of the deterministic Raft simulator: election, replication,
a leader crash, and a network partition — no sockets, fully reproducible.

    python examples/sim_demo.py
"""

from dchat.cluster import SimCluster


def show(c, label):
    print(f"\n--- {label} ---")
    for nid in c.ids:
        n = c.nodes[nid]
        mark = "  (down)" if nid in c.down else ""
        print(f"  {nid}: {n.role.value:<9} term={n.current_term} "
              f"leader={n.leader_id} log={len(n.log)} commit={n.commit_index}{mark}")


c = SimCluster(5, seed=42)
leader = c.wait_for_leader()
show(c, f"{leader.node_id} elected leader")

for i in range(5):
    c.client_send(f"hello #{i}", sender="alice")
c.run_for(0.5)
show(c, "5 messages replicated to every node")
print("  transcript on n3:", c.states["n3"].transcript())

print(f"\n>>> crashing the leader ({leader.node_id})")
c.stop(leader.node_id)
new_leader = c.wait_for_leader(timeout=c.now + 5)
show(c, f"{new_leader.node_id} took over")

for i in range(3):
    c.client_send(f"after-crash #{i}", sender="bob")
c.run_for(0.5)
print("  transcript on", new_leader.node_id, ":",
      c.states[new_leader.node_id].transcript())

print("\n>>> partitioning {n0,n1} | {n2,n3,n4}")
c.partition({"n0", "n1"}, {"n2", "n3", "n4"})
c.run_for(2.0)
show(c, "only the majority side has a leader / makes progress")

print("\n>>> healing the partition, sending one more message")
c.heal()
c.client_send("reunited", sender="carol")
c.run_for(3.0)
logs = {nid: v for nid, v in c.all_message_logs().items() if nid not in c.down}
same = len(set(tuple(v) for v in logs.values())) == 1
print(f"  every live node's transcript identical: {same}")
print(f"  final message count: {len(next(iter(logs.values())))}")
assert same, logs
