"""Raft safety and liveness, exercised through the deterministic SimCluster."""

import pytest

from dchat.cluster import SimCluster
from dchat.raft import Role


def test_single_leader_elected():
    c = SimCluster(5, seed=1)
    leader = c.wait_for_leader()
    assert leader.role == Role.LEADER
    assert c.leaders() == [leader.node_id]
    # every follower agrees on the term and the leader
    for nid in c.ids:
        if nid != leader.node_id:
            assert c.nodes[nid].current_term == leader.current_term
            assert c.nodes[nid].leader_id == leader.node_id


def test_replication_reaches_every_node_in_order():
    c = SimCluster(5, seed=2)
    c.wait_for_leader()
    for i in range(10):
        c.client_send(f"msg-{i}", sender="alice")
    c.run_for(1.0)
    logs = c.all_message_logs()
    expected = [f"msg-{i}" for i in range(10)]
    for nid, texts in logs.items():
        assert texts == expected, nid


def test_new_leader_after_leader_crash_keeps_committed_history():
    c = SimCluster(5, seed=3)
    old = c.wait_for_leader()
    for i in range(5):
        c.client_send(f"before-{i}")
    committed_before = c.states[old.node_id].transcript()

    c.stop(old.node_id)                      # leader crashes
    c.run_for(0.1)
    new = c.wait_for_leader(timeout=c.now + 5)
    assert new.node_id != old.node_id

    for i in range(5):
        c.client_send(f"after-{i}")
    c.run_for(1.0)

    # every still-running node has the full, ordered history
    survivors = [n for n in c.ids if n != old.node_id]
    expected = [f"before-{i}" for i in range(5)] + [f"after-{i}" for i in range(5)]
    for nid in survivors:
        assert [m["text"] for m in c.states[nid].messages] == expected
    # and nothing committed before the crash was lost
    assert c.states[survivors[0]].transcript()[:5] == committed_before


def test_minority_partition_cannot_elect_or_commit():
    c = SimCluster(5, seed=4)
    leader = c.wait_for_leader()
    majority = set(list(c.ids)[:3])
    minority = set(list(c.ids)[3:])
    # make sure the leader is in the majority side
    if leader.node_id in minority:
        majority, minority = minority | {leader.node_id}, minority - {leader.node_id}
        majority = set(list(c.ids)[:3])
    c.partition(majority, minority)
    c.run_for(2.0)

    # minority side: no leader, no progress
    assert all(c.nodes[n].role != Role.LEADER for n in minority) or \
           all(not c.nodes[n].is_committed(c.nodes[n].last_log_index)
               for n in minority if c.nodes[n].last_log_index > c.nodes[n].commit_index)
    # majority side elects (or keeps) a leader and can still commit
    maj_leaders = [n for n in majority if c.nodes[n].role == Role.LEADER]
    assert len(maj_leaders) == 1

    # heal and confirm the minority catches up
    c.heal()
    c.client_send("after-heal")
    c.run_for(2.0)
    logs = c.all_message_logs()
    ref = logs[maj_leaders[0]]
    for nid in c.ids:
        assert logs[nid] == ref


def test_term_monotonic_and_no_two_leaders_same_term():
    c = SimCluster(5, seed=7)
    seen: dict[int, str] = {}
    for _ in range(400):
        c.step()
        for nid, node in c.nodes.items():
            if node.role == Role.LEADER:
                assert seen.setdefault(node.current_term, nid) == nid, \
                    f"two leaders in term {node.current_term}"
        # random churn
        if c.now % 1.0 < 0.01 and c.leaders():
            victim = c.leaders()[0]
            c.stop(victim)
            c.run_for(0.05)
            c.start(victim)


def test_dropped_messages_still_converge():
    c = SimCluster(5, seed=11, loss=0.2)      # 20% packet loss
    c.wait_for_leader(timeout=8.0)
    for i in range(8):
        c.client_send(f"lossy-{i}", timeout=8.0)
    c.run_for(3.0)
    logs = c.all_message_logs()
    ref = [f"lossy-{i}" for i in range(8)]
    for nid, texts in logs.items():
        assert texts == ref, nid


def test_isolated_node_with_inflated_term_rejoins_without_losing_history():
    """A node alone in a partition keeps bumping its term. When it rejoins with
    a term far higher than the working majority's, committed history must
    survive even though it forces a fresh election."""
    c = SimCluster(5, seed=5)
    leader = c.wait_for_leader()
    for i in range(6):
        c.client_send(f"committed-{i}")

    lonely = next(n for n in c.ids if n != leader.node_id)   # isolate a follower
    rest = {n for n in c.ids if n != lonely}
    c.partition({lonely}, rest)
    c.run_for(3.0)                      # the lonely node spins its term way up
    a_majority_node = next(iter(rest))
    assert c.nodes[lonely].current_term > c.nodes[a_majority_node].current_term
    for i in range(3):
        c.client_send(f"more-{i}")      # majority keeps working

    c.heal()
    c.client_send("final")
    c.run_for(4.0)

    expected = ([f"committed-{i}" for i in range(6)]
                + [f"more-{i}" for i in range(3)] + ["final"])
    for nid, texts in c.all_message_logs().items():
        assert texts == expected, nid


def test_single_node_cluster_is_its_own_leader():
    c = SimCluster(1, seed=1)
    leader = c.wait_for_leader()
    c.client_send("solo")
    c.run_for(0.2)
    assert [m["text"] for m in c.states[leader.node_id].messages] == ["solo"]
