import pytest

from cluster.state import Entry, PersistentState
from cluster import cluster_pb2
from cluster.node import Node


def test_state_round_trip_and_commit_visibility(tmp_path):
    path = tmp_path / "node.json"
    state = PersistentState(path)
    state.append(Entry(1, 2, "a", "secret"))
    assert state.data == {}
    state.apply_commit(0)
    assert state.data == {}
    state.apply_commit(1)
    assert state.data == {"a": "secret"}
    restored = PersistentState(path)
    assert restored.term == 0
    assert restored.commit_index == 1
    assert restored.data == {"a": "secret"}


def test_reconcile_truncates_uncommitted_suffix(tmp_path):
    state = PersistentState(tmp_path / "node.json")
    state.append(Entry(1, 1, "a", "old"))
    state.append(Entry(2, 1, "b", "uncommitted"))
    state.reconcile([Entry(2, 2, "b", "new"), Entry(3, 2, "c", "value")])
    assert state.log == [
        Entry(1, 1, "a", "old"),
        Entry(2, 2, "b", "new"),
        Entry(3, 2, "c", "value"),
    ]


def test_committed_suffix_cannot_be_truncated(tmp_path):
    state = PersistentState(tmp_path / "node.json")
    state.append(Entry(1, 1, "a", "old"))
    state.apply_commit(1)
    try:
        state.reconcile([Entry(1, 2, "a", "new")])
    except ValueError as error:
        assert "committed" in str(error)
    else:
        raise AssertionError("committed log entry was truncated")


def test_truncate_to_removes_only_uncommitted_suffix(tmp_path):
    state = PersistentState(tmp_path / "node.json")
    state.append(Entry(1, 1, "a", "one"))
    state.append(Entry(2, 1, "b", "two"))
    state.truncate_to(1)
    assert state.last_index == 1
    state.apply_commit(1)
    with pytest.raises(ValueError):
        state.truncate_to(0)


def test_vote_is_unique_and_requires_current_log(tmp_path):
    node = Node("n1", "127.0.0.1:0", {}, str(tmp_path))
    first = node.RequestVote(
        cluster_pb2.VoteRequest(
            candidate_id="n2", term=1, last_log_index=0, last_log_term=0
        ),
        None,
    )
    second = node.RequestVote(
        cluster_pb2.VoteRequest(
            candidate_id="n3", term=1, last_log_index=0, last_log_term=0
        ),
        None,
    )
    assert first.vote_granted
    assert not second.vote_granted


def test_vote_rejects_candidate_with_older_log(tmp_path):
    node = Node("n1", "127.0.0.1:0", {}, str(tmp_path))
    node.state.append(Entry(1, 2, "key", "value"))
    response = node.RequestVote(
        cluster_pb2.VoteRequest(
            candidate_id="n2", term=3, last_log_index=0, last_log_term=0
        ),
        None,
    )
    assert not response.vote_granted
    assert node.state.term == 3


def test_higher_term_forces_follower_and_persists(tmp_path):
    node = Node("n1", "127.0.0.1:0", {}, str(tmp_path))
    node.role = "leader"
    node.state.term = 1
    response = node.RequestVote(
        cluster_pb2.VoteRequest(
            candidate_id="n2", term=4, last_log_index=0, last_log_term=0
        ),
        None,
    )
    assert response.vote_granted
    assert node.state.term == 4
    assert node.role.value == "follower"
    assert PersistentState(tmp_path / "n1.json").term == 4
