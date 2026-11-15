import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from cluster import cluster_pb2, cluster_pb2_grpc
from cluster.node import Node, Role
from cluster.state import Entry


@pytest.fixture
def cluster():
    data_dir = tempfile.mkdtemp()
    ports = {f"node{i}": f"127.0.0.1:{64500 + i}" for i in range(1, 5)}
    nodes = []
    for node_id, address in ports.items():
        peers = {key: value for key, value in ports.items() if key != node_id}
        node = Node(
            node_id,
            address,
            peers,
            data_dir,
            election_range=(0.2, 0.4),
            heartbeat_timeout=0.6,
            fetch_interval=0.05,
        )
        node.start()
        nodes.append(node)
    stubs = {
        node_id: cluster_pb2_grpc.ClusterServiceStub(
            grpc.insecure_channel(address)
        )
        for node_id, address in ports.items()
    }
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        leaders = [
            node_id
            for node_id, stub in stubs.items()
            if stub.Status(cluster_pb2.StatusRequest()).role == Role.LEADER.value
        ]
        if len(leaders) == 1:
            break
        time.sleep(0.05)
    else:
        pytest.fail("cluster did not elect one leader")
    time.sleep(1.2)
    try:
        yield nodes, stubs, ports, leaders[0]
    finally:
        for node in nodes:
            node.stop()


def test_write_and_read_all_nodes(cluster):
    _, stubs, _, leader = cluster
    leader = next(
        node_id
        for node_id, stub in stubs.items()
        if stub.Status(cluster_pb2.StatusRequest()).role == Role.LEADER.value
    )
    result = stubs[leader].ClientWrite(
        cluster_pb2.WriteRequest(key="course", value="distributed"), timeout=3
    )
    assert result.success
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if all(
            stub.Read(cluster_pb2.ReadRequest(key="course")).found
            for stub in stubs.values()
        ):
            return
        time.sleep(0.05)
    pytest.fail("committed value did not converge to all nodes")


def test_leader_failure_elects_new_term_and_writes(cluster):
    nodes, stubs, _, leader = cluster
    old_term = stubs[leader].Status(cluster_pb2.StatusRequest()).term
    next(node for node in nodes if node.node_id == leader).stop()
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        statuses = {
            node_id: stub.Status(cluster_pb2.StatusRequest())
            for node_id, stub in stubs.items()
            if node_id != leader
        }
        new_leaders = [
            node_id for node_id, status in statuses.items()
            if status.role == Role.LEADER.value
        ]
        if len(new_leaders) == 1 and statuses[new_leaders[0]].term > old_term:
            new_leader = new_leaders[0]
            time.sleep(1.0)
            assert stubs[new_leader].ClientWrite(
                cluster_pb2.WriteRequest(key="after", value="failure"), timeout=3
            ).success
            return
        time.sleep(0.05)
    pytest.fail("cluster did not elect a replacement leader")


def test_two_failures_block_new_writes(cluster):
    nodes, stubs, _, leader = cluster
    followers = [node for node in nodes if node.node_id != leader][:2]
    for follower in followers:
        follower.stop()
    time.sleep(0.9)
    stopped_ids = {follower.node_id for follower in followers}
    active_leader = next(
        (
            node_id
            for node_id, stub in stubs.items()
            if node_id not in stopped_ids
            and stub.Status(cluster_pb2.StatusRequest()).role == Role.LEADER.value
        ),
        leader,
    )
    result = stubs[active_leader].ClientWrite(
        cluster_pb2.WriteRequest(key="blocked", value="no-quorum"), timeout=3
    )
    assert not result.success
    assert "quorum" in result.message or "leader" in result.message


def test_fetch_reconciles_same_index_conflict(tmp_path):
    leader = Node("leader", "127.0.0.1:0", {}, str(tmp_path / "leader"))
    follower = Node("follower", "127.0.0.1:0", {}, str(tmp_path / "follower"))
    leader.role = Role.LEADER
    leader.state.term = 3
    leader.state.append(Entry(1, 3, "key", "new"))
    follower.state.term = 3
    follower.state.append(Entry(1, 2, "key", "old"))
    response = leader.Fetch(
        cluster_pb2.FetchRequest(
            node_id="follower",
            term=3,
            last_log_index=1,
            prev_log_index=1,
            prev_log_term=2,
        ),
        None,
    )
    assert not response.success
    follower._handle_fetch_response(response)
    assert follower.state.log == [Entry(1, 3, "key", "new")]


def test_concurrent_writes_commit_distinct_entries(cluster):
    _, stubs, _, leader = cluster
    leader = next(
        node_id
        for node_id, stub in stubs.items()
        if stub.Status(cluster_pb2.StatusRequest()).role == Role.LEADER.value
    )

    def write(index):
        return stubs[leader].ClientWrite(
            cluster_pb2.WriteRequest(key=f"parallel-{index}", value=str(index)),
            timeout=5,
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(write, range(4)))

    assert all(response.success for response in responses)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if all(
            stub.Read(cluster_pb2.ReadRequest(key=f"parallel-{index}")).found
            for stub in stubs.values()
            for index in range(4)
        ):
            return
        time.sleep(0.05)
    pytest.fail("concurrent committed values did not converge")
