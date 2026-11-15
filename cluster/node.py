from __future__ import annotations

import argparse
import json
import logging
import os
import random
import threading
import time
from concurrent import futures
from enum import Enum

import grpc

from . import cluster_pb2, cluster_pb2_grpc
from .state import Entry, PersistentState

LOG = logging.getLogger(__name__)
QUORUM = 3


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class Node(cluster_pb2_grpc.ClusterServiceServicer):
    def __init__(
        self,
        node_id: str,
        address: str,
        peers: dict[str, str],
        data_dir: str,
        election_range: tuple[float, float] = (1.0, 2.0),
        heartbeat_timeout: float = 0.8,
        fetch_interval: float = 0.1,
    ):
        self.node_id = node_id
        self.address = address
        self.peers = peers
        self.state = PersistentState(os.path.join(data_dir, f"{node_id}.json"))
        self.role = Role.FOLLOWER
        self.leader_id: str | None = None
        self.election_range = election_range
        self.heartbeat_timeout = heartbeat_timeout
        self.fetch_interval = fetch_interval
        self.last_contact = time.monotonic()
        self.election_deadline = self._new_election_deadline()
        self.replica_contact: dict[str, float] = {}
        self.match_index: dict[str, int] = {peer: 0 for peer in peers}
        self.vote_count = 0
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.server: grpc.Server | None = None

    def _new_election_deadline(self) -> float:
        return time.monotonic() + random.uniform(*self.election_range)

    def _log_event(self, event: str, **fields: object) -> None:
        payload = {
            "event": event,
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.state.term,
            "commit_index": self.state.commit_index,
            "last_log_index": self.state.last_index,
            **fields,
        }
        LOG.info(json.dumps(payload, sort_keys=True))

    def start(self) -> grpc.Server:
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
        cluster_pb2_grpc.add_ClusterServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.address)
        self.server.start()
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._fetch_loop, daemon=True).start()
        self._log_event("node_started", address=self.address)
        return self.server

    def stop(self) -> None:
        self.stop_event.set()
        if self.server:
            self.server.stop(grace=0)

    def _set_term(self, term: int) -> None:
        if term > self.state.term:
            self.state.term = term
            self.state.voted_for = None
            self.role = Role.FOLLOWER
            self.leader_id = None
            self.state.save()
            self.election_deadline = self._new_election_deadline()
            self._log_event("term_updated", new_term=term)

    def _election_loop(self) -> None:
        deadline = time.monotonic() + random.uniform(*self.election_range)
        while not self.stop_event.wait(0.05):
            with self.lock:
                if self.role == Role.LEADER:
                    continue
                if time.monotonic() < self.election_deadline:
                    continue
            self._start_election()

    def _start_election(self) -> None:
        with self.lock:
            self.role = Role.CANDIDATE
            self.state.term += 1
            self.state.voted_for = self.node_id
            term = self.state.term
            self.vote_count = 1
            self.state.save()
            self.election_deadline = self._new_election_deadline()
            self._log_event("role_changed", new_role=Role.CANDIDATE.value)
        request = cluster_pb2.VoteRequest(
            candidate_id=self.node_id,
            term=term,
            last_log_index=self.state.last_index,
            last_log_term=self.state.last_term,
        )
        for peer, address in self.peers.items():
            try:
                response = cluster_pb2_grpc.ClusterServiceStub(
                    grpc.insecure_channel(address)
                ).RequestVote(request, timeout=0.4)
                with self.lock:
                    if response.term > self.state.term:
                        self._set_term(response.term)
                    elif self.role == Role.CANDIDATE and response.vote_granted:
                        self.vote_count += 1
                        if self.vote_count >= QUORUM:
                            self.role = Role.LEADER
                            self.leader_id = self.node_id
                            self.replica_contact = {}
                            self.match_index = {peer_id: 0 for peer_id in self.peers}
                            self._log_event("role_changed", new_role=Role.LEADER.value)
                            self._announce_leader(term)
            except grpc.RpcError:
                continue

    def _announce_leader(self, term: int) -> None:
        request = cluster_pb2.LeaderAnnouncement(
            leader_id=self.node_id, term=term
        )
        for peer, address in self.peers.items():
            try:
                response = cluster_pb2_grpc.ClusterServiceStub(
                    grpc.insecure_channel(address)
                ).AnnounceLeader(request, timeout=0.4)
                with self.lock:
                    if response.term > self.state.term:
                        self._set_term(response.term)
            except grpc.RpcError:
                self._log_event("rpc_error", rpc="AnnounceLeader", peer=peer)

    def _fetch_loop(self) -> None:
        while not self.stop_event.wait(self.fetch_interval):
            with self.lock:
                leader = self.leader_id
                is_leader = self.role == Role.LEADER
                term = self.state.term
                last_index = self.state.last_index
                last_term = self.state.last_term
            if is_leader or not leader or leader not in self.peers:
                continue
            try:
                response = cluster_pb2_grpc.ClusterServiceStub(
                    grpc.insecure_channel(self.peers[leader])
                ).Fetch(
                    cluster_pb2.FetchRequest(
                        node_id=self.node_id,
                        term=term,
                        last_log_index=last_index,
                        prev_log_index=last_index,
                        prev_log_term=last_term,
                    ),
                    timeout=0.5,
                )
                self._handle_fetch_response(response)
            except grpc.RpcError:
                continue

    def _handle_fetch_response(self, response: cluster_pb2.FetchResponse) -> None:
        with self.lock:
            if response.term > self.state.term:
                self._set_term(response.term)
            if response.leader:
                self.leader_id = response.leader
                if (
                    response.term == self.state.term
                    and response.leader != self.node_id
                    and self.role != Role.FOLLOWER
                ):
                    self.role = Role.FOLLOWER
                    self._log_event(
                        "role_changed",
                        new_role=Role.FOLLOWER.value,
                        reason="leader_contact",
                    )
            entries = [
                Entry(item.index, item.term, item.key, item.value)
                for item in response.entries
            ]
            if (
                not response.success
                and response.leader_last_index < self.state.last_index
            ):
                self.state.truncate_to(response.leader_last_index)
                self._log_event(
                    "log_truncated",
                    leader=response.leader,
                    truncated_to=response.leader_last_index,
                )
            if entries:
                self.state.reconcile(entries)
            self.state.apply_commit(response.commit_index)
            self.last_contact = time.monotonic()
            if response.leader and response.term == self.state.term:
                self.election_deadline = self._new_election_deadline()
            self._log_event(
                "fetch_applied",
                leader=response.leader,
                success=response.success,
                received_entries=len(entries),
            )

    def ClientWrite(self, request, context):
        with self.lock:
            if self.role != Role.LEADER:
                self._log_event(
                    "write_rejected",
                    reason="not_leader",
                    known_leader=self.leader_id or "",
                )
                return cluster_pb2.WriteResponse(
                    success=False,
                    message="write must be sent to the leader",
                    leader=self.leader_id or "",
                    term=self.state.term,
                )
            if not self._has_quorum():
                self._log_event("write_rejected", reason="quorum_unavailable")
                return cluster_pb2.WriteResponse(
                    success=False,
                    message="write rejected: quorum unavailable",
                    leader=self.node_id,
                    term=self.state.term,
                )
            entry = Entry(self.state.last_index + 1, self.state.term, request.key, request.value)
            self.state.append(entry)
            operation_id = f"{self.node_id}-{self.state.term}-{entry.index}"
            self._log_event("log_appended", operation_id=operation_id)
        return self._commit_if_replicated(entry, operation_id)

    def _has_quorum(self) -> bool:
        active = 1 + sum(time.monotonic() - timestamp <= self.heartbeat_timeout
                         for timestamp in self.replica_contact.values())
        return active >= QUORUM

    def _advance_commit(self) -> None:
        with self.lock:
            for index in range(self.state.commit_index + 1, self.state.last_index + 1):
                confirmations = 1 + sum(
                    matched >= index for matched in self.match_index.values()
                )
                if confirmations < QUORUM:
                    break
                self.state.apply_commit(index)
                self._log_event("commit_advanced", committed_index=index)

    def _commit_if_replicated(self, entry: Entry, operation_id: str):
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            self._advance_commit()
            if self.state.commit_index >= entry.index:
                self._log_event("write_committed", operation_id=operation_id)
                return cluster_pb2.WriteResponse(
                    success=True, message="committed", leader=self.node_id,
                    index=entry.index, term=self.state.term
                )
            time.sleep(self.fetch_interval)
        if self.state.commit_index >= entry.index:
            self._log_event("write_committed", operation_id=operation_id)
            return cluster_pb2.WriteResponse(
                success=True, message="committed", leader=self.node_id,
                index=entry.index, term=self.state.term
            )
        return cluster_pb2.WriteResponse(
            success=False, message="write not committed: quorum unavailable",
            leader=self.node_id, index=entry.index, term=self.state.term
        )

    def Read(self, request, context):
        with self.lock:
            if request.key not in self.state.data:
                return cluster_pb2.ReadResponse(
                    found=False, message="key not found", commit_index=self.state.commit_index
                )
            return cluster_pb2.ReadResponse(
                found=True, value=self.state.data[request.key],
                commit_index=self.state.commit_index
            )

    def Status(self, request, context):
        with self.lock:
            return cluster_pb2.StatusResponse(
                node_id=self.node_id, role=self.role.value, term=self.state.term,
                commit_index=self.state.commit_index, last_log_index=self.state.last_index,
                leader=self.leader_id or ""
            )

    def Fetch(self, request, context):
        with self.lock:
            if request.term > self.state.term:
                self._set_term(request.term)
            if request.term < self.state.term:
                return cluster_pb2.FetchResponse(
                    term=self.state.term, leader=self.leader_id or "",
                    success=False, message="stale term"
                )
            if self.role == Role.LEADER:
                self.replica_contact[request.node_id] = time.monotonic()
                prefix_matches = (
                    request.prev_log_index <= self.state.last_index
                    and (
                        request.prev_log_index == 0
                        or self.state.log[request.prev_log_index - 1].term
                        == request.prev_log_term
                    )
                )
                if prefix_matches:
                    start = min(request.prev_log_index, self.state.last_index)
                    self.match_index[request.node_id] = min(
                        request.last_log_index, self.state.last_index
                    )
                else:
                    start = max(min(request.prev_log_index, self.state.last_index) - 1, 0)
                entries = self.state.log[start:]
                self._advance_commit()
                return cluster_pb2.FetchResponse(
                    term=self.state.term, leader=self.node_id,
                    entries=[cluster_pb2.LogEntry(**entry.__dict__) for entry in entries],
                    commit_index=self.state.commit_index,
                    leader_last_index=self.state.last_index,
                    success=prefix_matches,
                    next_index=start + 1,
                    matched_index=self.match_index.get(request.node_id, 0),
                )
            if self.leader_id and self.leader_id != self.node_id:
                return cluster_pb2.FetchResponse(
                    term=self.state.term, leader=self.leader_id, success=False
                )
            return cluster_pb2.FetchResponse(term=self.state.term, success=False)

    def RequestVote(self, request, context):
        with self.lock:
            if request.term > self.state.term:
                self._set_term(request.term)
            if request.term < self.state.term:
                return cluster_pb2.VoteResponse(term=self.state.term, vote_granted=False)
            candidate_log_is_current = (
                request.last_log_term > self.state.last_term
                or (
                    request.last_log_term == self.state.last_term
                    and request.last_log_index >= self.state.last_index
                )
            )
            can_vote = self.state.voted_for in (None, request.candidate_id)
            granted = can_vote and candidate_log_is_current
            if granted:
                self.state.voted_for = request.candidate_id
                self.leader_id = request.candidate_id
                self.last_contact = time.monotonic()
                self.election_deadline = self._new_election_deadline()
                self.state.save()
            return cluster_pb2.VoteResponse(term=self.state.term, vote_granted=granted)

    def AnnounceLeader(self, request, context):
        with self.lock:
            if request.term < self.state.term:
                return cluster_pb2.LeaderAck(term=self.state.term, accepted=False)
            if request.term > self.state.term:
                self._set_term(request.term)
            if request.leader_id == self.node_id:
                return cluster_pb2.LeaderAck(term=self.state.term, accepted=False)
            self.role = Role.FOLLOWER
            self.leader_id = request.leader_id
            self.last_contact = time.monotonic()
            self.election_deadline = self._new_election_deadline()
            self.state.save()
            self._log_event("leader_accepted", leader=request.leader_id)
            return cluster_pb2.LeaderAck(term=self.state.term, accepted=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", default=os.environ.get("NODE_ID", "node1"))
    parser.add_argument("--address", default=os.environ.get("NODE_ADDRESS", "[::]:50051"))
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    parser.add_argument("--peers", default=os.environ.get("PEERS", ""))
    args = parser.parse_args()
    peers = dict(item.split("=", 1) for item in args.peers.split(",") if item)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    node = Node(args.id, args.address, peers, args.data_dir)
    node.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
