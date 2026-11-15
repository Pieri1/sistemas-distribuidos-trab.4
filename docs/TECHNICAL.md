# Technical Overview

## Architecture

The system contains one CLI client and four identical gRPC nodes. Each node
can become a follower, candidate, or leader. Nodes store their durable state
in one JSON document per volume:

- current term and granted vote;
- ordered operation log;
- commit index;
- materialized committed key-value data.

Writes are accepted only by the leader. A new log entry receives the current
term and the next sequential index. The leader acknowledges the client only
after the entry is confirmed by a majority of three nodes.

## Pull flow

Followers periodically call `Fetch` on the known leader. The request carries
the follower term, last log index, and previous log term. The leader uses this
prefix information to detect divergence, returns missing entries and the
current commit index, and records the follower heartbeat.

Followers reconcile missing or conflicting suffixes, apply only the received
commit index, and expose committed values through `Read`. Uncommitted log
entries remain durable but are not visible in the materialized data.

## Election flow

When a follower stops receiving successful leader communication before its
randomized election deadline, it increments its term, votes for itself, and
requests votes through gRPC. A node votes at most once per term and rejects
candidates with older logs. A candidate becomes leader after reaching the
three-node quorum. A higher observed term forces the receiving node back to
follower and clears its previous vote.

## Client and operations

`cluster.client` supports `write`, `read`, and `status`. A rejected write
includes the known leader when available. The client can cache that leader ID
and reuse the corresponding address on later commands. Nodes emit structured
JSON logs containing role, term, commit index, log index, and operation
identifiers.

## Requirements covered

- Python implementation with gRPC-only node communication.
- Four-node topology with a fixed majority quorum of three.
- Leader election, single-vote-per-term, and candidate log validation.
- Pull-based replication and heartbeat-based replica liveness tracking.
- Commit confirmation before successful client responses.
- Durable state and recovery from persistent JSON volumes.
- Eventual reads from every node using committed data only.
- Log conflict detection, truncation, and follower resynchronization.
- Docker Compose execution plus local and container-level validation scripts.

## Validation result

The local suite has 13 passing tests. `make compose-smoke`, 
`make compose-failures`, Python compilation, and `docker compose config` have
also passed. The repository is ready for demonstration of normal replication,
leader failure, follower restart, and quorum rejection.

The full crash-before-replication divergence demonstration and the manual
inspection of uncommitted visibility remain presentation scenarios rather
than independent automated end-to-end checks.

## Design choices and limits

JSON was chosen for transparent persistence and easy inspection during a
demonstration. Docker Compose provides deterministic local networking and
persistent volumes. The quorum is intentionally fixed to three because the
required topology has four nodes. The implementation is educational and does
not include authentication, TLS, dynamic membership, or production-grade
parallel replication management.
