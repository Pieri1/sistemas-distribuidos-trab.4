# Pull Replication Cluster

A compact distributed-systems project that implements a four-node data
replication cluster in Python using gRPC, Docker Compose, and a Pull-based
replication model.

## Highlights

- Raft-style leader election with randomized election timeouts.
- Four interchangeable nodes: follower, candidate, and leader.
- Pull replication through periodic `FetchRequest` heartbeats.
- Majority quorum of three nodes before acknowledging writes.
- Durable JSON state for terms, votes, logs, commit indexes, and committed data.
- Eventual reads from any node, exposing committed data only.
- Conflict detection and suffix reconciliation after failures.
- JSON-line operational logs for terms, roles, commits, and operations.

## Run

```bash
docker compose up --build -d
python -m cluster.client --address localhost:50051 status
python -m cluster.client --address localhost:50051 \
  --nodes node1=localhost:50051,node2=localhost:50052,node3=localhost:50053,node4=localhost:50054 \
  write greeting "hello distributed systems"
python -m cluster.client --address localhost:50052 read greeting
```

The project is also ready for the included devcontainer:

```bash
pip install -r requirements.txt
make proto
pytest -q
```

## Validation

```bash
make compose-smoke
make compose-failures
docker compose config
```

The smoke scenario validates normal replication. The failure scenario covers
follower recovery and write rejection when the cluster loses quorum.

See [docs/TECHNICAL.md](docs/TECHNICAL.md) for the architecture, data flow,
requirements, design decisions, and known limitations. For a live,
step-by-step demonstration, use [docs/guide.md](docs/guide.md).
