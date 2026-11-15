# Presentation and Validation Guide

This guide is designed for a live demonstration. It avoids relying only on
pre-built scenario scripts: each important action can be entered manually and
explained while the cluster is running.

## 1. What is implemented

The presentation should connect each requirement to the implementation:

| Requirement | Implementation |
|---|---|
| Four independent cluster nodes | `docker-compose.yml` |
| Client process | `cluster/client.py` |
| Follower, candidate, and leader roles | `cluster/node.py:Role` |
| Randomized election timeout | `cluster/node.py:_new_election_deadline` |
| Election and one vote per term | `RequestVote` and `_start_election` |
| Candidate log validation | `RequestVote` log comparison |
| Pull replication and heartbeats | `Fetch` and `_fetch_loop` |
| Fixed quorum of three | `cluster/node.py:QUORUM` |
| Commit only after majority confirmation | `_advance_commit` |
| Durable term, vote, log, commit, and data | `cluster/state.py:PersistentState` |
| Read committed data from any node | `Read` and `PersistentState.data` |
| Conflict truncation and resynchronization | `reconcile` and `truncate_to` |
| Structured operational logs | `Node._log_event` |

All node-to-node communication uses the gRPC contract in
`proto/cluster.proto`.

## 2. Initial preparation

Run these commands from the repository root:

```bash
pip install -r requirements.txt
make proto
docker compose config
```

Run the complete local test suite visibly:

```bash
pytest -v
```

This includes persistence, commit visibility, voting rules, election,
replication, quorum rejection, conflict reconciliation, concurrent writes,
and client leader-cache behavior.

## 3. Start the cluster manually

Use one terminal for the cluster:

```bash
docker compose down -v
docker compose up --build -d
docker compose ps
docker compose logs -f
```

Use a second terminal for client commands. The four public addresses are:

```text
node1: localhost:50051
node2: localhost:50052
node3: localhost:50053
node4: localhost:50054
```

Inspect every node and identify the response with `"role": "leader"`:

```bash
python -m cluster.client --address localhost:50051 status
python -m cluster.client --address localhost:50052 status
python -m cluster.client --address localhost:50053 status
python -m cluster.client --address localhost:50054 status
```

Explain that `term`, `commit_index`, and `last_log_index` are returned by
`Status` and also appear in the JSON logs.

## 4. Scenario 1: normal write and distributed reads

Assume `node2` is the current leader. Replace the address if another node was
elected:

```bash
python -m cluster.client --address localhost:50052 \
  write classroom "distributed systems"
```

Read the same key from every node:

```bash
python -m cluster.client --address localhost:50051 read classroom
python -m cluster.client --address localhost:50052 read classroom
python -m cluster.client --address localhost:50053 read classroom
python -m cluster.client --address localhost:50054 read classroom
```

Expected evidence:

- the write response has `"success": true` and `"message": "committed"`;
- all four reads have `"found": true`;
- the logs show `log_appended`, `commit_advanced`, and `write_committed`;
- all nodes eventually show the same committed value.

## 5. Scenario 2: leader failure and automatic election

Stop the current leader. For example, if it is `node2`:

```bash
docker compose stop node2
sleep 3
```

Query the remaining nodes:

```bash
python -m cluster.client --address localhost:50051 status
python -m cluster.client --address localhost:50053 status
python -m cluster.client --address localhost:50054 status
```

Expected evidence:

- one remaining node becomes leader;
- the new leader has a higher `term`;
- the transition is follower -> candidate -> leader in the logs;
- the remaining cluster can accept a new write after the election.

For example, if `node3` becomes leader:

```bash
python -m cluster.client --address localhost:50053 \
  write after-election "new term"
```

## 6. Scenario 3: loss of quorum blocks writes

Reset the demonstration so this scenario starts with four nodes:

```bash
docker compose down
docker compose up -d
sleep 3
```

After identifying the leader, stop two followers. If `node2` is leader:

```bash
docker compose stop node1 node3
```

Attempt a write on the leader:

```bash
python -m cluster.client --address localhost:50052 \
  write blocked "should not commit"
```

Expected evidence:

- the response is unsuccessful;
- the message contains `quorum unavailable`;
- no new committed value appears;
- leader logs show `write_rejected` with
  `reason: quorum_unavailable`.

This demonstrates the fixed majority requirement: one leader plus only one
active follower is insufficient.

## 7. Scenario 4A: follower restart and catch-up

Reset the cluster:

```bash
docker compose down
docker compose up -d
sleep 3
```

Identify the leader, then stop one follower. Example:

```bash
docker compose stop node1
```

Make a successful write through the leader:

```bash
python -m cluster.client --address localhost:50052 \
  write delayed "recovered through fetch"
```

Restart the follower and wait for its pull loop:

```bash
docker compose start node1
sleep 3
python -m cluster.client --address localhost:50051 read delayed
```

Expected evidence:

- the write succeeds while one follower is offline because three nodes remain;
- the restarted node initially lacks the value;
- the restarted node receives the missing suffix via `Fetch`;
- its `commit_index` advances and the value becomes visible through `Read`;
- logs show `fetch_applied`.

## 8. Scenario 4B: divergent uncommitted suffix

The normal client intentionally waits for commit, so it cannot expose an
uncommitted write by itself. The divergence rule is implemented and covered by
the local conflict test, but a live demonstration requires controlled
inspection of the node state.

Show the implementation-level proof:

```bash
pytest -v tests/test_cluster.py -k conflict
```

This test creates a leader entry and a follower entry at the same index with
different terms, sends a `FetchResponse`, and verifies that the follower keeps
the leader's entry.

During the explanation, connect this result to:

- `Fetch` prefix validation;
- `PersistentState.reconcile`;
- `PersistentState.truncate_to`;
- preservation of committed entries;
- follower resynchronization through the next fetch cycle.

A future debug-only RPC could make this scenario fully interactive, but it is
not part of the production client flow because production writes must not
return before quorum commit.

## 9. Scenario 5: uncommitted data isolation

Show the state-level behavior directly:

```bash
pytest -v tests/test_state.py -k commit_visibility
```

The test appends an entry and verifies that `state.data` is empty. Only after
`apply_commit` is called does the key appear in the materialized database.

Explain the distinction:

- `log` stores received operations, including uncommitted ones;
- `commit_index` identifies the durable committed prefix;
- `data` contains only applied committed entries;
- `Read` consults `data`, never the uncommitted log suffix.

For a live Compose inspection, locate a node volume after a fetch and observe
that a key is absent from `data` until the leader sends a new commit index.

## 10. Persistence demonstration

Inspect the JSON state inside a running container:

```bash
docker compose exec node1 sh -lc 'cat /data/node1.json'
```

Point out these fields:

```text
term
voted_for
log
commit_index
data
```

Restart the service without removing its volume:

```bash
docker compose restart node1
docker compose exec node1 sh -lc 'cat /data/node1.json'
```

The state remains available because Compose volumes are persistent. Do not use
`docker compose down -v` between the steps of this persistence demonstration.

## 11. Logs to explain

The node logs are JSON lines. Useful events include:

- `node_started`;
- `role_changed`;
- `term_updated`;
- `leader_accepted`;
- `log_appended`;
- `fetch_applied`;
- `commit_advanced`;
- `write_committed`;
- `write_rejected`;
- `log_truncated`;
- `rpc_error`.

Useful fields are `node_id`, `role`, `term`, `commit_index`,
`last_log_index`, `leader`, and `operation_id`.

## 12. Cleanup

After the presentation:

```bash
docker compose down -v
```

This removes containers, networks, and demonstration volumes. Use it only
after persistence and restart scenarios are complete.

