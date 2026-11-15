#!/usr/bin/env bash
set -euo pipefail

cache="$(mktemp)"
cleanup() {
  docker compose logs --no-color >"${COMPOSE_FAILURE_LOG:-/tmp/compose-failures.log}" 2>&1 || true
  docker compose down -v || true
  rm -f "$cache"
}
trap cleanup EXIT

docker compose up --build -d

leader_service=""
leader_port=""
for _ in $(seq 1 40); do
  for service in node1 node2 node3 node4; do
    port="$(docker compose port "$service" 5005${service#node} | sed -E 's/.*:([0-9]+)$/\1/')"
    status="$(python -m cluster.client --address "localhost:${port}" status 2>/dev/null || true)"
    if printf '%s' "$status" | grep -q '"role": "leader"'; then
      leader_service="$service"
      leader_port="$port"
      break 2
    fi
  done
  sleep 0.25
done
test -n "$leader_service"

follower_service="node1"
test "$follower_service" != "$leader_service" || follower_service="node2"
follower_port="$(docker compose port "$follower_service" 5005${follower_service#node} | sed -E 's/.*:([0-9]+)$/\1/')"

# Scenario 4A: follower catch-up after restart, with its volume preserved.
docker compose stop "$follower_service"
python -m cluster.client --address "localhost:${leader_port}" \
  --leader-cache "$cache" write delayed-key delayed-value
docker compose start "$follower_service"
for _ in $(seq 1 40); do
  read_result="$(python -m cluster.client --address "localhost:${follower_port}" read delayed-key 2>/dev/null || true)"
  if printf '%s' "$read_result" | grep -q '"found": true'; then
    break
  fi
  sleep 0.25
done
printf '%s' "$read_result" | grep -q '"found": true'

# Scenario 3: stop two followers and verify quorum rejection.
for _ in $(seq 1 40); do
  for service in node1 node2 node3 node4; do
    port="$(docker compose port "$service" 5005${service#node} | sed -E 's/.*:([0-9]+)$/\1/')"
    status="$(python -m cluster.client --address "localhost:${port}" status 2>/dev/null || true)"
    if printf '%s' "$status" | grep -q '"role": "leader"'; then
      leader_service="$service"
      leader_port="$port"
      break 2
    fi
  done
  sleep 0.25
done
test -n "$leader_service"
second_follower="node2"
test "$second_follower" != "$leader_service" || second_follower="node3"
test "$second_follower" != "$follower_service" || second_follower="node4"
docker compose stop "$second_follower" "$follower_service"
sleep 1
blocked="$(python -m cluster.client --address "localhost:${leader_port}" \
  --leader-cache "$cache" write blocked-key blocked-value || true)"
printf '%s' "$blocked" | grep -Eq 'quorum|leader'
