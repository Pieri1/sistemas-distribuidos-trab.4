#!/usr/bin/env bash
set -euo pipefail

trap 'docker compose down' EXIT
docker compose up --build -d

leader=""
for _ in $(seq 1 40); do
  for port in 50051 50052 50053 50054; do
    status="$(python -m cluster.client --address "localhost:${port}" status 2>/dev/null || true)"
    if printf '%s' "$status" | grep -q '"role": "leader"'; then
      leader="$port"
      break 2
    fi
  done
  sleep 0.25
done

test -n "$leader"
python -m cluster.client --address "localhost:${leader}" \
  write smoke-test compose
sleep 0.5
for port in 50051 50052 50053 50054; do
  python -m cluster.client --address "localhost:${port}" read smoke-test
done
