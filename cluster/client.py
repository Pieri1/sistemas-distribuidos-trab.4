from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import grpc

from . import cluster_pb2, cluster_pb2_grpc


def stub(address: str):
    return cluster_pb2_grpc.ClusterServiceStub(grpc.insecure_channel(address))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True)
    parser.add_argument(
        "--nodes",
        default=os.environ.get("CLUSTER_NODES", ""),
        help="node_id=address pairs, comma separated, for reusing the last leader",
    )
    parser.add_argument(
        "--leader-cache",
        default=os.environ.get("LEADER_CACHE", ".cluster-leader"),
    )
    parser.add_argument("command", choices=["write", "read", "status"])
    parser.add_argument("key", nargs="?")
    parser.add_argument("value", nargs="?")
    args = parser.parse_args()
    nodes = dict(item.split("=", 1) for item in args.nodes.split(",") if item)
    target = args.address
    cache = Path(args.leader_cache)
    if cache.exists() and nodes:
        cached_leader = cache.read_text(encoding="utf-8").strip()
        target = nodes.get(cached_leader, target)
    service = stub(target)
    if args.command == "write":
        if args.key is None or args.value is None:
            parser.error("write requires KEY VALUE")
        result = service.ClientWrite(cluster_pb2.WriteRequest(key=args.key, value=args.value))
    elif args.command == "read":
        if args.key is None:
            parser.error("read requires KEY")
        result = service.Read(cluster_pb2.ReadRequest(key=args.key))
    else:
        result = service.Status(cluster_pb2.StatusRequest())
    if getattr(result, "leader", "") and nodes and args.command == "write":
        cache.write_text(result.leader, encoding="utf-8")
    print(json.dumps({field.name: getattr(result, field.name) for field in result.DESCRIPTOR.fields}, sort_keys=True))


if __name__ == "__main__":
    main()
