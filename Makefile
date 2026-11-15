PROTO=proto/cluster.proto

.PHONY: proto test compose-smoke compose-failures

proto:
	python -m grpc_tools.protoc -I proto --python_out=cluster --grpc_python_out=cluster $(PROTO)
	sed -i 's/^import cluster_pb2/from . import cluster_pb2/' cluster/cluster_pb2_grpc.py

test:
	python -m pytest

compose-smoke:
	bash scripts/compose_smoke.sh

compose-failures:
	bash scripts/compose_failures.sh
