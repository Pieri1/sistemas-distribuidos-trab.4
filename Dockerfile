FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml requirements.txt ./
COPY proto ./proto
COPY cluster ./cluster
RUN pip install --no-cache-dir -e .[dev] \
    && python -m grpc_tools.protoc -I proto --python_out=cluster --grpc_python_out=cluster proto/cluster.proto \
    && sed -i 's/^import cluster_pb2/from . import cluster_pb2/' cluster/cluster_pb2_grpc.py
COPY . .

ENTRYPOINT ["python", "-m"]
