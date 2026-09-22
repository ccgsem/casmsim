#!/usr/bin/env bash
# Regenerate gRPC/protobuf stubs from proto/casm_runner/v1/casm_runner.proto.
#
# Run from the repo root:
#   bash scripts/regen_proto.sh
#
# Requires grpcio-tools in the active environment:
#   pip install grpcio-tools
set -euo pipefail

PROTO_SRC="proto/casm_runner/v1"
OUT="casmsim/proto"

python -m grpc_tools.protoc \
    --proto_path="$PROTO_SRC" \
    --python_out="$OUT" \
    --pyi_out="$OUT" \
    --grpc_python_out="$OUT" \
    casm_runner.proto

# protoc emits a bare `import casm_runner_pb2` in the grpc stub; fix it to
# the full package path so the module is importable as casmsim.proto.
sed -i 's/^import casm_runner_pb2/from casmsim.proto import casm_runner_pb2/' \
    "$OUT/casm_runner_pb2_grpc.py"

echo "Regenerated:"
echo "  $OUT/casm_runner_pb2.py"
echo "  $OUT/casm_runner_pb2.pyi"
echo "  $OUT/casm_runner_pb2_grpc.py"
echo "Commit generated Python and typing files together with any .proto changes."
