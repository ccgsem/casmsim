"""Generated gRPC contract stubs for the loopback runner transport.

Source
------
proto/casm_runner/v1/casm_runner.proto  (SPDX-License-Identifier: MIT)

Regenerate with
---------------
    python -m grpc_tools.protoc \\
        --proto_path=proto/casm_runner/v1 \\
        --python_out=casmsim/proto \\
        --grpc_python_out=casmsim/proto \\
        casm_runner.proto

    # Fix the relative import that protoc emits for grpc stubs:
    sed -i 's/^import casm_runner_pb2/from casmsim.proto import casm_runner_pb2/' \\
        casmsim/proto/casm_runner_pb2_grpc.py

Do not edit the generated files directly.  Commit both pb2 files together
with the .proto source whenever the contract changes.

Generator: grpcio-tools (Protobuf Python Version: 7.35.1)
"""
