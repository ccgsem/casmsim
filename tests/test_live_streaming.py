"""Live StreamObs behaviour: push while running, pre-Start connect, worker capacity, stale manifests."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import grpc
import pyarrow as pa
import pytest

from casmsim.grpc_runner import DEFAULT_MAX_WORKERS, ENDPOINT_FILENAME, start_control_server
from casmsim.observation_broker import ObservationBroker
from casmsim.proto import casm_runner_pb2 as pb2, casm_runner_pb2_grpc as pb2_grpc


def _stub(run_dir: Path) -> tuple[grpc.Channel, pb2_grpc.SimulatorControlStub]:
    address = json.loads((run_dir / ENDPOINT_FILENAME).read_text())["control"]["address"]
    channel = grpc.insecure_channel(address)
    return channel, pb2_grpc.SimulatorControlStub(channel)


def _values(batch: pb2.ObsBatch) -> list[int]:
    return pa.ipc.open_stream(pa.py_buffer(batch.arrow_ipc)).read_all().column("value").to_pylist()


class _SteppedRun:
    """A start_run callback that publishes one batch per release() call."""

    def __init__(self, broker: ObservationBroker, ticks: int, channels: tuple[str, ...] = ("agents",)) -> None:
        self.broker = broker
        self.ticks = ticks
        self.channels = channels
        self.step = threading.Semaphore(0)

    def __call__(self, run_id: str, config_json: bytes) -> None:
        for tick in range(self.ticks):
            assert self.step.acquire(timeout=5)
            for channel in self.channels:
                self.broker.publish(channel, pa.table({"value": [tick]}))
        self.broker.close()

    def release(self, n: int = 1) -> None:
        for _ in range(n):
            self.step.release()


def test_stream_obs_pushes_batches_while_the_run_is_in_progress(tmp_path):
    broker = ObservationBroker()
    run = _SteppedRun(broker, ticks=3)
    server = start_control_server(tmp_path, broker, run)
    channel, stub = _stub(tmp_path)
    try:
        stub.Start(pb2.StartRequest(run_id="r", config_json=b"{}"))
        stream = stub.StreamObs(pb2.StreamObsRequest(run_id="r", channel="agents"))

        run.release()
        first = next(stream)
        assert (first.tick, _values(first)) == (0, [0])
        assert stub.GetState(pb2.GetStateRequest(run_id="r")).state == pb2.RUN_STATE_RUNNING

        run.release(2)
        rest = list(stream)
        assert [(b.tick, _values(b)) for b in rest] == [(1, [1]), (2, [2])]
    finally:
        channel.close()
        server.stop(0).wait()


def test_stream_obs_connected_before_start_receives_every_batch(tmp_path):
    broker = ObservationBroker()
    run = _SteppedRun(broker, ticks=2)
    server = start_control_server(tmp_path, broker, run)
    channel, stub = _stub(tmp_path)
    received: list[int] = []
    try:
        consumer = threading.Thread(
            target=lambda: received.extend(
                b.tick for b in stub.StreamObs(pb2.StreamObsRequest(run_id="r", channel="agents"))
            )
        )
        consumer.start()
        time.sleep(0.1)
        stub.Start(pb2.StartRequest(run_id="r", config_json=b"{}"))
        run.release(2)
        consumer.join(5)
        assert not consumer.is_alive()
        assert received == [0, 1]
    finally:
        channel.close()
        server.stop(0).wait()


def test_stream_obs_times_out_when_no_run_starts(tmp_path):
    broker = ObservationBroker()
    server = start_control_server(tmp_path, broker, lambda run_id, cfg: None, start_timeout=0.2)
    channel, stub = _stub(tmp_path)
    try:
        with pytest.raises(grpc.RpcError) as error:
            list(stub.StreamObs(pb2.StreamObsRequest(run_id="r", channel="agents")))
        assert error.value.code() is grpc.StatusCode.DEADLINE_EXCEEDED
    finally:
        channel.close()
        server.stop(0).wait()


def test_concurrent_streams_leave_capacity_for_control_calls(tmp_path):
    channels = tuple(f"ch{i}" for i in range(8))
    assert len(channels) < DEFAULT_MAX_WORKERS
    broker = ObservationBroker()
    run = _SteppedRun(broker, ticks=1, channels=channels)
    server = start_control_server(tmp_path, broker, run)
    channel, stub = _stub(tmp_path)
    results: dict[str, list[int]] = {}
    try:
        stub.Start(pb2.StartRequest(run_id="r", config_json=b"{}"))
        consumers = [
            threading.Thread(
                target=lambda name=name: results.__setitem__(
                    name, [b.tick for b in stub.StreamObs(pb2.StreamObsRequest(run_id="r", channel=name))]
                )
            )
            for name in channels
        ]
        for consumer in consumers:
            consumer.start()
        time.sleep(0.2)
        # Eight blocked streams must not starve unary control RPCs.
        state = stub.GetState(pb2.GetStateRequest(run_id="r"), timeout=2)
        assert state.state == pb2.RUN_STATE_RUNNING
        run.release()
        for consumer in consumers:
            consumer.join(5)
        assert results == {name: [0] for name in channels}
    finally:
        channel.close()
        server.stop(0).wait()


def test_start_control_server_replaces_stale_endpoint_manifest(tmp_path):
    tmp_path.chmod(0o700)
    stale = tmp_path / ENDPOINT_FILENAME
    stale.write_text(json.dumps({"control": {"address": "127.0.0.1:1", "protocol": "casm.runner.v1"}}))
    # A read-only leftover cannot be overwritten in place; it must be removed first.
    stale.chmod(0o400)

    server = start_control_server(tmp_path, ObservationBroker(), lambda run_id, cfg: None)
    try:
        manifest = json.loads(stale.read_text())
        assert manifest["control"]["address"] != "127.0.0.1:1"
        assert stale.stat().st_mode & 0o777 == 0o600
    finally:
        server.stop(0).wait()


@pytest.mark.parametrize(
    ("start_tick", "code"),
    [(-1, grpc.StatusCode.INVALID_ARGUMENT), (5, grpc.StatusCode.OUT_OF_RANGE)],
)
def test_stream_obs_rejects_invalid_cursors(tmp_path, start_tick, code):
    broker = ObservationBroker()
    run = _SteppedRun(broker, ticks=1)
    server = start_control_server(tmp_path, broker, run)
    channel, stub = _stub(tmp_path)
    try:
        stub.Start(pb2.StartRequest(run_id="r", config_json=b"{}"))
        with pytest.raises(grpc.RpcError) as error:
            list(stub.StreamObs(pb2.StreamObsRequest(run_id="r", channel="agents", start_tick=start_tick), timeout=2))
        assert error.value.code() is code
    finally:
        run.release()
        channel.close()
        server.stop(0).wait()
