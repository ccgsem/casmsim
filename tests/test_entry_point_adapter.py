"""Integration tests for the runner.entry_point resolution path.

Exercises the full path from ``resolve_adapter`` through ``run_submitted_model``
and the gRPC SimulatorControl server, using ``casmsim.adapters.toy:ToyRunnerAdapter``
as the simulation engine — no MPI or casmsocial required.
"""

from __future__ import annotations

import json
import time

import grpc
import pyarrow as pa

from casmsim.adapters.toy import ToyRunnerAdapter
from casmsim.grpc_runner import (
    _BrokerObservationAdapter,
    resolve_adapter,
    run_submitted_model,
    start_control_server,
)
from casmsim.observation_broker import ObservationBroker
from casmsim.protocols import ObservationAdapter, RunnerModelAdapter
from casmsim.proto import casm_runner_pb2 as pb2, casm_runner_pb2_grpc as pb2_grpc
from casmsim.run_state import RunState


# ---------------------------------------------------------------------------
# Unit tests: resolve_adapter
# ---------------------------------------------------------------------------


def test_resolve_adapter_entry_point_returns_toy_adapter():
    params = {
        "runner.entry_point": "casmsim.adapters.toy:ToyRunnerAdapter",
        "toy.ticks": 5,
    }
    adapter = resolve_adapter(None, params)
    assert isinstance(adapter, ToyRunnerAdapter)


def test_resolve_adapter_entry_point_takes_precedence_over_plugins():
    """runner.entry_point wins even if model.plugins is also present."""
    params = {
        "runner.entry_point": "casmsim.adapters.toy:ToyRunnerAdapter",
        "model.plugins": ["some.plugin"],
        "model.name": "SomeModel",
        "toy.ticks": 1,
    }
    adapter = resolve_adapter(None, params)
    assert isinstance(adapter, ToyRunnerAdapter)


def test_resolve_adapter_raises_on_bad_entry_point_format():
    import pytest

    with pytest.raises(ValueError, match="module:Class"):
        resolve_adapter(None, {"runner.entry_point": "casmsim.adapters.toy.ToyRunnerAdapter"})


def test_resolve_adapter_raises_on_missing_class():
    import pytest

    with pytest.raises(ImportError):
        resolve_adapter(None, {"runner.entry_point": "casmsim.adapters.toy:NoSuchClass"})


def test_resolve_adapter_raises_on_empty_params():
    import pytest

    with pytest.raises(ValueError, match="runner.entry_point"):
        resolve_adapter(None, {})


def test_resolve_adapter_does_not_implicitly_select_a_casmsocial_adapter():
    """Model packages, rather than casmsim, select their own integration adapter."""
    import pytest

    with pytest.raises(ValueError, match="runner.entry_point"):
        resolve_adapter(None, {"model.name": "wake", "model.plugins": ["wake.plugin"]})


# ---------------------------------------------------------------------------
# Protocol conformance: ToyRunnerAdapter
# ---------------------------------------------------------------------------


def test_toy_adapter_satisfies_runner_model_adapter_protocol():
    assert isinstance(ToyRunnerAdapter(None, {}), RunnerModelAdapter)


def test_toy_adapter_run_produces_correct_observations():
    class _Sink:
        def __init__(self):
            self.published: list[tuple[str, pa.Table]] = []
            self.flushed = False

        def publish(self, channel: str, table: pa.Table) -> None:
            self.published.append((channel, table))

        def flush(self) -> None:
            self.flushed = True

    sink = _Sink()
    assert isinstance(sink, ObservationAdapter)

    adapter = ToyRunnerAdapter(None, {"toy.ticks": 4, "toy.channel": "data"})
    adapter.add_observer(sink)
    adapter.start()

    assert adapter.get_state()[0] == RunState.Completed
    assert len(sink.published) == 4
    assert all(ch == "data" for ch, _ in sink.published)
    assert [t.column("tick")[0].as_py() for _, t in sink.published] == [0, 1, 2, 3]
    assert sink.flushed


def test_toy_adapter_cancel_before_start():
    class _Sink:
        published: list = []
        flushed = False

        def publish(self, channel, table):
            self.published.append(table)

        def flush(self):
            self.flushed = True

    sink = _Sink()
    adapter = ToyRunnerAdapter(None, {"toy.ticks": 10})
    adapter.add_observer(sink)
    adapter.cancel()
    adapter.start()
    assert adapter.get_state()[0] == RunState.Failed
    assert sink.flushed


# ---------------------------------------------------------------------------
# Integration: run_submitted_model via runner.entry_point (gRPC round-trip)
# ---------------------------------------------------------------------------


def test_run_submitted_model_entry_point_end_to_end(tmp_path):
    """Full gRPC round-trip using runner.entry_point → ToyRunnerAdapter."""
    broker = ObservationBroker()
    config = json.dumps(
        {
            "runner.entry_point": "casmsim.adapters.toy:ToyRunnerAdapter",
            "toy.ticks": 3,
            "toy.channel": "ticks",
        }
    ).encode()

    control = start_control_server(
        tmp_path,
        broker,
        lambda run_id, cfg: run_submitted_model(run_id, cfg, broker),
    )
    try:
        manifest = json.loads((tmp_path / "runner_endpoints.json").read_text())
        channel = grpc.insecure_channel(manifest["control"]["address"])
        stub = pb2_grpc.SimulatorControlStub(channel)

        stub.Start(pb2.StartRequest(run_id="toy-run-1", config_json=config))

        # Poll until completed (toy adapter is fast; allow up to 5 s)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            state = stub.GetState(pb2.GetStateRequest(run_id="toy-run-1")).state
            if state in {pb2.RUN_STATE_COMPLETED, pb2.RUN_STATE_FAILED}:
                break
            time.sleep(0.02)

        assert state == pb2.RUN_STATE_COMPLETED

        batches = list(stub.StreamObs(pb2.StreamObsRequest(run_id="toy-run-1", channel="ticks")))
        assert len(batches) == 3
        assert [b.tick for b in batches] == [0, 1, 2]

        # Each batch should carry tick + value columns
        for i, batch in enumerate(batches):
            table = pa.ipc.open_stream(pa.py_buffer(batch.arrow_ipc)).read_all()
            assert table.column("tick")[0].as_py() == i

        channel.close()
    finally:
        control.stop(0).wait()


def test_broker_observation_adapter_publish_and_flush():
    broker = ObservationBroker()
    adapter = _BrokerObservationAdapter(broker)
    assert isinstance(adapter, ObservationAdapter)

    table = pa.table({"x": [1, 2, 3]})
    adapter.publish("ch", table)
    read = broker.read("ch")
    assert len(read.batches) == 1
    assert read.batches[0].table.equals(table)

    adapter.flush()
    assert broker.closed
