"""Protocol conformance tests for casmsim.

Verifies that every concrete RunnerModelAdapter and ObservationAdapter
implementation in casmsim satisfies the runtime-checkable protocols using
``isinstance`` — the final guarantee that the structural typing contract
holds at runtime, not just statically.

These tests catch any regression where a method is renamed or removed from
a concrete class without updating the protocol (or vice versa).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from casmsim.observation_broker import ObservationBroker
from casmsim.protocols import ObservationAdapter, RunnerModelAdapter
from casmsim.run_state import RunState

# ---------------------------------------------------------------------------
# Shared fixture adapters
# ---------------------------------------------------------------------------


def _toy_adapter() -> Any:
    from casmsim.adapters.toy import ToyRunnerAdapter

    return ToyRunnerAdapter(None, {"toy.ticks": 0, "toy.channel": "test"})


def _repast4py_adapter() -> Any:
    from casmsim.adapters.repast4py import Repast4pyAdapter

    class _MinimalModel:
        def __init__(self, comm, params):
            pass

        sim_time = 0.0
        tick = 0

        class schedule:
            @staticmethod
            def execute():
                return False

    class _Adapter(Repast4pyAdapter):
        MODEL_CLASS = _MinimalModel

    return _Adapter(None, {})


def _broker_observation_adapter() -> Any:
    """The internal _BrokerObservationAdapter used in grpc_runner."""
    from casmsim.grpc_runner import _BrokerObservationAdapter

    return _BrokerObservationAdapter(ObservationBroker())


# ---------------------------------------------------------------------------
# RunnerModelAdapter conformance
# ---------------------------------------------------------------------------


class TestRunnerModelAdapterConformance:
    """Every concrete RunnerModelAdapter must satisfy isinstance checks."""

    @pytest.mark.parametrize(
        "make_adapter",
        [
            _toy_adapter,
            _repast4py_adapter,
        ],
        ids=["ToyRunnerAdapter", "Repast4pyAdapter"],
    )
    def test_isinstance_runner_model_adapter(self, make_adapter):
        adapter = make_adapter()
        assert isinstance(adapter, RunnerModelAdapter), (
            f"{type(adapter).__name__} does not satisfy RunnerModelAdapter protocol"
        )

    @pytest.mark.parametrize(
        "make_adapter",
        [
            _toy_adapter,
            _repast4py_adapter,
        ],
        ids=["ToyRunnerAdapter", "Repast4pyAdapter"],
    )
    def test_has_all_protocol_methods(self, make_adapter):
        adapter = make_adapter()
        for method in ("start", "cancel", "get_state", "add_observer"):
            assert callable(getattr(adapter, method, None)), f"{type(adapter).__name__} missing callable '{method}'"

    @pytest.mark.parametrize(
        "make_adapter",
        [
            _toy_adapter,
            _repast4py_adapter,
        ],
        ids=["ToyRunnerAdapter", "Repast4pyAdapter"],
    )
    def test_get_state_returns_correct_shape(self, make_adapter):
        adapter = make_adapter()
        result = adapter.get_state()
        assert isinstance(result, tuple), "get_state must return a tuple"
        assert len(result) == 3, "get_state must return (RunState, float, int)"
        state, sim_time, tick = result
        assert isinstance(state, RunState)
        assert isinstance(sim_time, float)
        assert isinstance(tick, int)

    @pytest.mark.parametrize(
        "make_adapter",
        [
            _toy_adapter,
            _repast4py_adapter,
        ],
        ids=["ToyRunnerAdapter", "Repast4pyAdapter"],
    )
    def test_add_observer_accepts_observation_adapter(self, make_adapter):
        adapter = make_adapter()
        obs = _broker_observation_adapter()
        adapter.add_observer(obs)  # must not raise


# ---------------------------------------------------------------------------
# ObservationAdapter conformance
# ---------------------------------------------------------------------------


class _CapturingObserver:
    """Minimal in-test ObservationAdapter."""

    def __init__(self):
        self.published = []
        self.flushed = False

    def publish(self, channel: str, table: pa.Table) -> None:
        self.published.append((channel, table))

    def flush(self) -> None:
        self.flushed = True


class TestObservationAdapterConformance:
    def test_broker_adapter_isinstance(self):
        obs = _broker_observation_adapter()
        assert isinstance(obs, ObservationAdapter), (
            "_BrokerObservationAdapter does not satisfy ObservationAdapter protocol"
        )

    def test_capturing_observer_isinstance(self):
        obs = _CapturingObserver()
        assert isinstance(obs, ObservationAdapter), "_CapturingObserver does not satisfy ObservationAdapter protocol"

    def test_broker_adapter_has_protocol_methods(self):
        obs = _broker_observation_adapter()
        assert callable(getattr(obs, "publish", None))
        assert callable(getattr(obs, "flush", None))

    def test_broker_adapter_publish_and_flush(self):
        broker = ObservationBroker()
        from casmsim.grpc_runner import _BrokerObservationAdapter

        obs = _BrokerObservationAdapter(broker)
        table = pa.table({"x": [1, 2, 3]})
        obs.publish("ch", table)
        obs.flush()
        # After flush the broker is closed; reading should return the batch
        read = broker.read("ch", start_batch_id=0)
        assert read is not None
        assert len(read.batches) == 1
        assert read.batches[0].table.num_rows == 3


# ---------------------------------------------------------------------------
# Cross-adapter wiring
# ---------------------------------------------------------------------------


class TestAdapterObserverWiring:
    """Adapters that accept ObservationAdapter must call publish + flush."""

    def test_toy_adapter_calls_observer(self):
        from casmsim.adapters.toy import ToyRunnerAdapter

        obs = _CapturingObserver()
        adapter = ToyRunnerAdapter(None, {"toy.ticks": 2, "toy.channel": "out"})
        adapter.add_observer(obs)
        adapter.start()
        assert obs.flushed
        assert len(obs.published) == 2
        for channel, table in obs.published:
            assert channel == "out"
            assert isinstance(table, pa.Table)

    def test_repast4py_adapter_calls_observer(self):
        from casmsim.adapters.repast4py import Repast4pyAdapter

        tables_seen = []

        class _Schedule:
            _n = 0

            def execute(self):
                if self._n >= 3:
                    return False
                self._n += 1
                return True

        class _Model:
            schedule = _Schedule()
            sim_time = 0.0
            tick = 0

        def obs_fn(model):
            t = pa.table({"tick": [model.tick]})
            tables_seen.append(t)
            return {"ticks": t}

        obs = _CapturingObserver()
        adapter = Repast4pyAdapter(None, {}, model=_Model(), observation_fn=obs_fn)
        adapter.add_observer(obs)
        adapter.start()

        assert obs.flushed
        assert len(obs.published) == 3
        for channel, _ in obs.published:
            assert channel == "ticks"
