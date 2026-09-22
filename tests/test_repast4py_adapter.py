"""Tests for casmsim.adapters.repast4py.Repast4pyAdapter."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from casmsim.adapters.repast4py import Repast4pyAdapter
from casmsim.run_state import RunState

# ---------------------------------------------------------------------------
# Minimal fake repast4py schedule
# ---------------------------------------------------------------------------


class _FakeSchedule:
    """Drives N ticks then returns False."""

    def __init__(self, ticks: int = 3) -> None:
        self._ticks = ticks
        self._executed = 0

    def execute(self) -> bool:
        if self._executed >= self._ticks:
            return False
        self._executed += 1
        return True


class _FakeModel:
    """Minimal model with schedule, sim_time, and tick.

    Accepts optional (comm, params) so MODEL_CLASS instantiation works.
    """

    def __init__(self, comm_or_ticks: Any = None, params_or_none: Any = None, *, ticks: int = 3) -> None:
        # Support both _FakeModel(ticks=N) and _FakeModel(comm, params)
        if isinstance(comm_or_ticks, int):
            ticks = comm_or_ticks
        self.schedule = _FakeSchedule(ticks)
        self._tick = 0
        self._initialized = False
        self._finalized = False

    @property
    def tick(self) -> int:
        return self.schedule._executed

    @property
    def sim_time(self) -> float:
        return float(self.tick)

    def initialize(self) -> None:
        self._initialized = True

    def finalize(self) -> None:
        self._finalized = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_adapter(ticks: int = 3, observation_fn=None) -> tuple[Repast4pyAdapter, _FakeModel]:
    model = _FakeModel(ticks=ticks)
    adapter = Repast4pyAdapter(None, {}, model=model, observation_fn=observation_fn)
    return adapter, model


def _null_observer() -> MagicMock:
    obs = MagicMock()
    obs.publish = MagicMock()
    obs.flush = MagicMock()
    return obs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_initial_state_is_pending(self):
        adapter, _ = _make_adapter()
        state, sim_time, tick = adapter.get_state()
        assert state == RunState.Pending

    def test_start_transitions_to_completed(self):
        adapter, _ = _make_adapter()
        adapter.start()
        state, _, _ = adapter.get_state()
        assert state == RunState.Completed

    def test_initialize_and_finalize_called(self):
        adapter, model = _make_adapter()
        adapter.start()
        assert model._initialized
        assert model._finalized

    def test_tick_advances(self):
        adapter, model = _make_adapter(ticks=5)
        adapter.start()
        _, _, tick = adapter.get_state()
        assert tick == 5

    def test_observer_flush_called_on_completion(self):
        adapter, _ = _make_adapter()
        obs = _null_observer()
        adapter.add_observer(obs)
        adapter.start()
        obs.flush.assert_called_once()

    def test_model_class_instantiation(self):
        class MyAdapter(Repast4pyAdapter):
            MODEL_CLASS = _FakeModel

        adapter = MyAdapter(None, {})
        adapter.start()
        state, _, _ = adapter.get_state()
        assert state == RunState.Completed

    def test_model_class_none_raises(self):
        with pytest.raises(TypeError, match="MODEL_CLASS"):
            Repast4pyAdapter(None, {})


class TestCancellation:
    def test_cancel_before_start_marks_failed(self):
        adapter, _ = _make_adapter()
        adapter.cancel()
        adapter.start()
        state, _, _ = adapter.get_state()
        assert state == RunState.Failed

    def test_cancel_during_start_stops_schedule(self):
        ticks_run: list[int] = []

        class _CancellingSchedule(_FakeSchedule):
            def __init__(self, adapter_ref, ticks=10):
                super().__init__(ticks)
                self._adapter_ref = adapter_ref

            def execute(self) -> bool:
                result = super().execute()
                ticks_run.append(self._executed)
                if self._executed == 3:
                    self._adapter_ref.cancel()  # trigger cancel mid-run
                return result

        adapter = Repast4pyAdapter.__new__(Repast4pyAdapter)
        model = _FakeModel(ticks=10)
        adapter_holder: list[Repast4pyAdapter] = []
        Repast4pyAdapter.__init__(adapter, None, {}, model=model)
        adapter_holder.append(adapter)
        model.schedule = _CancellingSchedule(adapter, ticks=10)

        adapter.start()
        state, _, _ = adapter.get_state()
        assert state == RunState.Failed
        # Must have stopped before completing all 10 ticks
        assert len(ticks_run) < 10

    def test_observer_flush_still_called_after_cancel(self):
        adapter, _ = _make_adapter()
        obs = _null_observer()
        adapter.add_observer(obs)
        adapter.cancel()
        adapter.start()
        obs.flush.assert_called_once()


class TestFailure:
    def test_exception_in_schedule_marks_failed(self):
        class _BoomSchedule:
            def execute(self):
                raise RuntimeError("boom")

        model = _FakeModel()
        model.schedule = _BoomSchedule()
        adapter = Repast4pyAdapter(None, {}, model=model)
        with pytest.raises(RuntimeError, match="boom"):
            adapter.start()
        state, _, _ = adapter.get_state()
        assert state == RunState.Failed

    def test_finalize_called_even_on_exception(self):
        class _BoomSchedule:
            def execute(self):
                raise RuntimeError("boom")

        model = _FakeModel()
        model.schedule = _BoomSchedule()
        adapter = Repast4pyAdapter(None, {}, model=model)
        try:
            adapter.start()
        except RuntimeError:
            pass
        assert model._finalized


class TestObservations:
    def test_observation_fn_called_per_tick(self):
        call_count = []

        def obs_fn(model):
            call_count.append(model.tick)
            return {}

        adapter, _ = _make_adapter(ticks=4, observation_fn=obs_fn)
        adapter.start()
        assert len(call_count) == 4

    def test_observation_fn_publishes_tables(self):
        def obs_fn(model):
            return {"out": pa.table({"tick": pa.array([model.tick])})}

        adapter, _ = _make_adapter(ticks=3, observation_fn=obs_fn)
        obs = _null_observer()
        adapter.add_observer(obs)
        adapter.start()

        assert obs.publish.call_count == 3
        for i, c in enumerate(obs.publish.call_args_list):
            channel, table = c.args
            assert channel == "out"
            assert table.num_rows == 1

    def test_empty_table_not_published(self):
        def obs_fn(model):
            return {"empty": pa.table({"x": pa.array([], type=pa.int32())})}

        adapter, _ = _make_adapter(ticks=2, observation_fn=obs_fn)
        obs = _null_observer()
        adapter.add_observer(obs)
        adapter.start()
        obs.publish.assert_not_called()

    def test_no_observer_observation_fn_still_runs(self):
        called = []

        def obs_fn(model):
            called.append(True)
            return {}

        adapter, _ = _make_adapter(ticks=2, observation_fn=obs_fn)
        # No observer added
        adapter.start()
        # obs_fn runs but publish is never called (no observer)
        assert len(called) == 2
