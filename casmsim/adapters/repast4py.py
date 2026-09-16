"""Generic repast4py schedule adapter for casmsim (Path A).

Wraps any repast4py-based model as a :class:`casmsim.protocols.RunnerModelAdapter`
so it can be hosted by :mod:`casmsim.grpc_runner` without depending on the
casmsocial `CasmPop` or `Models` registry.

Protocol contract
-----------------
The *model object* passed to :class:`Repast4pyAdapter` must implement::

    class MyModel:
        schedule: repast4py.schedule.SharedScheduleRunner   # required
        sim_time: float                                      # required
        tick: int                                            # required

        def initialize(self) -> None: ...       # called once before schedule.execute()
        def finalize(self) -> None: ...         # called once after the last tick

Both ``initialize`` and ``finalize`` are optional: the adapter checks for them
with ``hasattr`` and skips gracefully if absent.

Observation callback
--------------------
To stream per-tick Arrow tables, pass ``observation_fn`` to the constructor.
It receives the model object after each completed tick and must return a
``dict[str, pa.Table]`` mapping channel names to tables (return ``{}`` to
emit nothing for that tick)::

    def my_obs(model) -> dict[str, pa.Table]:
        return {"agents": pa.table({"x": [...], "y": [...]})}

    adapter = Repast4pyAdapter(comm, params, model=MyModel(comm, params),
                               observation_fn=my_obs)

Runner entry point
------------------
Register as an entry point by subclassing and setting ``MODEL_CLASS``::

    # mypackage/adapters/casmsim.py
    from casmsim.adapters.repast4py import Repast4pyAdapter
    from mypackage.model import MyModel

    class MyAdapter(Repast4pyAdapter):
        MODEL_CLASS = MyModel

    # params["runner.entry_point"] = "mypackage.adapters.casmsim:MyAdapter"
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Callable

import pyarrow as pa

from casmsim.mpi_lifecycle import CooperativeCancelToken, is_rank0
from casmsim.protocols import ObservationAdapter
from casmsim.run_state import RunState

if TYPE_CHECKING:
    pass

ObservationFn = Callable[[Any], dict[str, pa.Table]]


class Repast4pyAdapter:
    """RunnerModelAdapter wrapping a generic repast4py model.

    Args:
        comm:           MPI communicator (or ``None`` in single-process mode).
        params:         Simulation parameter dict.
        model:          An instantiated model object with a ``schedule``
                        attribute (repast4py ``SharedScheduleRunner``).
                        If ``None``, :attr:`MODEL_CLASS` is instantiated with
                        ``(comm, params)``.
        observation_fn: Per-tick callback; receives the model, returns
                        ``dict[channel, pa.Table]``.  Called on rank 0 only.
    """

    #: Override in subclasses to enable zero-argument adapter resolution.
    MODEL_CLASS: type | None = None

    def __init__(
        self,
        comm: Any,
        params: dict,
        *,
        model: Any | None = None,
        observation_fn: ObservationFn | None = None,
    ) -> None:
        if model is None:
            if self.MODEL_CLASS is None:
                raise TypeError(
                    "Either pass model= or set MODEL_CLASS on the subclass."
                )
            model = self.MODEL_CLASS(comm, params)

        self._comm = comm
        self._params = dict(params)
        self._model = model
        self._observation_fn = observation_fn
        self._observer: ObservationAdapter | None = None
        self._cancel = CooperativeCancelToken(comm)

        self._lock = threading.Lock()
        self._state = RunState.Pending

    # ------------------------------------------------------------------
    # RunnerModelAdapter protocol
    # ------------------------------------------------------------------

    def add_observer(self, observer: ObservationAdapter) -> None:
        self._observer = observer

    def start(self) -> None:
        """Initialize the model, execute the schedule, finalize."""
        with self._lock:
            if self._cancel.is_set():
                self._state = RunState.Failed
                if self._observer is not None:
                    self._observer.flush()
                return
            self._state = RunState.Running

        try:
            if hasattr(self._model, "initialize"):
                self._model.initialize()

            self._run_schedule()

            with self._lock:
                if self._state == RunState.Running:
                    self._state = RunState.Completed
        except Exception:
            with self._lock:
                self._state = RunState.Failed
            raise
        finally:
            if hasattr(self._model, "finalize"):
                try:
                    self._model.finalize()
                except Exception:
                    pass
            if self._observer is not None:
                self._observer.flush()

    def cancel(self) -> None:
        """Request cooperative cancellation (safe from any thread)."""
        self._cancel.request()
        with self._lock:
            if self._state == RunState.Running:
                self._state = RunState.Failed

    def get_state(self) -> tuple[RunState, float, int]:
        """Return ``(state, sim_time, tick)`` — thread-safe."""
        with self._lock:
            state = self._state
        sim_time = float(getattr(self._model, "sim_time", 0))
        tick = int(getattr(self._model, "tick", 0))
        return state, sim_time, tick

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_schedule(self) -> None:
        """Drive the repast4py schedule, polling for cancellation each tick."""
        schedule = self._model.schedule
        while True:
            # Broadcast cancel flag from rank 0 to all ranks once per tick.
            if self._cancel.is_set(broadcast=True):
                with self._lock:
                    self._state = RunState.Failed
                return
            if not schedule.execute():
                # schedule.execute() returns False (or None) when exhausted.
                break
            if is_rank0(self._comm):
                self._emit_observations()

    def _emit_observations(self) -> None:
        """Call observation_fn and publish resulting tables (rank 0 only)."""
        if self._observation_fn is None:
            return
        try:
            tables = self._observation_fn(self._model)
        except Exception:
            return
        if self._observer is None:
            return
        for channel, table in (tables or {}).items():
            if isinstance(table, pa.Table) and table.num_rows > 0:
                self._observer.publish(channel, table)


__all__ = ["ObservationFn", "Repast4pyAdapter"]
