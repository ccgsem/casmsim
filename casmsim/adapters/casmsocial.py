"""casmsim adapter wrapping CasmPop models via the casmsocial Models factory.

This adapter is the bridge between the generic casmsim runner runtime and the
casmsocial model registry.  It is invoked when a submitted run's config
contains ``model.plugins`` (the casmsocial convention) rather than a direct
``runner.entry_point`` import path.

Usage (indirect — the grpc_runner resolves this automatically):

    adapter = CasmPopAdapter(MPI.COMM_WORLD, params)
    adapter.add_observer(observer)
    adapter.start()          # blocks until the model run completes

Direct ``runner.entry_point`` usage is also supported:

    params["runner.entry_point"] = "casmsim.adapters.casmsocial:CasmPopAdapter"
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pyarrow as pa

from casmsim.protocols import ObservationAdapter
from casmsim.run_state import RunState

if TYPE_CHECKING:
    from mpi4py import MPI


class _ObservationBridge:
    """Casmsocial Observer that forwards model output tables to an ObservationAdapter.

    Registered on the model after the model's own observers (step_priority=100)
    so each tick snapshot is complete before it is published.
    """

    # Import casmsocial.observer lazily; the casmsim package itself must not
    # hard-depend on casmsocial at import time.
    step_priority = 100

    def __init__(self, observer: ObservationAdapter) -> None:
        self._observer = observer
        self._tick = 0

    def initialize(self, model) -> None:  # noqa: ANN001
        pass

    def on_step(self, model) -> None:  # noqa: ANN001
        tables: dict[str, pa.Table] = model.get_observer_output_tables()
        for channel, table in tables.items():
            self._observer.publish(channel, table)
        self._tick += 1

    def on_end(self, model) -> None:  # noqa: ANN001
        tables: dict[str, pa.Table] = model.get_observer_output_tables()
        for channel, table in tables.items():
            self._observer.publish(channel, table)
        self._observer.flush()

    def get_output_tables(self, model) -> dict:  # noqa: ANN001
        """Satisfy CasmPop's observer aggregation contract.

        CasmPop calls ``get_output_tables`` on every registered observer when
        assembling the combined model output snapshot.  The forwarding bridge
        owns no tables of its own — it is a pass-through sink — so it always
        returns an empty dict to avoid double-counting output that was already
        forwarded via ``on_step`` / ``on_end``.
        """
        return {}

    @property
    def tick(self) -> int:
        return self._tick


class CasmPopAdapter:
    """RunnerModelAdapter wrapping any CasmPop model registered via the casmsocial factory.

    Resolves the model class from ``params["model.name"]`` using the casmsocial
    ``Models`` factory after importing the modules listed in
    ``params.get("model.plugins", [])``.

    Thread-safety: ``cancel()`` and ``get_state()`` are safe to call from a
    thread other than the one running ``start()``.
    """

    def __init__(self, comm: MPI.Comm, params: dict) -> None:  # type: ignore[name-defined]
        self._comm = comm
        self._params = dict(params)
        self._observer: ObservationAdapter | None = None
        self._bridge: _ObservationBridge | None = None

        self._lock = threading.Lock()
        self._state = RunState.Pending
        self._sim_time: float = 0.0
        self._cancelled = False

    # ------------------------------------------------------------------
    # RunnerModelAdapter protocol
    # ------------------------------------------------------------------

    def add_observer(self, observer: ObservationAdapter) -> None:
        """Register the casmsim observation sink.  Must be called before ``start()``."""
        self._observer = observer

    def start(self) -> None:
        """Load plugins, create the model, and run it to completion.

        Blocks until the model finishes or raises.  Calls ``observer.flush()``
        (via the bridge's ``on_end``) before returning on normal completion.
        """
        # Lazy imports — keeps casmsim importable without casmsocial installed.
        from casmsocial.__main__ import load_builtin_models
        from casmsocial.factory import Models, load_models

        params = self._params
        # Disable the model's own Arrow Flight server; the broker handles live obs.
        params["observers.arrow_server.enabled"] = False

        load_builtin_models()
        plugins = params.get("model.plugins", [])
        if plugins:
            load_models(plugins)

        model_cls = Models.create_model(params["model.name"])
        model = model_cls(self._comm, params)

        # Wire observation bridge before the model's own observers run.
        if self._observer is not None:
            self._bridge = _ObservationBridge(self._observer)
            model.add_observer(self._bridge)

        with self._lock:
            if self._cancelled:
                with self._lock:
                    self._state = RunState.Failed
                if self._observer is not None:
                    self._observer.flush()
                return
            self._state = RunState.Running

        try:
            model.start()
        except Exception:
            with self._lock:
                self._state = RunState.Failed
            if self._observer is not None:
                self._observer.flush()
            raise

        with self._lock:
            if self._state == RunState.Running:
                self._state = RunState.Completed

    def cancel(self) -> None:
        """Request cooperative cancellation.

        CasmPop models do not currently poll a cancel flag between ticks,
        so this sets the flag for the next opportunity (model startup or a
        future cancel-hook addition) and marks the state.
        """
        with self._lock:
            self._cancelled = True
            if self._state == RunState.Running:
                self._state = RunState.Failed

    def get_state(self) -> tuple[RunState, float, int]:
        """Return ``(state, sim_time, tick)`` — thread-safe."""
        with self._lock:
            state = self._state
        tick = self._bridge.tick if self._bridge is not None else 0
        return state, float(tick), tick


__all__ = ["CasmPopAdapter"]
