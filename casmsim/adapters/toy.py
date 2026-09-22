"""Minimal in-process RunnerModelAdapter for integration testing.

``ToyRunnerAdapter`` runs a trivial simulation loop (N ticks, one Arrow table
published per tick) without any MPI or external dependencies.  It is the
canonical fixture for testing the ``runner.entry_point`` resolution path in
``casmsim.grpc_runner.resolve_adapter``.

Usage (direct):
    adapter = ToyRunnerAdapter(comm=None, params={"toy.ticks": 3, "toy.channel": "agents"})
    adapter.add_observer(observer)
    adapter.start()

Usage (via runner.entry_point):
    params["runner.entry_point"] = "casmsim.adapters.toy:ToyRunnerAdapter"
    params["toy.ticks"] = 3
    adapter = resolve_adapter(comm, params)
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pyarrow as pa

from casmsim.protocols import ObservationAdapter
from casmsim.run_state import RunState


class ToyRunnerAdapter:
    """A trivially correct RunnerModelAdapter with no external dependencies.

    Parameters (via params dict):
        toy.ticks (int):   Number of simulation ticks to run.  Default: 3.
        toy.channel (str): Observation channel name.  Default: ``"toy_output"``.
        toy.tick_delay (float): Seconds to sleep per tick (for concurrency tests). Default: 0.
    """

    def __init__(self, comm: Any, params: dict) -> None:
        self._ticks: int = int(params.get("toy.ticks", 3))
        self._channel: str = str(params.get("toy.channel", "toy_output"))
        self._tick_delay: float = float(params.get("toy.tick_delay", 0.0))
        self._observer: ObservationAdapter | None = None

        self._lock = threading.Lock()
        self._state = RunState.Pending
        self._current_tick = 0
        self._cancelled = False

    # ------------------------------------------------------------------
    # RunnerModelAdapter protocol
    # ------------------------------------------------------------------

    def add_observer(self, observer: ObservationAdapter) -> None:
        self._observer = observer

    def start(self) -> None:
        """Run N ticks, publish one Arrow table per tick, then flush."""
        with self._lock:
            self._state = RunState.Running

        try:
            for tick in range(self._ticks):
                with self._lock:
                    if self._cancelled:
                        self._state = RunState.Failed
                        if self._observer:
                            self._observer.flush()
                        return
                    self._current_tick = tick

                table = pa.table({"tick": [tick], "value": [float(tick**2)]})
                if self._observer:
                    self._observer.publish(self._channel, table)

                if self._tick_delay > 0:
                    time.sleep(self._tick_delay)

            with self._lock:
                self._state = RunState.Completed
        except Exception:
            with self._lock:
                self._state = RunState.Failed
            raise
        finally:
            if self._observer:
                self._observer.flush()

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            if self._state == RunState.Running:
                self._state = RunState.Failed

    def get_state(self) -> tuple[RunState, float, int]:
        with self._lock:
            return self._state, float(self._current_tick), self._current_tick


__all__ = ["ToyRunnerAdapter"]
