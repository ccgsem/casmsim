"""Core protocols for the casmsim runner runtime.

`ObservationAdapter` and `RunnerModelAdapter` are the two stable interfaces
that connect any simulation engine to the casmsim gRPC runner.  They are
defined here as `@runtime_checkable` Protocols so that:

  - Static type checkers can verify structural compatibility.
  - `isinstance(obj, RunnerModelAdapter)` works at runtime for adapter
    registration and introspection.

Adapters live in `casmsim.adapters.*`; the gRPC server that consumes them
lives in `casmsim.grpc_runner`.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pyarrow as pa

from casmsim.run_state import RunState


@runtime_checkable
class ObservationAdapter(Protocol):
    """Sink for Arrow IPC observation batches published by a running model.

    The casmsim gRPC runner passes a concrete implementation of this protocol
    to `RunnerModelAdapter.add_observer`.  The model publishes tables to named
    channels; the adapter forwards them to the Arrow Flight server.
    """

    def publish(self, channel: str, table: pa.Table) -> None:
        """Publish an Arrow table to the named observation channel.

        Args:
            channel: Logical channel name (e.g. ``"agents"``, ``"places"``).
            table:   Immutable Arrow table representing one observation batch.
                     The schema must be consistent across batches on the same
                     channel within a single run.
        """
        ...

    def flush(self) -> None:
        """Flush any buffered batches and signal end-of-stream on all channels.

        Called once by `RunnerModelAdapter.start` when the simulation
        completes normally.  Implementations should block until all in-flight
        batches have been forwarded.
        """
        ...


@runtime_checkable
class RunnerModelAdapter(Protocol):
    """Protocol that any simulation model must satisfy to run inside casmsim.

    Implementations may wrap:
      - A casmsocial CasmPop model (via ``casmsim.adapters.casmsocial``).
      - A standalone repast4py model (via ``casmsim.adapters.repast4py``).
      - Any other simulation engine.

    The gRPC runner calls `start()` on a background thread after the client
    issues ``SimulatorControl.Start``; `cancel()` and `get_state()` may be
    called concurrently from the gRPC thread pool and must therefore be
    thread-safe.
    """

    def start(self) -> None:
        """Start the simulation and block until it completes or is cancelled.

        Raises on unrecoverable error.  Must call `observer.flush()` before
        returning when the simulation completes normally.
        """
        ...

    def cancel(self) -> None:
        """Request cooperative cancellation.

        Sets a flag that the model polls at tick boundaries.  The model is
        responsible for calling `observer.flush()` before `start()` returns
        after a cancel request.

        This method must be thread-safe; it may be called from a different
        thread while `start()` is running.
        """
        ...

    def get_state(self) -> tuple[RunState, float, int]:
        """Return the current run state as ``(state, sim_time, tick)``.

        ``sim_time`` is the simulation clock in the model's native time unit
        (seconds, hours, or ticks — documented by the adapter).  ``tick`` is
        the zero-based discrete tick counter.

        This method must be thread-safe.
        """
        ...

    def add_observer(self, observer: ObservationAdapter) -> None:
        """Register an observation sink.

        Called once by the gRPC runner before `start()`.  The adapter stores
        the reference and calls `observer.publish(channel, table)` during the
        simulation at whatever cadence makes sense for the model.
        """
        ...


__all__ = [
    "ObservationAdapter",
    "RunnerModelAdapter",
]
