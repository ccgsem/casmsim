"""Loopback gRPC SimulatorControl server and generic model resolver for casmsim.

Contains two layers:

1. **gRPC server layer** (``start_control_server``, ``SimulatorControlServicer``) —
   transport-agnostic; no knowledge of any simulation engine.

2. **Model resolution layer** (``run_submitted_model``, ``resolve_adapter``) —
   resolves the right ``RunnerModelAdapter`` from submitted run params via
   ``runner.entry_point`` (``module:Class``).  casmsim carries no reference to
   any model package; launchers (e.g. ``casmsocial.grpc_runner``) inject the
   appropriate entry_point before forwarding config_json.

``casmsocial.grpc_runner`` wraps ``run_submitted_model`` to inject
``runner.entry_point = "casmsocial.adapters.runner:CasmPopAdapter"`` for
``model.plugins`` / ``model.name`` runs before forwarding here.
"""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Callable, Iterator
from concurrent import futures
from pathlib import Path
from threading import Lock, Thread
from typing import Any, cast

import grpc
import pyarrow as pa
from pyarrow import ipc

from casmsim.observation_broker import ObservationBroker, ObservationCursorExpiredError
from casmsim.proto import casm_runner_pb2 as pb2, casm_runner_pb2_grpc as pb2_grpc
from casmsim.protocols import RunnerModelAdapter

ENDPOINT_FILENAME = "runner_endpoints.json"


def secure_run_directory(path: Path) -> None:
    """Create a local run directory and require owner-only permissions."""
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    if path.stat().st_mode & 0o077:
        raise PermissionError(f"run directory must be owner-only: {path}")


class SimulatorControlServicer(pb2_grpc.SimulatorControlServicer):
    """Atomically accepts one run and exposes its broker-backed observations."""

    def __init__(
        self,
        broker: ObservationBroker,
        start_run: Callable[[str, bytes], None],
        cancel_run: Callable[[], bool | None] | None = None,
    ) -> None:
        self._broker = broker
        self._start_run = start_run
        self._cancel_run = cancel_run
        self._cancel_requested = False
        self._lock = Lock()
        self._run_id: str | None = None
        self._state = pb2.RUN_STATE_INITIALIZING
        self._status_message: str = ""
        self._worker: Thread | None = None

    def Start(self, request: pb2.StartRequest, context: grpc.ServicerContext) -> pb2.StartResponse:
        if not request.run_id or not request.config_json:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "run_id and config_json are required")
        with self._lock:
            if self._run_id is not None:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "this process already accepted a run")
            self._run_id = request.run_id  # Reserve before calling user code: fixes concurrent Start races.
            self._state = pb2.RUN_STATE_RUNNING
            self._worker = Thread(
                target=self._run,
                args=(request.run_id, request.config_json),
                name=f"casmsim-run-{request.run_id}",
                daemon=True,
            )
            self._worker.start()
        return pb2.StartResponse(run_id=request.run_id)

    def _run(self, run_id: str, config_json: bytes) -> None:
        """Run the model off the gRPC request thread and expose terminal state.

        Failure details (including exception text that may contain credentials
        or internal paths) are logged to stderr but never surfaced in the gRPC
        status message returned to the control plane.
        """
        import logging
        import traceback

        try:
            self._start_run(run_id, config_json)
        except Exception as exc:
            logging.getLogger(__name__).error("runner failed for run_id=%s\n%s", run_id, traceback.format_exc())
            with self._lock:
                if self._state != pb2.RUN_STATE_CANCELLED:
                    self._state = pb2.RUN_STATE_FAILED
                    self._status_message = f"Run failed ({type(exc).__name__}); see runner stderr.log."
            self._broker.close()
            return
        with self._lock:
            if self._state == pb2.RUN_STATE_RUNNING:
                self._state = pb2.RUN_STATE_CANCELLED if self._cancel_requested else pb2.RUN_STATE_COMPLETED
                self._status_message = "Cancelled at model boundary" if self._cancel_requested else ""
        self._broker.close()

    def Cancel(self, request: pb2.CancelRequest, context: grpc.ServicerContext) -> pb2.CancelResponse:
        """Cooperative cancellation — acknowledged only if a cancel hook is wired up.

        Without a hook returns ``acknowledged=False``. The hook must promptly
        signal the worker, not wait for it. State remains Running until the
        worker returns, including output flush; exceptions still report Failed.
        """
        with self._lock:
            if request.run_id != self._run_id:
                context.abort(grpc.StatusCode.NOT_FOUND, "unknown run_id")
            if self._state != pb2.RUN_STATE_RUNNING or self._cancel_run is None:
                return pb2.CancelResponse(acknowledged=False)
            if not self._cancel_requested:
                if self._cancel_run() is False:
                    return pb2.CancelResponse(acknowledged=False)
                self._cancel_requested = True
                self._status_message = "Cancellation requested; waiting for model shutdown"
            return pb2.CancelResponse(acknowledged=True)

    def GetState(self, request: pb2.GetStateRequest, context: grpc.ServicerContext) -> pb2.StateResponse:
        with self._lock:
            if request.run_id != self._run_id:
                context.abort(grpc.StatusCode.NOT_FOUND, "unknown run_id")
            return pb2.StateResponse(
                run_id=request.run_id,
                state=self._state,
                status_message=self._status_message,
            )

    def StreamObs(self, request: pb2.StreamObsRequest, context: grpc.ServicerContext) -> Iterator[pb2.ObsBatch]:
        with self._lock:
            if request.run_id != self._run_id:
                context.abort(grpc.StatusCode.NOT_FOUND, "unknown run_id")
        try:
            result = self._broker.read(request.channel, start_batch_id=request.start_tick)
        except ObservationCursorExpiredError as error:
            context.abort(grpc.StatusCode.OUT_OF_RANGE, str(error))
        for batch in result.batches:
            if not context.is_active():
                return
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, batch.table.schema) as writer:
                writer.write_table(batch.table)
            yield pb2.ObsBatch(channel=batch.channel, tick=batch.batch_id, arrow_ipc=sink.getvalue().to_pybytes())


def start_control_server(
    run_dir: Path,
    broker: ObservationBroker,
    start_run: Callable[[str, bytes], None],
    *,
    cancel_run: Callable[[], bool | None] | None = None,
) -> grpc.Server:
    """Start a loopback-only control server and write its endpoint manifest."""
    secure_run_directory(run_dir)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb2_grpc.add_SimulatorControlServicer_to_server(SimulatorControlServicer(broker, start_run, cancel_run), server)
    port = server.add_insecure_port("127.0.0.1:0")
    if not port:
        raise RuntimeError("could not bind loopback gRPC control listener")
    server.start()
    (run_dir / ENDPOINT_FILENAME).write_text(
        json.dumps({"control": {"address": f"127.0.0.1:{port}", "protocol": "casm.runner.v1"}}) + "\n"
    )
    os.chmod(run_dir / ENDPOINT_FILENAME, 0o600)
    return server


# ---------------------------------------------------------------------------
# Generic model resolver
# ---------------------------------------------------------------------------


class _BrokerObservationAdapter:
    """Bridge from ObservationBroker (``close()``) to ObservationAdapter (``flush()``).

    The ``ObservationAdapter`` protocol uses ``flush()`` as its end-of-stream
    signal; ``ObservationBroker`` uses ``close()``.  This wrapper makes a
    broker satisfy the protocol so adapters need not import the broker directly.
    """

    def __init__(self, broker: ObservationBroker) -> None:
        self._broker = broker

    def publish(self, channel: str, table: pa.Table) -> None:  # noqa: ANN001
        self._broker.publish(channel, table)

    def flush(self) -> None:
        self._broker.close()


def _import_entry_point(entry_point: str) -> Any:
    """Resolve ``"module.path:ClassName"`` to the class object."""
    if ":" not in entry_point:
        raise ValueError(f"runner.entry_point must be in 'module:Class' form, got: {entry_point!r}")
    module_path, class_name = entry_point.rsplit(":", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ImportError(f"Cannot find {class_name!r} in module {module_path!r}")
    return cls


def resolve_adapter(comm: Any, params: dict) -> RunnerModelAdapter:
    """Return an instantiated ``RunnerModelAdapter`` for the given params.

    Resolution: ``runner.entry_point`` → dynamic ``module:Class`` import.

    The caller is responsible for injecting ``runner.entry_point`` before
    calling this function.  For casmsocial models the launcher sets::

        params["runner.entry_point"] = "casmsocial.adapters.runner:CasmPopAdapter"

    casmsim itself carries no reference to casmsocial or any other model package.

    Args:
        comm:   MPI communicator (``mpi4py.MPI.Comm`` or compatible).
        params: Decoded run-config dict (mutated in place to add runtime keys).
    """
    entry_point = params.get("runner.entry_point")
    if entry_point:
        cls = _import_entry_point(entry_point)
        return cast(RunnerModelAdapter, cls(comm, params))

    raise ValueError(
        "Cannot resolve a RunnerModelAdapter: params must contain "
        "'runner.entry_point' (module:Class).  "
        "For casmsocial models the launcher injects "
        "'casmsocial.adapters.runner:CasmPopAdapter' automatically."
    )


def run_submitted_model(run_id: str, config_json: bytes, broker: ObservationBroker) -> None:
    """Generic entry point for a submitted run.

    Decodes ``config_json``, resolves the appropriate ``RunnerModelAdapter``
    via :func:`resolve_adapter`, wires the broker as the observation sink, and
    calls ``adapter.start()``.

    Raises on unrecoverable error; the gRPC server's ``_run`` thread will catch
    and mark the run as failed.
    """
    try:
        from mpi4py import MPI as _MPI

        comm = _MPI.COMM_WORLD
    except ImportError:
        # mpi4py is optional when the adapter doesn't need MPI
        # (e.g. runner.entry_point adapters running in a single process).
        comm = None

    params = json.loads(config_json)
    if not isinstance(params, dict):
        raise ValueError("config_json must encode a JSON object of model parameters")
    params = dict(params)
    params["simulation.run_id"] = run_id
    # The broker-backed Flight server is the live observation transport.
    params["observers.arrow_server.enabled"] = False

    adapter = resolve_adapter(comm, params)
    adapter.add_observer(_BrokerObservationAdapter(broker))
    adapter.start()
