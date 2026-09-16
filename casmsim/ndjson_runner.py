"""NDJSON v1 external runner base class for casmsim (Path B).

Subclass :class:`NdjsonRunnerBase`, implement :meth:`run_simulation`, and
call ``raise SystemExit(self.main())`` from your entry point::

    class MyRunner(NdjsonRunnerBase):
        def run_simulation(self, request: dict, artifact_root: Path) -> None:
            # ... run the model, write artifact files ...
            table = pa.table({"tick": [0], "value": [1.0]})
            path = artifact_root / "tick_0.arrow"
            with pa.ipc.new_file(path, table.schema) as writer:
                writer.write_table(table)
            self.emit_observation(
                channel="output",
                format="arrow_ipc_file",
                path=path.name,
                batch_index=0,
                row_offset=0,
                rows=table.num_rows,
                nbytes=path.stat().st_size,
            )

    if __name__ == "__main__":
        raise SystemExit(MyRunner(runner_version="1.0.0").main())

Protocol contract (v1)
----------------------
Event lifecycle on stdout (NDJSON, one JSON object per line):

    ready       ← first event; advertises runner_version and capabilities
    state       ← ``state="running"`` after the model starts
    observation ← emitted *before* each artifact batch is fully readable
                  (the file must already be written and immutable)
    diagnostic  ← optional info/warning/error messages
    result      ← terminal event; ``outcome`` is one of completed/failed/cancelled

Exit codes:
    0   completed
    3   failed
    143 cancelled (SIGTERM received)

The casmservice NDJSON backend validates every event against the
``RunnerEvent`` Pydantic model.  Any deviation marks the run as failed.
"""

from __future__ import annotations

import json
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, TextIO

ObservationFormat = Literal["arrow_ipc_file", "parquet"]
DiagnosticLevel = Literal["info", "warning", "error"]

PROTOCOL_VERSION = "1"
EXIT_BY_OUTCOME: dict[str, int] = {"completed": 0, "failed": 3, "cancelled": 143}

# Capabilities advertised by default.  The NDJSON backend requires the requested
# format ("arrow_ipc_file" or "parquet"), "cancel_sigterm", and
# "immutable_observation_batches".
DEFAULT_CAPABILITIES: list[str] = [
    "arrow_ipc_file",
    "parquet",
    "cancel_sigterm",
    "immutable_observation_batches",
]


class NdjsonRunnerBase:
    """Base class for NDJSON v1 external runners.

    Handles all event serialization, sequence numbering, cancellation via
    SIGTERM, and the ``main()`` lifecycle.  Subclasses only need to implement
    :meth:`run_simulation`.

    Args:
        runner_version: Semver string included in the ``ready`` event.
        capabilities:   List of advertised capability strings.  Defaults to
                        :data:`DEFAULT_CAPABILITIES`.  Must include at least
                        the observation format the client will request, plus
                        ``"cancel_sigterm"`` and
                        ``"immutable_observation_batches"``.
        output:         Stream for NDJSON event output.  Defaults to
                        ``sys.stdout``.  Override in tests.
    """

    def __init__(
        self,
        runner_version: str,
        capabilities: list[str] | None = None,
        output: TextIO | None = None,
    ) -> None:
        self._runner_version = runner_version
        self._capabilities = capabilities if capabilities is not None else list(DEFAULT_CAPABILITIES)
        self._output: TextIO = output or sys.stdout

        self._sequence = 0
        self._run_id: str = ""
        self._cancelled = False

    # ------------------------------------------------------------------
    # Subclass interface
    # ------------------------------------------------------------------

    def run_simulation(self, request: dict, artifact_root: Path) -> None:
        """Run the simulation.  Override in subclasses.

        Args:
            request:       Decoded request dict (from ``request.json`` /
                           stdin).  Contains ``run_id``, ``model``,
                           ``params``, ``observation``, and
                           ``artifact_root`` keys.
            artifact_root: Directory into which observation artifact files
                           must be written.  All paths passed to
                           :meth:`emit_observation` are relative to this
                           directory.

        Call :meth:`emit_observation` after each artifact file is fully
        written and immutable.  Call :meth:`emit_diagnostic` for non-fatal
        log messages.

        Raises on unrecoverable error (the runner exits with code 3).
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Emission helpers (for use inside run_simulation)
    # ------------------------------------------------------------------

    def emit_observation(
        self,
        *,
        channel: str,
        format: ObservationFormat,  # noqa: A002
        path: str | Path,
        batch_index: int,
        row_offset: int,
        rows: int,
        nbytes: int,
    ) -> None:
        """Emit an ``observation`` event for a fully-written artifact file.

        Args:
            channel:     Logical channel name (e.g. ``"agents"``).
            format:      ``"arrow_ipc_file"`` or ``"parquet"``.
            path:        Relative path from ``artifact_root`` to the file.
            batch_index: Zero-based batch index for this channel.
            row_offset:  Cumulative row offset before this batch.
            rows:        Number of rows in this batch.
            nbytes:      File size in bytes (must match the actual file).
        """
        self._emit(
            type="observation",
            channel=channel,
            format=format,
            path=str(path),
            batch_index=batch_index,
            row_offset=row_offset,
            rows=rows,
            bytes=nbytes,
        )

    def emit_diagnostic(self, level: DiagnosticLevel, message: str) -> None:
        """Emit a ``diagnostic`` event."""
        self._emit(type="diagnostic", level=level, message=message)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def main(self, argv: list[str] | None = None) -> int:  # noqa: ARG002
        """Run the full NDJSON v1 lifecycle.  Returns an exit code.

        1. Reads the request dict from stdin (JSON).
        2. Emits ``ready``.
        3. Installs a SIGTERM handler for cooperative cancellation.
        4. Emits ``state: running``.
        5. Calls :meth:`run_simulation`.
        6. Emits ``result: completed`` (or ``failed`` / ``cancelled``).
        7. Returns the appropriate exit code.
        """
        try:
            raw = sys.stdin.read()
            request = json.loads(raw)
        except Exception as exc:
            # Can't even read the request — write a plain error to stderr and exit.
            print(f"casmsim.ndjson_runner: failed to read request: {exc}", file=sys.stderr)
            return EXIT_BY_OUTCOME["failed"]

        self._run_id = str(request.get("run_id", ""))
        artifact_root = Path(request.get("artifact_root", "."))
        artifact_root.mkdir(parents=True, exist_ok=True)

        # Advertise capabilities
        self._emit(
            type="ready",
            runner_version=self._runner_version,
            capabilities=self._capabilities,
        )

        # Install cooperative SIGTERM handler
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def _handle_sigterm(signum, frame):  # noqa: ANN001
            self._cancelled = True
            signal.signal(signal.SIGTERM, original_sigterm)

        signal.signal(signal.SIGTERM, _handle_sigterm)

        # Transition to running
        self._emit(type="state", state="running", message="simulation starting")

        outcome = "completed"
        message = "simulation completed successfully"
        try:
            if self._cancelled:
                outcome = "cancelled"
                message = "cancelled before simulation started"
            else:
                self.run_simulation(request, artifact_root)
                if self._cancelled:
                    outcome = "cancelled"
                    message = "cancelled after simulation completed"
        except KeyboardInterrupt:
            outcome = "cancelled"
            message = "simulation interrupted"
        except Exception as exc:
            outcome = "failed"
            message = f"simulation raised {type(exc).__name__}: {exc}"
            self.emit_diagnostic("error", message)
        finally:
            signal.signal(signal.SIGTERM, original_sigterm)

        self._emit(type="result", outcome=outcome, message=message)
        return EXIT_BY_OUTCOME[outcome]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _emit(self, **fields) -> None:
        """Serialize and write one NDJSON event to the output stream."""
        event = {
            "protocol_version": PROTOCOL_VERSION,
            "run_id": self._run_id,
            "sequence": self._sequence,
            "time": datetime.now(timezone.utc).isoformat(),
            **fields,
        }
        self._output.write(json.dumps(event, separators=(",", ":")) + "\n")
        self._output.flush()
        self._sequence += 1


__all__ = [
    "DEFAULT_CAPABILITIES",
    "EXIT_BY_OUTCOME",
    "NdjsonRunnerBase",
    "PROTOCOL_VERSION",
]
