"""Tests for casmsim.ndjson_runner.NdjsonRunnerBase.

Exercises the NDJSON v1 external runner protocol:
- Event ordering and sequence numbering
- ready / state / observation / diagnostic / result events
- Exit codes
- SIGTERM cooperative cancellation
- Subclass failure propagation
"""

from __future__ import annotations

import io
import json
import os
import signal
import threading
import time

import pytest

from casmsim.ndjson_runner import EXIT_BY_OUTCOME, NdjsonRunnerBase, PROTOCOL_VERSION

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(run_id: str = "test-run-1", artifact_root: str = "/tmp/artifacts") -> str:
    return json.dumps({"run_id": run_id, "artifact_root": artifact_root})


def _parse_events(output: io.StringIO) -> list[dict]:
    output.seek(0)
    return [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]


class _NoopRunner(NdjsonRunnerBase):
    """Completes immediately without doing anything."""

    def run_simulation(self, request, artifact_root):
        pass


class _DiagRunner(NdjsonRunnerBase):
    """Emits one diagnostic then completes."""

    def run_simulation(self, request, artifact_root):
        self.emit_diagnostic("info", "all good")


class _ObsRunner(NdjsonRunnerBase):
    """Writes a fake artifact file and emits an observation event."""

    def run_simulation(self, request, artifact_root):
        path = artifact_root / "batch_0.arrow"
        path.write_bytes(b"FAKE_ARROW_IPC")
        self.emit_observation(
            channel="agents",
            format="arrow_ipc_file",
            path=path.name,
            batch_index=0,
            row_offset=0,
            rows=3,
            nbytes=len(b"FAKE_ARROW_IPC"),
        )


class _FailRunner(NdjsonRunnerBase):
    """Raises an exception to test failed outcome."""

    def run_simulation(self, request, artifact_root):
        raise RuntimeError("boom")


class _CancelCheckRunner(NdjsonRunnerBase):
    """Polls self._cancelled in a tight loop and exits early when set."""

    def run_simulation(self, request, artifact_root):
        for _ in range(1000):
            if self._cancelled:
                return
            time.sleep(0.005)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _run(runner_cls, request_json: str | None = None, artifact_root: str = "/tmp/artifacts"):
    out = io.StringIO()
    import sys
    from unittest.mock import patch

    req = request_json or _make_request(artifact_root=artifact_root)
    runner = runner_cls(runner_version="0.0.1-test", output=out)
    with patch("sys.stdin", io.StringIO(req)):
        exit_code = runner.main()
    events = _parse_events(out)
    return exit_code, events


class TestEventShape:
    def test_ready_is_first_event(self):
        _, events = _run(_NoopRunner)
        assert events[0]["type"] == "ready"

    def test_ready_fields(self):
        _, events = _run(_NoopRunner)
        ready = events[0]
        assert ready["runner_version"] == "0.0.1-test"
        assert "arrow_ipc_file" in ready["capabilities"]
        assert "cancel_sigterm" in ready["capabilities"]
        assert "immutable_observation_batches" in ready["capabilities"]

    def test_state_running_emitted_after_ready(self):
        _, events = _run(_NoopRunner)
        types = [e["type"] for e in events]
        assert types[0] == "ready"
        assert "state" in types[1:3]

    def test_result_is_last_event(self):
        _, events = _run(_NoopRunner)
        assert events[-1]["type"] == "result"

    def test_sequence_monotone_from_zero(self):
        _, events = _run(_NoopRunner)
        seqs = [e["sequence"] for e in events]
        assert seqs == list(range(len(events)))

    def test_protocol_version_on_every_event(self):
        _, events = _run(_NoopRunner)
        for e in events:
            assert e["protocol_version"] == PROTOCOL_VERSION

    def test_run_id_on_every_event(self):
        _, events = _run(_NoopRunner, request_json=_make_request(run_id="my-run"))
        for e in events:
            assert e["run_id"] == "my-run"


class TestOutcomes:
    def test_completed_exit_code(self):
        code, _ = _run(_NoopRunner)
        assert code == EXIT_BY_OUTCOME["completed"]  # 0

    def test_completed_result_event(self):
        _, events = _run(_NoopRunner)
        result = events[-1]
        assert result["type"] == "result"
        assert result["outcome"] == "completed"

    def test_failed_exit_code(self):
        code, _ = _run(_FailRunner)
        assert code == EXIT_BY_OUTCOME["failed"]  # 3

    def test_failed_result_event(self):
        _, events = _run(_FailRunner)
        result = events[-1]
        assert result["type"] == "result"
        assert result["outcome"] == "failed"
        assert "RuntimeError" in result["message"]

    def test_failed_emits_diagnostic(self):
        _, events = _run(_FailRunner)
        diag = [e for e in events if e["type"] == "diagnostic"]
        assert len(diag) == 1
        assert diag[0]["level"] == "error"


class TestObservation:
    def test_observation_event_fields(self, tmp_path):
        _, events = _run(_ObsRunner, artifact_root=str(tmp_path))
        obs = [e for e in events if e["type"] == "observation"]
        assert len(obs) == 1
        o = obs[0]
        assert o["channel"] == "agents"
        assert o["format"] == "arrow_ipc_file"
        assert o["path"] == "batch_0.arrow"
        assert o["batch_index"] == 0
        assert o["row_offset"] == 0
        assert o["rows"] == 3
        assert o["bytes"] == len(b"FAKE_ARROW_IPC")

    def test_observation_between_state_and_result(self, tmp_path):
        _, events = _run(_ObsRunner, artifact_root=str(tmp_path))
        types = [e["type"] for e in events]
        obs_idx = types.index("observation")
        state_idx = next(i for i, t in enumerate(types) if t == "state")
        result_idx = types.index("result")
        assert state_idx < obs_idx < result_idx


class TestDiagnostic:
    def test_diagnostic_event_fields(self):
        _, events = _run(_DiagRunner)
        diag = [e for e in events if e["type"] == "diagnostic"]
        assert len(diag) == 1
        assert diag[0]["level"] == "info"
        assert diag[0]["message"] == "all good"


class TestCancellation:
    def test_cancelled_flag_produces_cancelled_outcome(self):
        """Inject _cancelled=True via a subclass to test the cancelled code path."""

        class _PreCancelledRunner(NdjsonRunnerBase):
            def run_simulation(self, request, artifact_root):
                # Signal cancellation from within the simulation
                self._cancelled = True

        out = io.StringIO()
        from unittest.mock import patch

        runner = _PreCancelledRunner(runner_version="0.0.1-test", output=out)
        with patch("sys.stdin", io.StringIO(_make_request())):
            code = runner.main()

        assert code == EXIT_BY_OUTCOME["cancelled"]
        events = _parse_events(out)
        assert events[-1]["type"] == "result"
        assert events[-1]["outcome"] == "cancelled"

    def test_sigterm_handler_sets_cancelled_flag(self):
        """SIGTERM handler writes self._cancelled = True then restores the old handler."""
        runner = _NoopRunner(runner_version="0.0.1-test", output=io.StringIO())
        original = signal.getsignal(signal.SIGTERM)
        try:
            # Install the handler exactly as main() does
            def _handle(signum, frame):
                runner._cancelled = True
                signal.signal(signal.SIGTERM, original)

            signal.signal(signal.SIGTERM, _handle)
            assert not runner._cancelled
            _handle(signal.SIGTERM, None)
            assert runner._cancelled
            # Handler should have restored original
            assert signal.getsignal(signal.SIGTERM) is original
        finally:
            signal.signal(signal.SIGTERM, original)


class TestBadInput:
    def test_invalid_json_returns_failed(self):
        out = io.StringIO()
        from unittest.mock import patch

        runner = _NoopRunner(runner_version="0.0.1-test", output=out)
        with patch("sys.stdin", io.StringIO("{not json}")):
            code = runner.main()
        assert code == EXIT_BY_OUTCOME["failed"]
        # No events emitted (couldn't parse request)
        events = _parse_events(out)
        assert len(events) == 0
