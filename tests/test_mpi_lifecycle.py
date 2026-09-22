"""Tests for casmsim.mpi_lifecycle (single-process / no mpi4py required)."""

from __future__ import annotations

import threading

import pytest

from casmsim.mpi_lifecycle import (
    CooperativeCancelToken,
    abort,
    barrier,
    get_comm,
    is_rank0,
    rank,
    size,
)


class TestNullComm:
    """All functions must degrade gracefully when comm=None."""

    def test_rank_none(self):
        assert rank(None) == 0

    def test_size_none(self):
        assert size(None) == 1

    def test_is_rank0_none(self):
        assert is_rank0(None) is True

    def test_barrier_none_is_noop(self):
        barrier(None)  # must not raise

    def test_get_comm_returns_none_or_comm(self):
        # In a test env without mpi4py, returns None; with mpi4py, returns COMM_WORLD
        result = get_comm()
        assert result is None or hasattr(result, "Get_rank")


class TestCooperativeCancelToken:
    def test_not_set_initially(self):
        token = CooperativeCancelToken()
        assert not token.is_set()

    def test_request_sets_flag(self):
        token = CooperativeCancelToken()
        token.request()
        assert token.is_set()

    def test_clear_resets_flag(self):
        token = CooperativeCancelToken()
        token.request()
        token.clear()
        assert not token.is_set()

    def test_broadcast_false_no_error_without_comm(self):
        token = CooperativeCancelToken(comm=None)
        token.request()
        assert token.is_set(broadcast=False)

    def test_broadcast_true_no_error_without_comm(self):
        # Without a real comm, broadcast=True must degrade silently.
        token = CooperativeCancelToken(comm=None)
        token.request()
        assert token.is_set(broadcast=True)

    def test_thread_safety(self):
        import time

        token = CooperativeCancelToken()
        results = []

        def setter():
            time.sleep(0.01)  # let checker start first
            token.request()

        def checker():
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                results.append(token.is_set())
                if results[-1]:
                    break
                time.sleep(0.001)

        t1 = threading.Thread(target=setter)
        t2 = threading.Thread(target=checker)
        t2.start()
        t1.start()
        t1.join()
        t2.join()
        assert any(results), "checker never saw the flag set"

    def test_is_set_without_request_is_false(self):
        token = CooperativeCancelToken()
        assert token.is_set(broadcast=True) is False


class TestAbort:
    def test_abort_raises_system_exit_without_comm(self):
        with pytest.raises(SystemExit) as exc_info:
            abort(None, exit_code=99)
        assert exc_info.value.code == 99
