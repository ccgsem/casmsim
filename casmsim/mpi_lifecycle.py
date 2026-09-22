"""MPI rank management and cooperative cancellation for casmsim runners.

Provides lightweight helpers that work whether or not mpi4py is installed.
When mpi4py is unavailable (single-process test environments, CI), all calls
degrade gracefully: rank is always 0, barrier ops are no-ops, and the cancel
token is a plain threading flag.

Usage in a RunnerModelAdapter::

    from casmsim.mpi_lifecycle import get_comm, is_rank0, CooperativeCancelToken

    class MyAdapter:
        def __init__(self, comm, params):
            self._comm = comm or get_comm()
            self._cancel = CooperativeCancelToken(self._comm)

        def cancel(self) -> None:
            self._cancel.request()          # rank 0 sets the flag

        def start(self) -> None:
            schedule = build_schedule(...)
            while schedule.next_tick is not None:
                if self._cancel.is_set(broadcast=True):   # all ranks check
                    break
                schedule.execute_one_tick()
"""

from __future__ import annotations

import threading
from typing import Any

# ---------------------------------------------------------------------------
# MPI utilities
# ---------------------------------------------------------------------------


def get_comm() -> Any | None:
    """Return ``MPI.COMM_WORLD`` or ``None`` if mpi4py is unavailable."""
    try:
        from mpi4py import MPI  # noqa: PLC0415

        return MPI.COMM_WORLD
    except ImportError:
        return None


def rank(comm: Any | None) -> int:
    """Return the process rank (0 when *comm* is None)."""
    if comm is None:
        return 0
    return comm.Get_rank()  # type: ignore[no-any-return]


def size(comm: Any | None) -> int:
    """Return the communicator size (1 when *comm* is None)."""
    if comm is None:
        return 1
    return comm.Get_size()  # type: ignore[no-any-return]


def is_rank0(comm: Any | None) -> bool:
    """True when the calling process is rank 0 (or there is no communicator)."""
    return rank(comm) == 0


def barrier(comm: Any | None) -> None:
    """Block until all ranks reach this call; no-op when *comm* is None."""
    if comm is not None:
        comm.Barrier()


def abort(comm: Any | None, exit_code: int = 1) -> None:
    """Call ``MPI_Abort`` on all ranks; falls back to ``raise SystemExit``."""
    if comm is not None:
        try:
            comm.Abort(exit_code)
        except Exception:
            pass
    raise SystemExit(exit_code)


# ---------------------------------------------------------------------------
# Cooperative cancellation
# ---------------------------------------------------------------------------


class CooperativeCancelToken:
    """Thread- and rank-safe cancel flag for multi-rank repast4py runners.

    Rank 0 sets the flag (via :meth:`request`); all ranks poll it via
    :meth:`is_set`.  When *broadcast* is ``True`` in :meth:`is_set`, the flag
    value is propagated to every rank with a single ``Bcast`` call before
    returning, so ranks that never call :meth:`request` still see the
    cancellation.

    In single-process environments (*comm* is ``None``) the broadcast is
    skipped and the flag is a plain :class:`threading.Event`.

    Args:
        comm: MPI communicator, or ``None`` for single-process use.
    """

    def __init__(self, comm: Any | None = None) -> None:
        self._comm = comm
        self._event = threading.Event()

    def request(self) -> None:
        """Set the cancel flag (safe to call from any thread on rank 0)."""
        self._event.set()

    def is_set(self, *, broadcast: bool = False) -> bool:
        """Return ``True`` if cancellation has been requested.

        Args:
            broadcast: When ``True`` and a communicator is available, broadcast
                       the flag from rank 0 to all other ranks.  Pass
                       ``broadcast=True`` at the top of each simulation tick;
                       pass ``False`` for cheap intra-tick polls.
        """
        flag = self._event.is_set()
        if broadcast and self._comm is not None:
            # Bcast a single int from root=0 to all ranks.
            buf = [1 if flag else 0]
            self._comm.Bcast(buf, root=0)
            flag = bool(buf[0])
            if flag:
                self._event.set()
        return flag

    def clear(self) -> None:
        """Reset the flag (useful in tests)."""
        self._event.clear()


__all__ = [
    "CooperativeCancelToken",
    "abort",
    "barrier",
    "get_comm",
    "is_rank0",
    "rank",
    "size",
]
