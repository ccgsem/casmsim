"""Run lifecycle states for the casmsim runner protocol."""

from __future__ import annotations

from enum import Enum


class RunState(Enum):
    """Lifecycle state of a submitted simulation run.

    Integer values are stable across the wire (gRPC proto and NDJSON v1)
    and match the casmservice.RunState enum so the two can be compared by
    value without a conversion step once casmsim is a standalone package.
    """

    Pending = 0
    Running = 1
    Completed = 2
    Failed = 3


__all__ = ["RunState"]
