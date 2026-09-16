"""casmsim — simulation runner runtime for repast4py and compatible engines.

Provides the stable protocol layer between casmservice and any simulation
engine: the RunnerModelAdapter protocol, gRPC SimulatorControl server,
Arrow Flight observation broker, and MPI lifecycle utilities.

casmsim is designed to be spun off as a standalone package once the
RunnerModelAdapter protocol stabilises across multiple implementations.
"""

from casmsim.mpi_lifecycle import CooperativeCancelToken, get_comm, is_rank0
from casmsim.ndjson_runner import NdjsonRunnerBase
from casmsim.protocols import ObservationAdapter, RunnerModelAdapter
from casmsim.run_state import RunState

__all__ = [
    "CooperativeCancelToken",
    "NdjsonRunnerBase",
    "ObservationAdapter",
    "RunnerModelAdapter",
    "RunState",
    "get_comm",
    "is_rank0",
]
