# casmsim

gRPC/Arrow Flight runner transport for agent-based models.

`casmsim` provides a loopback runner protocol for launching, observing, and cancelling simulation runs. Model packages implement its `RunnerModelAdapter` protocol and supply an explicit `runner.entry_point`; `casmsim` has no dependency on a particular simulation framework.

## Architecture

```
control plane
    │  gRPC (casm.runner.v1)
    ▼
casmsim.grpc_runner.SimulatorControlServicer
    │
    └─ <runner.entry_point>                         ← model-provided RunnerModelAdapter
    │
    ▼  Arrow Flight
casmsim.flight_server / ObservationBroker
    │
    ▼
control plane observation stream
```

## Installation

```bash
pip install casmsim
```

## Protocols

Implement `casmsim.protocols.RunnerModelAdapter` to plug any model into the runner:

```python
from casmsim.protocols import RunnerModelAdapter, ObservationAdapter
from casmsim.run_state import RunState

class MyModelAdapter(RunnerModelAdapter):
    def start(self, run_id: str, config: dict, observation: ObservationAdapter) -> None:
        ...
    def cancel(self) -> None:
        ...
```

Register via `pyproject.toml` entry point:

```toml
[project.entry-points."casmsim.adapters"]
my_model = "my_package.adapter:MyModelAdapter"
```

Then submit a run with `runner.entry_point = "my_package.adapter:MyModelAdapter"` in the config JSON.

## Proto regeneration

```bash
bash scripts/regen_proto.sh
```

## License

MIT — see [LICENSE](LICENSE).

> **Note:** Model integrations belong in their respective model packages. For example, CASMSocial supplies its own runner adapter and injects it through `runner.entry_point`.
