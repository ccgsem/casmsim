# casmsim

gRPC/Arrow Flight runner transport for [CASMSocial](https://github.com/ccgsem/casmsocial) agent-based models.

`casmsim` provides the loopback runner protocol that the CASMSocial control plane uses to launch, observe, and cancel simulation runs. It is distributed as a separate package so that non-CASMSocial frameworks (Repast4Py, XDevs, plain Python) can implement the `RunnerModelAdapter` protocol and run under the same control plane without taking a dependency on CASMSocial itself.

## Architecture

```
control plane
    │  gRPC (casm.runner.v1)
    ▼
casmsim.grpc_runner.SimulatorControlServicer
    │
    ├─ casmsim.adapters.casmsocial.CasmPopAdapter   ← casmsocial models
    ├─ casmsim.adapters.repast4py.Repast4PyAdapter  ← Repast4Py models
    └─ <runner.entry_point>                         ← any RunnerModelAdapter
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
# with casmsocial model support:
pip install "casmsim[casmsocial]"
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

> **Note:** This package was extracted from the `casmsocial` internal `casmsim` subpackage. The pre-extraction history is archived at [ccgsem/casmsim-pre-social](https://github.com/ccgsem/casmsim-pre-social).
