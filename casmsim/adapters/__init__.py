"""casmsim.adapters — ready-made RunnerModelAdapter implementations.

Bundled adapters
----------------
:class:`~casmsim.adapters.repast4py.Repast4pyAdapter`
    Generic adapter for any repast4py model with a ``schedule`` attribute.
    Subclass and set ``MODEL_CLASS`` for zero-config entry-point resolution.

:class:`~casmsim.adapters.toy.ToyRunnerAdapter`
    Minimal in-process adapter for testing (no MPI or repast4py required).

:class:`~casmsim.adapters.casmsocial.CasmPopAdapter`
    Wraps casmsocial ``CasmPop`` models via the ``Models`` factory.
    Activated automatically when ``model.plugins`` is present in params;
    requires casmsocial to be installed.

External adapters
-----------------
External model packages implement ``RunnerModelAdapter`` themselves and
declare a dependency on ``casmsim``.  casmsim does not reference them.
Use ``runner.entry_point = "mypackage.adapters:MyAdapter"`` to load any
conforming adapter at runtime without a static import.
"""

from casmsim.adapters.repast4py import Repast4pyAdapter
from casmsim.adapters.toy import ToyRunnerAdapter

__all__ = [
    "Repast4pyAdapter",
    "ToyRunnerAdapter",
]
