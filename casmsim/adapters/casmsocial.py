"""Removed — casmsim must not reference external model packages.

CasmPopAdapter now lives in casmsocial:

    from casmsocial.adapters.runner import CasmPopAdapter
"""

raise ImportError("casmsim.adapters.casmsocial has been removed. Import from casmsocial.adapters.runner instead.")
