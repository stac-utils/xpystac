import importlib

import pytest
from packaging.version import Version


# Copied from https://github.com/zarr-developers/VirtualiZarr/blob/9c3d0f90cc79fa20fe33833e244ae28a1ee91f17/virtualizarr/tests/__init__.py#L17-L34
def _importorskip(
    modname: str, minversion: str | None = None
) -> tuple[bool, pytest.MarkDecorator]:
    try:
        mod = importlib.import_module(modname)
        has = True
        if minversion is not None:
            v = getattr(mod, "__version__", "999")
            if Version(v) < Version(minversion):
                raise ImportError("Minimum version not satisfied")
    except ImportError:
        has = False

    reason = f"requires {modname}"
    if minversion is not None:
        reason += f">={minversion}"
    func = pytest.mark.skipif(not has, reason=reason)
    return has, func


has_planetary_computer, requires_planetary_computer = _importorskip(
    "planetary_computer"
)


has_icechunk, requires_icechunk = _importorskip("icechunk")


# icechunk 2.x dropped Python 3.11 wheels, so CI's 3.11 job is stuck on the
# last 3.11-compatible release (1.1.21), which can only read Icechunk v1
# repositories. A fixture built with a newer icechunk (v2 format) needs this
# stricter gate, not just "icechunk is importable".
has_icechunk_v2, requires_icechunk_v2 = _importorskip("icechunk", minversion="2.0")


has_rustac, requires_rustac = _importorskip("rustac")


# copied from https://github.com/stac-utils/pystac-client/blob/v0.6.0/tests/helpers.py#L7-L11
STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "EARTH-SEARCH": "https://earth-search.aws.element84.com/v1",
    "MLHUB": "https://api.radiant.earth/mlhub/v1",
}
