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


# copied from https://github.com/stac-utils/pystac-client/blob/v0.6.0/tests/helpers.py#L7-L11
STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "EARTH-SEARCH": "https://earth-search.aws.element84.com/v1",
    "MLHUB": "https://api.radiant.earth/mlhub/v1",
}
