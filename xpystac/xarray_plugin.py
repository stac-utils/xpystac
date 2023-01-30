import functools

import pystac
import xarray
from xarray.backends import BackendEntrypoint

from .utils import _import_optional_dependency


@functools.singledispatch
def to_xarray(item, **kwargs) -> xarray.Dataset:
    if hasattr(item, "get_all_items"):
        item_collection = item.get_all_items()
        return to_xarray(item_collection)
    raise TypeError


@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
def _(
    obj: pystac.Item | pystac.ItemCollection,
    drop_variables: str | list[str] = None,
    **kwargs,
) -> xarray.Dataset:
    stackstac = _import_optional_dependency("stackstac")
    if drop_variables is not None:
        raise KeyError("``drop_variables`` not implemented for pystac items")
    return stackstac.stack(obj, **kwargs).to_dataset(dim="band", promote_attrs=True)


@to_xarray.register
def _(obj: pystac.Asset, **kwargs) -> xarray.Dataset:
    open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})

    if obj.media_type == pystac.MediaType.JSON and "index" in obj.roles:
        requests = _import_optional_dependency("requests")
        fsspec = _import_optional_dependency("fsspec")
        r = requests.get(obj.href)
        r.raise_for_status()
        try:
            import planetary_computer

            refs = planetary_computer.sign(r.json())
        except ImportError:
            refs = r.json()
        mapper = fsspec.get_mapper("reference://", fo=refs)
        default_kwargs = {"engine": "zarr", "consolidated": False, "chunks": {}}
        return xarray.open_dataset(mapper, **default_kwargs, **open_kwargs, **kwargs)

    if obj.media_type == pystac.MediaType.COG:
        default_kwargs = {"engine", "rasterio"}
    elif obj.media_type == "application/vnd+zarr":
        default_kwargs = {"engine": "zarr"}
    else:
        default_kwargs = {}

    ds = xarray.open_dataset(obj.href, **default_kwargs, **open_kwargs, **kwargs)
    return ds


class STACBackend(BackendEntrypoint):
    def open_dataset(
        self,
        obj,
        *,
        drop_variables: str | list[str] = None,
        **kwargs,
    ):
        return to_xarray(obj, drop_variables=drop_variables, **kwargs)

    open_dataset_parameters = ["obj", "drop_variables"]

    def guess_can_open(self, obj):
        return isinstance(obj, (pystac.Asset, pystac.Item, pystac.ItemCollection))

    description = "Open pystac objects in Xarray"

    url = "https://github.com/jsignell/xpystac"
