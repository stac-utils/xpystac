import functools
from typing import List, Union

import pystac
import xarray

from xpystac.utils import _import_optional_dependency


@functools.singledispatch
def to_xarray(item, **kwargs) -> xarray.Dataset:
    """Given a pystac object return an xarray dataset"""
    is_pystac_client_obj = hasattr(item, "item_collection")
    if is_pystac_client_obj:
        item_collection = item.item_collection()
        return to_xarray(item_collection, **kwargs)
    raise TypeError


@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
def _(
    obj: Union[pystac.Item, pystac.ItemCollection],
    drop_variables: Union[str, List[str]] = None,
    **kwargs,
) -> xarray.Dataset:
    stackstac = _import_optional_dependency("stackstac")
    if drop_variables is not None:
        raise KeyError("``drop_variables`` not implemented for pystac items")
    return stackstac.stack(obj, **kwargs).to_dataset(dim="band", promote_attrs=True)


@to_xarray.register
def _(obj: pystac.Asset, **kwargs) -> xarray.Dataset:
    open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})

    storage_options = obj.extra_fields.get("xarray:storage_options", None)
    if storage_options:
        open_kwargs["storage_options"] = storage_options

    if obj.media_type == pystac.MediaType.JSON and {"index", "references"}.intersection(
        obj.roles
    ):
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
        _import_optional_dependency("rioxarray")
        default_kwargs = {"engine": "rasterio"}
    elif obj.media_type == "application/vnd+zarr":
        _import_optional_dependency("zarr")
        default_kwargs = {"engine": "zarr"}
    else:
        default_kwargs = {}

    ds = xarray.open_dataset(obj.href, **{**default_kwargs, **open_kwargs, **kwargs})
    return ds
