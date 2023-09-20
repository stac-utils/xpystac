import functools
from typing import List, Literal, Mapping, Union

import pystac
import xarray

from xpystac.utils import _import_optional_dependency, _is_item_search


@functools.singledispatch
def to_xarray(
    obj,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    **kwargs,
) -> xarray.Dataset:
    """Given a pystac object return an xarray dataset

    When stacking multiple items, an optional ``stacking_library`` argument
    is accepted. It defaults to ``odc.stac`` if available and otherwise ``stackstac``.
    Control the behavior by setting ``stacking_library``
    """
    if _is_item_search(obj):
        item_collection = obj.item_collection()
        return to_xarray(item_collection, stacking_library=stacking_library, **kwargs)
    raise TypeError


@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
def _(
    obj: Union[pystac.Item, pystac.ItemCollection],
    drop_variables: Union[str, List[str], None] = None,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    **kwargs,
) -> xarray.Dataset:
    if drop_variables is not None:
        raise KeyError("``drop_variables`` not implemented for pystac items")

    if stacking_library is None:
        try:
            _import_optional_dependency("odc.stac")
            stacking_library = "odc.stac"
        except ImportError:
            _import_optional_dependency("stackstac")
            stacking_library = "stackstac"
    elif stacking_library not in ["odc.stac", "stackstac"]:
        raise ValueError(f"{stacking_library=} is not a valid option")

    if stacking_library == "odc.stac":
        odc_stac = _import_optional_dependency("odc.stac")
        if isinstance(obj, pystac.Item):
            items = [obj]
        else:
            items = [i for i in obj]
        return odc_stac.load(items, **{"chunks": {"x": 1024, "y": 1024}, **kwargs})
    elif stacking_library == "stackstac":
        stackstac = _import_optional_dependency("stackstac")
        return stackstac.stack(obj, **kwargs).to_dataset(dim="band", promote_attrs=True)


@to_xarray.register
def _(
    obj: pystac.Asset,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    **kwargs,
) -> xarray.Dataset:
    default_kwargs: Mapping = {"chunks": {}}
    open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})

    storage_options = obj.extra_fields.get("xarray:storage_options", None)
    if storage_options:
        open_kwargs["storage_options"] = storage_options

    if obj.media_type == pystac.MediaType.JSON and {"index", "references"}.intersection(
        set(obj.roles) if obj.roles else set()
    ):
        requests = _import_optional_dependency("requests")
        fsspec = _import_optional_dependency("fsspec")
        r = requests.get(obj.href)
        r.raise_for_status()
        try:
            import planetary_computer  # type: ignore

            refs = planetary_computer.sign(r.json())
        except ImportError:
            refs = r.json()

        mapper = fsspec.get_mapper("reference://", fo=refs)
        default_kwargs = {
            **default_kwargs,
            "engine": "zarr",
            "consolidated": False,
        }
        return xarray.open_dataset(
            mapper, **{**default_kwargs, **open_kwargs, **kwargs}
        )

    if obj.media_type == pystac.MediaType.COG:
        _import_optional_dependency("rioxarray")
        default_kwargs = {**default_kwargs, "engine": "rasterio"}
    elif obj.media_type == "application/vnd+zarr":
        _import_optional_dependency("zarr")
        default_kwargs = {**default_kwargs, "engine": "zarr"}

    ds = xarray.open_dataset(obj.href, **{**default_kwargs, **open_kwargs, **kwargs})
    return ds
