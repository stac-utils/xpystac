import functools
from typing import Callable, List, Literal, Mapping, Union

import pystac
import xarray

from xpystac.utils import _import_optional_dependency, _is_item_search


@functools.singledispatch
def to_xarray(
    obj,
    *,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    patch_urls: Union[None, Callable[[str], str]] = None,
    **kwargs,
) -> xarray.Dataset:
    """Given a PySTAC object return an xarray dataset.

    The behavior of this method depends on the type of PySTAC object:

    * Asset: if the asset points to a kerchunk file or a zarr file,
      reads the metadata in that file to construct the coordinates of the
      dataset. If the asset points to a COG, read that.
    * Item: stacks all the assets into a dataset with 1 more dimension than
      any given asset.
    * ItemCollection (output of pystac_client.search): stacks all the
      assets in all the items into a dataset with 2 more dimensions than
      any given asset.

    Parameters
    ----------
    obj : PySTAC object (Item, ItemCollection, Asset)
        The object from which to read data.
    stacking_library : "odc.stac", "stackstac", optional
        When stacking multiple items, this argument determines which library
        to use. Defaults to ``odc.stac`` if available and otherwise ``stackstac``.
    patch_urls : Callable, optional
        Function that takes a string and returns an altered string. Normally used 
        to sign urls before trying to read data from them. For instance when working
        with Planetary Computer this argument should be set to ``pc.sign``.
    """
    if _is_item_search(obj):
        item_collection = obj.item_collection()
        return to_xarray(item_collection, stacking_library=stacking_library, patch_urls=patch_urls, **kwargs)
    raise TypeError


@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
def _(
    obj: Union[pystac.Item, pystac.ItemCollection],
    drop_variables: Union[str, List[str], None] = None,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    patch_urls: Union[None, Callable[[str], str]] = None,
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
        return odc_stac.load(items, **{"chunks": {"x": 1024, "y": 1024}, patch_urls: patch_urls, **kwargs})
    elif stacking_library == "stackstac":
        stackstac = _import_optional_dependency("stackstac")
        if patch_urls:
            if isinstance(obj, pystac.STACObject):
                obj = patch_urls(obj)
            else:
                obj = [patch_urls(o) for o in obj]
        return stackstac.stack(obj, **kwargs).to_dataset(dim="band", promote_attrs=True)


@to_xarray.register
def _(
    obj: pystac.Asset,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    patch_urls: Union[None, Callable[[str], str]] = None,
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
        refs = r.json()
        if patch_urls is not None:
            refs = patch_urls(refs)

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

    href = obj.href
    if patch_urls is not None:
        href = patch_urls(href)

    ds = xarray.open_dataset(href, **{**default_kwargs, **open_kwargs, **kwargs})
    return ds
