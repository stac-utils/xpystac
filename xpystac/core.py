import functools
from typing import List, Literal, Mapping, Union

import pystac
import xarray

from xpystac._xstac_kerchunk import _stac_to_kerchunk
from xpystac.utils import _import_optional_dependency, _is_item_search


@functools.singledispatch
def to_xarray(
    obj,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:
    """Given a pystac object return an xarray dataset

    When stacking multiple items, an optional ``stacking_library`` argument
    is accepted. It defaults to ``odc.stac`` if available and otherwise ``stackstac``.
    Control the behavior by setting ``stacking_library``

    User ``allow_kerchunk`` (True by default) to control whether this reader tries to
    interpret kerchunk attributes if provided (either in the data-cube extension or
    as a full asset with ``references`` or ``index`` as the role).
    """
    if _is_item_search(obj):
        item_collection = obj.item_collection()
        return to_xarray(
            item_collection,
            stacking_library=stacking_library,
            allow_kerchunk=allow_kerchunk,
            **kwargs,
        )
    raise TypeError


@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
@to_xarray.register(list)
def _(
    obj: Union[pystac.Item, pystac.ItemCollection, List[pystac.Item]],
    drop_variables: Union[str, List[str], None] = None,
    stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:
    if drop_variables is not None:
        raise KeyError("``drop_variables`` not implemented for pystac items")

    if allow_kerchunk:
        first_obj = obj if isinstance(obj, pystac.Item) else next(i for i in obj)
        is_kerchunked = any("kerchunk:" in k for k in first_obj.properties.keys())
        if is_kerchunked:
            kerchunk_combine = _import_optional_dependency("kerchunk.combine")
            fsspec = _import_optional_dependency("fsspec")

            if isinstance(obj, (list, pystac.ItemCollection)):
                refs = kerchunk_combine.MultiZarrToZarr(
                    [_stac_to_kerchunk(item) for item in obj],
                    concat_dims=kwargs.get("concat_dims", "time"),
                ).translate()
            else:
                refs = _stac_to_kerchunk(obj)

            mapper = fsspec.filesystem("reference", fo=refs).get_mapper()
            default_kwargs = {
                "chunks": {},
                "engine": "zarr",
                "consolidated": False,
            }

            return xarray.open_dataset(mapper, **{**default_kwargs, **kwargs})

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
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:
    default_kwargs: Mapping = {"chunks": {}}
    open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})

    storage_options = obj.extra_fields.get("xarray:storage_options", None)
    if storage_options:
        open_kwargs["storage_options"] = storage_options

    if (
        allow_kerchunk
        and obj.media_type == pystac.MediaType.JSON
        and {"index", "references"}.intersection(set(obj.roles) if obj.roles else set())
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

        mapper = fsspec.filesystem("reference", fo=refs).get_mapper()
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
