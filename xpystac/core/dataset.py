import functools
from collections.abc import Callable

import pystac
import xarray

from xpystac._xstac_kerchunk import _stac_to_kerchunk
from xpystac.utils import _import_optional_dependency


@functools.singledispatch
def to_xarray(
    obj,
    *,
    patch_url: None | Callable[[str], str] = None,
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:
    """Given a PySTAC object return an xarray dataset.

    The behavior of this method depends on the type of PySTAC object:

    * Asset: if the asset points to a kerchunk file or a zarr file,
      reads the metadata in that file to construct the coordinates of the
      dataset. If the asset points to a COG, read that.
    * Item: stacks all the assets into a dataset with 1 more dimension than
      any given asset.

    Parameters
    ----------
    obj : PySTAC object (Item, ItemCollection, Asset)
        The object from which to read data.
    patch_url : Callable, optional
        Function that takes a string or pystac object and returns an altered
        version. Normally used to sign urls before trying to read data from
        them. For instance when working with Planetary Computer this argument
        should be set to ``pc.sign``.
    allow_kerchunk : bool, (True by default)
        Control whether this reader tries to interpret kerchunk attributes
        if provided (either in the data-cube extension or as a regular asset
        with ``references`` or ``index`` as the role).
    """
    raise TypeError


@to_xarray.register(pystac.Item)
def _(
    obj: pystac.Item,
    drop_variables: str | list[str] | None = None,
    patch_url: None | Callable[[str], str] = None,
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
                "engine": "zarr",
                "consolidated": False,
            }

            return xarray.open_dataset(mapper, **{**default_kwargs, **kwargs})


@to_xarray.register
def _(
    obj: pystac.Asset,
    patch_url: None | Callable[[str], str] = None,
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:
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
        r = requests.get(obj.href)
        r.raise_for_status()

        refs = r.json()
        if patch_url is not None:
            refs = patch_url(refs)

        default_kwargs = {
            "engine": "kerchunk",
        }
        return xarray.open_dataset(refs, **{**default_kwargs, **open_kwargs, **kwargs})

    if obj.media_type == pystac.MediaType.COG:
        _import_optional_dependency("rioxarray")
        default_kwargs = {"engine": "rasterio"}
    elif obj.media_type in ["application/vnd+zarr", "application/vnd.zarr"]:
        _import_optional_dependency("zarr")
        zarr_kwargs = {}
        if "zarr:consolidated" in obj.extra_fields:
            zarr_kwargs["consolidated"] = obj.extra_fields["zarr:consolidated"]
        if "zarr:zarr_format" in obj.extra_fields:
            zarr_kwargs["zarr_format"] = obj.extra_fields["zarr:zarr_format"]
        default_kwargs = {**zarr_kwargs, "engine": "zarr"}
    elif obj.media_type == "application/vnd.zarr+icechunk":
        from xpystac._icechunk import read_icechunk

        return read_icechunk(obj)

    href = obj.href
    if patch_url is not None:
        href = patch_url(href)

    ds = xarray.open_dataset(href, **{**default_kwargs, **open_kwargs, **kwargs})
    return ds
