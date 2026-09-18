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

    # No kerchunk reference on the item itself: fall back to opening a Zarr /
    # icechunk asset directly. xpystac dispatches Zarr-like assets in the
    # registered ``pystac.Asset`` handler below.
    dataset_media_types = {
        "application/vnd+zarr",
        "application/vnd.zarr",
        "application/vnd.zarr+icechunk",
    }
    dataset_assets = [
        a for a in obj.assets.values() if a.media_type in dataset_media_types
    ]
    if dataset_assets:
        return to_xarray(dataset_assets[0], patch_url=patch_url, **kwargs)

    observed = sorted({a.media_type or "(unset)" for a in obj.assets.values()})
    has_cog = any(
        a.media_type == pystac.MediaType.COG for a in obj.assets.values()
    )
    msg = (
        f"Item {obj.id!r} has no Zarr or kerchunk asset that xpystac can open "
        f"as a dataset; observed asset media_types: {observed}."
    )
    if has_cog:
        msg += (
            " For COG / image assets, use stackstac or odc-stac to stack "
            "across items."
        )
    raise ValueError(msg)


@to_xarray.register( pystac.Asset )
def _(
    obj: pystac.Asset,
    patch_url: None | Callable[[str], str] = None,
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:
    open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})

    # MKM 18 Oct 2024
    #TODO : Check if the obj is list instance or pystac.Asset instance and
    # accordingly if just one asset pass it through xarray,
    # else, collect the assets and pass to xarray as open_mfdataset.
    # In case of tar balls, extract each of them and the store these paths
    # send the list of these zarr stores to xarray.open_mfdataset()
    # It should work.

    default_kwargs = {}

    # Check the type of the 'obj'
    if isinstance(obj, pystac.Asset):
        print("Asset as input!",flush=True)
        #open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})

        storage_options = obj.extra_fields.get("xarray:storage_options", None)
        if storage_options:
            open_kwargs["storage_options"] = storage_options

        if (
            allow_kerchunk
            and obj.media_type == pystac.MediaType.JSON
            and {"index", "references"}.intersection(set(obj.roles)
                                            if obj.roles else set())
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
            return xarray.open_dataset(refs, **{**default_kwargs,
                                            **open_kwargs, **kwargs})


        if obj.media_type == pystac.MediaType.COG:
            _import_optional_dependency("rioxarray")
            default_kwargs = {**default_kwargs, "engine": "rasterio"}
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

        # Handling the 'archive' extension,
        # as of now only plain '*.tar' files are handled.
        elif obj.media_type == "application/x-tar":
            zarr =  _import_optional_dependency("zarr")
            if 'TarStore' not in zarr.storage.__all__:
                raise ImportError("zarr.storage.TarStore not found! " \
                    "Please update 'zarr' to the latest version.")
            else:
                print(f"Opening tarstore : {obj.href}")
                # MKM With new tarstore implementation in zarr-python
                with zarr.storage.TarStore(obj.href, mode = 'r') as tar_store:
                    return xarray.open_zarr(tar_store, **kwargs)


        href = obj.href
        if patch_url is not None:
            href = patch_url(href)
            ds = xarray.open_dataset(href, **{**default_kwargs,
                                        **open_kwargs, **kwargs})
            return ds


@to_xarray.register( list )
def _(
    obj: list[pystac.Asset],
    patch_url: None | Callable[[str], str] = None,
    allow_kerchunk: bool = True,
    **kwargs,
) -> xarray.Dataset:

    if not isinstance( obj, list ):
        raise TypeError('Input is not a list of assets!')

    if not isinstance( obj[0], pystac.Asset ):
        raise TypeError('Input is not a list of assets!')

    open_kwargs = obj[0].extra_fields.get("xarray:open_kwargs", {})



    print("List of Assets as input!",flush=True)
    # Creates a list of assets from the list of items.
    # Concates all the zarr stores from each tar ball and
    # creates the xarray Dataset,with engine as 'zarr'.
    # Returns the xarray Dataset created above,
    # ( for this particular use case )

    open_kwargs = obj[0].extra_fields.get("xarray:open_kwargs", {})

    storage_options = obj[0].extra_fields.get("xarray:storage_options", None)
    if storage_options:
        open_kwargs["storage_options"] = storage_options

    ref_media_type = obj[0].media_type
    zarr_store_list = []
    tqdm =  _import_optional_dependency("tqdm")
    for i in tqdm.tqdm(obj):
        # Check the type of the assets -- for homogenity ( all are tar balls )
        if i.media_type != ref_media_type:
            print(f"Encountered {i.to_dict()} which differs with {ref_media_type}!")
            # Empty Dataset
            return xarray.Dataset(data_vars=None, coords=None, attrs=None)

        if ref_media_type == "application/x-tar":
            print(f"Opening tarstore : {i.href}")
            # To be opened with new tarstore implementation in zarr-python
            zarr_store_list.append(i.href)
    
    zarr =  _import_optional_dependency("zarr")
    if 'TarStore' not in zarr.storage.__all__:
        raise ImportError("zarr.storage.TarStore not found! " \
                "Please update 'zarr' to the latest version.")
    else:
        # TODO: To fix the concat_dims etc. for hierarchical datasets.
        tarStoreList = [ zarr.storage.TarStore( storePath, mode='r')
                            for storePath in zarr_store_list ]
        return xarray.open_mfdataset( tarStoreList, engine = 'zarr' )
