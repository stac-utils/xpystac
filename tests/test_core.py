import pystac_client
import pytest

from tests.utils import STAC_URLS, requires_icechunk, requires_planetary_computer
from xpystac.core import to_xarray


def test_to_xarray_with_cog_asset(simple_cog):
    ds = to_xarray(simple_cog)
    assert ds


@requires_planetary_computer
def test_to_xarray_with_pystac_client_search_with_patch_url():
    import planetary_computer as pc
    from rasterio.errors import RasterioIOError

    client = pystac_client.Client.open(STAC_URLS["PLANETARY-COMPUTER"])
    search = client.search(
        intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
        collections=["sentinel-2-l2a"],
        datetime="2020-05-01",
    )
    item = next(search.items())
    asset = item.assets["B04"]

    with pytest.raises(RasterioIOError, match="HTTP response code: 409"):
        to_xarray(asset)

    to_xarray(asset, patch_url=pc.sign)


def test_to_xarray_with_bad_type():
    with pytest.raises(TypeError):
        to_xarray("foo")


@requires_planetary_computer
def test_to_xarray_reference_file():
    import planetary_computer as pc
    from fsspec.implementations.reference import ReferenceNotReachable

    client = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=pc.sign_inplace
    )
    collection = client.get_collection("nasa-nex-gddp-cmip6")
    assert collection is not None
    kerchunk_asset = collection.assets["ACCESS-CM2.historical"]

    with pytest.raises(ReferenceNotReachable):
        to_xarray(kerchunk_asset)

    ds = to_xarray(kerchunk_asset, patch_url=pc.sign, chunks={})
    assert not ds.lon.isnull().all(), "Coordinates should be populated"

    for da in ds.data_vars.values():
        if da.ndim >= 2:
            assert hasattr(da.data, "dask")


@requires_planetary_computer
def test_to_xarray_zarr():
    import planetary_computer as pc

    catalog = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=pc.sign_inplace
    )
    collection = catalog.get_collection("daymet-daily-hi")
    assert collection is not None
    zarr_asset = collection.assets["zarr-abfs"]

    ds = to_xarray(zarr_asset, chunks={})
    for da in ds.data_vars.values():
        if da.ndim >= 2:
            assert hasattr(da.data, "dask"), da.name


@requires_planetary_computer
def test_to_xarray_zarr_with_open_kwargs_engine():
    import planetary_computer as pc

    catalog = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=pc.sign_inplace
    )
    collection = catalog.get_collection("daymet-daily-hi")
    assert collection is not None
    zarr_asset = collection.assets["zarr-abfs"]
    zarr_asset.extra_fields["xarray:open_kwargs"]["engine"] = "zarr"

    to_xarray(zarr_asset)


@requires_planetary_computer
def test_to_xarray_zarr_with_zarr_extension():
    import planetary_computer as pc

    catalog = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=pc.sign_inplace
    )
    collection = catalog.get_collection("daymet-daily-hi")
    assert collection is not None
    zarr_asset = collection.assets["zarr-abfs"]

    # pop off the xarray-assets extension fields
    zarr_asset.extra_fields.pop("xarray:open_kwargs")
    zarr_asset.extra_fields["zarr:consolidated"] = True

    to_xarray(zarr_asset)


@pytest.mark.skip(reason="not yet supported with kerchunk >=0.2.8")
def test_to_xarray_with_item_with_kerchunk_attrs_in_data_cube(data_cube_kerchunk):
    ds = to_xarray([i for i in data_cube_kerchunk][-1])
    assert ds


@requires_icechunk
def test_to_xarray_virtual_icechunk(virtual_icechunk):
    # Get the latest version of the collection-level asset
    assets = virtual_icechunk.get_assets(role="latest-version")
    asset = next(iter(assets.values()))

    to_xarray(asset)
