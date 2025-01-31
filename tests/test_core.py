import dask.array
import pystac_client
import pytest
import xarray as xr

from tests.utils import STAC_URLS, requires_planetary_computer
from xpystac.core import to_xarray


def test_to_xarray_with_cog_asset(simple_cog):
    ds = to_xarray(simple_cog)
    assert ds


def test_to_xarray_with_pystac_client_search(simple_search):
    ds = to_xarray(simple_search)
    assert ds


def test_to_xarray_returns_dask_backed_object(simple_search):
    ds = to_xarray(simple_search)
    assert isinstance(ds.blue.data, dask.array.Array)
    assert ds.blue.data.npartitions > 1


def test_to_xarray_with_pystac_client_search_passes_kwargs_through(simple_search):
    ds = to_xarray(simple_search, bands=["red", "green", "blue"], chunks={})
    assert list(ds.data_vars) == ["red", "green", "blue"]
    assert ds.blue.data.npartitions == 1


@pytest.mark.parametrize("stacking_library", ["odc.stac", "stackstac"])
def test_to_xarray_with_different_stacking_library(simple_search, stacking_library):
    ds = to_xarray(simple_search, stacking_library=stacking_library)
    assert isinstance(ds, xr.Dataset)
    assert "band" not in ds.dims


def test_to_xarray_with_drop_variables_raises(simple_search):
    with pytest.raises(KeyError, match="not implemented for pystac items"):
        to_xarray(simple_search, drop_variables=["blue"])


def test_to_xarray_with_bad_type():
    with pytest.raises(TypeError):
        to_xarray("foo")


@requires_planetary_computer
def test_to_xarray_reference_file():
    import planetary_computer

    client = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=planetary_computer.sign_inplace
    )
    collection = client.get_collection("nasa-nex-gddp-cmip6")
    assert collection is not None
    kerchunk_asset = collection.assets["ACCESS-CM2.historical"]

    ds = to_xarray(kerchunk_asset)
    assert ds
    for da in ds.data_vars.values():
        if da.ndim >= 2:
            assert hasattr(da.data, "dask")


@requires_planetary_computer
def test_to_xarray_zarr():
    import planetary_computer

    catalog = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=planetary_computer.sign_inplace
    )
    collection = catalog.get_collection("daymet-daily-hi")
    assert collection is not None
    zarr_asset = collection.assets["zarr-abfs"]

    ds = to_xarray(zarr_asset)
    for da in ds.data_vars.values():
        if da.ndim >= 2:
            assert hasattr(da.data, "dask"), da.name


@requires_planetary_computer
def test_to_xarray_zarr_with_open_kwargs_engine():
    import planetary_computer

    catalog = pystac_client.Client.open(
        STAC_URLS["PLANETARY-COMPUTER"], modifier=planetary_computer.sign_inplace
    )
    collection = catalog.get_collection("daymet-daily-hi")
    assert collection is not None
    zarr_asset = collection.assets["zarr-abfs"]
    zarr_asset.extra_fields["xarray:open_kwargs"]["engine"] = "zarr"

    ds = to_xarray(zarr_asset)
    assert ds
