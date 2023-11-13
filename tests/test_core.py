import dask.array
import pytest
import xarray as xr

from xpystac.core import to_xarray


@pytest.mark.vcr
def test_to_xarray_with_cog_asset(simple_cog):
    ds = to_xarray(simple_cog)
    assert ds


@pytest.mark.vcr
def test_to_xarray_with_pystac_client_search(simple_search):
    ds = to_xarray(simple_search)
    assert ds


@pytest.mark.vcr
def test_to_xarray_returns_dask_backed_object(simple_search):
    ds = to_xarray(simple_search)
    assert isinstance(ds.blue.data, dask.array.Array)
    assert ds.blue.data.npartitions > 1


@pytest.mark.vcr
def test_to_xarray_with_pystac_client_search_passes_kwargs_through(simple_search):
    ds = to_xarray(simple_search, bands=["red", "green", "blue"], chunks={})
    assert list(ds.data_vars) == ["red", "green", "blue"]
    assert ds.blue.data.npartitions == 1


@pytest.mark.vcr
@pytest.mark.parametrize("stacking_library", ["odc.stac", "stackstac"])
def test_to_xarray_with_different_stacking_library(simple_search, stacking_library):
    ds = to_xarray(simple_search, stacking_library=stacking_library)
    assert isinstance(ds, xr.Dataset)


@pytest.mark.vcr
def test_to_xarray_with_pystac_client_search_with_patch_url(simple_signed_search):
    import planetary_computer as pc
    from rasterio.errors import RasterioIOError

    ds = to_xarray(
        simple_signed_search, assets=["B4", "B3", "B2"], stacking_library="odc.stac"
    )

    with pytest.raises(RasterioIOError, match="HTTP response code: 404"):
        ds.B01.max().compute()

    ds = to_xarray(
        simple_signed_search,
        assets=["B4", "B3", "B2"],
        stacking_library="odc.stac",
        patch_url=pc.sign,
    )
    assert ds.B01.max().compute() == 11080


@pytest.mark.vcr
def test_to_xarray_with_drop_variables_raises(simple_search):
    with pytest.raises(KeyError, match="not implemented for pystac items"):
        to_xarray(simple_search, drop_variables=["blue"])


@pytest.mark.vcr
def test_to_xarray_with_bad_type():
    with pytest.raises(TypeError):
        to_xarray("foo")


@pytest.mark.vcr
def test_to_xarray_reference_file(simple_reference_file):
    ds = to_xarray(simple_reference_file)
    assert ds

    for da in ds.data_vars.values():
        if da.ndim >= 2:
            assert hasattr(da.data, "dask")


@pytest.mark.vcr
def test_to_xarray_reference_file_needs_to_be_signed(simple_reference_file):
    import planetary_computer as pc

    ds = to_xarray(simple_reference_file)
    assert ds.lon.isnull().all(), "Coordinates should not be populated"

    ds = to_xarray(simple_reference_file, patch_url=pc.sign)
    assert not ds.lon.isnull().all(), "Coordinates should be populated"


@pytest.mark.vcr
def test_to_xarray_zarr(simple_zarr):
    ds = to_xarray(simple_zarr)
    for da in ds.data_vars.values():
        if da.ndim >= 2:
            assert hasattr(da.data, "dask"), da.name


@pytest.mark.vcr
def test_to_xarray_zarr_with_open_kwargs_engine(complex_zarr):
    ds = to_xarray(complex_zarr)
    assert ds
