import pytest

from xpystac.core import to_xarray


def test_to_xarray_with_cog_asset(simple_cog):
    ds = to_xarray(simple_cog)
    assert ds


def test_to_xarray_with_pystac_client_search(simple_search):
    ds = to_xarray(simple_search, assets=["blue", "green", "red"])
    assert ds


def test_to_xarray_with_drop_variables_raises(simple_search):
    with pytest.raises(KeyError, match="not implemented for pystac items"):
        to_xarray(simple_search, drop_variables=["blue"])


def test_to_xarray_with_bad_type():
    with pytest.raises(TypeError):
        to_xarray("foo")


def test_to_xarray_reference_file(simple_reference_file):
    ds = to_xarray(simple_reference_file)
    assert ds


def test_to_xarray_zarr(simple_zarr):
    ds = to_xarray(simple_zarr)
    assert ds


def test_to_xarray_zarr_with_open_kwargs_engine(complex_zarr):
    ds = to_xarray(complex_zarr)
    assert ds
