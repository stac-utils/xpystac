import pytest
import xarray


def test_xarray_open_dataset_can_guess_for_pystac_objects(simple_cog):
    ds = xarray.open_dataset(simple_cog)
    assert ds


def test_xarray_open_dataset_can_guess_for_pystac_client_searchs(simple_search):
    ds = xarray.open_dataset(simple_search, assets=["blue", "green", "red"])
    assert ds


def test_xarray_open_dataset_with_drop_variables_raises(simple_search):
    with pytest.raises(KeyError, match="not implemented for pystac items"):
        xarray.open_dataset(simple_search, engine="stac", drop_variables=["B0"])
