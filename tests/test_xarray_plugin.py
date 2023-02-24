import pytest
import xarray


def test_xarray_open_dataset_can_guess_for_pystac_objects(simple_cog):
    ds = xarray.open_dataset(simple_cog)
    assert ds


def test_xarray_open_dataset_cannot_guess_for_pystac_client_objects(simple_search):
    with pytest.raises(
        ValueError, match="Consider explicitly selecting one of the installed engines"
    ):
        xarray.open_dataset(simple_search)


def test_xarray_open_dataset_with_drop_variables_raises(simple_search):
    with pytest.raises(KeyError, match="not implemented for pystac items"):
        xarray.open_dataset(simple_search, engine="stac", drop_variables=["B0"])
