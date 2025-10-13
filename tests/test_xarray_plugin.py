import xarray


def test_xarray_open_dataset_can_guess_for_pystac_objects(simple_cog):
    ds = xarray.open_dataset(simple_cog)
    assert ds
