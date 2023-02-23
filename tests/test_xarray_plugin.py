import pystac
import xarray


def test_asset_xarray_open_dataset(simple_item):
    asset = simple_item.assets["visual"]
    assert asset.media_type == pystac.MediaType.COG
    ds = xarray.open_dataset(asset, engine="stac")
    assert ds
