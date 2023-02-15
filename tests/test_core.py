import pystac
from xpystac.core import to_xarray


def test_asset_to_xarray(simple_item):
    asset = simple_item.assets["visual"]
    assert asset.media_type == pystac.MediaType.COG
    ds = to_xarray(asset)
    assert ds
