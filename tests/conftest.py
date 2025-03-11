import pystac
import pystac_client
import pytest

from tests.utils import STAC_URLS


@pytest.fixture(scope="module")
def simple_item() -> pystac.Item:
    path = "https://raw.githubusercontent.com/stac-utils/pystac/v1.7.0/tests/data-files/examples/1.0.0/simple-item.json"
    return pystac.Item.from_file(path)


@pytest.fixture(scope="module")
def simple_cog(simple_item) -> pystac.Asset:
    asset = simple_item.assets["visual"]
    assert asset.media_type == pystac.MediaType.COG
    return asset


@pytest.fixture(scope="module")
def data_cube_kerchunk() -> pystac.ItemCollection:
    path = "tests/data/data-cube-kerchunk-item-collection.json"
    return pystac.ItemCollection.from_file(path)


@pytest.fixture(scope="module")
def simple_search() -> pystac_client.ItemSearch:
    client = pystac_client.Client.open(STAC_URLS["EARTH-SEARCH"])
    return client.search(
        intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
        collections=["sentinel-2-l2a"],
        datetime="2020-05-01",
    )
