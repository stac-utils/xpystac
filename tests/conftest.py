from pathlib import Path

import pystac
import pytest


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
    path = Path(__file__).parent / "data" / "data-cube-kerchunk-item-collection.json"
    return pystac.ItemCollection.from_file(path)


@pytest.fixture(scope="module")
def virtual_icechunk_collection() -> pystac.Collection:
    path = Path(__file__).parent / "data" / "virtual-icechunk-collection.json"
    return pystac.Collection.from_file(path)


@pytest.fixture(scope="module")
def virtual_icechunk_item() -> pystac.Item:
    path = Path(__file__).parent / "data" / "virtual-icechunk-item.json"
    return pystac.Item.from_file(path)
