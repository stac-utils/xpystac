import pystac
import pystac_client
import pytest
import vcr

# copied from https://github.com/stac-utils/pystac-client/blob/v0.6.0/tests/helpers.py#L7-L11
STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "EARTH-SEARCH": "https://earth-search.aws.element84.com/v0",
    "MLHUB": "https://api.radiant.earth/mlhub/v1",
}


@pytest.fixture(scope="module")
def simple_item() -> pystac.Item:
    path = "https://raw.githubusercontent.com/stac-utils/pystac/2.0/tests/data-files/examples/1.0.0/simple-item.json"
    return pystac.Item.from_file(path)


@pytest.fixture(scope="module")
def simple_cog(simple_item) -> pystac.Asset:
    asset = simple_item.assets["visual"]
    assert asset.media_type == pystac.MediaType.COG
    return asset


@pytest.fixture(scope="module")
def simple_search() -> pystac_client.ItemSearch:
    with vcr.use_cassette("tests/cassettes/fixtures/simple_search.yaml"):
        client = pystac_client.Client.open(STAC_URLS["EARTH-SEARCH"])
        return client.search(
            intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
            collections=["sentinel-s2-l2a-cogs"],
            datetime="2020-05-01",
        )
