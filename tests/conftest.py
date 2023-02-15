import pytest
import pystac


@pytest.fixture(scope="module")
def simple_item() -> pystac.Item:
    path = "https://raw.githubusercontent.com/stac-utils/pystac/2.0/tests/data-files/examples/1.0.0/simple-item.json"
    return pystac.Item.from_file(path)
