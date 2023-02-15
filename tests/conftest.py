from pathlib import Path

import pytest
import pystac

here = Path(__file__).parent.resolve()


@pytest.fixture(scope="module")
def simple_item() -> pystac.Item:
    path = here / "data-files" / "simple-item.json"
    return pystac.Item.from_file(str(path))
