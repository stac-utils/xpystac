import json
import pathlib

import pystac
import pytest

from xpystac import extensions
from xpystac.extensions import JSON, AssetInfo


@pytest.fixture
def alternate_asset_item(request):
    root = pathlib.Path(__file__).parent / "data"
    path = root / "alternate_asset.json"
    data = json.loads(path.read_text())

    return pystac.Item.from_dict(data)


@pytest.mark.parametrize(
    ["alternate", "expected_href", "expected_properties"],
    (
        pytest.param(
            None,
            "s3://naip-visualization/tx/2016/100cm/rgb/30097/m_3009743_sw_14_1_20160928.tif",
            {"alternate:name": "Amazon S3", "storage:refs": ["aws-std"]},
            id="default",
        ),
        pytest.param(
            "minio",
            "s3://mybucket/tx/2016/100cm/rgb/30097/m_3009743_sw_14_1_20160928.tif",
            {"alternate:name": "MinIO", "storage:refs": ["minio"]},
            id="minio",
        ),
    ),
)
def test_extract_alternate_asset(
    alternate_asset_item: pystac.Item,
    alternate: str | None,
    expected_href: str,
    expected_properties: dict[str, JSON],
) -> None:
    asset = alternate_asset_item.assets["CO_GEOTIFF_RGB"]

    actual: AssetInfo = extensions._extract_alternate_asset(asset, alternate)

    assert actual.href == expected_href
    assert actual.properties == expected_properties
