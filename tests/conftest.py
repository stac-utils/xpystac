import datetime

import numpy as np
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
def virtual_icechunk_collection() -> pystac.Collection:
    path = "tests/data/virtual-icechunk-collection.json"
    return pystac.Collection.from_file(path)


@pytest.fixture(scope="function")
def virtual_icechunk_item() -> pystac.Item:
    path = "tests/data/virtual-icechunk-item.json"
    return pystac.Item.from_file(path)


@pytest.fixture(scope="module")
def simple_search() -> pystac_client.ItemSearch:
    client = pystac_client.Client.open(STAC_URLS["EARTH-SEARCH"])
    return client.search(
        intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
        collections=["sentinel-2-l2a"],
        datetime="2020-05-01",
    )


@pytest.fixture(scope="function")
def local_virtual_icechunk_item(tmp_path) -> pystac.Item:
    icechunk = pytest.importorskip("icechunk")
    import zarr

    # The virtual chunk is a raw file holding [1.0, 2.0, 3.0] as float64 (24 bytes)
    (tmp_path / "foo.bin").write_bytes(np.array([1.0, 2.0, 3.0]).tobytes())
    data_href = tmp_path.as_uri() + "/"

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            data_href, icechunk.local_filesystem_store(str(tmp_path))
        )
    )
    storage = icechunk.local_filesystem_storage(str(tmp_path / "repo"))
    session = icechunk.Repository.create(storage, config).writable_session("main")
    zarr.create_array(
        session.store,
        name="foo",
        shape=(3,),
        dtype="f8",
        compressors=None,
        dimension_names=["x"],
    )
    session.store.set_virtual_ref("foo/c/0", data_href + "foo.bin", offset=0, length=24)
    session.commit("add virtual foo")

    item = pystac.Item(
        id="local-virtual-icechunk",
        geometry=None,
        bbox=None,
        datetime=datetime.datetime(2020, 1, 1),
        properties={"storage:schemes": {"local": {"type": "local"}}},
    )
    item.add_asset(
        "repo",
        pystac.Asset(
            href=(tmp_path / "repo").as_uri(),
            media_type="application/vnd.zarr+icechunk",
            roles=["data", "virtual"],
            extra_fields={"storage:refs": ["local"], "vrt:hrefs": [{"key": "data"}]},
        ),
    )
    item.add_asset(
        "data",
        pystac.Asset(href=data_href, extra_fields={"storage:refs": ["local"]}),
    )
    return item
