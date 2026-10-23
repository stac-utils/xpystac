"""Live, unmocked end-to-end test for the custom-s3 storage scheme.

Reads a stac-geoparquet catalog hosted on a real non-AWS S3-compatible
endpoint (OSN/esip, anonymously readable), via rustac, and opens a virtual
Icechunk asset (built with virtualizarr against real NetCDF4 files on the
same endpoint) with xr.open_dataset(..., engine="stac"). This exercises the
custom-s3 branch in both read_icechunk() and
construct_virtual_containers_config() — previously only _s3_scheme_kwargs()
had unit-level coverage (see test_icechunk_storage.py), nothing exercised the
virtual-chunk-container branch at all.

The fixture data (7 NERACOOS wave buoy NetCDF4 files + their virtual
Icechunk stores + this catalog) was generated once with scripts/ in this
repo and lives at s3://esip/rsignell/icechunk_demo/ (OSN, publicly
anonymously readable — no credentials needed to run this test).
"""

import pystac
import xarray as xr

from tests.utils import requires_icechunk, requires_rustac

CATALOG_URL = "https://usgs.osn.mghpcc.org/esip/rsignell/icechunk_demo/catalog.parquet"


@requires_icechunk
@requires_rustac
def test_read_virtual_icechunk_item_from_geoparquet_catalog_on_osn():
    import rustac

    items = rustac.DuckdbClient().search(CATALOG_URL, limit=10)
    assert len(items) == 7

    item = pystac.Item.from_dict(items[0])
    assert item.properties["storage:schemes"]["osn-esip"]["type"] == "custom-s3"

    asset = next(iter(item.get_assets(role="latest-version").values()))
    assert "virtual" in asset.roles

    ds = xr.open_dataset(asset, engine="stac")
    assert "significant_wave_height" in ds.data_vars
    assert ds["significant_wave_height"].size > 0
    assert bool(ds["significant_wave_height"].notnull().any())
