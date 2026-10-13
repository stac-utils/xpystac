"""Build one STAC Item per buoy (mirroring tests/data/virtual-icechunk-item.json's
shape, with a "custom-s3" storage scheme instead of "aws-s3"), write them to a
local stac-geoparquet file via rustac, then upload that file to OSN/esip.

storage:schemes lives on each Item's own properties (not a Collection) since
stac-geoparquet flattens features and loses collection-level context per row.

Run with (see scripts/README.md):
    uv run --with boto3 --with rustac --with pystac --with xarray --with h5netcdf \
        --with h5py python scripts/03_build_stac_and_geoparquet.py
"""

import sys

import boto3
import rustac
import xarray as xr
from fixtures_common import (
    AWS_PROFILE,
    BUCKET,
    BUOY_IDS,
    CATALOG_KEY,
    DATA_DIR,
    ENDPOINT,
    NETCDF_DIR_HREF,
    REGION,
    SCHEME_KEY,
    icechunk_href,
    netcdf_path,
)

STORAGE_EXTENSIONS = [
    "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    "https://stac-extensions.github.io/version/v1.2.0/schema.json",
    "https://stac-extensions.github.io/zarr/v1.1.0/schema.json",
    "https://stac-extensions.github.io/virtual-assets/v1.0.0/schema.json",
]

STORAGE_SCHEME = {
    "type": "custom-s3",
    "platform": ENDPOINT,
    "bucket": BUCKET,
    "region": REGION,
    "anonymous": True,  # readers (including xpystac's CI): bucket is public-read
}


def _drop_empty_structs(obj):
    """Recursively drop dict keys whose value is `{}` (breaks parquet writing,
    confirmed earlier this same session building a similar in-situ catalog)."""
    if isinstance(obj, dict):
        return {
            k: _drop_empty_structs(v)
            for k, v in obj.items()
            if not (isinstance(v, dict) and not v)
        }
    if isinstance(obj, list):
        return [_drop_empty_structs(v) for v in obj]
    return obj


def build_item(buoy: str) -> dict:
    ds = xr.open_dataset(netcdf_path(buoy))
    lon = float(ds["longitude"].values.reshape(-1)[0])
    lat = float(ds["latitude"].values.reshape(-1)[0])
    tmin = ds["time"].values.min()
    tmax = ds["time"].values.max()

    item = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": STORAGE_EXTENSIONS,
        "id": f"neracoos-{buoy.lower()}-waves",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "bbox": [lon, lat, lon, lat],
        "properties": {
            "datetime": None,
            "start_datetime": f"{tmin}".replace(" ", "T") + "Z",
            "end_datetime": f"{tmax}".replace(" ", "T") + "Z",
            "title": f"NERACOOS {buoy} significant wave height",
            "platform": buoy,
            "instruments": ["directional wave buoy"],
            "storage:schemes": {SCHEME_KEY: STORAGE_SCHEME},
        },
        "assets": {
            "icechunk": {
                "href": icechunk_href(buoy),
                "type": "application/vnd.zarr+icechunk",
                "title": f"Virtual Icechunk store for {buoy}_waves",
                "storage:refs": [SCHEME_KEY],
                "vrt:hrefs": [{"key": "netcdf", "href": "#/assets/netcdf"}],
                "roles": ["data", "references", "virtual", "latest-version"],
            },
            "netcdf": {
                "href": NETCDF_DIR_HREF,
                "type": "application/x-netcdf",
                "title": "Source NERACOOS ERDDAP NetCDF4 files (shared directory)",
                "storage:refs": [SCHEME_KEY],
                "roles": ["data"],
            },
        },
        "links": [],
        "collection": None,
    }
    return _drop_empty_structs(item)


def main() -> int:
    items = [build_item(buoy) for buoy in BUOY_IDS]
    for item in items:
        assert item["properties"]["storage:schemes"], item["id"]

    local_parquet = DATA_DIR / "catalog.parquet"
    rustac.write_sync(str(local_parquet), items)
    print(f"wrote {local_parquet} ({local_parquet.stat().st_size:,} bytes), {len(items)} items")

    s3 = boto3.client(
        "s3", endpoint_url=ENDPOINT,
        **_profile_kwargs(AWS_PROFILE),
    )
    s3.upload_file(str(local_parquet), BUCKET, CATALOG_KEY)
    print(f"uploaded to s3://{BUCKET}/{CATALOG_KEY}")
    return 0


def _profile_kwargs(profile: str) -> dict:
    session = boto3.Session(profile_name=profile)
    creds = session.get_credentials().get_frozen_credentials()
    return {
        "aws_access_key_id": creds.access_key,
        "aws_secret_access_key": creds.secret_key,
        **({"aws_session_token": creds.token} if creds.token else {}),
    }


if __name__ == "__main__":
    sys.exit(main())
