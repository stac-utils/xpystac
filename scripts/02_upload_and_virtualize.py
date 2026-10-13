"""Upload each buoy's NetCDF4 file to OSN/esip, then build+commit a virtual
Icechunk store referencing it (one store per buoy — single source file each,
so no concat/append-dim logic is needed, just the single-file build path).

Writer side uses real `osn-esip` credentials throughout (both for the S3
upload and for building/committing the Icechunk repo) — this is deliberately
asymmetric with the STAC item's `"anonymous": true` scheme, which is what
*readers* (including xpystac's CI) will use, since the esip bucket is
public-read but we still need real creds to write to it.

Run with (see scripts/README.md):
    uv run --with boto3 --with icechunk --with virtualizarr --with obstore \
        --with obspec_utils --with xarray --with h5netcdf --with h5py \
        python scripts/02_upload_and_virtualize.py
"""

import os
import sys

import boto3
import icechunk
from fixtures_common import (
    AWS_PROFILE,
    BUCKET,
    BUOY_IDS,
    ENDPOINT,
    NETCDF_DIR_HREF,
    REGION,
    icechunk_prefix,
    netcdf_href,
    netcdf_path,
)
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import S3Store
from virtualizarr.parsers import HDFParser
from virtualizarr.xarray import open_virtual_dataset


def _export_profile_credentials(profile: str) -> None:
    """Export AWS_* env vars from a named profile, so obstore/icechunk's
    from_env-style config (and obstore's own environment-variable checks)
    all pick up the same real write credentials consistently."""
    creds = boto3.Session(profile_name=profile).get_credentials()
    if creds is None:
        raise RuntimeError(f"No credentials found for AWS profile {profile!r}")
    frozen = creds.get_frozen_credentials()
    os.environ["AWS_ACCESS_KEY_ID"] = frozen.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
    if frozen.token:
        os.environ["AWS_SESSION_TOKEN"] = frozen.token


def upload_netcdf(s3, buoy: str) -> None:
    key = f"rsignell/icechunk_demo/netcdf/{buoy}.nc"
    local_path = netcdf_path(buoy)
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        if head["ContentLength"] == local_path.stat().st_size:
            print(f"{buoy}: s3://{BUCKET}/{key} already uploaded (size matches), skipping")
            return
    except s3.exceptions.ClientError:
        pass
    print(f"{buoy}: uploading to s3://{BUCKET}/{key} ...")
    s3.upload_file(str(local_path), BUCKET, key)


def already_virtualized(storage, config, virtual_credentials) -> bool:
    try:
        repo = icechunk.Repository.open(
            storage, config=config, authorize_virtual_chunk_access=virtual_credentials
        )
    except Exception:
        return False
    return next(repo.ancestry(branch="main"), None) is not None


def virtualize_buoy(buoy: str) -> None:
    source_href = netcdf_href(buoy)

    build_registry = ObjectStoreRegistry(
        {
            f"s3://{BUCKET}": S3Store(
                bucket=BUCKET,
                endpoint=ENDPOINT,
                region=REGION,
                virtual_hosted_style_request=False,
            )
        }
    )
    parser = HDFParser()
    vds = open_virtual_dataset(
        source_href,
        registry=build_registry,
        parser=parser,
        loadable_variables=["time", "latitude", "longitude"],
        decode_times=True,
    )

    storage = icechunk.s3_storage(
        bucket=BUCKET,
        prefix=icechunk_prefix(buoy),
        endpoint_url=ENDPOINT,
        region=REGION,
        force_path_style=True,
        from_env=True,
    )
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            NETCDF_DIR_HREF,
            icechunk.s3_store(
                s3_compatible=True,
                endpoint_url=ENDPOINT,
                region=REGION,
                force_path_style=True,
            ),
        )
    )
    virtual_credentials = icechunk.containers_credentials(
        {NETCDF_DIR_HREF: icechunk.s3_from_env_credentials()}
    )

    if already_virtualized(storage, config, virtual_credentials):
        print(f"{buoy}: icechunk store already has a commit on main, skipping virtualize")
        return

    try:
        repo = icechunk.Repository.open(
            storage, config=config, authorize_virtual_chunk_access=virtual_credentials
        )
    except Exception:
        repo = icechunk.Repository.create(
            storage, config=config, authorize_virtual_chunk_access=virtual_credentials
        )

    session = repo.writable_session("main")
    vds.vz.to_icechunk(session.store)
    snapshot_id = session.commit(f"Initial virtual store for NERACOOS {buoy}_waves")
    print(f"{buoy}: committed snapshot {snapshot_id} to {storage}")


def main() -> int:
    _export_profile_credentials(AWS_PROFILE)
    s3 = boto3.client("s3", endpoint_url=ENDPOINT)

    for buoy in BUOY_IDS:
        upload_netcdf(s3, buoy)

    for buoy in BUOY_IDS:
        virtualize_buoy(buoy)

    return 0


if __name__ == "__main__":
    sys.exit(main())
