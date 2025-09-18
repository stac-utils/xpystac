import warnings

import icechunk
import pystac
import xarray as xr

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)


def read_virtual_icechunk(asset: pystac.Asset) -> xr.Dataset:
    """Read a collection-level virtual icechunk asset

    The asset's parent collection must contain:
     * "storage:schemes" where at least one key matches "storage:ref" in the asset
     * another asset that matches the key in the primary asset's ["vrt:hrefs"] list
    """
    virtual_asset = asset

    # --- Get storage schemes off the parent collection
    collection = asset.owner
    storage_schemes = collection.extra_fields["storage:schemes"]

    # --- Create icechunk storage for virtual store
    virtual_storage_refs = virtual_asset.extra_fields["storage:refs"]
    if len(virtual_storage_refs) != 1:
        raise ValueError("Only supports one storage:ref per virtual asset")

    virtual_storage_scheme = storage_schemes.get(virtual_storage_refs[0])
    if not virtual_storage_scheme["type"] == "aws-s3":
        raise ValueError("Only S3 buckets are currently supported")

    virtual_bucket = virtual_storage_scheme["bucket"]
    virtual_region = virtual_storage_scheme["region"]
    virtual_anonymous = virtual_storage_scheme.get("anonymous", False)
    virtual_prefix = virtual_asset.href.split(f"{virtual_bucket}/")[1]

    storage = icechunk.s3_storage(
        bucket=virtual_bucket,
        prefix=virtual_prefix,
        region=virtual_region,
        anonymous=virtual_anonymous,
        from_env=not virtual_anonymous,
    )

    # --- Configure icechunk storage for data store
    data_buckets = virtual_asset.extra_fields["vrt:hrefs"]

    if len(data_buckets) != 1:
        raise ValueError("Only supports one vrt:href per virtual asset")

    data_asset = collection.assets[data_buckets[0]["key"]]
    data_href = data_asset.href

    data_storage_refs = data_asset.extra_fields["storage:refs"]
    if len(data_storage_refs) != 1:
        raise ValueError("Only supports one storage:ref per data asset")

    data_storage_scheme = collection.extra_fields["storage:schemes"].get(
        data_storage_refs[0]
    )
    if not data_storage_scheme["type"] == "aws-s3":
        raise ValueError("Only S3 buckets are currently supported")

    data_region = data_storage_scheme["region"]
    data_anonymous = data_storage_scheme.get("anonymous", False)

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(data_href, icechunk.s3_store(region=data_region))
    )
    if data_anonymous:
        credentials = icechunk.s3_anonymous_credentials()
    else:
        credentials = icechunk.s3_from_env_credentials()

    virtual_credentials = icechunk.containers_credentials({data_href: credentials})

    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=virtual_credentials,
    )

    # --- Open icechunk session at a particular branch/tag/snapshot
    if version := virtual_asset.extra_fields.get("version"):
        if version in repo.list_branches():
            icechunk_kwargs = {"branch": version}
        elif version in repo.list_tags():
            icechunk_kwargs = {"tag": version}
        else:
            icechunk_kwargs = {"snapshot_id": version}

    session = repo.readonly_session(**icechunk_kwargs)

    return xr.open_zarr(session.store, zarr_format=3, consolidated=False)
