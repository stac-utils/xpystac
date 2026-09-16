import warnings

import icechunk
import pystac
import xarray as xr
from xarray.backends import ZarrBackendEntrypoint

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)


def construct_virtual_containers_config(
    owner: pystac.Collection | pystac.Item, asset: pystac.Asset
):
    # --- Configure icechunk storage for data store
    data_buckets = asset.extra_fields["vrt:hrefs"]

    if len(data_buckets) != 1:
        raise ValueError("Only supports one vrt:href per asset")

    data_asset = owner.assets[data_buckets[0]["key"]]
    data_href = data_asset.href

    data_storage_refs = data_asset.extra_fields["storage:refs"]
    if len(data_storage_refs) != 1:
        raise ValueError("Only supports one storage:ref per data asset")

    if isinstance(owner, pystac.Item):
        fields = owner.properties
    else:
        fields = owner.extra_fields
    data_storage_scheme = fields["storage:schemes"].get(data_storage_refs[0])
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
    return config, virtual_credentials


def read_icechunk(asset: pystac.Asset, **kwargs) -> xr.Dataset:
    """Read a icechunk asset

    The asset's parent must contain:
     * "storage:schemes" where at least one key matches "storage:ref" in the asset

    For virtual assets the parent must contain:
     * another asset that matches the key in the primary asset's ["vrt:hrefs"] list
    """
    # --- Get storage schemes off the parent
    owner = asset.owner
    if isinstance(owner, pystac.Item):
        fields = owner.properties
    else:
        fields = owner.extra_fields
    storage_schemes = fields["storage:schemes"]

    # --- Create icechunk storage from asset fields
    storage_refs = asset.extra_fields["storage:refs"]
    if len(storage_refs) != 1:
        raise ValueError("Only supports one storage:ref per asset")

    storage_scheme = storage_schemes.get(storage_refs[0])
    if storage_scheme["type"] not in ("aws-s3", "gcs"):
        raise ValueError("Only S3 and GCS buckets are currently supported")

    bucket = storage_scheme["bucket"]
    anonymous = storage_scheme.get("anonymous", False)
    prefix = asset.href.split(f"{bucket}/", 1)[1]

    if storage_scheme["type"] == "aws-s3":
        region = storage_scheme["region"]
        storage = icechunk.s3_storage(
            bucket=bucket,
            prefix=prefix,
            region=region,
            anonymous=anonymous,
            from_env=not anonymous,
        )
    elif storage_scheme["type"] == "gcs":
        storage = icechunk.gcs_storage(
            bucket=bucket,
            prefix=prefix,
            anonymous=anonymous,
            from_env=not anonymous,
        )

    if "virtual" in asset.roles:
        config, virtual_credentials = construct_virtual_containers_config(owner, asset)
        repo_kwargs = dict(
            config=config, authorize_virtual_chunk_access=virtual_credentials
        )
    else:
        repo_kwargs = dict(config=icechunk.RepositoryConfig.default())

    repo = icechunk.Repository.open(storage=storage, **repo_kwargs)

    # --- Open icechunk session at a particular branch/tag/snapshot
    if branch := asset.extra_fields.get("icechunk:branch"):
        session_kwargs = {"branch": branch}
    elif tag := asset.extra_fields.get("icechunk:tag"):
        session_kwargs = {"tag": tag}
    elif snapshot_id := asset.extra_fields.get("icechunk:snapshot_id"):
        session_kwargs = {"snapshot_id": snapshot_id}
    elif version := asset.extra_fields.get("version"):
        if version in repo.list_branches():
            session_kwargs = {"branch": version}
        elif version in repo.list_tags():
            session_kwargs = {"tag": version}
        else:
            session_kwargs = {"snapshot_id": version}
    else:
        session_kwargs = {"branch": "main"}

    session = repo.readonly_session(**session_kwargs)

    open_kwargs = asset.extra_fields.get("xarray:open_kwargs", {})
    zarr_kwargs = {
        "consolidated": asset.extra_fields.get("zarr:consolidated", False),
        "zarr_format": asset.extra_fields.get("zarr:zarr_format", 3),
    }
    return ZarrBackendEntrypoint().open_dataset(
        session.store,
        **{**zarr_kwargs, **open_kwargs, **kwargs},
    )
