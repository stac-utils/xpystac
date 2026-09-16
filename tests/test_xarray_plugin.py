import numpy as np
import pystac
import xarray
from xarray.backends import ZarrBackendEntrypoint

import xpystac._icechunk as xpystac_icechunk


def test_xarray_open_dataset_can_guess_for_pystac_objects(simple_cog):
    ds = xarray.open_dataset(simple_cog)
    assert ds


def test_xarray_open_dataset_icechunk_delegates_to_zarr_backend(monkeypatch):
    item = pystac.Item(
        id="icechunk",
        geometry=None,
        bbox=None,
        datetime=None,
        properties={
            "start_datetime": "2026-01-01T00:00:00Z",
            "end_datetime": "2026-01-01T01:00:00Z",
            "storage:schemes": {
                "archive": {
                    "type": "gcs",
                    "bucket": "example-bucket",
                }
            },
        },
    )
    item.add_asset(
        "zarr",
        pystac.Asset(
            href="gs://example-bucket/weather.icechunk",
            media_type="application/vnd.zarr+icechunk",
            roles=["data"],
            extra_fields={
                "storage:refs": ["archive"],
                "zarr:consolidated": False,
                "zarr:zarr_format": 3,
            },
        ),
    )
    asset = item.assets["zarr"]

    class Session:
        store = object()

    class Repository:
        def readonly_session(self, **kwargs):
            assert kwargs == {"branch": "main"}
            return Session()

    monkeypatch.setattr(
        xpystac_icechunk.icechunk,
        "gcs_storage",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(
        xpystac_icechunk.icechunk.Repository,
        "open",
        lambda **kwargs: Repository(),
    )

    backend_kwargs = {}

    def open_dataset(self, store, **kwargs):
        assert store is Session.store
        backend_kwargs.update(kwargs)
        return xarray.Dataset({"value": ("x", np.arange(6))})

    monkeypatch.setattr(ZarrBackendEntrypoint, "open_dataset", open_dataset)

    ds = xarray.open_dataset(
        asset,
        engine="stac",
        chunks={"x": 2},
        decode_timedelta=True,
        decode_coords="all",
    )

    assert ds.value.chunks == ((2, 2, 2),)
    assert backend_kwargs == {
        "consolidated": False,
        "zarr_format": 3,
        "drop_variables": None,
        "decode_timedelta": True,
        "decode_coords": "all",
    }
