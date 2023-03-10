# xpystac
xpystac provides the glue that allows `xarray.open_dataset` to accept pystac objects.

The goal is that as long as this library is in your env, you should never need to think about it.

## Example

Search collection of COGs example:

```python
import pystac_client
import xarray as xr


catalog = pystac_client.Client.open(
    "https://earth-search.aws.element84.com/v0"
)

search = catalog.search(
    intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
    collections=['sentinel-s2-l2a-cogs'],
    datetime="2020-04-01/2020-05-01",
)

xr.open_dataset(search, engine="stac")
```

Reference file example:

```python
import planetary_computer
import xarray as xr
import pystac_client


catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

collection = catalog.get_collection("nasa-nex-gddp-cmip6")
asset = collection.assets["ACCESS-CM2.historical"]

xr.open_dataset(asset)
```


## Install

```bash
pip install git+https://github.com/jsignell/xpystac
```

## How it works

When you call ``xarray.open_dataset(object, engine="stac")`` this library maps that open call to the correct library.
Depending on the ``type`` of ``object`` that might be [stackstac](https://github.com/gjoseph92/stackstac)
or back to ``xarray.open_dataset`` itself but with the engine and other options pulled from the pystac object.

## Prior Art

This work is inspired by https://github.com/TomAugspurger/staccontainers and the discussion in https://github.com/stac-utils/pystac/issues/846
