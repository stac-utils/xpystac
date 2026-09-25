# xpystac
xpystac provides the glue that allows `xarray.open_dataset` and `xarray.open_datatree` to accept pystac assets.

The goal is that as long as this library is in your env, you should never need to think about it.
It will read data for an asset pointing to a COG, a zarr store, a virtual icechunk store, or a kerchunk reference file.

This library does not try to replace the need for the stacking and mosaicing that odc-stac and stackstac provide.
When loading a pystac item where each asset contains a COG representing a particular band you are better off using 
those libraries directly.

## Install

```bash
pip install xpystac
```

## Examples

### Read from a COG

```python
import pystac
import xarray as xr

item = pystac.Item.from_file(
    "https://raw.githubusercontent.com/stac-utils/pystac/v1.12.2/tests/data-files/examples/1.0.0/simple-item.json"
)
asset = item.assets["visual"]

xr.open_dataset(asset)
```

### Read from a virtual Icechunk store

```python
import pystac
import xarray as xr

collection = pystac.Collection.from_file(
    "https://raw.githubusercontent.com/stac-utils/xpystac/refs/heads/main/tests/data/virtual-icechunk-collection.json"
)

# Get the latest version of the collection-level asset
assets = collection.get_assets(role="latest-version")
asset = next(iter(assets.values()))

xr.open_dataset(asset)
```

Here are a few examples from the [Planetary Computer Docs](https://planetarycomputer.microsoft.com/docs/overview/about) which has some good examples of collection-level assets used to catalog zarr stores and
kerchunk reference files.

```python
import planetary_computer
import pystac_client
import xarray as xr


catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)
```

### Read from a kerchunk reference file ([ref](https://planetarycomputer.microsoft.com/dataset/nasa-nex-gddp-cmip6#Example-Notebook)):

```python
collection = catalog.get_collection("nasa-nex-gddp-cmip6")
asset = collection.assets["ACCESS-CM2.historical"]

xr.open_dataset(asset, patch_url=planetary_computer.sign)
```

### Read from a zarr file ([ref](https://planetarycomputer.microsoft.com/docs/quickstarts/reading-zarr-data/))

```python
collection = catalog.get_collection("daymet-daily-hi")
asset = collection.assets["zarr-abfs"]

xr.open_dataset(asset, patch_url=planetary_computer.sign)
```

Note that this zarr asset uses the xarray-assets extension to store `open_kwargs` and `storage_options` which xpystac can then pass along to `xr.open_dataset`.

### Open as a datatree

```python
import xarray as xr
import pystac_client


client = pystac_client.Client.open("https://stac-api.grid4earth.eu")
items = client.search(collections=["sentinel-2-l2a"]).item_collection()
item = items.items[0]
asset = item.assets["product"]

tree = xr.open_datatree(asset, engine="stac")
```

## How it works

When you call ``xarray.open_dataset(object, engine="stac")`` this library maps that `open` call to the correct library.
Depending on the ``type`` of ``object`` that might be
back to ``xarray.open_dataset`` itself but with the engine and other options pulled from the pystac object.

## Prior Art

This work is inspired by https://github.com/TomAugspurger/staccontainers and the discussion in https://github.com/stac-utils/pystac/issues/846
