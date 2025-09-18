# xpystac
xpystac provides the glue that allows `xarray.open_dataset` to accept pystac objects.

The goal is that as long as this library is in your env, you should never need to think about it.

- **Open one asset**: Reads data for an asset pointing to a COG, a zarr store, or a kerchunk reference file.
- **Open one item**: Reads data for all the assets in a particular item (commonly each COG represents a band).
 - **Open many items**: Reads all the assets in all the items for a particular item collection
iterable of items, or output of pystac_client.Client.search.

## What works

| file format | one asset (item or collection-level) | one item | many items | 
| ----------- | --------- | -------- | ---------- | 
| COG | x | x | x |
| Zarr | x | | |
| Kerchunk | x | x* | x* |
| virtual Icechunk | x | | |

\* _if stored in item alongside the datacube extension properties_


## Install

```bash
pip install xpystac
```

## Examples

### Open a single asset

Read from a COG

```python
import pystac
import xarray as xr

item = pystac.Item.from_file(
    "https://raw.githubusercontent.com/stac-utils/pystac/v1.12.2/tests/data-files/examples/1.0.0/simple-item.json"
)
asset = item.assets["visual"]

xr.open_dataset(asset)
```

Read from a virtual Icechunk store

```python
import pystac
import xarray as xr

collection = pystac.Collection.from_file("https://raw.githubusercontent.com/stac-utils/main/tests/data/virtual-icechunk-collection.json")

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

Read from a kerchunk reference file ([ref](https://planetarycomputer.microsoft.com/dataset/nasa-nex-gddp-cmip6#Example-Notebook)):

```python
collection = catalog.get_collection("nasa-nex-gddp-cmip6")
asset = collection.assets["ACCESS-CM2.historical"]

xr.open_dataset(asset, patch_url=planetary_computer.sign)
```

Read from a zarr file ([ref](https://planetarycomputer.microsoft.com/docs/quickstarts/reading-zarr-data/))

```python
collection = catalog.get_collection("daymet-daily-hi")
asset = collection.assets["zarr-abfs"]

xr.open_dataset(asset, patch_url=planetary_computer.sign)
```

Note that this zarr asset uses the xarray-assets extension to store `open_kwargs` and `storage_options` which xpystac can then pass along to `xr.open_dataset`.

### Open a single item

A single item containing many COGs:

```python
import pystac
import xarray as xr


item = pystac.Item.from_file(
    "https://earth-search.aws.element84.com/v1/collections/landsat-c2-l2/items/LC09_L2SR_081108_20250311_02_T2"
)

xr.open_dataset(item)
```
This takes advantage of a stacking library (either
[odc-stac](https://github.com/opendatacube/odc-stac) or [stackstac](https://github.com/gjoseph92/stackstac) - configurable via the `stacking_library` option)

### Open many items

Read all the data from the search results for a collection of COGs:

```python
import pystac_client
import xarray as xr


catalog = pystac_client.Client.open(
    "https://earth-search.aws.element84.com/v1",
)

search = catalog.search(
    intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
    collections=['sentinel-2-l2a'],
    datetime="2022-04-01/2022-05-01",
)

xr.open_dataset(search, engine="stac")
```

Read data from an item collection that uses the exploratory approach of storing kerchunked metadata within the datacube extension metadata:

```python
import pystac
import xarray as xr

item_collection = pystac.ItemCollection.from_file(
    "https://raw.githubusercontent.com/stac-utils/xpystac/main/tests/data/data-cube-kerchunk-item-collection.json"
)

xr.open_dataset(item_collection)
```

## How it works

When you call ``xarray.open_dataset(object, engine="stac")`` this library maps that `open` call to the correct library.
Depending on the ``type`` of ``object`` that might be a stacking library (either
[odc-stac](https://github.com/opendatacube/odc-stac) or [stackstac](https://github.com/gjoseph92/stackstac))
or back to ``xarray.open_dataset`` itself but with the engine and other options pulled from the pystac object.

## Prior Art

This work is inspired by https://github.com/TomAugspurger/staccontainers and the discussion in https://github.com/stac-utils/pystac/issues/846
