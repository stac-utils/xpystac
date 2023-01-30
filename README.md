# xpystac
For extending `xarray.open_dataset` to accept pystac objects

## Example

```python
from pystac_client import Client
import xarray as xr


client = Client.open("https://earth-search.aws.element84.com/v0")

search = client.search(
    intersects=dict(type="Point", coordinates=[-105.78, 35.79]),
    collections=['sentinel-s2-l2a-cogs'],
    datetime="2020-04-01/2020-05-01",
)

xr.open_dataset(search, engine="stac")
```

## Install

```bash
pip install git+https://github.com/jsignell/xpystac
```

## Prior Art

This work is inspired by https://github.com/TomAugspurger/staccontainers
