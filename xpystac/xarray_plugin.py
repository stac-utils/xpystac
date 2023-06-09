from typing import List, Union

import pystac
from xarray.backends import BackendEntrypoint

from xpystac.core import to_xarray


class STACBackend(BackendEntrypoint):
    def open_dataset(
        self,
        obj,
        *,
        drop_variables: Union[str, List[str]] = None,
        **kwargs,
    ):
        return to_xarray(obj, drop_variables=drop_variables, **kwargs)

    open_dataset_parameters = ["obj", "drop_variables"]

    def guess_can_open(self, obj):
        return isinstance(obj, (pystac.Asset, pystac.Item, pystac.ItemCollection))

    description = "Open pystac objects in Xarray"

    url = "https://github.com/stac-utils/xpystac"
