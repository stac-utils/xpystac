from typing import Any, Iterable, Union

import pystac
from xarray.backends import BackendEntrypoint

from xpystac.core import to_xarray


class STACBackend(BackendEntrypoint):
    description = "Open pystac objects in Xarray"
    open_dataset_parameters = ("filename_or_obj", "drop_variables")
    url = "https://github.com/stac-utils/xpystac"

    def open_dataset(
        self,
        filename_or_obj: Any,
        drop_variables: Union[str, Iterable[str], None] = None,
        **kwargs,
    ):
        return to_xarray(filename_or_obj, drop_variables=drop_variables, **kwargs)

    def guess_can_open(self, filename_or_obj: Any):
        return isinstance(
            filename_or_obj, (pystac.Asset, pystac.Item, pystac.ItemCollection)
        )
