from typing import Any, Iterable, Literal, Union

import pystac
from xarray.backends import BackendEntrypoint

from xpystac.core import to_xarray
from xpystac.utils import _is_item_search


class STACBackend(BackendEntrypoint):
    description = "Open STAC objects in Xarray"
    open_dataset_parameters = ("filename_or_obj", "drop_variables")
    url = "https://github.com/stac-utils/xpystac"

    def open_dataset(
        self,
        filename_or_obj: Any,
        drop_variables: Union[str, Iterable[str], None] = None,
        stacking_library: Union[Literal["odc.stac", "stackstac"], None] = None,
        **kwargs,
    ):
        """Given a PySTAC object return an xarray dataset

        The behavior of this method depends on the type of PySTAC object:

        * Asset: if the asset points to a kerchunk file or a zarr file,
          reads the metadata in that file to construct the coordinates of the
          dataset. If the asset points to a COG, read that.
        * Item: stacks all the assets into a dataset with 1 more dimension than
          any given asset.
        * ItemCollection (output of pystac_client.search): stacks all the
          assets in all the items into a dataset with 2 more dimensions than
          any given asset.

        Parameters
        ----------
        filename_or_obj : PySTAC object (Item, ItemCollection, Asset)
            The object from which to read data.
        stacking_library : "odc.stac", "stackstac", optional
            When stacking multiple items, this argument determines which library
            to use. Defaults to ``odc.stac`` if available and otherwise ``stackstac``.
        """
        return to_xarray(
            filename_or_obj,
            drop_variables=drop_variables,
            stacking_library=stacking_library,
            **kwargs,
        )

    def guess_can_open(self, filename_or_obj: Any):
        return isinstance(
            filename_or_obj, (pystac.Asset, pystac.Item, pystac.ItemCollection)
        ) or _is_item_search(filename_or_obj)
