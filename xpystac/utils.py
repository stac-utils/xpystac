import importlib
from typing import Any


def _import_optional_dependency(name):
    try:
        module = importlib.import_module(name)
    except ImportError as e:
        raise ImportError(f"Missing optional dependency '{name}'") from e
    return module


def _is_item_search(obj: Any):
    """Whether object is an instance of class pystac_client.ItemSearch.

    Note: this function does not import pystac_client in the interest of
    speed.
    """
    return obj.__class__.__name__ == "ItemSearch"
