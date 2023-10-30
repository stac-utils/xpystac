import json
from typing import Any, Dict

import pystac


def _stac_to_kerchunk(item: pystac.Item, kerchunk_version: int = 1) -> Dict[str, Any]:
    """
    Copied from xstac to support python < 3.10
    ref: https://github.com/stac-utils/xstac/blob/1.2.0/xstac/_kerchunk.py

    Derive Kerchunk indices from a STAC item.
    """
    refs = {}
    refs[".zgroup"] = json.dumps(item.properties["kerchunk:zgroup"])
    refs[".zattrs"] = json.dumps(item.properties["kerchunk:zattrs"])

    for attr in ["cube:dimensions", "cube:variables"]:
        cd = item.properties[attr]
        for k in cd:
            refs[f"{k}/.zarray"] = json.dumps(cd[k]["kerchunk:zarray"])
            # TODO: derive from datacube stuff, ARRAY_DIMENSIONS
            refs[f"{k}/.zattrs"] = json.dumps(cd[k]["kerchunk:zattrs"])
            for i in cd[k]["kerchunk:value"]:
                refs[f"{k}/{i}"] = cd[k]["kerchunk:value"][i]

    d = {"version": kerchunk_version, "refs": refs}
    return d
