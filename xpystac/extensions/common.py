from dataclasses import dataclass

from xpystac.typing import JSON


@dataclass
class AssetInfo:
    href: str
    properties: dict[str, JSON]
