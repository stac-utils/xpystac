from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

if TYPE_CHECKING:
    import pystac


@dataclass
class AssetInfo:
    href: str
    properties: dict[str, JSON]


def _extract_alternate_asset(asset: pystac.Asset, alternate: str | None) -> AssetInfo:
    excludes = {"href", "alternate"}

    properties = {k: v for k, v in asset.extra_fields.items() if k not in excludes}
    if alternate is None:
        href = asset.href
        additional_properties = {}
    else:
        alternate_assets = asset.extra_fields.get("alternate")
        if alternate_assets is None:
            raise ValueError(
                "Alternate asset name given but no alternate assets found."
            )

        alternate_asset = alternate_assets.get(alternate)
        if alternate_asset is None:
            raise ValueError(
                f"alternate asset {alternate} not found."
                " Choose one of {', '.join(sorted(alternate_assets))}"
            )

        href = alternate_asset["href"]
        additional_properties = {
            k: v for k, v in alternate_asset.items() if k not in excludes
        }

    return AssetInfo(href, properties | additional_properties)


def _extract_parent_attribute(
    obj: pystac.Asset, attr: str
) -> dict[str, dict[str, JSON]] | None:
    if isinstance(obj.owner, pystac.Item):
        fields = obj.owner.properties
    else:
        fields = obj.owner.extra_fields

    return fields.get(attr)


def _resolve_refs(
    refs: list[str] | None, schemes: dict[str, dict[str, JSON]]
) -> list[dict[str, JSON]] | None:
    if refs is None:
        return None

    missing_refs = [ref for ref in refs if ref not in schemes]
    if missing_refs:
        raise ValueError("selected unknown refs: {', '.join(missing_refs)}")

    return [schemes[ref] for ref in refs]
