from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

if TYPE_CHECKING:
    from typing import Literal

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


def _extract_parent_attribute(obj: pystac.Asset, attr: str) -> JSON:
    if isinstance(obj.owner, pystac.Item):
        fields = obj.owner.properties
    else:
        fields = obj.owner.extra_fields

    return fields.get(attr)


def _resolve_refs(
    refs: list[str], schemes: dict[str, dict[str, JSON]]
) -> list[dict[str, JSON]]:
    missing_refs = [ref for ref in refs if ref not in schemes]
    if missing_refs:
        raise ValueError("selected unknown refs: {', '.join(missing_refs)}")

    return [schemes[ref] for ref in refs]


def extract_scheme(
    info: AssetInfo, kind: Literal["storage", "auth"]
) -> dict[str, JSON] | None:
    refs = cast(list[str], info.properties.get(f"{kind}:refs", []))
    if not refs:
        return None

    schemes = cast(
        dict[str, dict[str, JSON]], _extract_parent_attribute(info, f"{kind}:schemes")
    )
    if schemes is None:
        raise ValueError(
            f"{kind}:refs found but no {kind}:schemes on the parent object"
        )

    if len(refs) != 1:
        raise NotImplementedError(
            f"Only one {kind}:ref per asset is currently supported"
        )
    [scheme] = _resolve_refs(refs, schemes)

    return scheme
