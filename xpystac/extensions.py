from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

if TYPE_CHECKING:
    import pystac


@dataclass
class AssetInfo:
    href: str
    additional_data: dict[str, JSON]
    storage_refs: list[str] | None
    auth_refs: list[str] | None

    def resolve_storage(self, schemes: dict[str, JSON]) -> dict[str, JSON] | None:
        refs = self.storage_refs
        if refs is None:
            return None

        return {ref: schemes.get(ref) for ref in refs}

    def resolve_auth(self, schemes: dict[str, JSON]) -> dict[str, JSON] | None:
        refs = self.auth_refs
        if refs is None:
            return None

        return {ref: schemes.get(ref) for ref in refs}


def _extract_alternate_asset(asset: pystac.Asset, alternate: str | None) -> AssetInfo:
    excludes = {"href", "storage:refs", "auth:refs", "alternate:name"}

    asset_name = asset.extra_fields.get("alternate:name")
    if alternate is None or alternate == asset_name:
        href = asset.href
        storage_refs = asset.extra_fields.get("storage:refs")
        auth_refs = asset.extra_fields.get("auth:refs")
        additional_data = {
            k: v for k, v in asset.extra_fields.items() if k not in excludes
        }
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
        storage_refs = alternate_asset.get("storage:refs")
        auth_refs = alternate_asset.get("auth:refs")
        additional_data = {
            k: v
            for k, v in (asset.extra_fields | alternate_asset).items()
            if k not in excludes
        }

    return AssetInfo(href, additional_data, storage_refs, auth_refs)
