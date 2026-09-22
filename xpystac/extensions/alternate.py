import pystac

from xpystac.extensions.common import AssetInfo


def extract_alternate_asset(asset: pystac.Asset, alternate: str | None) -> AssetInfo:
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
