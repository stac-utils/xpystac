from typing import Literal, cast

import pystac

from xpystac.typing import JSON


def _resolve_refs(
    refs: list[str], schemes: dict[str, dict[str, JSON]]
) -> list[dict[str, JSON]]:
    missing_refs = [ref for ref in refs if ref not in schemes]
    if missing_refs:
        raise ValueError("selected unknown refs: {', '.join(missing_refs)}")

    return [schemes[ref] for ref in refs]


def _extract_parent_attribute(obj: pystac.Asset, attr: str) -> JSON:
    if isinstance(obj.owner, pystac.Item):
        fields = obj.owner.properties
    else:
        fields = obj.owner.extra_fields

    return fields.get(attr)


def extract_scheme(
    asset: pystac.Asset, kind: Literal["storage", "auth"]
) -> dict[str, JSON] | None:
    refs = cast(list[str], asset.properties.get(f"{kind}:refs", []))
    if not refs:
        return None

    schemes = cast(
        dict[str, dict[str, JSON]], _extract_parent_attribute(asset, f"{kind}:schemes")
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
