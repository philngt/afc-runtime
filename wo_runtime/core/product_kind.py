from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from wo_runtime.core.domain import current_workspace, domain_id_from_context, domain_resource_path, load_domain_profile
from wo_runtime.workspace import Workspace


def _normalize(value: Any) -> str:
    return str(value or "").casefold().replace("_", "-").strip()


def _text_blob(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        if isinstance(value, dict):
            parts.append(_text_blob(*value.values()))
        elif isinstance(value, list):
            parts.append(_text_blob(*value))
        elif value is not None:
            parts.append(str(value))
    return _normalize(" ".join(parts))


def flatten_product_context(context: dict[str, Any] | None = None) -> dict[str, Any]:
    context = context or {}
    if not isinstance(context, dict):
        return {}
    product = context.get("product") if isinstance(context.get("product"), dict) else {}
    source = context.get("source") if isinstance(context.get("source"), dict) else {}
    product_source = product.get("source") if isinstance(product.get("source"), dict) else {}
    merged: dict[str, Any] = {}
    merged.update(context)
    merged.update(source)
    merged.update(product)
    merged.update(product_source)
    merged["_raw"] = context
    return merged


def load_context_file(path: str | Path | None, *, root: Path | None = None) -> dict[str, Any]:
    if not path:
        return {}
    resolved = Path(path)
    if not resolved.is_absolute() and root is not None:
        resolved = root / resolved
    try:
        data = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def product_kind_dir(context: dict[str, Any] | None = None, *, workspace: Workspace | None = None) -> Path:
    ws = current_workspace(workspace)
    domain_id = domain_id_from_context(context, workspace=ws)
    return domain_resource_path(domain_id, "productKindProfiles", workspace=ws, must_exist=True)


def load_product_kind_profiles(
    context: dict[str, Any] | None = None,
    *,
    workspace: Workspace | None = None,
) -> list[dict[str, Any]]:
    base = product_kind_dir(context, workspace=workspace)
    if not base.exists() or not base.is_dir():
        return []
    profiles: list[dict[str, Any]] = []
    for path in sorted(base.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict) and data.get("id"):
            profiles.append(data)
    return profiles


def _any_match(needles: Any, haystack: str) -> bool:
    if not isinstance(needles, list):
        return False
    return any(_normalize(item) and _normalize(item) in haystack for item in needles)


def _all_match(needles: Any, haystack: str) -> bool:
    if not isinstance(needles, list) or not needles:
        return False
    return all(_normalize(item) and _normalize(item) in haystack for item in needles)


def _profile_matches(profile: dict[str, Any], context: dict[str, Any]) -> bool:
    match = profile.get("match") if isinstance(profile.get("match"), dict) else {}
    explicit = _normalize(context.get("productKind") or context.get("kind"))
    product_type = _normalize(context.get("productType"))
    product_skill = _normalize(context.get("productSkill"))
    formats = _text_blob(context.get("format"), context.get("formats"))
    blob = _text_blob(
        context.get("title"),
        context.get("productTitle"),
        context.get("buyer"),
        context.get("notes"),
        context.get("source"),
        context.get("sourceItems"),
        context.get("sourceProducts"),
        context.get("_raw"),
    )
    if explicit and (explicit == _normalize(profile.get("id")) or _any_match(match.get("explicitAny"), explicit)):
        return True
    if _any_match(match.get("productTypeAny"), product_type):
        return True
    if _any_match(match.get("productSkillAny"), product_skill):
        return True
    if _any_match(match.get("formatAny"), formats):
        return True
    if _all_match(match.get("textAll"), blob) and (
        not match.get("textAny") or _any_match(match.get("textAny"), blob)
    ):
        return True
    return _any_match(match.get("textAny"), blob)


def _default_kind_id(
    context: dict[str, Any],
    profiles: list[dict[str, Any]],
    workspace: Workspace,
    explicit_default: str | None,
) -> str:
    if explicit_default:
        return explicit_default
    domain_id = domain_id_from_context(context, workspace=workspace)
    domain = load_domain_profile(domain_id, workspace=workspace)
    value = domain.get("defaultProductKindId") or workspace.config.get("defaultProductKindId")
    if value:
        return str(value)
    if profiles:
        return str(profiles[0].get("id") or "default")
    return "default"


def resolve_product_kind_profile(
    context: dict[str, Any] | None = None,
    *,
    workspace: Workspace | None = None,
    default_id: str | None = None,
) -> dict[str, Any]:
    ws = current_workspace(workspace)
    flattened = flatten_product_context(context)
    profiles = load_product_kind_profiles(flattened, workspace=ws)
    by_id = {_normalize(profile.get("id")): profile for profile in profiles}
    explicit = _normalize(flattened.get("productKind") or flattened.get("kind"))
    if explicit in by_id:
        return dict(by_id[explicit])
    for profile in profiles:
        if _profile_matches(profile, flattened):
            return dict(profile)
    fallback_id = _default_kind_id(flattened, profiles, ws, default_id)
    fallback = by_id.get(_normalize(fallback_id))
    if fallback:
        return dict(fallback)
    return {"id": fallback_id, "match": {}, "links": {}}


def infer_product_kind(context: dict[str, Any] | None = None, *, workspace: Workspace | None = None) -> str:
    return str(resolve_product_kind_profile(context, workspace=workspace).get("id") or "default")


def product_kind_link(
    context: dict[str, Any] | None,
    link_name: str,
    default: Any = None,
    *,
    workspace: Workspace | None = None,
) -> Any:
    profile = resolve_product_kind_profile(context, workspace=workspace)
    links = profile.get("links") if isinstance(profile.get("links"), dict) else {}
    return links.get(link_name, default)

