from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from wo_runtime.workspace import Workspace, load_workspace
from wo_runtime.core.validation import validate_resource_id


def current_workspace(workspace: Workspace | None = None) -> Workspace:
    return workspace or load_workspace()


def workspace_path(workspace: Workspace | None, key: str, default: str) -> Path:
    ws = current_workspace(workspace)
    ref = Path(str(ws.config.get(key) or default))
    return ref if ref.is_absolute() else ws.root / ref


def _source(context: dict[str, Any]) -> dict[str, Any]:
    value = context.get("source")
    return value if isinstance(value, dict) else {}


def domain_id_from_context(
    context: dict[str, Any] | None = None,
    *,
    workspace: Workspace | None = None,
    env_name: str = "DOMAIN_ID",
) -> str:
    ws = current_workspace(workspace)
    context = context if isinstance(context, dict) else {}
    source = _source(context)
    value = (
        context.get("domainId")
        or context.get("domain_id")
        or source.get("domainId")
        or source.get("domain_id")
        or os.environ.get(env_name)
        or os.environ.get("PRODUCT_DOMAIN_ID")
        or ws.config.get("defaultDomainId")
    )
    return str(value or "default")


def domains_dir(workspace: Workspace | None = None) -> Path:
    return workspace_path(workspace, "domainsDir", "domains")


def domain_dir(domain_id: str, workspace: Workspace | None = None) -> Path:
    return domains_dir(workspace) / validate_resource_id(domain_id, "domainId")


def domain_profile_path(domain_id: str, workspace: Workspace | None = None) -> Path:
    safe_id = validate_resource_id(domain_id, "domainId")
    packaged = domain_dir(safe_id, workspace) / "domain.json"
    if packaged.exists():
        return packaged
    return domains_dir(workspace) / f"{safe_id}.json"


def load_domain_profile(
    domain_id: str | None = None,
    context: dict[str, Any] | None = None,
    *,
    workspace: Workspace | None = None,
    must_exist: bool = False,
) -> dict[str, Any]:
    ws = current_workspace(workspace)
    resolved = domain_id or domain_id_from_context(context, workspace=ws)
    path = domain_profile_path(str(resolved), ws)
    try:
        profile = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        if must_exist:
            raise
        return {"id": str(resolved), "resources": {}}
    except Exception:
        if must_exist:
            raise
        return {"id": str(resolved), "resources": {}}
    if isinstance(profile, dict):
        profile.setdefault("id", str(resolved))
        profile.setdefault("resources", {})
        return profile
    if must_exist:
        raise ValueError(f"domain profile must be a JSON object: {path}")
    return {"id": str(resolved), "resources": {}}


def resource_ref(domain_id: str, key: str, *, workspace: Workspace | None = None) -> str:
    profile = load_domain_profile(domain_id, workspace=workspace)
    resources = profile.get("resources") if isinstance(profile.get("resources"), dict) else {}
    return str(resources.get(key) or "")


def _resolver_policy(domain_id: str, workspace: Workspace | None = None) -> dict[str, bool]:
    profile = load_domain_profile(domain_id, workspace=workspace)
    policy = profile.get("resolverPolicy") if isinstance(profile.get("resolverPolicy"), dict) else {}
    return {
        "allow_external": bool(policy.get("allowExternalContracts", False) or policy.get("allowExternalResources", False)),
        "allow_root_fallback": bool(policy.get("allowRootContractFallback", False)),
    }


def domain_resource_path(
    domain_id: str,
    key: str,
    *,
    workspace: Workspace | None = None,
    legacy: str | Path | None = None,
    must_exist: bool = False,
) -> Path:
    ws = current_workspace(workspace)
    ref = resource_ref(domain_id, key, workspace=ws)
    base = domain_dir(domain_id, ws)
    candidates: list[Path] = []
    if ref:
        raw = Path(ref)
        if raw.is_absolute():
            if not _resolver_policy(domain_id, ws)["allow_external"]:
                raise PermissionError(f"external resource is disabled for domain {domain_id}: {raw}")
            candidates.append(raw)
        else:
            candidates.append(base / raw)
            candidates.append(ws.root / raw)
    if legacy is not None:
        raw_legacy = Path(legacy)
        candidates.append(raw_legacy if raw_legacy.is_absolute() else ws.root / raw_legacy)
    for candidate in candidates:
        if candidate.exists() or not must_exist:
            return candidate
    if candidates:
        return candidates[0]
    return ws.root / str(legacy or "")


def load_domain_resource_json(
    context: dict[str, Any] | str | None,
    key: str,
    *,
    workspace: Workspace | None = None,
    legacy: str | Path | None = None,
) -> dict[str, Any]:
    ws = current_workspace(workspace)
    domain_id = context if isinstance(context, str) else domain_id_from_context(context, workspace=ws)
    path = domain_resource_path(str(domain_id), key, workspace=ws, legacy=legacy, must_exist=True)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}
