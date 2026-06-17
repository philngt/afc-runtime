from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from wo_runtime.core.domain import domain_resource_path
from wo_runtime.workspace import Workspace


PACK_VERSION = "context_pack.v1"
DEFAULT_PACK_DIR = "context/packs"


def utc_ts() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def data_root(workspace: Workspace) -> Path:
    return workspace.data_root if workspace.data_root.is_absolute() else workspace.root / workspace.data_root


def context_pack_config(workspace: Workspace) -> dict[str, Any]:
    cfg = workspace.config.get("contextPacks")
    return cfg if isinstance(cfg, dict) else {}


def context_packs_enabled(workspace: Workspace) -> bool:
    return bool(context_pack_config(workspace).get("enabled", False))


def inline_input_max_bytes(workspace: Workspace) -> int:
    cfg = context_pack_config(workspace)
    try:
        return int(cfg.get("inlineInputMaxBytes", 16000))
    except Exception:
        return 16000


def pack_root(workspace: Workspace) -> Path:
    cfg = context_pack_config(workspace)
    raw = Path(str(cfg.get("packDir") or DEFAULT_PACK_DIR))
    return raw if raw.is_absolute() else data_root(workspace) / raw


def rel(workspace: Workspace, path: Path) -> str:
    try:
        return path.resolve().relative_to(workspace.root.resolve()).as_posix()
    except Exception:
        try:
            return path.resolve().relative_to(data_root(workspace).resolve()).as_posix()
        except Exception:
            return str(path)


def resolve_ref(workspace: Workspace, ref: str | Path) -> Path:
    raw = Path(ref)
    if raw.is_absolute():
        return raw
    root_path = workspace.root / raw
    if root_path.exists():
        return root_path
    return data_root(workspace) / raw


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def write_text_atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(value, encoding="utf-8")
    tmp.replace(path)


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    write_text_atomic(path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_json(value: Any) -> str:
    return sha256_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def safe_segment(value: Any, default: str = "none") -> str:
    text = str(value or default).strip().casefold()
    text = re.sub(r"[^a-z0-9_.-]+", "-", text).strip("-._")
    return text[:80] or default


def dedupe_refs(refs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for ref in refs:
        value = str(ref or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def source_entries(workspace: Workspace, refs: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    entries: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for ref_value in dedupe_refs(refs):
        path = resolve_ref(workspace, ref_value)
        exists = path.exists() and path.is_file()
        entry: dict[str, Any] = {
            "ref": ref_value,
            "path": rel(workspace, path),
            "exists": exists,
        }
        if exists:
            try:
                entry["sha256"] = sha256_file(path)
                entry["bytes"] = path.stat().st_size
            except Exception as exc:
                entry["exists"] = False
                entry["warning"] = f"{type(exc).__name__}: {exc}"
                warnings.append({"code": "context_pack_source_unreadable", "ref": ref_value, "error": entry["warning"]})
        else:
            entry["sha256"] = ""
            entry["bytes"] = 0
            warnings.append({"code": "context_pack_source_missing", "ref": ref_value, "path": entry["path"]})
        entries.append(entry)
    return entries, warnings


def section_name(index: int, ref_value: str, source_hash: str) -> str:
    stem = safe_segment(Path(ref_value).name or ref_value, "section")
    return f"{index:03d}-{stem}-{source_hash[:10]}.md"


def build_compiled_pack(
    workspace: Workspace,
    pack_dir: Path,
    entries: list[dict[str, Any]],
    scope: dict[str, Any],
    source_hash: str,
) -> tuple[str, list[dict[str, Any]]]:
    sections_dir = pack_dir / "sections"
    compiled_parts = [
        f"[CONTEXT-PACK: {scope.get('packKey', '')}]",
        f"version: {PACK_VERSION}",
        f"kind: {scope.get('kind', '')}",
        f"runtime: {scope.get('runtime', '')}",
        f"role: {scope.get('role', '')}",
        f"domain: {scope.get('domainId', '')}",
        f"pipeline: {scope.get('pipelineId', '')}",
        f"productKind: {scope.get('productKindId', '')}",
        f"sourceHash: {source_hash}",
        "[/CONTEXT-PACK]",
    ]
    enriched: list[dict[str, Any]] = []
    for index, entry in enumerate(entries, start=1):
        updated = dict(entry)
        if not entry.get("exists"):
            enriched.append(updated)
            continue
        path = resolve_ref(workspace, str(entry["ref"]))
        body = path.read_text(encoding="utf-8", errors="replace").strip()
        name = section_name(index, str(entry["ref"]), str(entry.get("sha256") or source_hash))
        section_path = sections_dir / name
        section_body = f"[CONTEXT: {entry['ref']}]\n{body}\n[/CONTEXT: {entry['ref']}]\n"
        write_text_atomic(section_path, section_body)
        updated["sectionRef"] = rel(workspace, section_path)
        enriched.append(updated)
        compiled_parts.append(section_body.strip())
    return "\n\n".join(compiled_parts).rstrip() + "\n", enriched


def pack_ref(workspace: Workspace, manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_type": "context_pack_ref",
        "version": manifest.get("version") or PACK_VERSION,
        "kind": manifest.get("kind") or "",
        "packKey": manifest.get("packKey") or "",
        "ref": manifest.get("manifestRef") or "",
        "compiledRef": manifest.get("compiledRef") or "",
        "sourceHash": manifest.get("sourceHash") or "",
        "status": manifest.get("status") or "unknown",
        "sourceCount": len(manifest.get("sourceFiles") or []),
        "warnings": manifest.get("warnings") or [],
    }


def ensure_context_pack(
    workspace: Workspace,
    *,
    runtime_name: str,
    role: str,
    capability: str = "",
    domain_id: str = "",
    pipeline_id: str = "",
    product_kind_id: str = "",
    source_refs: list[str],
    pack_kind: str = "shared",
) -> dict[str, Any]:
    entries, warnings = source_entries(workspace, source_refs)
    scope = {
        "kind": pack_kind,
        "runtime": runtime_name or "shared",
        "role": role or "",
        "capability": capability or "",
        "domainId": domain_id or "",
        "pipelineId": pipeline_id or "",
        "productKindId": product_kind_id or "",
        "version": PACK_VERSION,
    }
    source_hash = hash_json({
        "scope": scope,
        "sources": [
            {
                "ref": entry.get("ref"),
                "sha256": entry.get("sha256"),
                "bytes": entry.get("bytes"),
                "exists": entry.get("exists"),
            }
            for entry in entries
        ],
    })
    key_parts = [
        pack_kind,
        runtime_name or "shared",
        role or capability or "role",
        domain_id or "domain",
        pipeline_id or "pipeline",
        product_kind_id or "kind",
        source_hash[:12],
    ]
    pack_key = "-".join(safe_segment(part) for part in key_parts)
    scope["packKey"] = pack_key
    root = pack_root(workspace)
    directory = root / pack_key
    manifest_path = directory / "manifest.json"
    compiled_path = directory / "compiled.md"
    existing = read_json(manifest_path)
    if (
        existing.get("artifact_type") == "context_pack"
        and existing.get("sourceHash") == source_hash
        and compiled_path.exists()
    ):
        return pack_ref(workspace, existing)

    compiled, enriched_entries = build_compiled_pack(workspace, directory, entries, scope, source_hash)
    write_text_atomic(compiled_path, compiled)
    status = "fresh" if not warnings else "warning"
    manifest = {
        "artifact_type": "context_pack",
        "version": PACK_VERSION,
        "createdAt": utc_ts(),
        "packKey": pack_key,
        "kind": pack_kind,
        "runtime": runtime_name or "shared",
        "role": role or "",
        "capability": capability or "",
        "domainId": domain_id or "",
        "pipelineId": pipeline_id or "",
        "productKindId": product_kind_id or "",
        "sourceHash": source_hash,
        "status": status,
        "manifestRef": rel(workspace, manifest_path),
        "compiledRef": rel(workspace, compiled_path),
        "sourceFiles": enriched_entries,
        "warnings": warnings,
    }
    write_json_atomic(manifest_path, manifest)
    return pack_ref(workspace, manifest)


def product_kind_profile_ref(workspace: Workspace, domain_id: str, product_kind_id: str) -> str:
    if not domain_id or not product_kind_id:
        return ""
    try:
        base = domain_resource_path(domain_id, "productKindProfiles", workspace=workspace, must_exist=True)
    except Exception:
        return ""
    path = base / f"{product_kind_id}.json"
    return rel(workspace, path) if path.exists() else ""


def shared_context_source_refs(workspace: Workspace, payload: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    refs.append(rel(workspace, workspace.config_path))
    domain = payload.get("domain") if isinstance(payload.get("domain"), dict) else {}
    pipeline = payload.get("pipeline") if isinstance(payload.get("pipeline"), dict) else {}
    product_kind = payload.get("productKind") if isinstance(payload.get("productKind"), dict) else {}
    for value in (domain.get("profileRef"), pipeline.get("profileRef")):
        if value:
            refs.append(str(value))
    kind_ref = product_kind_profile_ref(
        workspace,
        str(domain.get("id") or ""),
        str(product_kind.get("id") or ""),
    )
    if kind_ref:
        refs.append(kind_ref)
    refs.extend(str(ref) for ref in (payload.get("guidanceRefs") or []) if ref)
    for contract in payload.get("contracts") or []:
        if isinstance(contract, dict) and contract.get("ref"):
            refs.append(str(contract["ref"]))
    return dedupe_refs(refs)


def ensure_shared_context_pack(workspace: Workspace, payload: dict[str, Any]) -> dict[str, Any]:
    if not context_packs_enabled(workspace):
        return {
            "artifact_type": "context_pack_ref",
            "version": PACK_VERSION,
            "kind": "shared",
            "status": "disabled",
        }
    capability = str(payload.get("capability") or "")
    role = f"worker-{capability}" if capability else ""
    domain = payload.get("domain") if isinstance(payload.get("domain"), dict) else {}
    pipeline = payload.get("pipeline") if isinstance(payload.get("pipeline"), dict) else {}
    product_kind = payload.get("productKind") if isinstance(payload.get("productKind"), dict) else {}
    return ensure_context_pack(
        workspace,
        runtime_name="shared",
        role=role,
        capability=capability,
        domain_id=str(domain.get("id") or ""),
        pipeline_id=str(pipeline.get("id") or ""),
        product_kind_id=str(product_kind.get("id") or ""),
        source_refs=shared_context_source_refs(workspace, payload),
        pack_kind="shared-guidance",
    )


def input_refs(workspace: Workspace, task: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    items = task.get("inputs") if isinstance(task.get("inputs"), list) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("task_id") or "").strip()
        alias = str(item.get("as") or task_id).strip()
        if not task_id:
            continue
        path = data_root(workspace) / "results" / f"{task_id}.json"
        payload = read_json(path)
        result = payload.get("result") if isinstance(payload, dict) else None
        artifact_type = result.get("artifact_type") if isinstance(result, dict) else ""
        ref: dict[str, Any] = {
            "as": alias,
            "taskId": task_id,
            "resultRef": rel(workspace, path),
            "status": payload.get("status") or ("missing" if not payload else "unknown"),
            "artifact_type": artifact_type or "",
            "exists": path.exists(),
        }
        if path.exists():
            try:
                ref["sha256"] = sha256_file(path)
                ref["bytes"] = path.stat().st_size
            except Exception:
                ref["sha256"] = ""
                ref["bytes"] = 0
        refs.append(ref)
    return refs


def compact_worker_context(payload: dict[str, Any], *, inline_max_bytes: int = 16000) -> dict[str, Any]:
    compact = dict(payload)
    input_by_alias = {
        str(item.get("as") or item.get("taskId") or ""): item
        for item in compact.get("inputRefs") or []
        if isinstance(item, dict)
    }
    resolved = payload.get("resolvedInputs") if isinstance(payload.get("resolvedInputs"), dict) else {}
    compact_resolved: dict[str, Any] = {}
    for alias, value in resolved.items():
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        if len(encoded.encode("utf-8")) <= inline_max_bytes:
            compact_resolved[alias] = value
            continue
        ref = input_by_alias.get(str(alias), {})
        compact_resolved[alias] = {
            "omitted": True,
            "reason": "large_input",
            "bytes": len(encoded.encode("utf-8")),
            "resultRef": ref.get("resultRef") or "",
            "taskId": ref.get("taskId") or "",
        }
    compact["resolvedInputs"] = compact_resolved
    compact["contextView"] = {
        "mode": "worker",
        "inlineInputMaxBytes": inline_max_bytes,
    }
    return compact
