from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from wo_runtime.core.contracts import contract_path
from wo_runtime.core.domain import (
    current_workspace,
    domain_id_from_context,
    domain_profile_path,
    domain_resource_path,
    load_domain_profile,
)
from wo_runtime.core.pipeline import pipeline_profile_path
from wo_runtime.core.product_kind import resolve_product_kind_profile
from wo_runtime.workspace import Workspace


TASK_AREAS = ["pending", "claimed", "done", "failed", "invalid"]


def rel(workspace: Workspace, path: Path) -> str:
    try:
        return path.resolve().relative_to(workspace.root).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(workspace.data_root.resolve()).as_posix()
        except ValueError:
            return str(path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def unwrap_result(payload: dict[str, Any]) -> Any:
    if isinstance(payload, dict) and "result" in payload:
        return payload.get("result")
    return payload


def task_dirs(workspace: Workspace) -> list[Path]:
    tasks_root = workspace.data_root / "tasks"
    dirs = [tasks_root / area for area in TASK_AREAS]
    dirs.extend(sorted(tasks_root.glob("ready-*")))
    return [path for path in dirs if path.exists()]


def find_task(task_id: str, *, workspace: Workspace | None = None) -> tuple[dict[str, Any], Path, str]:
    ws = current_workspace(workspace)
    wanted = f"{task_id}.json"
    for directory in task_dirs(ws):
        path = directory / wanted
        if path.exists():
            return read_json(path), path, directory.name
    raise FileNotFoundError(f"task not found: {task_id}")


def resolve_domain_and_pipeline(task: dict[str, Any], *, workspace: Workspace | None = None) -> tuple[str, str]:
    ws = current_workspace(workspace)
    product = task.get("product") if isinstance(task.get("product"), dict) else {}
    source = product.get("source") if isinstance(product.get("source"), dict) else {}
    domain_id = (
        task.get("domainId")
        or product.get("domainId")
        or source.get("domainId")
        or domain_id_from_context(product, workspace=ws)
    )
    profile = load_domain_profile(str(domain_id), product, workspace=ws)
    pipeline_id = (
        task.get("pipelineId")
        or product.get("pipelineId")
        or source.get("pipelineId")
        or profile.get("defaultPipelineId")
        or ws.config.get("defaultPipelineId")
        or "default"
    )
    return str(domain_id), str(pipeline_id)


def resolve_inputs(task: dict[str, Any], *, workspace: Workspace | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ws = current_workspace(workspace)
    resolved: dict[str, Any] = {}
    warnings: list[dict[str, Any]] = []
    inputs = task.get("inputs") if isinstance(task.get("inputs"), list) else []
    for item in inputs:
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("task_id") or "").strip()
        alias = str(item.get("as") or task_id).strip()
        if not task_id:
            continue
        path = ws.data_root / "results" / f"{task_id}.json"
        payload = read_json(path)
        if not payload:
            warnings.append({"code": "input_result_missing", "taskId": task_id, "as": alias, "path": rel(ws, path)})
            continue
        if payload.get("status") != "success":
            warnings.append({
                "code": "input_result_not_success",
                "taskId": task_id,
                "as": alias,
                "status": payload.get("status"),
                "path": rel(ws, path),
            })
            continue
        if alias in resolved:
            existing = resolved[alias]
            if isinstance(existing, list):
                existing.append(unwrap_result(payload))
            else:
                resolved[alias] = [existing, unwrap_result(payload)]
        else:
            resolved[alias] = unwrap_result(payload)
    return resolved, warnings


def guidance_refs(domain_id: str, capability: str, *, workspace: Workspace | None = None) -> list[str]:
    ws = current_workspace(workspace)
    refs = []
    try:
        guidance_dir = domain_resource_path(domain_id, "roleGuidanceDir", workspace=ws, legacy=Path("domains") / domain_id / "role-guidance")
    except Exception:
        guidance_dir = ws.root / "domains" / domain_id / "role-guidance"
    for name in ["common.md", f"worker-{capability}.md", f"{capability}.md"]:
        path = guidance_dir / name
        if path.exists():
            refs.append(rel(ws, path))
    role_path = ws.root / "roles" / f"worker-{capability}.md"
    if role_path.exists():
        refs.append(rel(ws, role_path))
    return refs


def _configured_contract_ids(task: dict[str, Any], product_kind: dict[str, Any], workspace: Workspace) -> list[str]:
    capability = str(task.get("capability") or "")
    stage = str(task.get("stage") or "")
    ids: list[str] = []
    matrix = workspace.config.get("contextContracts") if isinstance(workspace.config.get("contextContracts"), dict) else {}
    for item in matrix.get("base") or ["task.schema.json", "result.schema.json"]:
        ids.append(str(item))
    capability_map = matrix.get("capabilities") if isinstance(matrix.get("capabilities"), dict) else {}
    for item in capability_map.get(capability) or []:
        ids.append(str(item))
    stage_map = matrix.get("stages") if isinstance(matrix.get("stages"), dict) else {}
    for item in stage_map.get(stage) or []:
        ids.append(str(item))
    kind_map = matrix.get("productKinds") if isinstance(matrix.get("productKinds"), dict) else {}
    kind_id = str(product_kind.get("id") or "")
    for item in kind_map.get(kind_id) or []:
        ids.append(str(item))
    for item in product_kind.get("contracts") or []:
        ids.append(str(item))
    return ids


def contract_refs(task: dict[str, Any], product_kind: dict[str, Any], *, workspace: Workspace | None = None) -> list[dict[str, Any]]:
    ws = current_workspace(workspace)
    product = task.get("product") if isinstance(task.get("product"), dict) else {}
    context = {**product, **task}
    out = []
    seen = set()
    for contract_id in _configured_contract_ids(task, product_kind, ws):
        if contract_id in seen:
            continue
        seen.add(contract_id)
        try:
            path = contract_path(contract_id, context, workspace=ws)
        except Exception as exc:
            out.append({"id": contract_id, "ref": "", "required": False, "warning": str(exc)})
            continue
        out.append({"id": contract_id, "ref": rel(ws, path), "required": True})
    return out


def required_audits(task: dict[str, Any], *, workspace: Workspace | None = None) -> list[str]:
    ws = current_workspace(workspace)
    capability = str(task.get("capability") or "")
    stage = str(task.get("stage") or "")
    matrix = ws.config.get("contextAudits") if isinstance(ws.config.get("contextAudits"), dict) else {}
    audits = []
    audits.extend(matrix.get("base") or ["input_resolution"])
    capabilities = matrix.get("capabilities") if isinstance(matrix.get("capabilities"), dict) else {}
    audits.extend(capabilities.get(capability) or [])
    stages = matrix.get("stages") if isinstance(matrix.get("stages"), dict) else {}
    audits.extend(stages.get(stage) or [])
    out: list[str] = []
    for audit in audits:
        value = str(audit)
        if value not in out:
            out.append(value)
    return out


def canonical_artifacts(task: dict[str, Any], *, workspace: Workspace | None = None) -> dict[str, Any]:
    ws = current_workspace(workspace)
    req_id = str(task.get("req_id") or "")
    result_refs = {}
    results_dir = ws.data_root / "results"
    for path in sorted(results_dir.glob(f"{req_id}*.json")) if results_dir.exists() else []:
        result_refs[path.stem] = rel(ws, path)
    return {
        "baseReqId": req_id,
        "activeReqId": req_id,
        "activeArtifactReqId": req_id,
        "promptDir": rel(ws, ws.data_root / "prompts" / req_id),
        "assetDir": rel(ws, ws.data_root / "assets" / req_id),
        "resultRefs": result_refs,
    }


def build_context(task_id: str, *, full: bool = False, workspace: Workspace | None = None) -> dict[str, Any]:
    ws = current_workspace(workspace)
    task, task_path, task_area = find_task(task_id, workspace=ws)
    product = task.get("product") if isinstance(task.get("product"), dict) else {}
    domain_id, pipeline_id = resolve_domain_and_pipeline(task, workspace=ws)
    product_context = {**product, **task, "domainId": domain_id, "pipelineId": pipeline_id}
    profile_path = domain_profile_path(domain_id, ws)
    pipeline_path = pipeline_profile_path(pipeline_id, ws)
    product_kind = resolve_product_kind_profile(product_context, workspace=ws)
    resolved_inputs, warnings = resolve_inputs(task, workspace=ws)
    payload: dict[str, Any] = {
        "artifact_type": "task_context",
        "taskId": str(task.get("id") or task_id),
        "reqId": str(task.get("req_id") or ""),
        "capability": str(task.get("capability") or ""),
        "stage": str(task.get("stage") or ""),
        "task": task,
        "taskRef": rel(ws, task_path),
        "taskArea": task_area,
        "product": product,
        "resolvedInputs": resolved_inputs,
        "domain": {
            "id": domain_id,
            "profileRef": rel(ws, profile_path),
        },
        "pipeline": {
            "id": pipeline_id,
            "profileRef": rel(ws, pipeline_path) if pipeline_path.exists() else "",
        },
        "productKind": product_kind,
        "contracts": contract_refs(task, product_kind, workspace=ws),
        "guidanceRefs": guidance_refs(domain_id, str(task.get("capability") or ""), workspace=ws),
        "requiredAudits": required_audits(task, workspace=ws),
        "canonicalArtifacts": canonical_artifacts(task, workspace=ws),
        "stateWarnings": warnings,
    }
    if full:
        payload["domain"]["profile"] = load_domain_profile(domain_id, product_context, workspace=ws)
        if pipeline_path.exists():
            payload["pipeline"]["profile"] = read_json(pipeline_path)
    return payload

