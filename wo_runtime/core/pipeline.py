from __future__ import annotations

import copy
import itertools
import json
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

from wo_runtime.core.domain import (
    current_workspace,
    domain_id_from_context,
    load_domain_profile,
    workspace_path,
)
from wo_runtime.core.validation import (
    QUEUE_CONTRACT_VERSION,
    QueueContractError,
    validate_identifier,
    validate_request_envelope,
    validate_resource_id,
    validate_task_envelope,
)
from wo_runtime.workspace import Workspace


class PlanError(RuntimeError):
    pass


def pipelines_dir(workspace: Workspace | None = None) -> Path:
    return workspace_path(workspace, "pipelinesDir", "pipelines")


def pipeline_profile_path(pipeline_id: str, workspace: Workspace | None = None) -> Path:
    safe_id = validate_resource_id(pipeline_id, "pipelineId")
    return pipelines_dir(workspace) / f"{safe_id}.json"


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PlanError(f"profile_not_found:{path.stem}") from exc
    except Exception as exc:
        raise PlanError(f"profile_invalid:{path.stem}") from exc
    if not isinstance(data, dict):
        raise PlanError(f"profile_invalid:{path.stem}")
    return data


def _merge_pipeline(parent: dict[str, Any], child: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(parent)
    for key, value in child.items():
        if key == "extends":
            continue
        if key == "defaults" and isinstance(value, dict):
            base = merged.get("defaults") if isinstance(merged.get("defaults"), dict) else {}
            merged["defaults"] = {**base, **value}
            continue
        if key == "stages" and "stages" not in child:
            continue
        merged[key] = copy.deepcopy(value)
    return merged


def load_pipeline(
    pipeline_id: str,
    *,
    workspace: Workspace | None = None,
    _seen: set[str] | None = None,
) -> dict[str, Any]:
    ws = current_workspace(workspace)
    seen = _seen or set()
    if pipeline_id in seen:
        raise PlanError(f"profile_invalid:{pipeline_id}:extends_cycle")
    seen.add(pipeline_id)
    path = pipeline_profile_path(pipeline_id, ws)
    data = _read_json_object(path)
    parent_id = data.get("extends")
    if parent_id:
        parent = load_pipeline(str(parent_id), workspace=ws, _seen=seen)
        data = _merge_pipeline(parent, data)
    data.setdefault("id", pipeline_id)
    data["_profileRef"] = path
    if not isinstance(data.get("stages"), list):
        raise PlanError(f"profile_invalid:{pipeline_id}:missing_stages")
    return data


def _source(request: dict[str, Any]) -> dict[str, Any]:
    source = request.get("source")
    return source if isinstance(source, dict) else {}


def _has_explicit_domain(request: dict[str, Any]) -> bool:
    source = _source(request)
    return bool(request.get("domainId") or request.get("domain_id") or source.get("domainId") or source.get("domain_id"))


def select_pipeline_id(
    workspace: Workspace,
    request: dict[str, Any],
    domain_profile: dict[str, Any] | None = None,
) -> str:
    source = _source(request)
    explicit = request.get("pipelineId") or request.get("pipeline_id") or source.get("pipelineId") or source.get("pipeline_id")
    if explicit:
        return str(explicit)
    selection = workspace.config.get("pipelineSelection") if isinstance(workspace.config.get("pipelineSelection"), dict) else {}
    signal_mode = request.get("signalMode") or source.get("signalMode")
    signal_modes = selection.get("signalModes") if isinstance(selection.get("signalModes"), dict) else {}
    if signal_mode and signal_modes.get(str(signal_mode)):
        return str(signal_modes[str(signal_mode)])
    workflow_mode = request.get("workflowMode") or source.get("workflowMode")
    workflow_modes = selection.get("workflowModes") if isinstance(selection.get("workflowModes"), dict) else {}
    if workflow_mode and workflow_modes.get(str(workflow_mode)):
        return str(workflow_modes[str(workflow_mode)])
    kind_defaults = selection.get("kindDefaults") if isinstance(selection.get("kindDefaults"), dict) else {}
    kind = str(request.get("kind") or "")
    if kind_defaults.get(kind):
        return str(kind_defaults[kind])
    domain = domain_profile or {}
    return str(domain.get("defaultPipelineId") or workspace.config.get("defaultPipelineId") or "default")


def resolve_domain_and_pipeline(
    workspace: Workspace,
    request: dict[str, Any],
) -> tuple[str, dict[str, Any], str, dict[str, Any]]:
    domain_id = validate_resource_id(domain_id_from_context(request, workspace=workspace), "domainId")
    domain = load_domain_profile(domain_id, request, workspace=workspace, must_exist=False)
    pipeline_id = validate_resource_id(select_pipeline_id(workspace, request, domain), "pipelineId")
    pipeline = load_pipeline(pipeline_id, workspace=workspace)
    defaults = pipeline.get("defaults") if isinstance(pipeline.get("defaults"), dict) else {}
    if not _has_explicit_domain(request) and defaults.get("domainId"):
        domain_id = validate_resource_id(defaults["domainId"], "pipeline.defaults.domainId")
        domain = load_domain_profile(domain_id, request, workspace=workspace, must_exist=False)
    return domain_id, domain, pipeline_id, pipeline


def _value_present(context: dict[str, Any], fields: list[str]) -> bool:
    return any(_get_path(context, field) not in (None, "", []) for field in fields)


def _default_fields(name: str) -> list[str]:
    plural = f"{name}s"
    if name == "locale":
        return ["locales", "locale"]
    if name == "channel":
        return ["channels", "channel", "marketplace"]
    if name == "format":
        return ["formats", "format"]
    return [plural, name]


def _apply_defaults(
    request: dict[str, Any],
    *,
    workspace: Workspace,
    domain_id: str,
    domain: dict[str, Any],
    pipeline_id: str,
    pipeline: dict[str, Any],
) -> dict[str, Any]:
    product = copy.deepcopy(request)
    defaults = pipeline.get("defaults") if isinstance(pipeline.get("defaults"), dict) else {}
    product["domainId"] = domain_id
    product["pipelineId"] = pipeline_id
    for field, domain_key in (
        ("formats", "defaultFormats"),
        ("locales", "defaultLocales"),
        ("channels", "defaultChannels"),
    ):
        axis = field[:-1] if field.endswith("s") else field
        aliases = _default_fields(axis)
        if _value_present(product, aliases):
            continue
        if defaults.get(field) not in (None, "", []):
            product[field] = copy.deepcopy(defaults[field])
        elif domain.get(domain_key) not in (None, "", []):
            product[field] = copy.deepcopy(domain[domain_key])
    for key, value in defaults.items():
        if key == "domainId":
            continue
        if product.get(key) in (None, "", []):
            product[key] = copy.deepcopy(value)
    return product


def _normalize(value: Any) -> str:
    return str(value or "").casefold().replace("_", "-").strip()


def _get_path(context: dict[str, Any], field: str) -> Any:
    cur: Any = context
    for part in str(field).split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _as_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [item for item in value if item not in (None, "")]
    return [value]


def _field_values(product: dict[str, Any], fields: list[str]) -> list[str]:
    for field in fields:
        values = [str(item) for item in _as_list(_get_path(product, field))]
        if values:
            return values
    return []


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _fanout_values(
    axis: str,
    *,
    product: dict[str, Any],
    workspace: Workspace,
    domain: dict[str, Any],
    pipeline: dict[str, Any],
) -> list[str]:
    alias_config = workspace.config.get("fanoutAliases") if isinstance(workspace.config.get("fanoutAliases"), dict) else {}
    fields = alias_config.get(axis) if isinstance(alias_config.get(axis), list) else _default_fields(axis)
    values = _field_values(product, [str(field) for field in fields])
    if values:
        return _unique(values)
    defaults = pipeline.get("defaults") if isinstance(pipeline.get("defaults"), dict) else {}
    values = _field_values(defaults, _default_fields(axis))
    if values:
        return _unique(values)
    domain_key = {
        "locale": "defaultLocales",
        "channel": "defaultChannels",
        "format": "defaultFormats",
    }.get(axis)
    if domain_key and isinstance(domain.get(domain_key), list):
        return _unique([str(item) for item in domain[domain_key]])
    fanout_defaults = workspace.config.get("fanoutDefaults") if isinstance(workspace.config.get("fanoutDefaults"), dict) else {}
    values = [str(item) for item in _as_list(fanout_defaults.get(axis))]
    return _unique(values)


def _matches_clause(clause: dict[str, Any], product: dict[str, Any]) -> bool:
    field = clause.get("field")
    value = _get_path(product, str(field)) if field else product
    values = _as_list(value)
    normalized_values = [_normalize(item) for item in values]
    if "exists" in clause:
        return bool(value not in (None, "", [])) is bool(clause["exists"])
    if "truthy" in clause:
        return bool(value) is bool(clause["truthy"])
    if "equals" in clause:
        return _normalize(value) == _normalize(clause["equals"])
    if "equalsAny" in clause:
        return any(item in normalized_values for item in [_normalize(raw) for raw in _as_list(clause["equalsAny"])])
    if "contains" in clause:
        needle = _normalize(clause["contains"])
        return any(needle in item for item in normalized_values)
    if "containsAny" in clause:
        needles = [_normalize(raw) for raw in _as_list(clause["containsAny"])]
        return any(needle and any(needle in item for item in normalized_values) for needle in needles)
    if "matchesAny" in clause:
        needles = [_normalize(raw) for raw in _as_list(clause["matchesAny"])]
        return any(item in needles for item in normalized_values)
    return False


def _matches_condition(condition: Any, product: dict[str, Any]) -> bool:
    if condition in (None, {}, []):
        return True
    if isinstance(condition, list):
        return all(_matches_condition(item, product) for item in condition)
    if not isinstance(condition, dict):
        return bool(condition)
    if "all" in condition:
        return all(_matches_condition(item, product) for item in _as_list(condition["all"]))
    if "any" in condition:
        return any(_matches_condition(item, product) for item in _as_list(condition["any"]))
    if "not" in condition:
        return not _matches_condition(condition["not"], product)
    return _matches_clause(condition, product)


def _stage_applies(stage: dict[str, Any], request: dict[str, Any], product: dict[str, Any]) -> bool:
    kinds = stage.get("appliesToKinds")
    if isinstance(kinds, list) and str(request.get("kind") or "") not in {str(kind) for kind in kinds}:
        return False
    return _matches_condition(stage.get("when") or stage.get("emitWhen"), product)


def _stage_refs(value: Any) -> list[str]:
    refs: list[str] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            raw = item.get("stage") or item.get("number") or item.get("task")
        else:
            raw = item
        if raw:
            refs.append(str(raw))
    return refs


def _axis_product(axes: list[str], values_by_axis: dict[str, list[str]]) -> list[dict[str, str]]:
    if not axes:
        return [{}]
    value_sets = [values_by_axis.get(axis, []) for axis in axes]
    if any(not values for values in value_sets):
        return []
    out = []
    for values in itertools.product(*value_sets):
        out.append({axis: value for axis, value in zip(axes, values)})
    return out


def _variant_suffix(axes: dict[str, str], order: list[str]) -> str:
    if not order:
        return ""
    return "." + ".".join(str(axes[axis]) for axis in order)


def _compatible(dep_axes: dict[str, str], current_axes: dict[str, str]) -> bool:
    for axis, value in dep_axes.items():
        if axis in current_axes and current_axes[axis] != value:
            return False
    return True


def _matching_variants(
    refs: list[str],
    current_axes: dict[str, str],
    variants_by_stage: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        for variant in variants_by_stage.get(ref, []):
            if not _compatible(variant["axes"], current_axes):
                continue
            task_id = str(variant["task"]["id"])
            if task_id in seen:
                continue
            seen.add(task_id)
            out.append(variant)
    return out


def _input_alias(dep_variant: dict[str, Any], target_stage: dict[str, Any]) -> str:
    dep_stage = dep_variant["stage"]
    dep_number = str(dep_stage.get("number") or "")
    mapping = target_stage.get("inputMap") if isinstance(target_stage.get("inputMap"), dict) else {}
    mapped = mapping.get(dep_number) or mapping.get(str(dep_stage.get("capability") or ""))
    if mapped:
        return str(mapped)
    return str(dep_stage.get("inputAs") or dep_stage.get("outputAs") or dep_stage.get("capability") or dep_stage.get("stage") or dep_number)


def _task_base(
    *,
    task_id: str,
    request_id: str,
    stage: dict[str, Any],
    product: dict[str, Any],
    domain_id: str,
    pipeline_id: str,
    axes: dict[str, str],
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "queue_contract": QUEUE_CONTRACT_VERSION,
        "id": task_id,
        "req_id": request_id,
        "capability": str(stage.get("capability") or ""),
        "stage": str(stage.get("stage") or stage.get("number") or ""),
        "depends_on": [],
        "inputs": [],
        "product": copy.deepcopy(product),
        "domainId": domain_id,
        "pipelineId": pipeline_id,
    }
    if product.get("ageBand") is not None:
        task["ageBand"] = product.get("ageBand")
    for axis, value in axes.items():
        task[axis] = value
    for field in ("modality", "prompts_glob", "prompts_glob_include", "out_dir", "out_ext", "opts", "provider"):
        if field in stage:
            task[field] = copy.deepcopy(stage[field])
    extra = stage.get("taskFields") or stage.get("task")
    if isinstance(extra, dict):
        task.update(copy.deepcopy(extra))
    return task


def compile_request(workspace: Workspace, request: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_request_envelope(request)
        request_id = validate_identifier(request.get("id") or request.get("req_id"), "request.id")
    except QueueContractError as exc:
        raise PlanError(f"request_invalid:{exc}") from exc
    domain_id, domain, pipeline_id, pipeline = resolve_domain_and_pipeline(workspace, request)
    product = _apply_defaults(
        request,
        workspace=workspace,
        domain_id=domain_id,
        domain=domain,
        pipeline_id=pipeline_id,
        pipeline=pipeline,
    )
    stages = [stage for stage in pipeline.get("stages", []) if isinstance(stage, dict)]
    variants_by_stage: dict[str, list[dict[str, Any]]] = {}
    tasks: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for stage in stages:
        number = str(stage.get("number") or "").strip()
        capability = str(stage.get("capability") or "").strip()
        if not number or not capability:
            warnings.append({"code": "stage_skipped_invalid", "stage": stage})
            continue
        if not _stage_applies(stage, request, product):
            variants_by_stage[number] = []
            continue
        depends_refs = _stage_refs(stage.get("dependsOn"))
        if depends_refs and not _matching_variants(depends_refs, {}, variants_by_stage):
            variants_by_stage[number] = []
            warnings.append({"code": "stage_skipped_no_emitted_dependencies", "stage": number})
            continue
        axes = [str(axis) for axis in _as_list(stage.get("fanout"))]
        values_by_axis = {
            axis: _fanout_values(axis, product=product, workspace=workspace, domain=domain, pipeline=pipeline)
            for axis in axes
        }
        axis_variants = _axis_product(axes, values_by_axis)
        if axes and not axis_variants:
            variants_by_stage[number] = []
            warnings.append({"code": "stage_skipped_empty_fanout", "stage": number, "axes": axes})
            continue
        stage_variants: list[dict[str, Any]] = []
        for current_axes in axis_variants:
            task_id = f"{request_id}.{number}{_variant_suffix(current_axes, axes)}"
            task = _task_base(
                task_id=task_id,
                request_id=request_id,
                stage=stage,
                product=product,
                domain_id=domain_id,
                pipeline_id=pipeline_id,
                axes=current_axes,
            )
            dep_variants = _matching_variants(depends_refs, current_axes, variants_by_stage)
            task["depends_on"] = [str(variant["task"]["id"]) for variant in dep_variants]
            input_refs = _stage_refs(stage.get("inputsFrom"))
            source_variants = dep_variants + _matching_variants(input_refs, current_axes, variants_by_stage)
            seen_inputs: set[tuple[str, str]] = set()
            inputs = []
            for variant in source_variants:
                alias = _input_alias(variant, stage)
                item = {"task_id": str(variant["task"]["id"]), "as": alias}
                key = (item["task_id"], item["as"])
                if key in seen_inputs:
                    continue
                seen_inputs.add(key)
                inputs.append(item)
            task["inputs"] = inputs
            variant = {"stage": stage, "axes": current_axes, "task": task}
            stage_variants.append(variant)
            tasks.append(task)
        variants_by_stage[number] = stage_variants
        if bool(stage.get("isRoot")) and stage_variants:
            for variant in stage_variants:
                variant["task"].pop("is_root", None)
            root_variant = sorted(stage_variants, key=lambda item: str(item["task"].get("id") or ""))[-1]
            root_variant["task"]["is_root"] = True

    roots = [task for task in tasks if task.get("is_root")]
    if len(roots) > 1:
        raise PlanError(f"dag_invalid:multiple_roots:{','.join(str(task.get('id')) for task in roots)}")
    if not roots and tasks:
        tasks[-1]["is_root"] = True
        warnings.append({"code": "root_auto_selected", "taskId": tasks[-1]["id"]})
    validate_tasks(tasks)
    return {
        "request": copy.deepcopy(request),
        "domain": {"id": domain_id},
        "pipeline": {"id": pipeline_id, "profileRef": str(pipeline.get("_profileRef") or "")},
        "tasks": tasks,
        "warnings": warnings,
    }


def validate_tasks(tasks: list[dict[str, Any]]) -> None:
    for task in tasks:
        try:
            validate_task_envelope(task, require_stage=True)
        except QueueContractError as exc:
            raise PlanError(f"dag_invalid:{exc}") from exc
    ids = [str(task.get("id") or "") for task in tasks]
    id_set = set(ids)
    if len(id_set) != len(ids):
        raise PlanError("dag_invalid:duplicate_task_ids")
    roots = [task for task in tasks if task.get("is_root")]
    if len(roots) != 1:
        raise PlanError(f"dag_invalid:root_count:{len(roots)}")
    for task in tasks:
        task_id = str(task.get("id") or "")
        for dep in task.get("depends_on") or []:
            dep_id = str(dep)
            if dep_id not in id_set:
                raise PlanError(f"dag_invalid:missing_dependency:{task_id}:{dep_id}")
            if dep_id == task_id:
                raise PlanError(f"dag_invalid:self_dependency:{task_id}")
    # Normalize traversal order for stable diagnostics without changing the
    # caller's task envelopes or dependency order. graphlib uses an iterative
    # cycle search, so valid deep pipelines do not hit Python's recursion limit.
    graph = {
        str(task["id"]): sorted(str(dep) for dep in task.get("depends_on") or [])
        for task in sorted(tasks, key=lambda task: str(task["id"]))
    }
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as exc:
        cycle = "->".join(exc.args[1])
        raise PlanError(f"dag_invalid:cycle:{cycle}") from exc
