from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from wo_runtime.core.context import build_context
from wo_runtime.core.context_packs import compact_worker_context, inline_input_max_bytes
from wo_runtime.core.contracts import contract_path
from wo_runtime.core.pipeline import PlanError, compile_request, validate_tasks
from wo_runtime.core.validation import QueueContractError, safe_file, validate_identifier, validate_request_envelope
from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, Workspace, ensure_data_root, load_workspace


def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _workspace(args: argparse.Namespace) -> Workspace:
    workspace = load_workspace(args.workspace_root, args.workspace_config)
    ensure_data_root(workspace)
    return workspace


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _event(workspace: Workspace, payload: dict[str, Any]) -> None:
    payload.setdefault("ts", ts())
    path = workspace.data_root / "events" / "log.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _load_request(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise PlanError("request_invalid:not_object")
    validate_request_envelope(data)
    return data


def _before_plan_request(workspace: Workspace, request: dict[str, Any], request_path: Path) -> dict[str, Any]:
    hooks = workspace.config.get("instanceHooks") if isinstance(workspace.config.get("instanceHooks"), dict) else {}
    module_name = str(hooks.get("planner") or "").strip()
    if not module_name:
        return request
    if str(workspace.root) not in sys.path:
        sys.path.insert(0, str(workspace.root))
    module = importlib.import_module(module_name)
    fn = getattr(module, "before_plan_request", None)
    if fn is None:
        return request
    updated = fn(request, request_path, workspace.data_root, workspace.root)
    if not isinstance(updated, dict):
        raise PlanError("planner_hook_invalid:before_plan_request_must_return_object")
    validate_request_envelope(updated)
    return updated


def _write_tasks(workspace: Workspace, tasks: list[dict[str, Any]]) -> None:
    pending = workspace.data_root / "tasks" / "pending"
    pending.mkdir(parents=True, exist_ok=True)
    for task in tasks:
        try:
            task_id = validate_identifier(task.get("id"), "task.id")
            path = safe_file(pending, task_id, field="task.id")
        except QueueContractError as exc:
            raise PlanError(f"dag_invalid:{exc}") from exc
        _write_json(path, task)


def _write_failed_final(workspace: Workspace, req_id: str, error: str, *, request_ref: str | None = None) -> None:
    payload = {
        "task_id": f"{req_id}.final",
        "status": "failed",
        "result": None,
        "error": error,
        "ts": ts(),
    }
    if request_ref:
        payload["requestRef"] = request_ref
    path = safe_file(workspace.data_root / "results", req_id, suffix=".final.json", field="request.id")
    _write_json(path, payload)


def _failure_request_id(candidate: str, source: Path) -> str:
    try:
        return validate_identifier(candidate, "request.id")
    except QueueContractError:
        digest = hashlib.sha256(str(source).encode("utf-8")).hexdigest()[:16]
        return f"invalid-{digest}"


def _processed_target(path: Path) -> Path:
    processed = path.parent / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    target = processed / path.name
    if not target.exists():
        return target
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return processed / f"{path.stem}.{stamp}{path.suffix}"


def _plan_request(workspace: Workspace, request: dict[str, Any]) -> dict[str, Any]:
    plan = compile_request(workspace, request)
    validate_tasks(plan["tasks"])
    return plan


def cmd_plan(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    path = Path(args.request_file)
    request = _before_plan_request(workspace, _load_request(path), path)
    plan = _plan_request(workspace, request)
    if args.write:
        _write_tasks(workspace, plan["tasks"])
        _event(workspace, {
            "event": "planned",
            "req_id": request.get("id"),
            "pipelineId": plan["pipeline"]["id"],
            "task_count": len(plan["tasks"]),
        })
        print(f"planned {request.get('id')}: {len(plan['tasks'])} task(s)")
    else:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
    return 0


def _plan_inbox_item(workspace: Workspace, path: Path, *, dry_run: bool) -> tuple[str, int]:
    req_id = path.stem
    try:
        request = _before_plan_request(workspace, _load_request(path), path)
        req_id = str(request.get("id") or req_id)
        plan = _plan_request(workspace, request)
    except Exception as exc:
        error = f"parse_error:{exc}"
        if not dry_run:
            req_id = _failure_request_id(req_id, path)
            _write_failed_final(workspace, req_id, error, request_ref=str(path))
            path.replace(_processed_target(path))
            _event(workspace, {"event": "plan_failed", "req_id": req_id, "error": error})
        return req_id, 0
    if not dry_run:
        _write_tasks(workspace, plan["tasks"])
        path.replace(_processed_target(path))
        _event(workspace, {
            "event": "planned",
            "req_id": req_id,
            "pipelineId": plan["pipeline"]["id"],
            "task_count": len(plan["tasks"]),
        })
    return req_id, len(plan["tasks"])


def cmd_plan_inbox(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    inbox = workspace.data_root / "inbox"
    paths = sorted(path for path in inbox.glob("*.json") if path.is_file()) if inbox.exists() else []
    total = 0
    for path in paths:
        req_id, count = _plan_inbox_item(workspace, path, dry_run=args.dry_run)
        total += count
        print(f"{'would plan' if args.dry_run else 'planned'} {req_id}: {count} task(s)")
        if args.once:
            break
    if not paths:
        print("inbox empty")
    else:
        print(f"task_count={total}")
    return 0


def cmd_context(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    payload = build_context(args.task_id, full=args.full, workspace=workspace)
    if args.view == "worker":
        payload = compact_worker_context(payload, inline_max_bytes=inline_input_max_bytes(workspace))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.fail_on_warnings and payload.get("stateWarnings"):
        return 1
    return 0


def cmd_contract_path(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    path = contract_path(args.contract_id, workspace=workspace, domain_id=args.domain_id, must_exist=not args.allow_missing)
    print(path)
    return 0


def _load_req_tasks(workspace: Workspace, req_id: str) -> list[dict[str, Any]]:
    tasks_root = workspace.data_root / "tasks"
    tasks = []
    dirs = [
        tasks_root / "pending",
        tasks_root / "claimed",
        tasks_root / "done",
        *(path for path in sorted(tasks_root.glob("ready-*")) if path.is_dir()),
    ]
    for directory in dirs:
        for path in sorted(directory.glob(f"{req_id}*.json")) if directory.exists() else []:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(data, dict) and data.get("req_id") == req_id:
                tasks.append(data)
    return tasks


def cmd_validate_dag(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    tasks = _load_req_tasks(workspace, args.req_id)
    if not tasks:
        print(f"no tasks found for req_id={args.req_id}", file=sys.stderr)
        return 1
    validate_tasks(tasks)
    print(f"dag ok: {args.req_id} ({len(tasks)} task(s))")
    return 0


def _workspace_parent() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    return parser


def build_parser() -> argparse.ArgumentParser:
    workspace = _workspace_parent()
    parser = argparse.ArgumentParser(prog="wo-runtime core")
    sub = parser.add_subparsers(dest="command")

    plan = sub.add_parser("plan", parents=[workspace], help="Compile one request into a DAG")
    plan.add_argument("--request-file", required=True)
    mode = plan.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print the compiled plan without writing tasks")
    mode.add_argument("--write", action="store_true", help="Write compiled tasks to tasks/pending")
    plan.set_defaults(func=cmd_plan)

    inbox = sub.add_parser("plan-inbox", parents=[workspace], help="Compile requests from inbox")
    inbox.add_argument("--once", action="store_true", help="Process at most one request")
    inbox.add_argument("--dry-run", action="store_true", help="Show what would be planned without writing or moving files")
    inbox.set_defaults(func=cmd_plan_inbox)

    context = sub.add_parser("context", parents=[workspace], help="Build context pack for a task")
    context.add_argument("task_id")
    context.add_argument("--full", action="store_true")
    context.add_argument("--view", choices=["full", "worker"], default="full")
    context.add_argument("--fail-on-warnings", action="store_true")
    context.set_defaults(func=cmd_context)

    contract = sub.add_parser("contract-path", parents=[workspace], help="Resolve a contract id/name to a file")
    contract.add_argument("contract_id")
    contract.add_argument("--domain-id")
    contract.add_argument("--allow-missing", action="store_true")
    contract.set_defaults(func=cmd_contract_path)

    validate = sub.add_parser("validate-dag", parents=[workspace], help="Validate planned task files for one request")
    validate.add_argument("--req-id", required=True)
    validate.set_defaults(func=cmd_validate_dag)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 2
    try:
        return int(func(args) or 0)
    except PlanError as exc:
        print(f"wo-runtime core: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"wo-runtime core: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
