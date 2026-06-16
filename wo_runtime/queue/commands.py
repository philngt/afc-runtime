from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from wo_runtime.workspace import Workspace, load_and_prepare


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _workspace(args: Any) -> Workspace:
    return load_and_prepare(
        getattr(args, "workspace_root", None),
        getattr(args, "workspace_config", None),
    )


def _event(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _move_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.replace(dst)


def claim(args: Any) -> int:
    if not getattr(args, "capability", None) or not getattr(args, "worker_id", None):
        print("usage: claim.sh <capability> <worker-id>", file=sys.stderr)
        return 2
    ws = _workspace(args)
    data_root = ws.data_root
    ready = data_root / "tasks" / f"ready-{args.capability}"
    claimed = data_root / "tasks" / "claimed"
    claimed.mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)

    for path in sorted(ready.glob("*.json")) if ready.is_dir() else []:
        target = claimed / path.name
        try:
            path.replace(target)
        except OSError:
            continue
        _event(
            data_root / "events" / "log.jsonl",
            {
                "event": "claim",
                "task": path.stem,
                "worker": args.worker_id,
                "ts": _ts(),
            },
        )
        print(target)
        return 0
    print("")
    return 0


def complete(args: Any) -> int:
    task_id = getattr(args, "task_id", None)
    raw_result = getattr(args, "json_result_value", None)
    worker_id = getattr(args, "worker_id", None) or "unknown"
    if not task_id or raw_result is None:
        print("usage: complete.sh <task-id> <json-result-value> [worker-id]", file=sys.stderr)
        return 2

    ws = _workspace(args)
    data_root = ws.data_root
    (data_root / "results").mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)
    (data_root / "tasks" / "done").mkdir(parents=True, exist_ok=True)
    ts = _ts()

    autocorrected = False
    try:
        result = json.loads(raw_result)
    except json.JSONDecodeError as exc:
        try:
            result = json.loads(worker_id)
        except json.JSONDecodeError:
            print(f"complete.sh: result argument must be strict JSON: {exc}", file=sys.stderr)
            return 2
        if not re.fullmatch(r"[A-Za-z0-9_.:@/+~-]{1,160}", raw_result):
            print(
                "complete.sh: result is not JSON and worker argument is JSON; refusing ambiguous argument order",
                file=sys.stderr,
            )
            return 2
        worker_id = raw_result
        autocorrected = True

    payload: dict[str, Any] = {
        "task_id": task_id,
        "worker": worker_id,
        "status": "success",
        "result": result,
        "ts": ts,
    }
    if autocorrected:
        payload["argOrderAutocorrected"] = True

    (data_root / "results" / f"{task_id}.json").write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    if autocorrected:
        _event(
            data_root / "events" / "log.jsonl",
            {
                "event": "complete_arg_order_autocorrected",
                "task": task_id,
                "worker": worker_id,
                "ts": ts,
            },
        )
    _event(
        data_root / "events" / "log.jsonl",
        {
            "event": "complete",
            "task": task_id,
            "status": "success",
            "worker": worker_id,
            "ts": ts,
        },
    )
    _move_if_exists(
        data_root / "tasks" / "claimed" / f"{task_id}.json",
        data_root / "tasks" / "done" / f"{task_id}.json",
    )
    print("ok")
    return 0


def fail(args: Any) -> int:
    task_id = getattr(args, "task_id", None)
    error = getattr(args, "error", None)
    worker_id = getattr(args, "worker_id", None) or "unknown"
    recovery_raw = getattr(args, "recovery_json", None) or ""
    artifact_type = getattr(args, "artifact_type", None) or ""
    if not task_id or error is None:
        print("usage: fail.sh <task-id> <error-string> [worker-id]", file=sys.stderr)
        return 2

    ws = _workspace(args)
    data_root = ws.data_root
    (data_root / "results").mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)
    (data_root / "tasks" / "done").mkdir(parents=True, exist_ok=True)
    ts = _ts()

    payload: dict[str, Any] = {
        "task_id": task_id,
        "worker": worker_id,
        "status": "failed",
        "result": None,
        "error": error,
        "ts": ts,
    }
    if artifact_type:
        payload["artifact_type"] = artifact_type
    if recovery_raw:
        try:
            payload["recovery"] = json.loads(recovery_raw)
        except Exception as exc:
            payload["recovery"] = {
                "repairMode": "manual_review",
                "reason": "invalid_recovery_json",
                "evidence": str(exc),
                "autoSafe": False,
            }
    (data_root / "results" / f"{task_id}.json").write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _move_if_exists(
        data_root / "tasks" / "claimed" / f"{task_id}.json",
        data_root / "tasks" / "done" / f"{task_id}.json",
    )
    _event(
        data_root / "events" / "log.jsonl",
        {
            "event": "complete",
            "task": task_id,
            "status": "failed",
            "worker": worker_id,
            "ts": ts,
        },
    )
    print("ok")
    return 0


def promote(args: Any) -> int:
    raw_path = getattr(args, "task_json_path", None)
    if not raw_path or not Path(raw_path).exists():
        print("usage: promote.sh <existing-task-file>", file=sys.stderr)
        return 2

    ws = _workspace(args)
    data_root = ws.data_root
    path = Path(raw_path)
    try:
        task = json.loads(path.read_text(encoding="utf-8"))
        capability = task.get("capability") or ""
    except Exception:
        capability = ""
    if not capability:
        print(f"could not parse capability from {raw_path}", file=sys.stderr)
        return 1

    ready = data_root / "tasks" / f"ready-{capability}"
    ready.mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)
    target = ready / path.name
    path.replace(target)
    _event(
        data_root / "events" / "log.jsonl",
        {
            "event": "promote",
            "task": path.stem,
            "capability": capability,
            "ts": _ts(),
        },
    )
    print(target)
    return 0


def _count(path: Path) -> int:
    if not path.is_dir():
        return 0
    return len(list(path.glob("*.json")))


def _pad(value: str) -> str:
    return f"{value:<22}"


def status(args: Any) -> int:
    ws = _workspace(args)
    data_root = ws.data_root
    print(f"=== {ws.root.name} status ===")
    print(f"{_pad('inbox/')} : {_count(data_root / 'inbox')}")
    print(f"{_pad('inbox/processed/')} : {_count(data_root / 'inbox' / 'processed')}")
    print(f"{_pad('tasks/pending/')} : {_count(data_root / 'tasks' / 'pending')}")

    ready_dirs = sorted((data_root / "tasks").glob("ready-*")) if (data_root / "tasks").is_dir() else []
    ready_dirs = [path for path in ready_dirs if path.is_dir()]
    if not ready_dirs:
        print(f"{_pad('tasks/ready-*')} : (no ready dirs yet)")
    else:
        for path in ready_dirs:
            rel = path.relative_to(data_root)
            print(f"{_pad(str(rel) + '/')} : {_count(path)}")

    print(f"{_pad('tasks/claimed/')} : {_count(data_root / 'tasks' / 'claimed')}")
    print(f"{_pad('tasks/done/')} : {_count(data_root / 'tasks' / 'done')}")
    print(f"{_pad('results/')} : {_count(data_root / 'results')}")

    finalized = len(list((data_root / "results").glob("*.final.json"))) if (data_root / "results").is_dir() else 0
    if finalized > 0:
        print(f"{_pad('  └ finalized')} : {finalized}")

    print()
    print("--- last 5 events ---")
    event_log = data_root / "events" / "log.jsonl"
    if event_log.exists():
        lines = event_log.read_text(encoding="utf-8").splitlines()
        for line in lines[-5:]:
            print(line)
    else:
        print("(no events yet)")
    return 0
