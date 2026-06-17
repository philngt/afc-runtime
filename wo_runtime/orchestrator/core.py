from __future__ import annotations

import argparse
import datetime as dt
import glob
import importlib
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wo_runtime.core.context import build_context
from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, load_and_prepare


REQUIRED_FIELDS = ("id", "req_id", "capability")


def ts() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        value = json.load(fh)
    return value if isinstance(value, dict) else {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)


def file_mtime(path: Path) -> dt.datetime | None:
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    except Exception:
        return None


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


@dataclass
class OrchestratorPaths:
    workspace_root: Path
    data_root: Path

    def p(self, *parts: str) -> Path:
        return self.data_root.joinpath(*parts)


class HookRunner:
    def __init__(self, module: Any | None, paths: OrchestratorPaths):
        self.module = module
        self.paths = paths

    @classmethod
    def load(cls, paths: OrchestratorPaths, config: dict[str, Any]) -> "HookRunner":
        hook_path = ((config.get("instanceHooks") or {}).get("orchestrator") or "").strip()
        if not hook_path:
            return cls(None, paths)
        if str(paths.workspace_root) not in sys.path:
            sys.path.insert(0, str(paths.workspace_root))
        try:
            module = importlib.import_module(hook_path)
        except Exception as exc:
            raise SystemExit(f"[orchestrator] cannot load hook {hook_path}: {type(exc).__name__}: {exc}") from exc
        return cls(module, paths)

    def call(self, name: str, *args: Any, default: Any = None, **kwargs: Any) -> Any:
        if self.module is None or not hasattr(self.module, name):
            return default
        fn = getattr(self.module, name)
        return fn(
            *args,
            **kwargs,
            data_root=self.paths.data_root,
            workspace_root=self.paths.workspace_root,
        )


class Orchestrator:
    def __init__(self, paths: OrchestratorPaths, hooks: HookRunner):
        self.paths = paths
        self.hooks = hooks

    def log(self, event: dict[str, Any]) -> None:
        event.setdefault("ts", ts())
        path = self.paths.p("events", "log.jsonl")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")

    def quarantine(self, path: Path, reason: str) -> None:
        target = self.paths.p("tasks", "invalid", path.name)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(path), str(target))
        except Exception:
            pass
        self.log({"event": "task_invalid", "path": self.rel(path), "reason": reason})
        self.hooks.call("after_task_invalid", self.rel(path), reason, default=None)
        print(f"[orch] quarantined {self.rel(path)}: {reason}", file=sys.stderr)

    def rel(self, path: Path) -> str:
        try:
            return path.relative_to(self.paths.data_root).as_posix()
        except ValueError:
            return str(path)

    def task_areas(self) -> list[Path]:
        fixed = [self.paths.p("tasks", "pending"), self.paths.p("tasks", "claimed"), self.paths.p("tasks", "done")]
        ready = [path for path in sorted(self.paths.p("tasks").glob("ready-*")) if path.is_dir()]
        return fixed + ready

    def latest_claims(self) -> dict[str, dict[str, Any]]:
        claims: dict[str, dict[str, Any]] = {}
        log_path = self.paths.p("events", "log.jsonl")
        if not log_path.exists():
            return claims
        for line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                event = json.loads(line)
            except Exception:
                continue
            if isinstance(event, dict) and event.get("event") == "claim" and event.get("task"):
                claims[str(event["task"])] = event
        return claims

    def reconcile_claimed_results_pass(self) -> None:
        claimed_dir = self.paths.p("tasks", "claimed")
        done_dir = self.paths.p("tasks", "done")
        results_dir = self.paths.p("results")
        archive_dir = self.paths.p("tasks", "_stale", "claimed-results", now().strftime("%Y%m%dT%H%M%SZ"))
        for claimed_path in sorted(claimed_dir.glob("*.json")) if claimed_dir.exists() else []:
            try:
                task = read_json(claimed_path)
            except Exception:
                continue
            task_id = str(task.get("id") or claimed_path.stem)
            result_path = results_dir / f"{task_id}.json"
            if not result_path.exists():
                continue
            try:
                result = read_json(result_path)
            except Exception:
                continue
            if result.get("task_id") not in {None, task_id}:
                continue
            if result.get("status") not in {"success", "failed"}:
                continue
            done_path = done_dir / claimed_path.name
            if done_path.exists():
                archive_dir.mkdir(parents=True, exist_ok=True)
                target = archive_dir / claimed_path.name
                shutil.move(str(claimed_path), str(target))
                self.log({
                    "event": "claimed_result_duplicate_archived",
                    "task": task_id,
                    "status": result.get("status"),
                    "from": self.rel(claimed_path),
                    "to": self.rel(target),
                })
                continue
            done_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(claimed_path), str(done_path))
            self.log({
                "event": "claimed_result_reconciled",
                "task": task_id,
                "status": result.get("status"),
                "from": self.rel(claimed_path),
                "to": self.rel(done_path),
            })

    def watchdog_pass(self) -> None:
        claims = self.latest_claims()
        default_timeout = int(os.environ.get("ORCH_CLAIM_TIMEOUT_SECONDS", "1800"))
        no_heartbeat_timeout = int(os.environ.get("ORCH_NO_HEARTBEAT_SECONDS", "600"))
        max_attempts = int(os.environ.get("ORCH_MAX_ATTEMPTS", "3"))
        claimed_dir = self.paths.p("tasks", "claimed")
        for claimed_path in sorted(claimed_dir.glob("*.json")) if claimed_dir.exists() else []:
            try:
                task = read_json(claimed_path)
            except Exception:
                continue
            task_id = str(task.get("id") or claimed_path.stem)
            claim_event = claims.get(task_id, {})
            claim_time = parse_ts(str(claim_event.get("ts") or ""))
            claimed_file_time = file_mtime(claimed_path)
            if claimed_file_time and (claim_time is None or claim_time < claimed_file_time):
                claim_time = claimed_file_time
            heartbeat = self.hooks.call("claimed_task_heartbeat_time", task, default=None)
            if not isinstance(heartbeat, dt.datetime):
                heartbeat = self.heartbeat_updated_at(task)
            base_time = heartbeat or claim_time
            if not base_time:
                continue
            timeout = default_timeout if heartbeat is not None else no_heartbeat_timeout
            hook_timeout = self.hooks.call(
                "claimed_task_timeout_seconds",
                task,
                has_heartbeat=heartbeat is not None,
                default=None,
            )
            if hook_timeout is not None:
                timeout = int(hook_timeout)
            age = (now() - base_time).total_seconds()
            if age < timeout:
                continue

            action = self.hooks.call(
                "before_claim_timeout",
                task,
                claimed_path,
                age_seconds=age,
                max_attempts=max_attempts,
                default=None,
            )
            if action == "handled":
                continue

            attempt = int(task.get("attempt", 0)) + 1
            if attempt > max_attempts:
                result_path = self.paths.p("results", f"{task_id}.json")
                write_json(result_path, {
                    "task_id": task_id,
                    "worker": "orchestrator",
                    "status": "failed",
                    "result": None,
                    "error": f"claim_timeout:max_attempts:{max_attempts}",
                    "ts": ts(),
                })
                done_path = self.paths.p("tasks", "done", claimed_path.name)
                done_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(claimed_path), str(done_path))
                self.log({"event": "claim_timeout_failed", "task": task_id, "attempt": attempt})
                self.hooks.call("after_claim_timeout_failed", task_id, max_attempts, default=None)
                continue
            cap = str(task.get("capability") or "")
            if not cap:
                continue
            task["attempt"] = attempt
            write_json(claimed_path, task)
            ready_path = self.paths.p("tasks", f"ready-{cap}", claimed_path.name)
            ready_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(claimed_path), str(ready_path))
            self.log({
                "event": "claim_timeout_requeued",
                "task": task_id,
                "capability": cap,
                "attempt": attempt,
                "age_seconds": int(age),
            })

    def heartbeat_updated_at(self, task: dict[str, Any]) -> dt.datetime | None:
        task_id = str(task.get("id") or "")
        if not task_id:
            return None
        candidates = [
            self.paths.data_root / "heartbeats" / f"{task_id}.json",
            self.paths.workspace_root / ".run" / "heartbeats" / f"{task_id}.json",
        ]
        seen: set[Path] = set()
        for path in candidates:
            if path in seen or not path.exists():
                continue
            seen.add(path)
            try:
                heartbeat = read_json(path)
            except Exception:
                continue
            if heartbeat.get("task_id") != task_id:
                continue
            try:
                pid = int(heartbeat.get("pid") or 0)
            except (TypeError, ValueError):
                pid = 0
            if pid and not pid_alive(pid):
                try:
                    path.unlink()
                except OSError:
                    pass
                continue
            return parse_ts(str(heartbeat.get("updated_at") or heartbeat.get("updatedAt") or ""))
        return None

    def recover_dep_failed_pass(self) -> None:
        backup_dir = self.paths.p("results", "_recovered")
        for result_path in sorted(self.paths.p("results").glob("*.json")):
            try:
                result = read_json(result_path)
            except Exception:
                continue
            if result.get("status") != "failed":
                continue
            error = str(result.get("error") or "")
            if not error.startswith("dep_failed:"):
                continue
            dep = error.split(":", 1)[1]
            dep_path = self.paths.p("results", f"{dep}.json")
            if not dep_path.exists():
                continue
            try:
                dep_result = read_json(dep_path)
            except Exception:
                continue
            if dep_result.get("status") != "success":
                continue
            task_id = str(result.get("task_id") or result_path.stem)
            task_path = self.paths.p("tasks", "done", f"{task_id}.json")
            if not task_path.exists():
                continue
            backup_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(result_path), str(backup_dir / f"{result_path.stem}.{ts()}.json"))
            pending_path = self.paths.p("tasks", "pending", task_path.name)
            pending_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(task_path), str(pending_path))
            final_path = self.paths.p("results", f"{task_id.split('.')[0]}.final.json")
            if final_path.exists():
                shutil.move(str(final_path), str(backup_dir / f"{final_path.stem}.{ts()}.json"))
            self.log({"event": "dep_fail_recovered", "task": task_id, "dep": dep})

    def promotion_pass(self) -> None:
        self.hooks.call("before_promotion_pass", default=None)
        pending_dir = self.paths.p("tasks", "pending")
        for task_path in sorted(pending_dir.glob("*.json")) if pending_dir.exists() else []:
            try:
                task = read_json(task_path)
            except Exception as exc:
                self.quarantine(task_path, f"json_parse_error:{exc}")
                continue
            missing = [field for field in REQUIRED_FIELDS if field not in task]
            if missing:
                self.quarantine(task_path, f"missing_fields:{','.join(missing)}")
                continue
            deps = task.get("depends_on", [])
            if deps is None:
                deps = []
            if not isinstance(deps, list):
                self.quarantine(task_path, "invalid_depends_on")
                continue

            hooked_task = self.hooks.call("before_pending_task", task, task_path, default=task)
            if hooked_task is None:
                continue
            task = hooked_task

            hold_until = parse_ts(str(task.get("hold_until") or ""))
            if hold_until and hold_until > now():
                continue
            if "hold_until" in task or "hold_reason" in task:
                for key in ("hold_until", "hold_reason", "held_by", "held_at"):
                    task.pop(key, None)
                write_json(task_path, task)

            ready = True
            failed_dep = None
            failed_dep_result = None
            for dep in deps:
                result_path = self.paths.p("results", f"{dep}.json")
                if not result_path.exists():
                    ready = False
                    break
                try:
                    dep_result = read_json(result_path)
                except Exception:
                    ready = False
                    break
                if dep_result.get("status") != "success":
                    failed_dep = str(dep)
                    failed_dep_result = dep_result
                    break
            if failed_dep is not None:
                defer = self.hooks.call(
                    "should_defer_dep_failure",
                    task,
                    failed_dep,
                    failed_dep_result or {},
                    default=None,
                )
                if defer:
                    if isinstance(defer, tuple):
                        _, reason = defer
                    elif isinstance(defer, dict):
                        reason = str(defer.get("reason") or "deferred")
                    else:
                        reason = "deferred"
                    changed = (
                        task.get("blocked_by_dep") != failed_dep
                        or task.get("blocked_reason") != reason
                        or task.get("blocked_status") != "waiting_for_recovery"
                    )
                    task["blocked_by_dep"] = failed_dep
                    task["blocked_reason"] = reason
                    task["blocked_status"] = "waiting_for_recovery"
                    if changed:
                        task["blocked_at"] = ts()
                        write_json(task_path, task)
                        self.log({"event": "dep_fail_deferred", "task": task["id"], "dep": failed_dep, "reason": reason})
                    continue
                result_path = self.paths.p("results", f"{task['id']}.json")
                write_json(result_path, {
                    "task_id": task["id"],
                    "worker": "orchestrator",
                    "status": "failed",
                    "result": None,
                    "error": f"dep_failed:{failed_dep}",
                    "ts": ts(),
                })
                done_path = self.paths.p("tasks", "done", task_path.name)
                done_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(task_path), str(done_path))
                self.log({"event": "dep_fail_propagated", "task": task["id"], "dep": failed_dep})
                continue
            if not ready:
                continue
            for key in ("blocked_by_dep", "blocked_reason", "blocked_status", "blocked_at"):
                task.pop(key, None)
            hooked_task = self.hooks.call("before_task_promote", task, default=task)
            if hooked_task is None:
                continue
            task = hooked_task
            try:
                workspace = load_and_prepare(self.paths.workspace_root)
                pack_cfg = workspace.config.get("contextPacks") if isinstance(workspace.config.get("contextPacks"), dict) else {}
                context = build_context(str(task.get("id") or task_path.stem), workspace=workspace)
                warnings = context.get("stateWarnings") if isinstance(context.get("stateWarnings"), list) else []
                if warnings:
                    self.log({
                        "event": "context_state_warning",
                        "task": task.get("id") or task_path.stem,
                        "capability": task.get("capability"),
                        "warnings": warnings,
                    })
                    if pack_cfg.get("strictOnPromote"):
                        first = warnings[0] if isinstance(warnings[0], dict) else {}
                        task["blocked_status"] = "context_warning"
                        task["blocked_reason"] = str(first.get("code") or "context_state_warning")
                        task["blocked_at"] = ts()
                        write_json(task_path, task)
                        continue
                pack = context.get("contextPack")
                if isinstance(pack, dict) and pack.get("status") != "disabled":
                    task["contextPack"] = pack
            except Exception as exc:
                self.log({
                    "event": "context_pack_warning",
                    "task": task.get("id") or task_path.stem,
                    "capability": task.get("capability"),
                    "error": f"{type(exc).__name__}: {exc}",
                })
                cfg = load_and_prepare(self.paths.workspace_root).config
                pack_cfg = cfg.get("contextPacks") if isinstance(cfg.get("contextPacks"), dict) else {}
                if pack_cfg.get("strictOnPromote"):
                    task["blocked_status"] = "context_pack_failed"
                    task["blocked_reason"] = f"{type(exc).__name__}: {exc}"
                    task["blocked_at"] = ts()
                    write_json(task_path, task)
                    continue
            write_json(task_path, task)
            cap = str(task["capability"])
            ready_path = self.paths.p("tasks", f"ready-{cap}", task_path.name)
            ready_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(task_path), str(ready_path))
            self.hooks.call("after_task_promote", task, default=None)
            self.log({"event": "promote", "task": task["id"], "capability": cap})

    def finalization_pass(self) -> None:
        reqs: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
        for area in self.task_areas():
            for task_path in glob.glob(str(area / "*.json")):
                path = Path(task_path)
                try:
                    task = read_json(path)
                except Exception:
                    continue
                req_id = task.get("req_id")
                if req_id:
                    reqs.setdefault(str(req_id), []).append((area, task))
        for req_id, items in reqs.items():
            final_path = self.paths.p("results", f"{req_id}.final.json")
            if final_path.exists():
                continue
            if not all(area == self.paths.p("tasks", "done") for area, _ in items):
                continue
            root_task = next((task for _, task in items if task.get("is_root")), None)
            if root_task is None:
                continue
            root_result_path = self.paths.p("results", f"{root_task['id']}.json")
            if not root_result_path.exists():
                continue
            try:
                root_result = read_json(root_result_path)
            except Exception:
                continue

            trace = []
            for _, task in sorted(items, key=lambda item: item[1]["id"]):
                result_path = self.paths.p("results", f"{task['id']}.json")
                short = str(task["id"]).split(".")[-1]
                if result_path.exists():
                    result = read_json(result_path)
                    if result.get("status") == "success":
                        trace.append(f"{short}={result.get('result')}")
                    else:
                        trace.append(f"{short}=FAIL:{result.get('error', '')}")
                else:
                    trace.append(f"{short}=?")
            final = {
                "req_id": req_id,
                "status": root_result.get("status"),
                "result": root_result.get("result"),
                "trace": trace,
                "ts": ts(),
            }
            if root_result.get("status") == "failed":
                final["error"] = root_result.get("error", "")

            task_list = [task for _, task in items]
            final = self.hooks.call(
                "before_final_write",
                req_id,
                task_list,
                root_task,
                root_result,
                final,
                default=final,
            )
            if not isinstance(final, dict):
                raise SystemExit(f"[orchestrator] before_final_write returned non-object for {req_id}")
            write_json(final_path, final)
            self.log({"event": "finalized", "req_id": req_id, "status": final.get("status")})
            if final.get("status") == "success":
                self.hooks.call("after_final_success", req_id, final, task_list, default=None)
            else:
                self.hooks.call("after_final_failed", req_id, final, task_list, default=None)
            print(f"[orch] finalized {req_id} -> {final.get('status')} result={final.get('result')}")

    def tick(self) -> None:
        self.hooks.call("before_tick", default=None)
        self.reconcile_claimed_results_pass()
        self.watchdog_pass()
        self.recover_dep_failed_pass()
        self.promotion_pass()
        self.finalization_pass()
        self.hooks.call("after_tick", default=None)


def build_orchestrator(workspace_root: str | None, workspace_config: str) -> Orchestrator:
    workspace = load_and_prepare(workspace_root, workspace_config)
    workspace_root_path = workspace.root.resolve()
    data_root = workspace.data_root if workspace.data_root.is_absolute() else workspace_root_path / workspace.data_root
    data_root = data_root.resolve()
    os.environ["PIPELINE_DATA_ROOT"] = str(data_root)
    data_root.mkdir(parents=True, exist_ok=True)
    paths = OrchestratorPaths(workspace_root=workspace_root_path, data_root=data_root)
    hooks = HookRunner.load(paths, workspace.config)
    return Orchestrator(paths, hooks)


def tick_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one generic orchestrator tick.")
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    args = parser.parse_args(argv)
    build_orchestrator(args.workspace_root, args.workspace_config).tick()
    return 0


def daemon_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the generic orchestrator daemon.")
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    parser.add_argument("--interval", type=float, default=float(os.environ.get("ORCH_INTERVAL", "5")))
    args = parser.parse_args(argv)
    print(f"[orchestrator-daemon] starting (interval={args.interval:g}s)")
    try:
        while True:
            try:
                build_orchestrator(args.workspace_root, args.workspace_config).tick()
            except Exception as exc:
                print(f"[orchestrator-daemon] tick failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("[orchestrator-daemon] stopping")
        return 130
