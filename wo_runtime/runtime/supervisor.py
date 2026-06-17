#!/usr/bin/env python
"""Generic headless runtime supervisor.

Drives ANY runtime that exposes a one-pass / headless "run this prompt" command,
described by runtime/runtimes/<name>.json. It watches each role's queue and launches
passes when work is present -- no role env is set (that would enable the interactive
Stop-hook watcher instead).

Concurrency: each role may run up to `max_inflight` passes at once (real intra-role
parallelism -- each `exec` claims a distinct ready task via the atomic queue). The per-role
cap comes from runtime/fleet.json (`replicas`) or --max-inflight (default 1).

Roles come from runtime/roles.json. Add a new runtime by dropping a descriptor in
runtime/runtimes/ -- no code change here.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from wo_runtime.runtime.context import resolve_context_prepend
from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, load_workspace

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


RUNTIME_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOT = Path.cwd()
STUCK_THRESHOLD = 3
LOG_LEVELS = {"debug": 10, "info": 20, "warn": 30, "error": 40}
ERROR_RE = re.compile(r"\b(error|failed|failure|exception|traceback|fatal)\b", re.I)
WARN_RE = re.compile(r"\b(warn|warning|blocked|stuck|retry|timeout)\b", re.I)
QUOTA_RE = re.compile(
    r"\b("
    r"quota|rate[- ]?limit|rate limited|too many requests|429|usage limit|"
    r"credit[s]? exhausted|insufficient quota|limit exceeded"
    r")\b",
    re.I,
)


@dataclass
class ChildLogState:
    quota_seen: bool = False
    last_quota_line: str = ""


@dataclass
class Invocation:
    process: subprocess.Popen[str]
    trigger: tuple[str, int, int]
    log_state: ChildLogState = field(default_factory=ChildLogState)
    log_thread: threading.Thread | None = None
    heartbeat_thread: threading.Thread | None = None


@dataclass
class RoleState:
    inflight: list[Invocation] = field(default_factory=list)
    blocked: set[str] = field(default_factory=set)   # stuck file paths, ignored when launching
    quota_pause_until: float = 0.0
    quota_attempts: int = 0
    quota_notified: bool = False
    last_head: str | None = None
    no_progress: int = 0


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"[supervisor] cannot load {path}: {exc}") from exc


def load_descriptor(root: Path, name: str) -> dict:
    d = load_json(root / "runtime" / "runtimes" / f"{name}.json")
    if "exec" not in d:
        raise SystemExit(f"[supervisor] runtime '{name}' has no 'exec' block (not headless-capable)")
    return d


def load_roles(root: Path) -> dict[str, dict]:
    return load_json(root / "runtime" / "roles.json")


def load_fleet(root: Path) -> dict[str, dict]:
    p = root / "runtime" / "fleet.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def resolve_bin(descriptor: dict, explicit: str | None) -> str:
    if explicit:
        return explicit
    env_key = descriptor.get("bin_env")
    if env_key and os.environ.get(env_key):
        return os.environ[env_key]
    for candidate in descriptor.get("bin", []):
        found = shutil.which(candidate)
        if found:
            return found
    raise SystemExit(
        f"[supervisor] cannot find runtime '{descriptor['name']}' binary; "
        f"set {descriptor.get('bin_env', 'the binary env override')}"
    )


def role_watches(cfg: dict) -> list[str]:
    w = cfg.get("watch")
    if not w:
        return []
    return [w] if isinstance(w, str) else list(w)


def pattern_for(cfg: dict, watch: str) -> str:
    m = cfg.get("trigger_match", "*.json")
    return str(m.get(watch, "*.json")) if isinstance(m, dict) else str(m)


def is_excluded(root: Path, cfg: dict, path: Path) -> bool:
    tpl = cfg.get("exclude_if_sibling_exists")
    if not tpl:
        return False
    name = path.name
    cands = [tpl.format(name=name), tpl.format(name=name.replace(".final.json", ".json"))]
    return any((root / c).exists() for c in cands)


def list_triggers(root: Path, cfg: dict, ignore: set[str]) -> list[tuple[str, int, int]]:
    out = []
    for watch in role_watches(cfg):
        base = root / watch
        for path in sorted(base.glob(pattern_for(cfg, watch))):
            rel = str(path.relative_to(root))
            if path.name.startswith(".") or not path.is_file() or rel in ignore:
                continue
            if is_excluded(root, cfg, path):
                continue
            st = path.stat()
            out.append((rel, st.st_mtime_ns, st.st_size))
    return out


def resolve_model(descriptor: dict, role_name: str, cfg: dict,
                  fleet: dict, explicit: str | None) -> str | None:
    """Resolution order (most specific first):
    1. explicit --model CLI arg
    2. fleet.json[<role>].model           -- deployment-time override
    3. roles.json[<role>].models[<runtime>] -- prompt-time hint
    4. runtime descriptor default_model
    """
    if explicit:
        return explicit
    fleet_model = (fleet.get(role_name) or {}).get("model")
    if fleet_model:
        return fleet_model
    by_rt = cfg.get("models") or {}
    return by_rt.get(descriptor["name"]) or descriptor.get("default_model")


def subst(args: list[str], mapping: dict[str, str]) -> list[str]:
    out = []
    for a in args:
        for k, v in mapping.items():
            a = a.replace("{" + k + "}", v)
        out.append(a)
    return out


def build_command(descriptor: dict, root: Path, cfg: dict, model: str | None,
                  role_name: str = "", trigger_path: str | None = None) -> list[str]:
    spec = descriptor["exec"]
    cmd = [descriptor["_bin"]] + subst(spec.get("args", []), {"cwd": str(root), "model": model or ""})
    if model and spec.get("model_flag"):
        cmd += subst(spec["model_flag"], {"model": model})
    payload_field = spec.get("payload")
    if payload_field and payload_field != "none":
        payload = str(cfg.get(payload_field, "")).replace("{worker_id}", str(cfg.get("worker_id", "")))
        prepend = resolve_context_prepend(descriptor, role_name, root, trigger_path=trigger_path)
        if prepend:
            payload = prepend + payload
        if spec.get("payload_position", "last") == "last":
            cmd.append(payload)
    return cmd


def printable(cmd: list[str]) -> str:
    return subprocess.list2cmdline(cmd) if os.name == "nt" else shlex.join(cmd)


def should_log(args: argparse.Namespace, level: str) -> bool:
    return LOG_LEVELS[level] >= LOG_LEVELS[args.log_level]


def log(args: argparse.Namespace, level: str, msg: str, *, file: Any = None) -> None:
    if should_log(args, level):
        print(msg, file=file or sys.stdout)


def child_line_level(line: str) -> str | None:
    if ERROR_RE.search(line):
        return "error"
    if WARN_RE.search(line):
        return "warn"
    return None


def stream_child_output(pipe: Any, role: str, trigger: str, child_log_level: str,
                        log_state: ChildLogState) -> None:
    traceback_lines_remaining = 0
    try:
        for line in pipe:
            text = line.rstrip("\n")
            if QUOTA_RE.search(text):
                log_state.quota_seen = True
                log_state.last_quota_line = text[-500:]
            if child_log_level == "none":
                continue
            if child_log_level == "all":
                print(text, flush=True)
                continue
            if traceback_lines_remaining > 0:
                print(f"[child:{role} {trigger}] {text}", flush=True)
                traceback_lines_remaining -= 1
                continue
            level = child_line_level(text)
            if not level:
                continue
            if LOG_LEVELS[level] >= LOG_LEVELS[child_log_level]:
                print(f"[child:{role} {trigger}] {text}", flush=True)
                if "Traceback (most recent call last):" in text:
                    traceback_lines_remaining = 30
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def notify_runtime_event(root: Path, event: str, role: str, trigger: str,
                         message: str, dedupe_key: str) -> None:
    script = root / "scripts" / "gateway" / "tg-notify-runtime.py"
    if not script.exists() or not os.environ.get("TELEGRAM_BOT_TOKEN"):
        return
    try:
        subprocess.run(
            [
                sys.executable,
                str(script),
                "--event",
                event,
                "--role",
                role,
                "--trigger",
                trigger,
                "--message",
                message,
                "--dedupe-key",
                dedupe_key,
            ],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except Exception:
        pass


def quota_backoff_seconds(attempt: int) -> float:
    base = float(os.environ.get("SUPERVISOR_QUOTA_BACKOFF_SECONDS", "300"))
    maximum = float(os.environ.get("SUPERVISOR_QUOTA_BACKOFF_MAX_SECONDS", "3600"))
    return min(maximum, base * (2 ** max(0, attempt - 1)))


def quota_state_path(root: Path, runtime: str, role: str) -> Path:
    safe_runtime = re.sub(r"[^A-Za-z0-9_.-]+", "_", runtime)
    return root / ".run" / "runtime-quota" / f"{safe_runtime}.json"


def load_quota_state(root: Path, runtime: str, role: str) -> dict[str, Any]:
    path = quota_state_path(root, runtime, role)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_quota_state(root: Path, runtime: str, role: str, data: dict[str, Any]) -> None:
    path = quota_state_path(root, runtime, role)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def clear_quota_state(root: Path, runtime: str, role: str) -> None:
    try:
        quota_state_path(root, runtime, role).unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


def project_python(root: Path) -> str:
    explicit = os.environ.get("PYTHON") or os.environ.get("PY")
    if explicit:
        return explicit
    for rel in ("venv/bin/python", ".venv/bin/python"):
        candidate = root / rel
        if candidate.exists():
            return str(candidate)
    return sys.executable


def runtime_env(root: Path, drop_env: set[str]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in drop_env}
    py = project_python(root)
    py_dir = str(Path(py).parent)
    env["PY"] = py
    env["PYTHON"] = py
    env["PYTHONPATH"] = os.pathsep.join(
        [str(root), str(RUNTIME_ROOT)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )
    env["PATH"] = py_dir + os.pathsep + env.get("PATH", "")
    return env


def build_startup_probe_command(descriptor: dict, root: Path, model: str | None) -> list[str]:
    spec = descriptor["exec"]
    cmd = [descriptor["_bin"]] + subst(spec.get("args", []), {"cwd": str(root), "model": model or ""})
    if model and spec.get("model_flag"):
        cmd += subst(spec["model_flag"], {"model": model})
    if spec.get("payload") and spec.get("payload") != "none":
        prompt = (
            "Runtime startup quota probe. Do not inspect files, do not modify files, "
            "do not claim queue tasks. Reply exactly: OK"
        )
        if spec.get("payload_position", "last") == "last":
            cmd.append(prompt)
    return cmd


def run_startup_probe(args: argparse.Namespace, root: Path, descriptor: dict,
                      drop_env: set[str]) -> None:
    quota_state = load_quota_state(root, args.runtime, "startup-probe")
    now_ts = time.time()
    pause_until = float(quota_state.get("pause_until") or 0.0)
    if pause_until > now_ts and not args.startup_probe_force:
        log(args, "warn", f"[supervisor] runtime={args.runtime} startup probe skipped; "
            f"quota circuit breaker active for {int(pause_until - now_ts)}s", file=sys.stderr)
        return

    model = args.model or descriptor.get("default_model")
    cmd = build_startup_probe_command(descriptor, root, model)
    if args.dry_run:
        log(args, "info", f"[supervisor] startup probe would run: {printable(cmd)}")
        return

    timeout = float(os.environ.get("SUPERVISOR_STARTUP_PROBE_TIMEOUT_SECONDS", "120"))
    log(args, "info", f"[supervisor] runtime={args.runtime} startup quota probe")
    try:
        proc = subprocess.run(
            cmd,
            cwd=root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            env=runtime_env(root, drop_env),
            check=False,
        )
    except subprocess.TimeoutExpired:
        log(args, "warn", f"[supervisor] runtime={args.runtime} startup probe timed out; "
            "leaving quota state unchanged", file=sys.stderr)
        return
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError) as exc:
        log(args, "error", f"[supervisor] runtime={args.runtime} startup probe launch failed: "
            f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return

    output = proc.stdout or ""
    quota_seen = bool(QUOTA_RE.search(output))
    if args.child_log_level != "none":
        for line in output.splitlines():
            if args.child_log_level == "all":
                print(f"[child:startup-probe startup-probe] {line}", flush=True)
                continue
            level = child_line_level(line)
            if level and LOG_LEVELS[level] >= LOG_LEVELS[args.child_log_level]:
                print(f"[child:startup-probe startup-probe] {line}", flush=True)

    had_quota_state = bool(quota_state)
    if quota_seen and proc.returncode != 0:
        attempt = int(quota_state.get("attempt", 0)) + 1
        delay = quota_backoff_seconds(attempt)
        state = {
            "runtime": args.runtime,
            "role": "startup-probe",
            "trigger": "startup-probe",
            "attempt": attempt,
            "pause_until": time.time() + delay,
            "pause_seconds": int(delay),
            "reason": next((line[-500:] for line in output.splitlines() if QUOTA_RE.search(line)), "quota_or_rate_limit"),
            "updated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        save_quota_state(root, args.runtime, "startup-probe", state)
        log(args, "warn", f"[supervisor] runtime={args.runtime} startup probe detected quota/rate-limit; "
            f"pausing runtime for {int(delay)}s", file=sys.stderr)
        notify_runtime_event(
            root,
            "quota_blocked",
            "startup-probe",
            "startup-probe",
            (
                f"Codex quota/rate limit detected during startup probe. "
                f"Supervisor paused the {args.runtime} runtime before launching queue tasks. "
                f"It will probe again after {int(delay)} seconds."
            ),
            f"{args.runtime}:startup-probe:quota_blocked",
        )
        return

    if proc.returncode == 0 and had_quota_state:
        clear_quota_state(root, args.runtime, "startup-probe")
        notify_runtime_event(
            root,
            "quota_resumed",
            "startup-probe",
            "startup-probe",
            f"Codex startup probe succeeded. The {args.runtime} runtime is available again.",
            f"{args.runtime}:startup-probe:quota_resumed:{int(time.time() // 3600)}",
        )


def heartbeat_child(root: Path, data_root: Path, trigger: str, runtime: str, role: str,
                   proc: subprocess.Popen[str]) -> None:
    interval = max(1.0, float(os.environ.get("SUPERVISOR_HEARTBEAT_SECONDS", "15")))
    task_id = Path(trigger).stem
    hb_dir = data_root / "heartbeats"
    hb_dir.mkdir(parents=True, exist_ok=True)
    hb_path = hb_dir / f"{task_id}.json"
    started_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        while proc.poll() is None:
            updated_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            data = {
                "task_id": task_id,
                "trigger": trigger,
                "runtime": runtime,
                "role": role,
                "pid": proc.pid,
                "dataRoot": str(data_root),
                "startedAt": started_at,
                "updatedAt": updated_at,
                "updated_at": updated_at,
            }
            tmp = hb_path.with_suffix(".tmp")
            try:
                tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                tmp.replace(hb_path)
            except OSError:
                pass
            time.sleep(interval)
    finally:
        try:
            hb_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def supervisable(descriptor: dict, roles: dict[str, dict]) -> dict[str, dict]:
    payload_field = descriptor["exec"].get("payload", "prompt")
    return {n: c for n, c in roles.items() if payload_field == "none" or c.get(payload_field)}


def run(args: argparse.Namespace) -> int:
    root_arg = args.workspace_root or args.root
    workspace = load_workspace(root_arg, args.workspace_config)
    root = workspace.root
    data_root = workspace.data_root if workspace.data_root.is_absolute() else root / workspace.data_root
    descriptor = load_descriptor(root, args.runtime)
    descriptor["_bin"] = resolve_bin(descriptor, args.bin)
    roles = supervisable(descriptor, load_roles(root))
    fleet = load_fleet(root)
    if args.role:
        missing = [r for r in args.role if r not in roles]
        if missing:
            raise SystemExit(f"[supervisor] unknown/unsupervisable roles: {', '.join(missing)}")
        roles = {r: roles[r] for r in args.role}

    def cap_for(name: str) -> int:
        if args.max_inflight is not None:
            return max(1, args.max_inflight)
        return max(1, int((fleet.get(name) or {}).get("replicas", 1)))

    role_env = descriptor.get("role_env", {})
    drop_env = set(role_env.values())
    state: dict[str, RoleState] = {n: RoleState() for n in roles}
    caps = {n: cap_for(n) for n in roles}
    if args.startup_probe:
        run_startup_probe(args, root, descriptor, drop_env)
        if args.startup_probe_only:
            return 0
    log(args, "info", f"[supervisor] runtime={args.runtime} root={root} dry_run={args.dry_run} "
        f"roles={{{', '.join(f'{n}x{caps[n]}' for n in roles)}}}")
    try:
        while True:
            for name, cfg in roles.items():
                s = state[name]
                alive = []
                for i in s.inflight:
                    if i.process.poll() is None:
                        alive.append(i)
                    else:
                        if i.log_thread:
                            i.log_thread.join(timeout=0.2)
                        if i.heartbeat_thread:
                            i.heartbeat_thread.join(timeout=0.2)
                        rc = i.process.returncode
                        trigger_name = i.trigger[0]
                        had_quota_state = bool(load_quota_state(root, args.runtime, name))
                        if i.log_state.quota_seen and rc != 0:
                            persisted = load_quota_state(root, args.runtime, name)
                            attempt = max(int(persisted.get("attempt", 0)), s.quota_attempts) + 1
                            s.quota_attempts = attempt
                            delay = quota_backoff_seconds(attempt)
                            s.quota_pause_until = time.time() + delay
                            save_quota_state(root, args.runtime, name, {
                                "runtime": args.runtime,
                                "role": name,
                                "trigger": trigger_name,
                                "attempt": attempt,
                                "pause_until": s.quota_pause_until,
                                "pause_seconds": int(delay),
                                "reason": i.log_state.last_quota_line or "quota_or_rate_limit",
                                "updated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            })
                            s.last_head, s.no_progress = None, 0
                            msg = (
                                f"[supervisor] role={name} trigger={trigger_name} quota/rate-limit detected; "
                                f"pausing runtime '{args.runtime}' for {int(delay)}s (attempt {attempt})."
                            )
                            log(args, "warn", msg, file=sys.stderr)
                            if not s.quota_notified:
                                notify_runtime_event(
                                    root,
                                    "quota_blocked",
                                    name,
                                    trigger_name,
                                    (
                                        f"Codex quota/rate limit detected for {name} on {trigger_name}. "
                                        f"Supervisor paused the {args.runtime} runtime before launching more Codex calls. "
                                        f"It will probe again after {int(delay)} seconds."
                                    ),
                                    f"{args.runtime}:{name}:quota_blocked",
                                )
                                s.quota_notified = True
                        elif rc == 0:
                            s.quota_attempts = 0
                            s.quota_pause_until = 0.0
                            if s.quota_notified or had_quota_state:
                                notify_runtime_event(
                                    root,
                                    "quota_resumed",
                                    name,
                                    trigger_name,
                                    (
                                        f"Codex quota appears available again. {name} completed a pass for "
                                        f"{trigger_name}; workflow has resumed."
                                    ),
                                    f"{args.runtime}:{name}:quota_resumed:{int(time.time() // 3600)}",
                                )
                                s.quota_notified = False
                                clear_quota_state(root, args.runtime, name)
                s.inflight = alive
                now_ts = time.time()
                quota_state = load_quota_state(root, args.runtime, name)
                pause_until = max(float(quota_state.get("pause_until") or 0.0), s.quota_pause_until)
                if pause_until > now_ts:
                    s.quota_pause_until = pause_until
                    s.last_head = None
                    s.no_progress = 0
                    continue
                ready = list_triggers(root, cfg, ignore=s.blocked)
                if not ready:
                    s.last_head = None
                    s.no_progress = 0
                    continue
                # stuck detection: head unchanged across scans while nothing is in flight
                head = ready[0][0]
                if not s.inflight:
                    if head == s.last_head:
                        s.no_progress += 1
                    else:
                        s.last_head, s.no_progress = head, 1
                    if s.no_progress >= STUCK_THRESHOLD:
                        s.blocked.add(head)
                        s.last_head, s.no_progress = None, 0
                        log(args, "warn", f"[supervisor] role={name} '{head}' reappeared {STUCK_THRESHOLD}x "
                            "uncleared; marking stuck, only NEW work will launch it.", file=sys.stderr)
                        continue
                targeted = {i.trigger[0] for i in s.inflight}
                slots = caps[name] - len(s.inflight)
                launchable = [t for t in ready if t[0] not in targeted][:max(0, slots)]
                for trig in launchable:
                    cmd = build_command(descriptor, root, cfg,
                                        resolve_model(descriptor, name, cfg, fleet, args.model),
                                        role_name=name,
                                        trigger_path=trig[0])
                    log(args, "info", f"[supervisor] role={name} trigger={trig[0]}")
                    if args.dry_run:
                        log(args, "info", f"[supervisor] would run: {printable(cmd)}")
                    else:
                        try:
                            proc = subprocess.Popen(
                                cmd, cwd=root, text=True,
                                env=runtime_env(root, drop_env),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                bufsize=1,
                            )
                        except (FileNotFoundError, NotADirectoryError, PermissionError, OSError) as exc:
                            # Common causes: runtime binary not found on PATH (e.g.
                            # codex.cmd absent under WSL), CWD missing, or perms.
                            # Mark this trigger blocked so the supervisor doesn't
                            # spin retrying the same broken command -- one role's
                            # launch failure must not crash the whole supervisor.
                            s.blocked.add(trig[0])
                            log(args, "error", f"[supervisor] role={name} trigger={trig[0]} LAUNCH FAILED: "
                                f"{type(exc).__name__}: {exc}", file=sys.stderr)
                            log(args, "error", f"[supervisor]   cmd[0]={cmd[0]!r}  cwd={root}", file=sys.stderr)
                            log(args, "error", f"[supervisor]   blocked '{trig[0]}'; fix the runtime bin or "
                                "remove the trigger, then restart.", file=sys.stderr)
                            continue
                        log_thread = None
                        log_state = ChildLogState()
                        if proc.stdout is not None:
                            log_thread = threading.Thread(
                                target=stream_child_output,
                                args=(proc.stdout, name, trig[0], args.child_log_level, log_state),
                                daemon=True,
                            )
                            log_thread.start()
                        heartbeat_thread = threading.Thread(
                            target=heartbeat_child,
                            args=(root, data_root, trig[0], args.runtime, name, proc),
                            daemon=True,
                        )
                        heartbeat_thread.start()
                        s.inflight.append(Invocation(
                            process=proc,
                            trigger=trig,
                            log_state=log_state,
                            log_thread=log_thread,
                            heartbeat_thread=heartbeat_thread,
                        ))

            if args.once:
                alive = [i for s in state.values() for i in s.inflight]
                if args.dry_run or not alive:
                    return 0
                return max(i.process.wait() for i in alive)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log(args, "warn", "[supervisor] stopping", file=sys.stderr)
        for s in state.values():
            for i in s.inflight:
                i.process.terminate()
        return 130


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runtime", required=True, help="runtime descriptor name (runtime/runtimes/<name>.json)")
    p.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    p.add_argument("--workspace-root", default=None)
    p.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    p.add_argument("--role", action="append", help="only this role (repeatable)")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--max-inflight", type=int, default=None,
                   help="concurrent passes per role (overrides runtime/fleet.json replicas)")
    p.add_argument("--model", help="model override (else roles.json models.<runtime> or descriptor default)")
    p.add_argument("--bin", dest="bin", help="runtime binary override")
    p.add_argument("--codex-bin", dest="bin", help=argparse.SUPPRESS)  # back-compat alias
    p.add_argument("--once", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--startup-probe", action="store_true",
                   help="run one queue-free runtime probe before launching queued work")
    p.add_argument("--startup-probe-only", action="store_true",
                   help="run the startup probe and exit without scanning or claiming queue work")
    p.add_argument("--startup-probe-force", action="store_true",
                   help="run the startup probe even when the runtime quota circuit breaker is active")
    p.add_argument("--log-level", choices=LOG_LEVELS, default="info",
                   help="minimum supervisor log level (default: info)")
    p.add_argument("--child-log-level", choices=["all", "warn", "error", "none"], default="all",
                   help="minimum child runtime output to keep (default: all)")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
