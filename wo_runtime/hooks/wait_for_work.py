from __future__ import annotations

import glob
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wo_runtime.runtime.context import resolve_context_prepend


STUCK_THRESHOLD = 3
CLAUDE_GUARDRAIL = (
    " Run EXACTLY ONE pass of this command, then stop. "
    "Do NOT call ScheduleWakeup, do NOT loop yourself, and do NOT edit or 'fix' any "
    "file under .claude/hooks/ or scripts/ -- that infrastructure is correct by design. "
    "If the same work keeps reappearing, it is a stuck/malformed item for a human to "
    "inspect -- not a bug in this hook."
)


@dataclass(frozen=True)
class HookConfig:
    runtime: str
    role_env: str
    worker_env: str
    protocol: str
    state_dir: Path
    timeout_env: str = ""
    default_timeout: float = 540.0


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _as_watch_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item]
    return []


class WaitForWork:
    def __init__(self, cfg: HookConfig, workspace_root: Path | None = None):
        self.cfg = cfg
        self.root = (workspace_root or Path.cwd()).resolve()
        self.role = os.environ.get(cfg.role_env, "").strip()
        self.worker_id = os.environ.get(cfg.worker_env, "").strip()
        self.context_prepend = ""
        self.context_warning = ""

    def run(self) -> int:
        if not self.role:
            return 0
        roles_path = self.root / "runtime" / "roles.json"
        role_map = _read_json(roles_path)
        if not role_map:
            if self.cfg.protocol == "stderr-exit2":
                print(f"[wait-for-work] cannot load {roles_path.relative_to(self.root)}", file=sys.stderr)
            else:
                print(f"[{self.cfg.runtime}-wait] cannot load {roles_path.relative_to(self.root)}", file=sys.stderr)
            return 0
        role_cfg = role_map.get(self.role)
        if not isinstance(role_cfg, dict):
            return 0
        watches = _as_watch_list(role_cfg.get("watch"))
        if not watches:
            return 0
        self._load_context()

        trigger_match = role_cfg.get("trigger_match", "*.json")
        exclude_tpl = role_cfg.get("exclude_if_sibling_exists")
        state_path = self._state_path()

        def pattern_for(watch: str) -> str:
            if isinstance(trigger_match, dict):
                return str(trigger_match.get(watch, "*.json"))
            return str(trigger_match)

        def is_excluded(path: str) -> bool:
            if not exclude_tpl:
                return False
            name = os.path.basename(path)
            candidates = [
                str(exclude_tpl).format(name=name),
                str(exclude_tpl).format(name=name.replace(".final.json", ".json")),
            ]
            return any((self.root / candidate).exists() for candidate in candidates)

        def find_trigger(ignore: set[str]) -> str | None:
            for watch in watches:
                base = self.root / watch
                for entry in sorted(glob.glob(str(base / pattern_for(watch)))):
                    path = Path(entry)
                    rel = path.relative_to(self.root).as_posix()
                    if path.name.startswith(".") or rel in ignore:
                        continue
                    if is_excluded(rel):
                        continue
                    return rel
            return None

        state = self._load_state(state_path)
        head = find_trigger(set())
        if head is None:
            self._save_state(state_path, None, 0)
            stuck: set[str] = set()
        elif head == state.get("trigger"):
            count = int(state.get("count", 0)) + 1
            self._save_state(state_path, head, count)
            if count < STUCK_THRESHOLD:
                self._continue(role_cfg, f"Work detected ({head}).")
            if self.cfg.protocol == "stderr-exit2":
                kickoff = str(role_cfg.get("kickoff") or "")
                print(
                    f"[wait-for-work] '{head}' reappeared {count}x without being cleared by "
                    f"{kickoff}; treating as stuck and ignoring it. Waking only on NEW work. "
                    f"A human should inspect this item.",
                    file=sys.stderr,
                )
            stuck = {head}
        else:
            self._save_state(state_path, head, 1)
            self._continue(role_cfg, f"Work detected ({head}).")

        deadline = time.time() + self._timeout_seconds()
        while time.time() < deadline:
            time.sleep(min(1.0, max(0.01, self._timeout_seconds())))
            trigger = find_trigger(stuck)
            if trigger:
                self._save_state(state_path, trigger, 1)
                self._continue(role_cfg, f"Work detected ({trigger}).")

        self._save_state(state_path, None, 0)
        self._continue(role_cfg, "Heartbeat: no new work detected.")
        return 0

    def _timeout_seconds(self) -> float:
        if self.cfg.timeout_env and os.environ.get(self.cfg.timeout_env):
            try:
                return float(os.environ[self.cfg.timeout_env])
            except ValueError:
                return self.cfg.default_timeout
        return self.cfg.default_timeout

    def _state_path(self) -> Path:
        state_key = f"{self.role}__{self.worker_id or 'default'}".replace("/", "_").replace("\\", "_")
        return self.root / self.cfg.state_dir / f"wait-{state_key}.json"

    def _load_state(self, state_path: Path) -> dict[str, Any]:
        return _read_json(state_path)

    def _save_state(self, state_path: Path, trigger: str | None, count: int) -> None:
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps({"trigger": trigger, "count": count}), encoding="utf-8")
        except OSError:
            pass

    def _load_context(self) -> None:
        desc_path = self.root / "runtime" / "runtimes" / f"{self.cfg.runtime}.json"
        if not desc_path.exists():
            return
        desc = _read_json(desc_path)
        if not desc.get("context"):
            return
        try:
            self.context_prepend = resolve_context_prepend(desc, self.role, self.root)
        except SystemExit as exc:
            self.context_warning = f"context prepend failed: {exc}"
        except Exception as exc:
            self.context_warning = f"context prepend errored: {type(exc).__name__}: {exc}"
        if self.context_warning:
            print(f"[{self.cfg.runtime}-wait] WARN role={self.role}: {self.context_warning}", file=sys.stderr)

    def _continue(self, role_cfg: dict[str, Any], reason: str) -> None:
        if self.cfg.protocol == "stdout-json-block":
            prompt = str(role_cfg.get("prompt", "")).replace(
                "{worker_id}", self.worker_id or str(role_cfg.get("worker_id", self.role))
            )
            body = (
                f"{reason} {prompt} Run exactly one pass and then stop. "
                "Do not edit `.codex/`, `.claude/`, or runtime scripts."
            )
            parts = []
            if self.context_prepend:
                parts.append(self.context_prepend.rstrip())
            if self.context_warning:
                parts.append(f"[CONTEXT-WARNING] {self.context_warning} -- proceeding without prepended skill context.")
            parts.append(body)
            print(json.dumps({"decision": "block", "reason": "\n\n".join(parts)}))
            raise SystemExit(0)

        kickoff = str(role_cfg.get("kickoff") or "")
        if reason.startswith("Heartbeat:"):
            print(
                "Heartbeat: no work for ~9 minutes. Do NOT run any command and do NOT change "
                "anything -- just acknowledge briefly and stop. This re-arms the work watcher.",
                file=sys.stderr,
            )
        else:
            print(f"{reason} Run {kickoff} to process one pass.{CLAUDE_GUARDRAIL}", file=sys.stderr)
        raise SystemExit(2)


def run_hook(
    runtime: str,
    *,
    role_env: str,
    worker_env: str,
    protocol: str,
    state_dir: str,
    timeout_env: str = "",
    default_timeout: float = 540.0,
    workspace_root: Path | None = None,
) -> int:
    cfg = HookConfig(
        runtime=runtime,
        role_env=role_env,
        worker_env=worker_env,
        protocol=protocol,
        state_dir=Path(state_dir),
        timeout_env=timeout_env,
        default_timeout=default_timeout,
    )
    return WaitForWork(cfg, workspace_root=workspace_root).run()
