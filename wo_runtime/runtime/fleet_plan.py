#!/usr/bin/env python
"""Emit a launch plan from runtime/fleet.json for `scripts/run.sh up`.

Groups roles by their assigned runtime. Headless (supervisor-capable) runtimes get one
supervisor covering all their roles; interactive runtimes get one AGENT line per replica.

Output (TAB-separated), one directive per line:
  SUP <runtime> <role1,role2,...>
  AGENT <runtime> <role> <replicas>
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, load_workspace

# Force LF newlines on Windows -- bash `read` consumers see a trailing CR
# otherwise, which contaminates the last token of each emitted line (e.g.
# `worker-listing\r` would fail supervisor's role-known check).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(newline="\n")

def has_supervisor(root: Path, rt: str) -> bool:
    try:
        d = json.loads((root / "runtime" / "runtimes" / f"{rt}.json").read_text(encoding="utf-8"))
        return "supervisor" in d.get("kinds", [])
    except Exception:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    args = parser.parse_args(argv)
    root = load_workspace(args.workspace_root, args.workspace_config).root

    roles = json.loads((root / "runtime" / "roles.json").read_text(encoding="utf-8"))
    fleet_path = root / "runtime" / "fleet.json"
    fleet = json.loads(fleet_path.read_text(encoding="utf-8")) if fleet_path.exists() else {}

    sup_roles: dict[str, list[str]] = {}
    agents: list[tuple[str, str, int]] = []
    for role in roles:
        spec = fleet.get(role, {"runtime": "claude", "replicas": 1})
        rt = spec.get("runtime", "claude")
        reps = max(1, int(spec.get("replicas", 1)))
        if has_supervisor(root, rt):
            sup_roles.setdefault(rt, []).append(role)  # replicas handled by supervisor via fleet
        else:
            agents.append((rt, role, reps))

    for rt, rs in sup_roles.items():
        print(f"SUP\t{rt}\t{','.join(rs)}")
    for rt, role, reps in agents:
        print(f"AGENT\t{rt}\t{role}\t{reps}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
