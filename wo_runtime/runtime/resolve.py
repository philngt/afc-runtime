#!/usr/bin/env python
"""Resolve an interactive launch command for <runtime> <role> from the descriptor +
runtime/roles.json. Prints TAB-separated lines for the shell/PowerShell launcher to exec:

  BIN<TAB><path>
  ENV<TAB><NAME><TAB><value>
  ARG<TAB><arg>
  ERR<TAB><message>        (with exit code 3)

Keeping descriptor logic here means start.sh / start.ps1 stay tiny and identical in
behaviour across runtimes.
"""
from __future__ import annotations
import argparse, json, os, shutil, sys
from pathlib import Path

from wo_runtime.runtime.context import resolve_context_prepend
from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, load_workspace

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


def fail(msg: str) -> None:
    print("ERR\t" + msg)
    sys.exit(3)


RUNTIME_ROOT = Path(__file__).resolve().parents[2]


def project_python(root: Path) -> str:
    explicit = os.environ.get("PYTHON") or os.environ.get("PY")
    if explicit:
        return explicit
    for rel in ("venv/bin/python", ".venv/bin/python"):
        candidate = root / rel
        if candidate.exists():
            return str(candidate)
    return sys.executable


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runtime", required=True)
    ap.add_argument("--role", required=True)
    ap.add_argument("--worker-id", default="")
    ap.add_argument("--model", default="")
    ap.add_argument("--workspace-root", default=None)
    ap.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    a = ap.parse_args(argv)
    root = load_workspace(a.workspace_root, a.workspace_config).root

    desc_path = root / "runtime" / "runtimes" / f"{a.runtime}.json"
    roles_path = root / "runtime" / "roles.json"
    if not desc_path.exists():
        fail(f"unknown runtime '{a.runtime}' (no {desc_path.relative_to(root)})")
    desc = json.loads(desc_path.read_text(encoding="utf-8"))
    roles = json.loads(roles_path.read_text(encoding="utf-8"))
    if a.role not in roles:
        fail(f"unknown role '{a.role}'. valid: {','.join(sorted(roles))}")
    inter = desc.get("interactive")
    if not inter:
        fail(f"runtime '{a.runtime}' has no interactive launcher")
    cfg = roles[a.role]

    binp = os.environ.get(desc.get("bin_env", "") or "", "")
    if not binp:
        for c in desc.get("bin", []):
            f = shutil.which(c)
            if f:
                binp = f
                break
    if not binp:
        fail(f"cannot find '{a.runtime}' binary; set {desc.get('bin_env', 'a binary override')}")

    wid = a.worker_id or cfg.get("worker_id") or a.role
    # Model resolution: --model > fleet.json.<role>.model > roles.json.<role>.models.<runtime> > descriptor default
    fleet_path = root / "runtime" / "fleet.json"
    fleet = {}
    if fleet_path.exists():
        try:
            fleet = json.loads(fleet_path.read_text(encoding="utf-8"))
        except Exception:
            fleet = {}
    fleet_model = (fleet.get(a.role) or {}).get("model") if isinstance(fleet, dict) else None
    model = (a.model or fleet_model
             or (cfg.get("models") or {}).get(a.runtime)
             or desc.get("default_model") or "")

    def sub(s: str) -> str:
        return (s.replace("{cwd}", str(root)).replace("{model}", model)
                 .replace("{role}", a.role).replace("{worker_id}", wid))

    out = ["BIN\t" + binp]
    py = project_python(root)
    out.append("ENV\tPY\t%s" % py)
    out.append("ENV\tPYTHON\t%s" % py)
    pythonpath = os.pathsep.join(
        [str(root), str(RUNTIME_ROOT)] + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
    )
    out.append("ENV\tPYTHONPATH\t%s" % pythonpath)
    out.append("ENV\tPATH\t%s%s%s" % (str(Path(py).parent), os.pathsep, os.environ.get("PATH", "")))
    for logical, env_name in (desc.get("role_env") or {}).items():
        out.append("ENV\t%s\t%s" % (env_name, a.role if logical == "role" else (wid if logical == "worker_id" else "")))

    args = [sub(x) for x in inter.get("args", [])]
    if model and inter.get("model_flag"):
        args += [sub(x) for x in inter["model_flag"]]
    payload_field = inter.get("payload", "none")
    if payload_field and payload_field != "none":
        payload = str(cfg.get(payload_field, "")).replace("{worker_id}", wid)
        prepend = resolve_context_prepend(desc, a.role, root)
        if prepend:
            payload = prepend + payload
        if inter.get("payload_position", "last") == "last":
            args.append(payload)
    for x in args:
        out.append("ARG\t" + x)
    print("\n".join(out))


if __name__ == "__main__":
    main()
