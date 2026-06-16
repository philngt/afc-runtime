from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG


RUNTIME_ROOT = Path(__file__).resolve().parents[2]
CLI = RUNTIME_ROOT / "bin" / "wo-runtime"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start one interactive runtime role session.")
    parser.add_argument("runtime")
    parser.add_argument("role")
    parser.add_argument("worker_id", nargs="?", default="")
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    parser.add_argument("--model", default="")
    args = parser.parse_args(argv)

    cmd = [
        sys.executable,
        str(CLI),
        "runtime",
        "resolve",
        "--runtime",
        args.runtime,
        "--role",
        args.role,
        "--worker-id",
        args.worker_id,
        "--workspace-config",
        args.workspace_config,
    ]
    if args.workspace_root:
        cmd.extend(["--workspace-root", args.workspace_root])
    if args.model:
        cmd.extend(["--model", args.model])
    resolved = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if resolved.returncode != 0:
        message = resolved.stdout.strip() or resolved.stderr.strip()
        if message.startswith("ERR\t"):
            message = message[4:]
        print(f"[start] {message}", file=sys.stderr)
        return 2

    bin_path = ""
    argv_out: list[str] = []
    for line in resolved.stdout.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        if parts[0] == "BIN" and len(parts) >= 2:
            bin_path = parts[1]
        elif parts[0] == "ENV" and len(parts) >= 3:
            os.environ[parts[1]] = parts[2]
        elif parts[0] == "ARG" and len(parts) >= 2:
            argv_out.append(parts[1])

    if not bin_path:
        print("[start] runtime resolver did not emit BIN", file=sys.stderr)
        return 2
    print(f"[start] runtime={args.runtime} role={args.role} worker={args.worker_id or args.role}")
    if args.runtime == "codex":
        print("[start] review and trust the project hook with /hooks when prompted.")
    os.execvpe(bin_path, [bin_path, *argv_out], os.environ)
    return 127
