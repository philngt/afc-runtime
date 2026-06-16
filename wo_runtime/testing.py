from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


def run_synthetic_selftest() -> int:
    cli = Path(__file__).resolve().parents[1] / "bin" / "wo-runtime"
    with tempfile.TemporaryDirectory(prefix="wo-runtime-selftest-") as raw:
        root = Path(raw)
        data = root / ".run" / "data"
        ready = data / "tasks" / "ready-wk"
        ready.mkdir(parents=True)
        (ready / "t1.json").write_text('{"id":"t1","req_id":"R","capability":"wk"}\n', encoding="utf-8")

        def run(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [sys.executable, str(cli), *args, "--workspace-root", str(root)],
                text=True,
                capture_output=True,
                cwd=root,
            )

        claim = run("queue", "claim", "wk", "w1")
        if claim.returncode != 0 or ".run/data/tasks/claimed/t1.json" not in claim.stdout:
            print(claim.stderr or claim.stdout, file=sys.stderr)
            return 1

        complete = run("queue", "complete", "t1", '{"ok":true}', "w1")
        if complete.returncode != 0:
            print(complete.stderr or complete.stdout, file=sys.stderr)
            return 1
        result = json.loads((data / "results" / "t1.json").read_text(encoding="utf-8"))
        if result.get("status") != "success" or result.get("result") != {"ok": True}:
            print("synthetic selftest: complete result mismatch", file=sys.stderr)
            return 1

        pending = data / "tasks" / "pending"
        pending.mkdir(parents=True, exist_ok=True)
        (pending / "t2.json").write_text('{"id":"t2","req_id":"R","capability":"wk"}\n', encoding="utf-8")
        promote = run("queue", "promote", str(pending / "t2.json"))
        if promote.returncode != 0 or ".run/data/tasks/ready-wk/t2.json" not in promote.stdout:
            print(promote.stderr or promote.stdout, file=sys.stderr)
            return 1

        fail_claim = run("queue", "claim", "wk", "w1")
        if fail_claim.returncode != 0:
            print(fail_claim.stderr or fail_claim.stdout, file=sys.stderr)
            return 1
        fail = run("queue", "fail", "t2", "boom", "w1")
        if fail.returncode != 0:
            print(fail.stderr or fail.stdout, file=sys.stderr)
            return 1
        failed = json.loads((data / "results" / "t2.json").read_text(encoding="utf-8"))
        if failed.get("status") != "failed":
            print("synthetic selftest: fail result mismatch", file=sys.stderr)
            return 1

    print("[wo-runtime selftest] OK")
    return 0
