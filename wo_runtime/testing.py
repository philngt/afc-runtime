from __future__ import annotations

import json
import os
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

        def run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
            child_env = os.environ.copy()
            if env:
                child_env.update(env)
            return subprocess.run(
                [sys.executable, str(cli), *args, "--workspace-root", str(root)],
                text=True,
                capture_output=True,
                cwd=root,
                env=child_env,
            )

        def check(condition: bool, message: str) -> bool:
            if not condition:
                print(f"synthetic selftest: {message}", file=sys.stderr)
                return False
            return True

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

        # Synthetic orchestrator DAG.
        for child in (data / "tasks").glob("*"):
            if child.is_dir():
                for item in child.glob("*.json"):
                    item.unlink()
        for item in (data / "results").glob("*.json"):
            item.unlink()
        (data / "tasks" / "pending").mkdir(parents=True, exist_ok=True)
        (data / "tasks" / "done").mkdir(parents=True, exist_ok=True)
        (data / "results").mkdir(parents=True, exist_ok=True)
        (data / "tasks" / "pending" / "D.t2.json").write_text(
            '{"id":"D.t2","req_id":"D","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        (data / "tasks" / "pending" / "D.t1.json").write_text(
            '{"id":"D.t1","req_id":"D","capability":"wk","depends_on":["D.t2"],"is_root":true}\n',
            encoding="utf-8",
        )
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        if not check((data / "tasks" / "ready-wk" / "D.t2.json").exists(), "orchestrator promotes dep-free task"):
            return 1
        if not check((data / "tasks" / "pending" / "D.t1.json").exists(), "orchestrator holds dependent task"):
            return 1
        (data / "results" / "D.t2.json").write_text('{"task_id":"D.t2","status":"success","result":"ok"}\n', encoding="utf-8")
        (data / "tasks" / "done" / "D.t2.json").write_text((data / "tasks" / "ready-wk" / "D.t2.json").read_text(encoding="utf-8"), encoding="utf-8")
        (data / "tasks" / "ready-wk" / "D.t2.json").unlink()
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        if not check((data / "tasks" / "ready-wk" / "D.t1.json").exists(), "orchestrator promotes after dependency success"):
            return 1
        (data / "results" / "D.t1.json").write_text('{"task_id":"D.t1","status":"success","result":"answer"}\n', encoding="utf-8")
        (data / "tasks" / "done" / "D.t1.json").write_text((data / "tasks" / "ready-wk" / "D.t1.json").read_text(encoding="utf-8"), encoding="utf-8")
        (data / "tasks" / "ready-wk" / "D.t1.json").unlink()
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        final = json.loads((data / "results" / "D.final.json").read_text(encoding="utf-8"))
        if not check(final.get("status") == "success" and final.get("result") == "answer", "orchestrator finalizes from root task"):
            return 1

        # Dependency failure propagation.
        for child in (data / "tasks").glob("*"):
            if child.is_dir():
                for item in child.glob("*.json"):
                    item.unlink()
        for item in (data / "results").glob("*.json"):
            item.unlink()
        (data / "tasks" / "pending" / "F.dep.json").write_text(
            '{"id":"F.dep","req_id":"F","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        (data / "tasks" / "pending" / "F.root.json").write_text(
            '{"id":"F.root","req_id":"F","capability":"wk","depends_on":["F.dep"],"is_root":true}\n',
            encoding="utf-8",
        )
        (data / "results" / "F.dep.json").write_text('{"task_id":"F.dep","status":"failed","error":"boom"}\n', encoding="utf-8")
        (data / "tasks" / "done" / "F.dep.json").write_text((data / "tasks" / "pending" / "F.dep.json").read_text(encoding="utf-8"), encoding="utf-8")
        (data / "tasks" / "pending" / "F.dep.json").unlink()
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        dep_failed = json.loads((data / "results" / "F.root.json").read_text(encoding="utf-8"))
        if not check(str(dep_failed.get("error", "")).startswith("dep_failed:F.dep"), "orchestrator propagates dependency failure"):
            return 1

        # Claimed result reconciliation and stale claimed requeue.
        for child in (data / "tasks").glob("*"):
            if child.is_dir():
                for item in child.glob("*.json"):
                    item.unlink()
        for item in (data / "results").glob("*.json"):
            item.unlink()
        (data / "tasks" / "claimed").mkdir(parents=True, exist_ok=True)
        (data / "tasks" / "claimed" / "R.t1.json").write_text(
            '{"id":"R.t1","req_id":"R","capability":"wk","depends_on":[],"is_root":true}\n',
            encoding="utf-8",
        )
        (data / "results" / "R.t1.json").write_text('{"task_id":"R.t1","status":"success","result":"reconciled"}\n', encoding="utf-8")
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        if not check((data / "tasks" / "done" / "R.t1.json").exists(), "orchestrator reconciles claimed terminal result"):
            return 1
        if not check((data / "results" / "R.final.json").exists(), "orchestrator finalizes reconciled task"):
            return 1

        (data / "tasks" / "claimed" / "S.t1.json").write_text(
            '{"id":"S.t1","req_id":"S","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        os.utime(data / "tasks" / "claimed" / "S.t1.json", (946684800, 946684800))
        tick = run("orchestrator", "tick", env={"ORCH_NO_HEARTBEAT_SECONDS": "1"})
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        stale = data / "tasks" / "ready-wk" / "S.t1.json"
        if not check(stale.exists(), "orchestrator requeues stale claimed task"):
            return 1
        stale_task = json.loads(stale.read_text(encoding="utf-8"))
        if not check(stale_task.get("attempt") == 1, "orchestrator increments stale attempt"):
            return 1

    print("[wo-runtime selftest] OK")
    return 0
