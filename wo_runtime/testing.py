from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from wo_runtime.artifacts import (
    ArtifactConflictError,
    ContentAddressedStore,
    idempotent_write_json,
)
from wo_runtime.core.validation import result_digest, validate_terminal_result_binding
from wo_runtime.status import RequestStatusReader
from wo_runtime.worker import (
    BoundClaim,
    RuntimeTerminal,
    WorkerError,
    load_bound_task,
    resolve_task_inputs,
)


def run_synthetic_selftest() -> int:
    cli = Path(__file__).resolve().parents[1] / "bin" / "wo-runtime"
    with tempfile.TemporaryDirectory(prefix="wo-runtime-selftest-") as raw:
        root = Path(raw)
        (root / "runtime").mkdir(parents=True, exist_ok=True)
        (root / "runtime" / "workspace.json").write_text(
            json.dumps(
                {
                    "workspaceRoot": ".",
                    "defaultDataRoot": ".run/data",
                    "domainsDir": "domains",
                    "pipelinesDir": "pipelines",
                    "contractsDir": "contracts",
                    "defaultDomainId": "demo",
                    "defaultPipelineId": "demo-linear",
                    "fanoutDefaults": {"locale": ["en-US"]},
                    "contextContracts": {"base": ["task.schema.json", "result.schema.json"]},
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (root / "domains" / "demo").mkdir(parents=True, exist_ok=True)
        (root / "domains" / "demo" / "domain.json").write_text(
            json.dumps(
                {
                    "id": "demo",
                    "defaultPipelineId": "demo-linear",
                    "defaultLocales": ["en-US"],
                    "resources": {},
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (root / "pipelines").mkdir(parents=True, exist_ok=True)
        (root / "pipelines" / "demo-linear.json").write_text(
            json.dumps(
                {
                    "id": "demo-linear",
                    "stages": [
                        {"number": "T1", "capability": "first", "stage": "first_pass", "inputAs": "first"},
                        {
                            "number": "T2",
                            "capability": "second",
                            "stage": "second_pass",
                            "dependsOn": ["T1"],
                            "fanout": ["locale"],
                            "isRoot": True,
                        },
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        data = root / ".run" / "data"
        ready = data / "tasks" / "ready-wk"
        ready.mkdir(parents=True)
        (ready / "t1.json").write_text(
            '{"queue_contract":"queue.v2","id":"t1","req_id":"R","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )

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

        manifest_path = data / "assets" / "selftest-manifest.json"
        created = idempotent_write_json(manifest_path, {"schemaVersion": 1})
        replayed = idempotent_write_json(manifest_path, {"schemaVersion": 1})
        conflict_rejected = False
        try:
            idempotent_write_json(manifest_path, {"schemaVersion": 2})
        except ArtifactConflictError:
            conflict_rejected = True
        store = ContentAddressedStore(data / "assets" / "objects")
        captured_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
        first_ref = store.write_bytes(
            "selftest",
            b"runtime-artifact",
            extension="bin",
            captured_at=captured_at,
        )
        replay_ref = store.write_bytes(
            "selftest",
            b"runtime-artifact",
            extension="bin",
            captured_at=captured_at,
        )
        if not check(
            created
            and not replayed
            and conflict_rejected
            and first_ref == replay_ref,
            "artifact writes are immutable and replay-safe",
        ):
            return 1

        claim = run("queue", "claim", "wk", "w1", "--task-id", "t1", "--json")
        if claim.returncode != 0 or ".run/data/tasks/claimed/t1.json" not in claim.stdout:
            print(claim.stderr or claim.stdout, file=sys.stderr)
            return 1

        claim_receipt = json.loads(claim.stdout)
        claim_environment = {
            "WO_RUNTIME_CLAIM_JSON": claim.stdout.strip(),
            "WO_RUNTIME_TASK_ID": claim_receipt["task_id"],
            "WO_RUNTIME_CLAIM_LEASE": claim_receipt["lease_id"],
            "WO_RUNTIME_CLAIM_ATTEMPT": str(claim_receipt["attempt"]),
            "WO_RUNTIME_CLAIM_EXPIRES_AT": claim_receipt["lease_expires_at"],
            "WO_RUNTIME_CLAIM_PATH": claim_receipt["path"],
        }
        try:
            bound_claim = BoundClaim.from_environment(root, environ=claim_environment)
            if bound_claim is None:
                raise WorkerError("missing claim")
            bound_task = load_bound_task(bound_claim)
            created_terminal = RuntimeTerminal(root, bound_claim).complete({"ok": True})
        except WorkerError as exc:
            print(f"synthetic selftest: receipt-bound worker failed: {exc}", file=sys.stderr)
            return 1
        if not check(
            bound_task.get("id") == "t1" and created_terminal is False,
            "receipt-bound worker commits through queue fencing",
        ):
            return 1
        complete = run(
            "queue", "complete", "t1", '{"ok":true}', "w1",
            "--lease-id", claim_receipt["lease_id"], "--attempt", str(claim_receipt["attempt"]),
        )
        if complete.returncode != 0:
            print(complete.stderr or complete.stdout, file=sys.stderr)
            return 1
        result = json.loads((data / "results" / "t1.json").read_text(encoding="utf-8"))
        if result.get("status") != "success" or result.get("result") != {"ok": True}:
            print("synthetic selftest: complete result mismatch", file=sys.stderr)
            return 1
        status = RequestStatusReader(data).project("R")
        if not check(
            status.state == "success"
            and status.successful == 1
            and status.total == 1
            and status.percent == 100,
            "status projection follows materialized tasks and terminal results",
        ):
            return 1
        resolved_inputs = resolve_task_inputs(
            root,
            {"inputs": [{"task_id": "t1", "as": "upstream"}]},
        )
        missing_input_rejected = False
        try:
            resolve_task_inputs(
                root,
                {"inputs": [{"task_id": "missing", "as": "upstream"}]},
            )
        except WorkerError:
            missing_input_rejected = True
        if not check(
            resolved_inputs == {"upstream": {"ok": True}}
            and missing_input_rejected,
            "worker input resolution is strict",
        ):
            return 1

        # A declared instance contract is a generic terminal boundary, not an
        # optional product hook concern.
        contracts = root / "contracts"
        contracts.mkdir(parents=True, exist_ok=True)
        (contracts / "contract-index.json").write_text(
            json.dumps({"contracts": {"answer.v1": "answer.schema.json"}}),
            encoding="utf-8",
        )
        (contracts / "answer.schema.json").write_text(
            json.dumps({
                "$id": "answer.v1",
                "type": "object",
                "required": ["contract", "answer"],
                "properties": {
                    "contract": {"const": "answer.v1"},
                    "answer": {"type": "string"},
                },
                "additionalProperties": False,
            }),
            encoding="utf-8",
        )
        contract_id = "contract-task"
        (ready / f"{contract_id}.json").write_text(
            json.dumps({
                "queue_contract": "queue.v2",
                "id": contract_id,
                "req_id": "CONTRACT",
                "capability": "wk",
                "depends_on": [],
                "outputContract": "answer.v1",
            }),
            encoding="utf-8",
        )
        contract_claim = run(
            "queue", "claim", "wk", "contract-worker",
            "--task-id", contract_id, "--json",
        )
        contract_receipt = json.loads(contract_claim.stdout)
        invalid_contract = run(
            "queue", "complete", contract_id, '{"contract":"answer.v1","answer":7}',
            "contract-worker",
            "--lease-id", contract_receipt["lease_id"],
            "--attempt", str(contract_receipt["attempt"]),
        )
        if not check(
            invalid_contract.returncode != 0
            and (data / "tasks" / "claimed" / f"{contract_id}.json").exists()
            and not (data / "results" / f"{contract_id}.json").exists(),
            "declared output contract rejects invalid terminal result before write",
        ):
            return 1
        valid_contract = run(
            "queue", "complete", contract_id,
            '{"contract":"answer.v1","answer":"ok"}', "contract-worker",
            "--lease-id", contract_receipt["lease_id"],
            "--attempt", str(contract_receipt["attempt"]),
        )
        if not check(
            valid_contract.returncode == 0
            and (data / "results" / f"{contract_id}.json").exists(),
            "declared output contract accepts valid fenced terminal result",
        ):
            return 1

        pending = data / "tasks" / "pending"
        pending.mkdir(parents=True, exist_ok=True)
        (pending / "t2.json").write_text(
            '{"queue_contract":"queue.v2","id":"t2","req_id":"R","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        promote = run("queue", "promote", str(pending / "t2.json"))
        if promote.returncode != 0 or ".run/data/tasks/ready-wk/t2.json" not in promote.stdout:
            print(promote.stderr or promote.stdout, file=sys.stderr)
            return 1

        fail_claim = run("queue", "claim", "wk", "w1", "--task-id", "t2", "--json")
        if fail_claim.returncode != 0:
            print(fail_claim.stderr or fail_claim.stdout, file=sys.stderr)
            return 1
        fail_receipt = json.loads(fail_claim.stdout)
        fail = run(
            "queue", "fail", "t2", "boom", "w1",
            "--lease-id", fail_receipt["lease_id"], "--attempt", str(fail_receipt["attempt"]),
        )
        if fail.returncode != 0:
            print(fail.stderr or fail.stdout, file=sys.stderr)
            return 1
        failed = json.loads((data / "results" / "t2.json").read_text(encoding="utf-8"))
        if failed.get("status") != "failed":
            print("synthetic selftest: fail result mismatch", file=sys.stderr)
            return 1

        # Queue contract v2: unclaimed completion is rejected.
        unclaimed = run(
            "queue", "complete", "ghost", '{"forged":true}', "w1",
            "--lease-id", "deadbeefdeadbeef", "--attempt", "0",
        )
        if not check(
            unclaimed.returncode != 0 and not (data / "results" / "ghost.json").exists(),
            "queue rejects unclaimed completion",
        ):
            return 1

        legacy_id = "legacy-envelope"
        (ready / f"{legacy_id}.json").write_text(
            json.dumps({"id": legacy_id, "req_id": "LEGACY", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        legacy_claim = run("queue", "claim", "wk", "model-worker", "--task-id", legacy_id, "--json")
        if not check(
            legacy_claim.returncode != 0
            and "task.queue_contract:not_queue_v2" in legacy_claim.stderr
            and (ready / f"{legacy_id}.json").exists(),
            "queue rejects legacy envelopes instead of upgrading them",
        ):
            return 1

        manual_id = "manual-gate"
        (ready / f"{manual_id}.json").write_text(
            json.dumps({
                "queue_contract": "queue.v2",
                "id": manual_id,
                "req_id": "MANUAL",
                "capability": "wk",
                "depends_on": [],
                "manualOnly": True,
            }),
            encoding="utf-8",
        )
        manual_claim = run("queue", "claim", "wk", "model-worker", "--task-id", manual_id, "--json")
        if not check(
            manual_claim.returncode != 0
            and (ready / f"{manual_id}.json").exists()
            and not (data / "tasks" / "claimed" / f"{manual_id}.json").exists(),
            "generic queue rejects manualOnly tasks",
        ):
            return 1

        # Targeted claim must reserve exactly the requested task, not queue head.
        for task_id in ("target-a", "target-b"):
            (ready / f"{task_id}.json").write_text(
                json.dumps({"queue_contract": "queue.v2", "id": task_id, "req_id": "TARGET", "capability": "wk", "depends_on": []}),
                encoding="utf-8",
            )
        target_claim = run("queue", "claim", "wk", "w-target", "--task-id", "target-b", "--json")
        if target_claim.returncode != 0:
            print(target_claim.stderr or target_claim.stdout, file=sys.stderr)
            return 1
        target_receipt = json.loads(target_claim.stdout)
        if not check(
            target_receipt.get("task_id") == "target-b"
            and (ready / "target-a.json").exists()
            and not (ready / "target-b.json").exists(),
            "targeted claim reserves exact task id",
        ):
            return 1
        target_done = run(
            "queue", "complete", "target-b", '{"targeted":true}', "w-target",
            "--lease-id", target_receipt["lease_id"], "--attempt", str(target_receipt["attempt"]),
        )
        if target_done.returncode != 0:
            print(target_done.stderr or target_done.stdout, file=sys.stderr)
            return 1

        hold_id = "provider-hold"
        (ready / f"{hold_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": hold_id, "req_id": "HOLD", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        hold_claim = run(
            "queue", "claim", "wk", "media-worker", "--task-id", hold_id,
            "--lease-ttl-seconds", "7200", "--json",
            env={"QUEUE_MAX_LEASE_TTL_SECONDS": "7200"},
        )
        hold_receipt = json.loads(hold_claim.stdout)
        claimed_at = datetime.strptime(hold_receipt["claimed_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        lease_expires_at = datetime.strptime(
            hold_receipt["lease_expires_at"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        if not check(
            (lease_expires_at - claimed_at).total_seconds() >= 7199,
            "claim accepts a bounded fixed initial lease budget",
        ):
            return 1
        hold_release = run(
            "queue", "requeue", hold_id, "media-worker",
            "--lease-id", hold_receipt["lease_id"],
            "--attempt", str(hold_receipt["attempt"]),
            "--reason", "provider_hold",
            "--no-increment-attempt",
            "--json",
        )
        held_task = json.loads((ready / f"{hold_id}.json").read_text(encoding="utf-8"))
        if not check(
            hold_release.returncode == 0
            and held_task.get("attempt", 0) == hold_receipt["attempt"]
            and "claim" not in held_task,
            "owned requeue releases provider hold without increment",
        ):
            print(hold_release.stderr or hold_release.stdout, file=sys.stderr)
            return 1

        too_long_id = "lease-too-long"
        (ready / f"{too_long_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": too_long_id, "req_id": "TTL", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        too_long_claim = run(
            "queue", "claim", "wk", "media-worker", "--task-id", too_long_id,
            "--lease-ttl-seconds", "7201", "--json",
            env={"QUEUE_MAX_LEASE_TTL_SECONDS": "7200"},
        )
        if not check(
            too_long_claim.returncode != 0 and (ready / f"{too_long_id}.json").exists(),
            "claim rejects a lease budget above the configured hard maximum",
        ):
            return 1

        # A late worker must not complete a task after watchdog requeue + new claim.
        stale_id = "stale-v2"
        (ready / f"{stale_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": stale_id, "req_id": "STALE", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        old_claim = run("queue", "claim", "wk", "worker-old", "--task-id", stale_id, "--json")
        old_receipt = json.loads(old_claim.stdout)
        stale_claimed_path = data / "tasks" / "claimed" / f"{stale_id}.json"
        stale_task = json.loads(stale_claimed_path.read_text(encoding="utf-8"))
        stale_task["claim"]["lease_expires_at"] = "2000-01-01T00:00:00Z"
        stale_claimed_path.write_text(json.dumps(stale_task), encoding="utf-8")
        stale_tick = run("orchestrator", "tick")
        if stale_tick.returncode != 0 or not (ready / f"{stale_id}.json").exists():
            print(stale_tick.stderr or stale_tick.stdout, file=sys.stderr)
            return 1
        new_claim = run("queue", "claim", "wk", "worker-new", "--task-id", stale_id, "--json")
        new_receipt = json.loads(new_claim.stdout)
        late = run(
            "queue", "complete", stale_id, '{"owner":"old"}', "worker-old",
            "--lease-id", old_receipt["lease_id"], "--attempt", str(old_receipt["attempt"]),
        )
        if not check(late.returncode != 0, "queue rejects stale lease completion"):
            return 1
        current = run(
            "queue", "complete", stale_id, '{"owner":"new"}', "worker-new",
            "--lease-id", new_receipt["lease_id"], "--attempt", str(new_receipt["attempt"]),
        )
        if current.returncode != 0:
            print(current.stderr or current.stdout, file=sys.stderr)
            return 1
        replay = run(
            "queue", "complete", stale_id, '{"owner":"new"}', "worker-new",
            "--lease-id", new_receipt["lease_id"], "--attempt", str(new_receipt["attempt"]),
        )
        conflict = run(
            "queue", "complete", stale_id, '{"owner":"different"}', "worker-new",
            "--lease-id", new_receipt["lease_id"], "--attempt", str(new_receipt["attempt"]),
        )
        if not check(replay.returncode == 0 and conflict.returncode != 0, "terminal writes are digest-idempotent"):
            return 1

        hard_lease_id = "hard-lease"
        (ready / f"{hard_lease_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": hard_lease_id, "req_id": "LEASE", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        hard_claim = run("queue", "claim", "wk", "lease-worker", "--task-id", hard_lease_id, "--json")
        hard_receipt = json.loads(hard_claim.stdout)
        hard_claimed_path = data / "tasks" / "claimed" / f"{hard_lease_id}.json"
        hard_task = json.loads(hard_claimed_path.read_text(encoding="utf-8"))
        hard_task["claim"]["lease_expires_at"] = "2000-01-01T00:00:00Z"
        hard_claimed_path.write_text(json.dumps(hard_task), encoding="utf-8")
        heartbeat_dir = root / ".run" / "heartbeats"
        heartbeat_dir.mkdir(parents=True, exist_ok=True)
        (heartbeat_dir / f"{hard_lease_id}.json").write_text(
            json.dumps({
                "task_id": hard_lease_id,
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }),
            encoding="utf-8",
        )
        hard_tick = run("orchestrator", "tick", env={"ORCH_CLAIM_TIMEOUT_SECONDS": "999999"})
        if not check(
            hard_tick.returncode == 0
            and (ready / f"{hard_lease_id}.json").exists()
            and json.loads((ready / f"{hard_lease_id}.json").read_text(encoding="utf-8")).get("attempt") == 1,
            "hard lease expiry requeues independently of fresh heartbeat",
        ):
            print(hard_tick.stderr or hard_tick.stdout, file=sys.stderr)
            return 1

        lease_authority_id = "lease-authority"
        (ready / f"{lease_authority_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": lease_authority_id, "req_id": "LEASE", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        authority_claim = run(
            "queue", "claim", "wk", "lease-authority-worker",
            "--task-id", lease_authority_id, "--lease-ttl-seconds", "7200", "--json",
        )
        authority_path = data / "tasks" / "claimed" / f"{lease_authority_id}.json"
        old = time.time() - 7200
        os.utime(authority_path, (old, old))
        authority_tick = run(
            "orchestrator", "tick",
            env={"ORCH_CLAIM_TIMEOUT_SECONDS": "0", "ORCH_NO_HEARTBEAT_SECONDS": "0"},
        )
        if not check(
            authority_claim.returncode == 0
            and authority_tick.returncode == 0
            and authority_path.exists()
            and not (ready / f"{lease_authority_id}.json").exists(),
            "unexpired queue.v2 lease is not shortened by heartbeat timeout",
        ):
            print(authority_tick.stderr or authority_tick.stdout, file=sys.stderr)
            return 1

        timeout_id = "lease-timeout-terminal"
        (ready / f"{timeout_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": timeout_id, "req_id": "LEASE", "capability": "wk", "depends_on": [], "attempt": 3}),
            encoding="utf-8",
        )
        timeout_claim = run("queue", "claim", "wk", "timeout-owner", "--task-id", timeout_id, "--json")
        timeout_path = data / "tasks" / "claimed" / f"{timeout_id}.json"
        timeout_task = json.loads(timeout_path.read_text(encoding="utf-8"))
        timeout_task["claim"]["lease_expires_at"] = "2000-01-01T00:00:00Z"
        timeout_path.write_text(json.dumps(timeout_task), encoding="utf-8")
        timeout_tick = run("orchestrator", "tick", env={"ORCH_MAX_ATTEMPTS": "3"})
        timeout_result = json.loads((data / "results" / f"{timeout_id}.json").read_text(encoding="utf-8"))
        timeout_done = json.loads((data / "tasks" / "done" / f"{timeout_id}.json").read_text(encoding="utf-8"))
        binding_ok = True
        try:
            validate_terminal_result_binding(timeout_done, timeout_result, expected_task_id=timeout_id)
        except Exception:
            binding_ok = False
        if not check(
            timeout_claim.returncode == 0
            and timeout_tick.returncode == 0
            and binding_ok
            and timeout_result.get("worker") == "timeout-owner"
            and timeout_result.get("result_digest") == result_digest(timeout_result),
            "watchdog max-attempt terminal preserves queue.v2 ownership and digest",
        ):
            print(timeout_tick.stderr or timeout_tick.stdout, file=sys.stderr)
            return 1

        traversal_request = root / "traversal-request.json"
        traversal_request.write_text(
            json.dumps({"id": "../../../../escaped", "kind": "product"}),
            encoding="utf-8",
        )
        traversal = run("core", "plan", "--request-file", str(traversal_request), "--write")
        if not check(
            traversal.returncode != 0
            and not (root / "escaped.T1.json").exists()
            and not (data / "tasks" / "pending" / "escaped.T1.json").exists(),
            "planner rejects traversal request ids before writing tasks",
        ):
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
            '{"queue_contract":"queue.v2","id":"D.t2","req_id":"D","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        (data / "tasks" / "pending" / "D.t1.json").write_text(
            '{"queue_contract":"queue.v2","id":"D.t1","req_id":"D","capability":"wk","depends_on":["D.t2"],"is_root":true}\n',
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
        d2_claim = json.loads(run("queue", "claim", "wk", "fixture", "--task-id", "D.t2", "--json").stdout)
        d2_complete = run(
            "queue", "complete", "D.t2", '"ok"', "fixture",
            "--lease-id", d2_claim["lease_id"], "--attempt", str(d2_claim["attempt"]),
        )
        if d2_complete.returncode != 0:
            print(d2_complete.stderr or d2_complete.stdout, file=sys.stderr)
            return 1
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        if not check((data / "tasks" / "ready-wk" / "D.t1.json").exists(), "orchestrator promotes after dependency success"):
            return 1
        d1_claim = json.loads(run("queue", "claim", "wk", "fixture", "--task-id", "D.t1", "--json").stdout)
        d1_complete = run(
            "queue", "complete", "D.t1", '"answer"', "fixture",
            "--lease-id", d1_claim["lease_id"], "--attempt", str(d1_claim["attempt"]),
        )
        if d1_complete.returncode != 0:
            print(d1_complete.stderr or d1_complete.stdout, file=sys.stderr)
            return 1
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
            '{"queue_contract":"queue.v2","id":"F.dep","req_id":"F","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        (data / "tasks" / "pending" / "F.root.json").write_text(
            '{"queue_contract":"queue.v2","id":"F.root","req_id":"F","capability":"wk","depends_on":["F.dep"],"is_root":true}\n',
            encoding="utf-8",
        )
        promote_failed_dep = run("orchestrator", "tick")
        if promote_failed_dep.returncode != 0:
            print(promote_failed_dep.stderr or promote_failed_dep.stdout, file=sys.stderr)
            return 1
        dep_claim = json.loads(run("queue", "claim", "wk", "fixture", "--task-id", "F.dep", "--json").stdout)
        dep_fail = run(
            "queue", "fail", "F.dep", "boom", "fixture",
            "--lease-id", dep_claim["lease_id"], "--attempt", str(dep_claim["attempt"]),
        )
        if dep_fail.returncode != 0:
            print(dep_fail.stderr or dep_fail.stdout, file=sys.stderr)
            return 1
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        dep_failed = json.loads((data / "results" / "F.root.json").read_text(encoding="utf-8"))
        if not check(str(dep_failed.get("error", "")).startswith("dep_failed:F.dep"), "orchestrator propagates dependency failure"):
            return 1

        # Claimed result reconciliation must fence forged terminal results.
        for child in (data / "tasks").glob("*"):
            if child.is_dir():
                for item in child.glob("*.json"):
                    item.unlink()
        for item in (data / "results").glob("*.json"):
            item.unlink()
        forged_id = "FORGED.t1"
        (data / "tasks" / "ready-wk" / f"{forged_id}.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": forged_id, "req_id": "FORGED", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        forged_claim = run("queue", "claim", "wk", "owner", "--task-id", forged_id, "--json")
        if forged_claim.returncode != 0:
            print(forged_claim.stderr or forged_claim.stdout, file=sys.stderr)
            return 1
        (data / "results" / f"{forged_id}.json").write_text(
            json.dumps({"task_id": forged_id, "status": "success", "result": "forged"}),
            encoding="utf-8",
        )
        forged_tick = run("orchestrator", "tick")
        invalid_results = list((data / "results" / "_invalid").glob(f"{forged_id}.*.json"))
        if not check(
            forged_tick.returncode == 0
            and (data / "tasks" / "claimed" / f"{forged_id}.json").exists()
            and not (data / "tasks" / "done" / f"{forged_id}.json").exists()
            and not (data / "results" / f"{forged_id}.json").exists()
            and bool(invalid_results),
            "orchestrator rejects and quarantines an unfenced forged terminal result",
        ):
            print(forged_tick.stderr or forged_tick.stdout, file=sys.stderr)
            return 1
        (data / "tasks" / "claimed" / f"{forged_id}.json").unlink()

        # Simulate the crash window after queue result write but before the
        # claimed task is moved to done, then let reconciliation finish it.
        (data / "tasks" / "ready-wk" / "R.t1.json").write_text(
            '{"queue_contract":"queue.v2","id":"R.t1","req_id":"R","capability":"wk","depends_on":[],"is_root":true}\n',
            encoding="utf-8",
        )
        reconcile_claim = run("queue", "claim", "wk", "reconciler", "--task-id", "R.t1", "--json")
        reconcile_receipt = json.loads(reconcile_claim.stdout)
        reconcile_complete = run(
            "queue", "complete", "R.t1", '"reconciled"', "reconciler",
            "--lease-id", reconcile_receipt["lease_id"], "--attempt", str(reconcile_receipt["attempt"]),
        )
        if reconcile_complete.returncode != 0:
            print(reconcile_complete.stderr or reconcile_complete.stdout, file=sys.stderr)
            return 1
        (data / "tasks" / "done" / "R.t1.json").replace(data / "tasks" / "claimed" / "R.t1.json")
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        if not check((data / "tasks" / "done" / "R.t1.json").exists(), "orchestrator reconciles claimed terminal result"):
            return 1
        if not check((data / "results" / "R.final.json").exists(), "orchestrator finalizes reconciled task"):
            return 1

        (data / "tasks" / "claimed" / "H.t1.json").write_text(
            '{"queue_contract":"queue.v2","id":"H.t1","req_id":"H","capability":"wk","depends_on":[]}\n',
            encoding="utf-8",
        )
        (data / "heartbeats").mkdir(parents=True, exist_ok=True)
        (data / "heartbeats" / "H.t1.json").write_text(
            json.dumps({
                "task_id": "H.t1",
                "runtime": "selftest",
                "role": "wk",
                "dataRoot": str(data),
                "updatedAt": "2999-01-01T00:00:00Z",
            }),
            encoding="utf-8",
        )
        tick = run("orchestrator", "tick")
        if tick.returncode != 0:
            print(tick.stderr or tick.stdout, file=sys.stderr)
            return 1
        if not check(
            (data / "tasks" / "invalid" / "H.t1.json").exists()
            and not (data / "tasks" / "claimed" / "H.t1.json").exists(),
            "orchestrator rejects a claimed task without Queue ownership even when heartbeat exists",
        ):
            return 1

        # Core planner/context CLI.
        for child in (data / "tasks").glob("*"):
            if child.is_dir():
                for item in child.glob("*.json"):
                    item.unlink()
        for item in (data / "results").glob("*.json"):
            item.unlink()
        request_path = root / "request.json"
        request_path.write_text('{"id":"REQ","kind":"product","title":"Demo","locales":["en-US"]}\n', encoding="utf-8")
        dry_run = run("core", "plan", "--request-file", str(request_path), "--dry-run")
        if dry_run.returncode != 0 or '"id": "REQ.T2.en-US"' not in dry_run.stdout:
            print(dry_run.stderr or dry_run.stdout, file=sys.stderr)
            return 1
        write_plan = run("core", "plan", "--request-file", str(request_path), "--write")
        if write_plan.returncode != 0:
            print(write_plan.stderr or write_plan.stdout, file=sys.stderr)
            return 1
        if not check((data / "tasks" / "pending" / "REQ.T1.json").exists(), "core planner writes first task"):
            return 1
        if not check((data / "tasks" / "pending" / "REQ.T2.en-US.json").exists(), "core planner writes fanout root task"):
            return 1
        planned = json.loads((data / "tasks" / "pending" / "REQ.T1.json").read_text(encoding="utf-8"))
        if not check(planned.get("queue_contract") == "queue.v2", "core planner emits the sole queue contract"):
            return 1
        validate = run("core", "validate-dag", "--req-id", "REQ")
        if validate.returncode != 0:
            print(validate.stderr or validate.stdout, file=sys.stderr)
            return 1
        context = run("core", "context", "REQ.T2.en-US")
        if context.returncode != 0 or '"artifact_type": "task_context"' not in context.stdout:
            print(context.stderr or context.stdout, file=sys.stderr)
            return 1

        planner_hook = root / "planner_hook.py"
        planner_hook.write_text(
            "def before_plan_request(request, request_path, data_root, workspace_root):\n"
            "    updated = dict(request)\n"
            "    updated['hookApplied'] = request_path.name\n"
            "    return updated\n"
            "\n"
            "def validate_task_result(task, result, status, data_root, workspace_root):\n"
            "    if isinstance(result, dict) and result.get('blocked'):\n"
            "        raise ValueError('blocked_by_test_policy')\n"
            "\n"
            "def finalization_decision(req_id, tasks, root_task, root_result, final, **kwargs):\n"
            "    if req_id == 'DEFER':\n"
            "        return {'action': 'defer', 'reason': 'awaiting_review'}\n"
            "    if req_id == 'REJECT':\n"
            "        return {'action': 'reject', 'reason': 'release_policy_rejected'}\n"
            "    return {'action': 'allow'}\n",
            encoding="utf-8",
        )
        workspace_path = root / "runtime" / "workspace.json"
        workspace_payload = json.loads(workspace_path.read_text(encoding="utf-8"))
        workspace_payload["instanceHooks"] = {
            "planner": "planner_hook",
            "queue": "planner_hook",
            "orchestrator": "planner_hook",
        }
        workspace_path.write_text(json.dumps(workspace_payload), encoding="utf-8")
        hook_request = root / "hook-request.json"
        hook_request.write_text('{"id":"HOOK","kind":"product","locales":["en-US"]}\n', encoding="utf-8")
        hook_plan = run("core", "plan", "--request-file", str(hook_request), "--dry-run")
        if not check(
            hook_plan.returncode == 0 and '"hookApplied": "hook-request.json"' in hook_plan.stdout,
            "planner invokes optional before_plan_request hook",
        ):
            print(hook_plan.stderr or hook_plan.stdout, file=sys.stderr)
            return 1

        hook_ready = data / "tasks" / "ready-wk"
        hook_ready.mkdir(parents=True, exist_ok=True)
        (hook_ready / "HOOKVAL.t1.json").write_text(
            json.dumps({"queue_contract": "queue.v2", "id": "HOOKVAL.t1", "req_id": "HOOKVAL", "capability": "wk", "depends_on": []}),
            encoding="utf-8",
        )
        hook_claim = run("queue", "claim", "wk", "hook-worker", "--task-id", "HOOKVAL.t1", "--json")
        hook_receipt = json.loads(hook_claim.stdout)
        blocked_result = run(
            "queue", "complete", "HOOKVAL.t1", '{"blocked":true}', "hook-worker",
            "--lease-id", hook_receipt["lease_id"], "--attempt", str(hook_receipt["attempt"]),
        )
        allowed_result = run(
            "queue", "complete", "HOOKVAL.t1", '{"blocked":false}', "hook-worker",
            "--lease-id", hook_receipt["lease_id"], "--attempt", str(hook_receipt["attempt"]),
        )
        if not check(
            blocked_result.returncode != 0 and allowed_result.returncode == 0,
            "queue invokes optional validate_task_result hook before success",
        ):
            return 1

        done_dir = data / "tasks" / "done"
        done_dir.mkdir(parents=True, exist_ok=True)
        results_dir = data / "results"
        results_dir.mkdir(parents=True, exist_ok=True)
        for req_id in ("DEFER", "REJECT"):
            task_id = f"{req_id}.root"
            lease_id = f"lease-{req_id.lower()}"
            task_doc = {
                "queue_contract": "queue.v2",
                "id": task_id,
                "req_id": req_id,
                "capability": "wk",
                "depends_on": [],
                "is_root": True,
                "attempt": 0,
                "claim": {
                    "lease_id": lease_id,
                    "worker": "fixture",
                    "attempt": 0,
                    "claimed_at": "2026-07-10T00:00:00Z",
                    "lease_expires_at": "2099-01-01T00:00:00Z",
                    "capability": "wk",
                },
            }
            (done_dir / f"{task_id}.json").write_text(
                json.dumps(task_doc),
                encoding="utf-8",
            )
            result_doc = {
                "queue_contract": "queue.v2",
                "task_id": task_id,
                "worker": "fixture",
                "status": "success",
                "result": {"ok": True},
                "lease_id": lease_id,
                "attempt": 0,
            }
            result_doc["result_digest"] = result_digest(result_doc)
            (results_dir / f"{task_id}.json").write_text(
                json.dumps(result_doc),
                encoding="utf-8",
            )
        hook_tick = run("orchestrator", "tick")
        rejected_final = json.loads((results_dir / "REJECT.final.json").read_text(encoding="utf-8"))
        if not check(
            hook_tick.returncode == 0
            and not (results_dir / "DEFER.final.json").exists()
            and rejected_final.get("status") == "failed"
            and rejected_final.get("error") == "release_policy_rejected",
            "orchestrator honors generic finalization defer/reject hook",
        ):
            print(hook_tick.stderr or hook_tick.stdout, file=sys.stderr)
            return 1

        # Supervisor pre-claims the exact trigger, binds its receipt into the
        # child environment, and enforces a hard process-group deadline.
        for child in (data / "tasks").glob("*"):
            if child.is_dir():
                for item in child.glob("*.json"):
                    item.unlink()
        for item in (data / "results").glob("*.json"):
            item.unlink()
        runtime_dir = root / "runtime" / "runtimes"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        (root / "runtime" / "roles.json").write_text(
            json.dumps({
                "slow-role": {
                    "watch": ".run/data/tasks/ready-slow",
                    "prompt": "one slow pass",
                    "worker_id": "slow-worker",
                    "max_runtime_seconds": 0.3,
                }
            }),
            encoding="utf-8",
        )
        binding_path = root / "slow-binding.json"
        sleeper = root / "slow-runtime.py"
        sleeper.write_text(
            "import json, os, sys, time\n"
            "from pathlib import Path\n"
            "Path(sys.argv[1]).write_text(os.environ.get('WO_RUNTIME_CLAIM_JSON', '{}'), encoding='utf-8')\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        (runtime_dir / "slow.json").write_text(
            json.dumps({
                "name": "slow",
                "kinds": ["supervisor"],
                "bin": [sys.executable],
                "exec": {
                    "args": [str(sleeper), str(binding_path)],
                    "payload": "none",
                },
            }),
            encoding="utf-8",
        )
        slow_ready = data / "tasks" / "ready-slow"
        slow_ready.mkdir(parents=True, exist_ok=True)
        (slow_ready / "TIMEOUT.t1.json").write_text(
            json.dumps({
                "queue_contract": "queue.v2",
                "id": "TIMEOUT.t1",
                "req_id": "TIMEOUT",
                "capability": "slow",
                "depends_on": [],
            }),
            encoding="utf-8",
        )
        started = time.monotonic()
        timeout_run = run(
            "runtime", "supervisor",
            "--runtime", "slow",
            "--role", "slow-role",
            "--once",
            "--invocation-timeout", "30",
            "--termination-grace", "0.1",
            "--child-log-level", "none",
        )
        elapsed = time.monotonic() - started
        rebound = json.loads((slow_ready / "TIMEOUT.t1.json").read_text(encoding="utf-8"))
        binding = json.loads(binding_path.read_text(encoding="utf-8")) if binding_path.exists() else {}
        if not check(
            timeout_run.returncode == 124
            and elapsed < 5
            and rebound.get("attempt") == 1
            and not (data / "tasks" / "claimed" / "TIMEOUT.t1.json").exists()
            and binding.get("task_id") == "TIMEOUT.t1"
            and bool(binding.get("lease_id"))
            and bool(binding.get("lease_expires_at")),
            "supervisor binds exact pre-claim and honors per-role hard timeout",
        ):
            print(timeout_run.stderr or timeout_run.stdout, file=sys.stderr)
            return 1

    from wo_runtime.pipeline_testing import run_pipeline_selftest
    from wo_runtime.runtime.structured_selftest import run_structured_selftest

    if run_pipeline_selftest() != 0:
        return 1
    if run_structured_selftest() != 0:
        return 1
    print("[wo-runtime selftest] OK")
    return 0
