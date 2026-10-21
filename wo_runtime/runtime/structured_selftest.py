"""Offline end-to-end checks for host-owned structured task completion."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from wo_runtime.queue.commands import (
    QueueStateError,
    claim_task,
    complete_claim,
    requeue_claim,
)
from wo_runtime.workspace import Workspace


RUNTIME_ROOT = Path(__file__).resolve().parents[2]
CLI = RUNTIME_ROOT / "bin" / "wo-runtime"
SCHEMA = {
    "type": "object",
    "required": ["answer"],
    "properties": {"answer": {"type": "integer"}},
    "additionalProperties": False,
}

# The fake provider can only produce a candidate in json-file mode. Its legacy
# branch deliberately uses the old receipt-bound worker API as a compatibility
# check. All files and subprocesses belong to a TemporaryDirectory fixture.
FAKE_CHILD = r'''
import json
import os
import sys
import time
from pathlib import Path

root = Path(sys.argv[1])
mode = sys.argv[2]
observed = {
    "queue_env": sorted(key for key in os.environ
                        if key == "WO_RUNTIME_TASK_ID"
                        or key.startswith("WO_RUNTIME_CLAIM_")),
}
if mode == "legacy":
    from wo_runtime.worker import BoundClaim, RuntimeTerminal
    observed["payload"] = sys.argv[3:]
    (root / "observed.json").write_text(json.dumps(observed), encoding="utf-8")
    claim = BoundClaim.from_environment(root)
    if claim is None:
        raise RuntimeError("legacy worker did not receive its claim")
    RuntimeTerminal(root, claim).complete({"answer": 42})
    raise SystemExit(0)

schema_path, output_path, context_path = map(Path, sys.argv[3:6])
context = json.loads(context_path.read_text(encoding="utf-8"))
observed.update({
    "schema_path": str(schema_path),
    "output_path": str(output_path),
    "context_path": str(context_path),
    "schema": json.loads(schema_path.read_text(encoding="utf-8")),
    "context": context,
    "output_exists_before": output_path.exists(),
    "payload": sys.argv[6:],
})
(root / "observed.json").write_text(json.dumps(observed), encoding="utf-8")
if mode == "missing":
    raise SystemExit(0)
if mode == "malformed":
    output_path.write_text('{"answer":', encoding="utf-8")
    raise SystemExit(0)
if mode == "wrong-schema":
    output_path.write_text('{"answer":"not-an-integer"}', encoding="utf-8")
    raise SystemExit(0)

seed = context.get("inputs", {}).get("source", {}).get("seed", 21)
output_path.write_text(json.dumps({"answer": seed * 2}), encoding="utf-8")
if mode == "nonzero":
    raise SystemExit(7)
if mode == "quota":
    print("synthetic rate limit", flush=True)
    raise SystemExit(7)
if mode == "timeout":
    time.sleep(30)
'''


class StructuredSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="wo-structured-selftest-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.data = self.root / ".run" / "data"
        self.task_id = "REQUEST.T1"
        self.ready = self.data / "tasks" / "ready-worker" / f"{self.task_id}.json"
        self.result = self.data / "results" / f"{self.task_id}.json"
        self.observed = self.root / "observed.json"
        config = {
            "workspaceRoot": ".",
            "defaultDataRoot": ".run/data",
            "defaultDomainId": "demo",
            "defaultPipelineId": "demo",
        }
        self._write(self.root / "runtime" / "workspace.json", config)
        self.workspace = Workspace(
            root=self.root,
            config_path=self.root / "runtime" / "workspace.json",
            config=config,
            data_root_raw=".run/data",
            data_root=self.data,
        )
        self._write(self.root / "domains" / "demo" / "domain.json", {
            "id": "demo", "resources": {},
        })
        self._write(self.root / "pipelines" / "demo.json", {"id": "demo", "stages": []})
        self._write(self.root / "contracts" / "answer.json", SCHEMA)
        self._write(self.root / "contracts" / "contract-index.json", {
            "contracts": {"answer.v1": "answer.json"},
        })
        self.child = self.root / "fake-child.py"
        self.child.write_text(FAKE_CHILD, encoding="utf-8")
        self.task: dict[str, Any] = {
            "queue_contract": "queue.v2",
            "id": self.task_id,
            "req_id": "REQUEST",
            "capability": "worker",
            "stage": "compute",
            "depends_on": [],
            "inputs": [],
            "outputContract": "answer.v1",
            "is_root": True,
        }
        self._write(self.ready, self.task)
        self._configure("success")

    @staticmethod
    def _write(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    @staticmethod
    def _read(path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    def _configure(
        self,
        mode: str,
        *,
        protocol: str | None = "json-file",
        runtime_seconds: float = 10,
    ) -> None:
        args = [str(self.child), str(self.root), mode]
        if mode != "legacy":
            args += ["{output_schema}", "{output_path}", "{task_context}"]
        spec: dict[str, Any] = {"args": args, "payload": "prompt"}
        if protocol is not None:
            spec["result_protocol"] = protocol
        self.descriptor = self.root / "runtime" / "runtimes" / "fake.json"
        self._write(self.descriptor, {
            "name": "fake", "kinds": ["supervisor"],
            "bin": [sys.executable], "exec": spec,
        })
        self._write(self.root / "runtime" / "roles.json", {
            "worker": {
                "watch": ".run/data/tasks/ready-worker",
                "prompt": "Return the answer required by this task.",
                "worker_id": "synthetic-worker",
                "max_runtime_seconds": runtime_seconds,
            },
        })

    def _run(self, *extra: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        child_env = {
            key: value for key, value in os.environ.items()
            if not key.startswith("WO_RUNTIME_") and key != "PIPELINE_DATA_ROOT"
        }
        if env:
            child_env.update(env)
        return subprocess.run(
            [sys.executable, str(CLI), "runtime", "supervisor",
             "--runtime", "fake", "--role", "worker", "--once",
             "--workspace-root", str(self.root), "--interval", "0.02",
             "--termination-grace", "0.1", "--child-log-level", "none", *extra],
            cwd=self.root, env=child_env, text=True, capture_output=True, timeout=15,
        )

    def _assert_requeued(self, run: subprocess.CompletedProcess[str], attempt: int) -> None:
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertFalse(self.result.exists(), run.stdout + run.stderr)
        self.assertTrue(self.ready.exists(), run.stdout + run.stderr)
        self.assertEqual(self._read(self.ready).get("attempt", 0), attempt)
        self.assertNotIn("claim", self._read(self.ready))
        self.assertFalse((self.data / "tasks" / "claimed" / self.ready.name).exists())

    def test_host_commits_valid_output_with_resolved_inputs_and_without_child_receipt(self) -> None:
        upstream = {
            "queue_contract": "queue.v2", "id": "REQUEST.T0", "req_id": "REQUEST",
            "capability": "source", "depends_on": [],
        }
        self._write(self.data / "tasks" / "ready-source" / "REQUEST.T0.json", upstream)
        receipt = claim_task(self.workspace, "source", "source-worker", task_id="REQUEST.T0")
        complete_claim(self.workspace, receipt, {"seed": 23})
        self.task["depends_on"] = ["REQUEST.T0"]
        self.task["inputs"] = [{"task_id": "REQUEST.T0", "as": "source"}]
        self._write(self.ready, self.task)
        run = self._run(env={
            "WO_RUNTIME_CLAIM_JSON": "inherited-receipt",
            "WO_RUNTIME_CLAIM_UNUSED": "inherited-extra-binding",
            "WO_RUNTIME_TASK_ID": "inherited-task",
        })
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        observed = self._read(self.observed)
        self.assertEqual(observed["queue_env"], [])
        self.assertEqual(observed["schema"], SCHEMA)
        self.assertEqual(observed["context"]["inputs"], {"source": {"seed": 23}})
        self.assertNotIn("claim", observed["context"]["task"])
        self.assertNotIn("_claim", observed["context"]["task"])
        self.assertNotIn("[QUEUE-BINDING]", " ".join(observed["payload"]))
        self.assertFalse(observed["output_exists_before"])
        output = Path(observed["output_path"])
        self.assertTrue(output.is_absolute())
        self.assertTrue(output.is_relative_to(self.data / "runtime-invocations" / self.task_id))
        self.assertEqual(output.parent, Path(observed["schema_path"]).parent)
        self.assertEqual(output.parent, Path(observed["context_path"]).parent)
        terminal = self._read(self.result)
        done = self._read(self.data / "tasks" / "done" / self.ready.name)
        self.assertEqual(terminal["result"], {"answer": 46})
        self.assertEqual(terminal["status"], "success")
        self.assertEqual(terminal["lease_id"], done["claim"]["lease_id"])
        self.assertEqual(terminal["worker"], "synthetic-worker")
        self.assertFalse(self.ready.exists())

    def test_missing_malformed_and_schema_invalid_candidates_requeue(self) -> None:
        for attempt, mode in enumerate(("missing", "malformed", "wrong-schema"), start=1):
            with self.subTest(mode=mode):
                self._configure(mode)
                self._assert_requeued(self._run(), attempt)

    def test_nonzero_exit_does_not_commit_a_valid_candidate(self) -> None:
        self._configure("nonzero")
        run = self._run()
        self._assert_requeued(run, 1)
        self.assertEqual(run.returncode, 7)
        self.assertEqual(self._read(Path(self._read(self.observed)["output_path"])), {"answer": 42})

    def test_timeout_does_not_commit_a_valid_candidate(self) -> None:
        self._configure("timeout", runtime_seconds=0.3)
        run = self._run()
        self._assert_requeued(run, 1)
        self.assertEqual(run.returncode, 124)
        self.assertEqual(self._read(Path(self._read(self.observed)["output_path"])), {"answer": 42})

    def test_quota_hold_preserves_attempt_and_does_not_commit(self) -> None:
        self._configure("quota")
        self._assert_requeued(self._run(), 0)

    def test_retry_uses_fresh_paths_and_cannot_reuse_the_previous_candidate(self) -> None:
        self._configure("nonzero")
        self._assert_requeued(self._run(), 1)
        first = self._read(self.observed)
        self._configure("missing")
        self._assert_requeued(self._run(), 2)
        second = self._read(self.observed)
        self.assertNotEqual(Path(first["output_path"]).parent, Path(second["output_path"]).parent)
        self.assertFalse(second["output_exists_before"])
        self.assertTrue(Path(first["output_path"]).exists())
        self.assertFalse(Path(second["output_path"]).exists())

    def test_preparation_failures_do_not_launch_or_consume_an_attempt(self) -> None:
        for prerequisite in ("contract", "dependency"):
            with self.subTest(prerequisite=prerequisite):
                task = dict(self.task)
                if prerequisite == "contract":
                    task.pop("outputContract")
                else:
                    task["inputs"] = [{"task_id": "REQUEST.absent", "as": "source"}]
                    task["depends_on"] = ["REQUEST.absent"]
                self._write(self.ready, task)
                self._assert_requeued(self._run(), 0)
                self.assertFalse(self.observed.exists())

    def test_instance_result_hook_still_rejects_schema_valid_candidates(self) -> None:
        config = dict(self.workspace.config)
        config["instanceHooks"] = {"queue": "reject_output"}
        self._write(self.workspace.config_path, config)
        (self.root / "reject_output.py").write_text(
            "import json\n"
            "from pathlib import Path\n"
            "def validate_task_result(task, result, status, data_root, workspace_root):\n"
            "    (Path(workspace_root) / 'hook-called.json').write_text(json.dumps(result))\n"
            "    raise ValueError('synthetic_result_rejected')\n",
            encoding="utf-8",
        )
        run = self._run()
        self._assert_requeued(run, 1)
        self.assertEqual(self._read(self.root / "hook-called.json"), {"answer": 42})
        self.assertIn("synthetic_result_rejected", run.stdout + run.stderr)

    def test_child_cannot_weaken_the_in_memory_output_schema(self) -> None:
        from wo_runtime.runtime.structured import (
            StructuredResultError,
            prepare_structured_output,
            read_structured_result,
        )

        receipt = claim_task(self.workspace, "worker", "worker", task_id=self.task_id)
        prepared = prepare_structured_output(self.workspace, receipt)
        self._write(prepared.schema_path, {})
        self._write(prepared.output_path, {"answer": "invalid"})
        with self.assertRaises(StructuredResultError):
            read_structured_result(prepared)
        self.assertFalse(self.result.exists())

    def test_late_candidate_cannot_overwrite_the_next_claims_success(self) -> None:
        from wo_runtime.runtime.structured import prepare_structured_output, read_structured_result

        old = claim_task(self.workspace, "worker", "old-worker", task_id=self.task_id)
        prepared = prepare_structured_output(self.workspace, old)
        self._write(prepared.output_path, {"answer": 42})
        requeue_claim(self.workspace, old, reason="synthetic interruption", increment_attempt=True)
        current = claim_task(self.workspace, "worker", "new-worker", task_id=self.task_id)
        complete_claim(self.workspace, current, {"answer": 99})
        with self.assertRaises(QueueStateError):
            complete_claim(self.workspace, old, read_structured_result(prepared))
        self.assertEqual(self._read(self.result)["result"], {"answer": 99})
        self.assertEqual(self._read(self.result)["lease_id"], current.lease_id)

    def test_dry_run_preserves_workspace_and_keeps_symbolic_output_paths(self) -> None:
        before = {str(path.relative_to(self.root)): path.read_bytes()
                  for path in self.root.rglob("*") if path.is_file()}
        run = self._run("--dry-run")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        for placeholder in ("{output_schema}", "{output_path}", "{task_context}"):
            self.assertIn(placeholder, run.stdout)
        after = {str(path.relative_to(self.root)): path.read_bytes()
                 for path in self.root.rglob("*") if path.is_file()}
        self.assertEqual(before, after)
        self.assertFalse(self.observed.exists())

    def test_startup_probe_and_invalid_protocol_fail_before_claim(self) -> None:
        for flag in ("--startup-probe", "--startup-probe-only"):
            with self.subTest(flag=flag):
                run = self._run(flag)
                self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertEqual(self._read(self.ready), self.task)
                self.assertFalse(self.observed.exists())
        for invalid in (None, "unknown"):
            with self.subTest(protocol=invalid):
                descriptor = self._read(self.descriptor)
                descriptor["exec"]["result_protocol"] = invalid
                self._write(self.descriptor, descriptor)
                run = self._run()
                self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertEqual(self._read(self.ready), self.task)
                self.assertFalse(self.observed.exists())

    def test_invalid_descriptor_payload_and_model_flags_fail_before_claim(self) -> None:
        cases = (
            {"payload": {}},
            {"payload": None},
            {"payload_position": "first"},
            {"model_flag": "--model"},
            {"model_flag": [None]},
            {"model_flag": None},
            {"args": ["{task_context}", "{output_path}"]},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self._configure("success")
                descriptor = self._read(self.descriptor)
                descriptor["exec"].update(changes)
                self._write(self.descriptor, descriptor)
                run = self._run()
                self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertEqual(self._read(self.ready), self.task)
                self.assertFalse(self.observed.exists())

    def test_context_file_only_role_runs_without_a_prompt_field(self) -> None:
        descriptor = self._read(self.descriptor)
        descriptor["exec"].pop("payload")
        self._write(self.descriptor, descriptor)
        roles_path = self.root / "runtime" / "roles.json"
        roles = self._read(roles_path)
        roles["worker"].pop("prompt")
        self._write(roles_path, roles)
        run = self._run()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertTrue(self.result.exists(), run.stdout + run.stderr)
        self.assertEqual(self._read(self.result)["result"], {"answer": 42})
        self.assertEqual(self._read(self.observed)["payload"], [])

    def test_legacy_receipt_workers_keep_owning_their_terminal_commits(self) -> None:
        for protocol in (None, "worker"):
            with self.subTest(protocol=protocol):
                self._configure("legacy", protocol=protocol)
                run = self._run()
                self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
                observed = self._read(self.observed)
                self.assertIn("WO_RUNTIME_CLAIM_JSON", observed["queue_env"])
                self.assertIn("WO_RUNTIME_TASK_ID", observed["queue_env"])
                self.assertIn("[QUEUE-BINDING]", " ".join(observed["payload"]))
                self.assertEqual(self._read(self.result)["result"], {"answer": 42})
                self.result.unlink()
                (self.data / "tasks" / "done" / self.ready.name).unlink()
                self._write(self.ready, self.task)


def run_structured_selftest() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StructuredSupervisorTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(run_structured_selftest())
