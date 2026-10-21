"""Publication regressions using only temporary, provider-free workspaces.

Run with ``python3 -m unittest wo_runtime.plan_testing -v``.
"""

from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from wo_runtime import plans
from wo_runtime.core.commands import _plan_inbox_item
from wo_runtime.core.pipeline import compile_request
from wo_runtime.orchestrator.core import HookRunner, Orchestrator, OrchestratorPaths
from wo_runtime.plans import (
    PlanConflictError,
    PlanPublicationError,
    load_request_plan,
    publish_plan,
    require_complete_plan,
    require_published_task,
    task_is_published,
)
from wo_runtime.queue.commands import QueueStateError, claim_task, complete_claim, promote
from wo_runtime.queue.locking import QueueBusyError
from wo_runtime.runtime.supervisor import list_triggers, preclaim_trigger
from wo_runtime.status import RequestStatusReader
from wo_runtime.workspace import Workspace


class PlanPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_cwd = Path.cwd()
        self.temporary = tempfile.TemporaryDirectory(prefix="wo-plan-test-")
        self.root = Path(self.temporary.name)
        self.data = self.root / "state"
        config = {
            "workspaceRoot": ".",
            "defaultDataRoot": str(self.data),
            "dataRootEnv": "WO_PLAN_TEST_DATA_ROOT",
            "defaultDomainId": "fixture",
            "defaultPipelineId": "linear",
            "contextPacks": {"enabled": False},
        }
        self.write(self.root / "runtime" / "workspace.json", config)
        self.write(self.root / "domains" / "fixture" / "domain.json", {"id": "fixture"})
        self.write(
            self.root / "pipelines" / "linear.json",
            {
                "id": "linear",
                "stages": [
                    {"number": "T1", "capability": "first", "stage": "first"},
                    {
                        "number": "T2",
                        "capability": "second",
                        "stage": "second",
                        "dependsOn": ["T1"],
                        "isRoot": True,
                    },
                ],
            },
        )
        self.workspace = Workspace(
            root=self.root,
            config_path=self.root / "runtime" / "workspace.json",
            config=config,
            data_root_raw=str(self.data),
            data_root=self.data,
        )
        self.request = {"id": "REQ", "instruction": "produce a verified fixture"}
        self.plan = compile_request(self.workspace, self.request)
        self.task_ids = [task["id"] for task in self.plan["tasks"]]
        self.manifest_path = self.data / "plans" / "REQ" / "manifest.json"
        self.commit_path = self.data / "plans" / "REQ" / "committed.json"
        paths = OrchestratorPaths(self.root, self.data)
        self.orchestrator = Orchestrator(paths, HookRunner(None, paths))
        self.environment = mock.patch.dict(os.environ, {"WO_PLAN_TEST_DATA_ROOT": str(self.data)})
        self.environment.start()

    def tearDown(self) -> None:
        os.chdir(self.original_cwd)
        self.environment.stop()
        self.temporary.cleanup()

    @staticmethod
    def write(path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    @staticmethod
    def read(path: Path) -> dict:
        return json.loads(path.read_text(encoding="utf-8"))

    def pending(self, task_id: str) -> Path:
        return self.data / "tasks" / "pending" / f"{task_id}.json"

    def publish(self) -> bool:
        return publish_plan(self.workspace, copy.deepcopy(self.request), copy.deepcopy(self.plan))

    def to_ready(self, task_id: str) -> Path:
        source = self.pending(task_id)
        task = self.read(source)
        target = self.data / "tasks" / f"ready-{task['capability']}" / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        source.replace(target)
        return target

    def fail_after_first_task_write(self) -> None:
        original = plans.idempotent_write_json
        written: list[Path] = []

        def interrupted(path: Path | str, payload: dict, **kwargs: object) -> bool:
            result = original(path, payload, **kwargs)
            destination = Path(path)
            if destination.parent == self.data / "tasks" / "pending":
                written.append(destination)
                raise OSError("injected interruption after publishing one task")
            return result

        with mock.patch.object(plans, "idempotent_write_json", side_effect=interrupted):
            with self.assertRaises((OSError, PlanPublicationError)):
                self.publish()
        self.assertEqual(len(written), 1, "the injected failure must interrupt task publication")
        self.assertTrue(self.manifest_path.exists())
        self.assertFalse(self.commit_path.exists())
        self.assertEqual(len(list((self.data / "tasks" / "pending").glob("*.json"))), 1)

    def test_new_plan_is_complete_and_bound(self) -> None:
        self.assertTrue(self.publish())
        manifest = load_request_plan(self.data, "REQ")
        self.assertIsNotNone(manifest)
        self.assertTrue(self.commit_path.is_file())
        tasks = [self.read(self.pending(task_id)) for task_id in self.task_ids]
        self.assertEqual({task["id"] for task in tasks}, set(self.task_ids))
        for task in tasks:
            self.assertEqual(task["plan_digest"], manifest["digest"])
            require_published_task(self.data, task)
            self.assertTrue(task_is_published(self.data, task))
        require_complete_plan(self.data, "REQ", tasks)

    def test_request_id_alias_remains_supported(self) -> None:
        request = {"req_id": "ALIAS", "instruction": "existing request envelope"}
        plan = compile_request(self.workspace, request)
        self.assertTrue(publish_plan(self.workspace, request, plan))
        self.assertEqual(load_request_plan(self.data, "ALIAS")["req_id"], "ALIAS")

    def test_replay_does_not_recreate_ready_claimed_or_done_task(self) -> None:
        self.publish()
        task_id = self.task_ids[0]
        manifest_bytes = self.manifest_path.read_bytes()
        ready = self.to_ready(task_id)
        ready_bytes = ready.read_bytes()
        self.assertFalse(self.publish())
        self.assertFalse(self.pending(task_id).exists())
        self.assertEqual(ready.read_bytes(), ready_bytes)

        receipt = claim_task(self.workspace, "first", "fixture", task_id=task_id)
        claimed = Path(receipt.path)
        claimed_bytes = claimed.read_bytes()
        self.assertFalse(self.publish())
        self.assertFalse(self.pending(task_id).exists())
        self.assertEqual(claimed.read_bytes(), claimed_bytes)

        complete_claim(self.workspace, receipt, {"artifact": "first-output"})
        done = self.data / "tasks" / "done" / f"{task_id}.json"
        terminal = self.data / "results" / f"{task_id}.json"
        done_bytes, terminal_bytes = done.read_bytes(), terminal.read_bytes()
        self.assertFalse(self.publish())
        self.assertFalse(self.pending(task_id).exists())
        self.assertEqual(done.read_bytes(), done_bytes)
        self.assertEqual(terminal.read_bytes(), terminal_bytes)
        self.assertEqual(self.manifest_path.read_bytes(), manifest_bytes)

    def test_different_request_content_preserves_existing_publication(self) -> None:
        self.publish()
        originals = {path: path.read_bytes() for path in self.data.rglob("*.json")}
        changed = {**self.request, "instruction": "different fixture"}
        changed_plan = compile_request(self.workspace, changed)
        with self.assertRaises(PlanConflictError):
            publish_plan(self.workspace, changed, changed_plan)
        for path, payload in originals.items():
            self.assertEqual(path.read_bytes(), payload, str(path))

    def test_interrupted_publication_is_not_visible_and_retry_finishes_it(self) -> None:
        self.fail_after_first_task_write()
        draft = load_request_plan(self.data, "REQ", require_committed=False)
        self.assertIsNotNone(draft)
        partial = self.read(self.pending(self.task_ids[0]))
        self.assertFalse(task_is_published(self.data, partial))
        with self.assertRaises(PlanPublicationError):
            require_published_task(self.data, partial)
        with contextlib.redirect_stdout(io.StringIO()):
            self.orchestrator.tick()
        self.assertFalse(any((self.data / "tasks").glob("ready-*/*.json")))
        self.assertFalse((self.data / "results" / "REQ.final.json").exists())

        self.assertTrue(self.publish())
        self.assertTrue(self.commit_path.exists())
        for task_id in self.task_ids:
            require_published_task(self.data, self.read(self.pending(task_id)))
        self.assertFalse(self.publish())

    def test_interrupted_manifest_staging_can_resume(self) -> None:
        original = plans.idempotent_write_json

        def interrupted(path: Path | str, payload: dict, **kwargs: object) -> bool:
            destination = Path(path)
            if destination == self.manifest_path:
                self.write(destination.with_name(".manifest.json.interrupted.tmp"), payload)
                raise OSError("injected interruption before the manifest is linked")
            return original(path, payload, **kwargs)

        with mock.patch.object(plans, "idempotent_write_json", side_effect=interrupted):
            with self.assertRaises((OSError, PlanPublicationError)):
                self.publish()
        self.assertFalse(self.manifest_path.exists())
        self.assertFalse(self.commit_path.exists())
        self.assertFalse(task_is_published(self.data, self.plan["tasks"][0]))
        status = RequestStatusReader(self.data).project("REQ")
        self.assertNotEqual(status.state, "success")
        self.assertEqual(status.successful, 0)
        self.assertTrue(self.publish())
        self.assertIsNotNone(load_request_plan(self.data, "REQ"))

    def test_interruption_before_commit_marker_keeps_all_tasks_hidden(self) -> None:
        original = plans.idempotent_write_json

        def interrupted(path: Path | str, payload: dict, **kwargs: object) -> bool:
            if Path(path) == self.commit_path:
                raise OSError("injected interruption before the commit marker")
            return original(path, payload, **kwargs)

        with mock.patch.object(plans, "idempotent_write_json", side_effect=interrupted):
            with self.assertRaises((OSError, PlanPublicationError)):
                self.publish()
        before = {task_id: self.pending(task_id).read_bytes() for task_id in self.task_ids}
        self.assertFalse(self.commit_path.exists())
        for task_id in self.task_ids:
            self.assertFalse(task_is_published(self.data, self.read(self.pending(task_id))))
        with contextlib.redirect_stdout(io.StringIO()):
            self.orchestrator.tick()
        self.assertFalse(any((self.data / "tasks").glob("ready-*/*.json")))
        self.assertEqual(RequestStatusReader(self.data).project("REQ").successful, 0)
        self.assertTrue(self.publish())
        for task_id, payload in before.items():
            self.assertEqual(self.pending(task_id).read_bytes(), payload)

    def test_old_incomplete_publication_locks_can_recover(self) -> None:
        for suffix, payload in (("EMPTY", ""), ("MALFORMED", '{"pid":')):
            with self.subTest(lock=suffix):
                request = {**self.request, "id": f"REQ_{suffix}"}
                plan = compile_request(self.workspace, request)
                lock = self.data / "plans" / ".locks" / "queue" / f"{request['id']}.lock"
                lock.parent.mkdir(parents=True, exist_ok=True)
                lock.write_text(payload, encoding="utf-8")
                old = time.time() - 86400
                os.utime(lock, (old, old))
                self.assertTrue(publish_plan(self.workspace, request, plan))
                self.assertFalse(lock.exists())

    def test_old_valid_live_owner_publication_lock_is_not_stolen(self) -> None:
        lock = self.data / "plans" / ".locks" / "queue" / "REQ.lock"
        self.write(lock, {"pid": os.getpid(), "created": time.time() - 86400, "purpose": "live-fixture"})
        original = lock.read_bytes()
        with self.assertRaises(QueueBusyError):
            self.publish()
        self.assertEqual(lock.read_bytes(), original)
        self.assertFalse(self.commit_path.exists())

    def test_concurrent_publish_has_one_commit(self) -> None:
        barrier = threading.Barrier(2)

        def contender() -> object:
            barrier.wait(timeout=5)
            try:
                return self.publish()
            except (QueueBusyError, PlanConflictError) as exc:
                return type(exc).__name__

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(contender) for _ in range(2)]
            outcomes = [future.result(timeout=10) for future in futures]
        self.assertEqual(sum(outcome is True for outcome in outcomes), 1, outcomes)
        self.assertTrue(all(outcome in (True, False, "QueueBusyError", "PlanConflictError") for outcome in outcomes))
        self.assertFalse(self.publish())
        self.assertEqual(
            {path.stem for path in (self.data / "tasks" / "pending").glob("*.json")},
            set(self.task_ids),
        )
        for task_id in self.task_ids:
            require_published_task(self.data, self.read(self.pending(task_id)))

    def test_uncommitted_plan_cannot_use_direct_promote_or_claim(self) -> None:
        self.fail_after_first_task_write()
        task_id = self.task_ids[0]
        args = SimpleNamespace(
            workspace_root=str(self.root),
            workspace_config="runtime/workspace.json",
            task_json_path=str(self.pending(task_id)),
        )
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            try:
                result = promote(args)
            except PlanPublicationError:
                pass
            else:
                self.assertNotEqual(result, 0)
        self.assertTrue(self.pending(task_id).exists())
        ready = self.to_ready(task_id)
        with self.assertRaises((PlanPublicationError, QueueStateError)):
            claim_task(self.workspace, "first", "fixture", task_id=task_id)
        self.assertTrue(ready.exists())
        self.assertFalse((self.data / "tasks" / "claimed" / ready.name).exists())

    def test_supervisor_ignores_uncommitted_queue_trigger(self) -> None:
        self.fail_after_first_task_write()
        ready = self.to_ready(self.task_ids[0])
        cfg = {"watch": str(ready.parent.relative_to(self.root)), "worker_id": "fixture"}
        self.assertEqual(list_triggers(self.root, cfg, set()), [])
        stat = ready.stat()
        trigger = (str(ready.relative_to(self.root)), stat.st_mtime_ns, stat.st_size)
        with self.assertRaises((PlanPublicationError, QueueStateError)):
            preclaim_trigger(self.workspace, cfg, trigger, max_runtime_seconds=5)

    def test_stripped_binding_does_not_downgrade_task_to_legacy(self) -> None:
        self.publish()
        task = self.read(self.pending(self.task_ids[0]))
        task.pop("plan_digest")
        self.assertFalse(task_is_published(self.data, task))
        with self.assertRaises(PlanPublicationError):
            require_published_task(self.data, task)

    def test_changed_scheduling_fields_are_not_published(self) -> None:
        self.publish()
        original = self.read(self.pending(self.task_ids[0]))
        mutations = {
            "depends_on": ["MISSING.T1"],
            "is_root": True,
            "capability": "different",
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                changed = {**original, field: value}
                self.assertFalse(task_is_published(self.data, changed))
                with self.assertRaises(PlanPublicationError):
                    require_published_task(self.data, changed)
                tasks = [changed, self.read(self.pending(self.task_ids[-1]))]
                with self.assertRaises(PlanPublicationError):
                    require_complete_plan(self.data, "REQ", tasks)

    def test_numeric_manual_flag_cannot_bypass_planned_manual_only_task(self) -> None:
        self.plan["tasks"][0]["manualOnly"] = True
        self.publish()
        task_id = self.task_ids[0]
        ready = self.to_ready(task_id)
        changed = self.read(ready)
        changed["manualOnly"] = 1
        self.write(ready, changed)
        self.assertFalse(task_is_published(self.data, changed))
        with self.assertRaises((PlanPublicationError, QueueStateError)):
            claim_task(self.workspace, "first", "fixture", task_id=task_id)
        self.assertTrue(ready.exists())

    def test_manifest_or_marker_mismatch_fails_closed(self) -> None:
        self.publish()
        task = self.read(self.pending(self.task_ids[0]))
        for path in (self.manifest_path, self.commit_path):
            with self.subTest(path=path.name):
                original = path.read_bytes()
                changed = self.read(path)
                changed["digest"] = "0" * 64
                self.write(path, changed)
                try:
                    with self.assertRaises(PlanPublicationError):
                        load_request_plan(self.data, "REQ")
                    self.assertFalse(task_is_published(self.data, task))
                    status = RequestStatusReader(self.data).project("REQ")
                    self.assertNotEqual(status.state, "success")
                    self.assertEqual(status.successful, 0)
                finally:
                    path.write_bytes(original)

    def test_uncommitted_status_cannot_accept_terminal_files(self) -> None:
        self.fail_after_first_task_write()
        for task_id in self.task_ids:
            self.write(
                self.data / "results" / f"{task_id}.json",
                {"task_id": task_id, "status": "success", "result": "not-published"},
            )
        status = RequestStatusReader(self.data).project("REQ")
        self.assertNotEqual(status.state, "success")
        self.assertEqual(status.successful, 0)
        self.assertEqual(status.total, len(self.task_ids))

    def test_missing_materialized_task_cannot_report_full_success(self) -> None:
        self.publish()
        for task_id in self.task_ids:
            ready = self.to_ready(task_id)
            capability = self.read(ready)["capability"]
            receipt = claim_task(self.workspace, capability, "fixture", task_id=task_id)
            complete_claim(self.workspace, receipt, {"artifact": task_id})
        self.assertEqual(RequestStatusReader(self.data).project("REQ").state, "success")
        (self.data / "tasks" / "done" / f"{self.task_ids[0]}.json").unlink()
        status = RequestStatusReader(self.data).project("REQ")
        self.assertNotEqual(status.state, "success")
        self.assertEqual(status.total, len(self.task_ids))
        self.assertLess(status.successful, status.total)
        self.assertLess(status.percent, 100)

    def test_missing_commit_marker_blocks_terminal_write(self) -> None:
        self.publish()
        task_id = self.task_ids[0]
        self.to_ready(task_id)
        receipt = claim_task(self.workspace, "first", "fixture", task_id=task_id)
        self.commit_path.unlink()
        with self.assertRaises((PlanPublicationError, QueueStateError)):
            complete_claim(self.workspace, receipt, {"artifact": "must-not-commit"})
        self.assertFalse((self.data / "results" / f"{task_id}.json").exists())
        self.assertTrue(Path(receipt.path).exists())

    def test_missing_expected_task_prevents_finalization(self) -> None:
        self.publish()
        root_id = self.task_ids[-1]
        ready = self.to_ready(root_id)
        receipt = claim_task(self.workspace, "second", "fixture", task_id=root_id)
        complete_claim(self.workspace, receipt, {"artifact": "root-output"})
        self.pending(self.task_ids[0]).unlink()
        done_task = self.read(self.data / "tasks" / "done" / ready.name)
        with self.assertRaises(PlanPublicationError):
            require_complete_plan(self.data, "REQ", [done_task])
        with contextlib.redirect_stdout(io.StringIO()):
            self.orchestrator.finalization_pass()
        self.assertFalse((self.data / "results" / "REQ.final.json").exists())

    def test_legacy_task_remains_compatible(self) -> None:
        task = {
            "queue_contract": "queue.v2",
            "id": "LEGACY.T1",
            "req_id": "LEGACY",
            "capability": "first",
            "stage": "first",
            "depends_on": [],
            "is_root": True,
        }
        ready = self.data / "tasks" / "ready-first" / "LEGACY.T1.json"
        self.write(ready, task)
        self.assertIsNone(load_request_plan(self.data, "LEGACY"))
        self.assertTrue(task_is_published(self.data, task))
        require_published_task(self.data, task)
        require_complete_plan(self.data, "LEGACY", [task])
        receipt = claim_task(self.workspace, "first", "legacy-worker", task_id=task["id"])
        complete_claim(self.workspace, receipt, {"artifact": "legacy-output"})
        result = self.read(self.data / "results" / "LEGACY.T1.json")
        self.assertEqual(result["status"], "success")

    def test_conflicting_inbox_cannot_overwrite_existing_final(self) -> None:
        self.publish()
        final_path = self.data / "results" / "REQ.final.json"
        self.write(final_path, {"req_id": "REQ", "status": "success", "result": {"artifact": "kept"}})
        original_final = final_path.read_bytes()
        original_manifest = self.manifest_path.read_bytes()
        incoming = self.data / "inbox" / "REQ.json"
        self.write(incoming, {**self.request, "instruction": "conflicting submission"})
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            try:
                _plan_inbox_item(self.workspace, incoming, dry_run=False)
            except PlanConflictError:
                pass
        self.assertEqual(final_path.read_bytes(), original_final)
        self.assertEqual(self.manifest_path.read_bytes(), original_manifest)

    def test_invalid_duplicate_inbox_cannot_fail_existing_request(self) -> None:
        self.publish()
        final_path = self.data / "results" / "REQ.final.json"
        original_manifest = self.manifest_path.read_bytes()
        for has_final in (False, True):
            with self.subTest(has_final=has_final):
                if has_final:
                    self.write(final_path, {"req_id": "REQ", "status": "success", "result": "kept"})
                original_final = final_path.read_bytes() if final_path.exists() else None
                incoming = self.data / "inbox" / "REQ.json"
                self.write(incoming, {**self.request, "pipelineId": "missing-pipeline"})
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    _plan_inbox_item(self.workspace, incoming, dry_run=False)
                self.assertEqual(
                    final_path.read_bytes() if final_path.exists() else None,
                    original_final,
                )
                self.assertEqual(self.manifest_path.read_bytes(), original_manifest)


if __name__ == "__main__":
    unittest.main()
