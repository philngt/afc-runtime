"""Deterministic compiler regressions and one real queue failure propagation."""
from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from wo_runtime.core.pipeline import PlanError, compile_request
from wo_runtime.core.validation import validate_terminal_result_binding
from wo_runtime.orchestrator.core import HookRunner, Orchestrator, OrchestratorPaths
from wo_runtime.queue.commands import claim_task, complete_claim, fail_claim
from wo_runtime.status import RequestStatusReader
from wo_runtime.workspace import Workspace


class PipelineCompilerTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="wo-pipeline-selftest-")
        self.addCleanup(temporary.cleanup)
        # The real orchestrator resolves its workspace by changing directory.
        # Restore it before the temporary instance is removed.
        self.addCleanup(os.chdir, Path.cwd())
        self.root = Path(temporary.name)
        self.data = self.root / ".run" / "data"
        config = {
            "workspaceRoot": ".",
            "defaultDataRoot": str(self.data),
            "dataRootEnv": "WO_PIPELINE_SELFTEST_DATA_ROOT",
            "defaultDomainId": "demo",
            "defaultPipelineId": "demo",
        }
        config_path = self.root / "runtime" / "workspace.json"
        self._write(config_path, config)
        self.workspace = Workspace(self.root, config_path, config, str(self.data), self.data)
        self._write(self.root / "domains" / "demo" / "domain.json", {
            "id": "demo", "resources": {},
        })
        self.profile_path = self.root / "pipelines" / "demo.json"

    @staticmethod
    def _write(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    @staticmethod
    def _read(path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _stage(number: str, **fields: Any) -> dict[str, Any]:
        return {"number": number, "capability": "work", **fields}

    def _compile(
        self, stages: list[Any], request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._write(self.profile_path, {"id": "demo", "stages": stages})
        return compile_request(self.workspace, request if request is not None else {"id": "REQ"})

    def test_reordered_diamond_emits_a_stable_topological_plan(self) -> None:
        stages = [
            self._stage("Join", dependsOn=["Left", "Right"], isRoot=True),
            self._stage("Right", dependsOn="Seed"),
            self._stage("Left", dependsOn="Seed"),
            self._stage("Seed"),
        ]
        plan = self._compile(stages)
        self.assertEqual(plan, self._compile(stages))
        tasks = {task["id"]: task for task in plan["tasks"]}
        self.assertEqual(set(tasks), {"REQ.Seed", "REQ.Left", "REQ.Right", "REQ.Join"})
        self.assertEqual(tasks["REQ.Join"]["depends_on"], ["REQ.Left", "REQ.Right"])
        self.assertEqual(tasks["REQ.Left"]["depends_on"], ["REQ.Seed"])
        self.assertEqual(tasks["REQ.Right"]["depends_on"], ["REQ.Seed"])
        positions = {task["id"]: index for index, task in enumerate(plan["tasks"])}
        for task in plan["tasks"]:
            for dependency in task["depends_on"]:
                self.assertLess(positions[dependency], positions[task["id"]])
        self.assertEqual(plan["warnings"], [])

    def test_existing_order_is_preserved_when_a_consumer_becomes_ready(self) -> None:
        plan = self._compile([
            self._stage("Seed"),
            self._stage("Derived", dependsOn="Seed"),
            self._stage("Independent"),
            self._stage("Final", dependsOn=["Derived", "Independent"], isRoot=True),
        ])
        self.assertEqual([task["id"] for task in plan["tasks"]], [
            "REQ.Seed", "REQ.Derived", "REQ.Independent", "REQ.Final",
        ])

    def test_deep_reversed_chain_does_not_depend_on_python_recursion_depth(self) -> None:
        count = sys.getrecursionlimit() + 25
        stages = [
            self._stage(f"T{index:05d}", **(
                {"dependsOn": [f"T{index - 1:05d}"]} if index else {}
            ))
            for index in range(count)
        ]
        stages[-1]["isRoot"] = True
        plan = self._compile(list(reversed(stages)))
        self.assertEqual(len(plan["tasks"]), count)
        self.assertEqual(plan["tasks"][0]["id"], "REQ.T00000")
        self.assertEqual(plan["tasks"][-1]["id"], f"REQ.T{count - 1:05d}")
        self.assertTrue(plan["tasks"][-1]["is_root"])

    def test_unknown_references_fail_even_with_a_valid_sibling_or_inactive_stage(self) -> None:
        for field in ("dependsOn", "inputsFrom"):
            for inactive in (False, True):
                with self.subTest(field=field, inactive=inactive):
                    stage = self._stage("End", **{field: ["Start", "TYPO"]}, isRoot=True)
                    if inactive:
                        stage["when"] = {"field": "enabled", "truthy": True}
                    with self.assertRaisesRegex(PlanError, "unknown_stage"):
                        self._compile([self._stage("Start"), stage])

    def test_malformed_stage_definitions_are_not_silently_skipped(self) -> None:
        cases = (
            None, "invalid", {}, {"number": "End"},
            {"number": True, "capability": "work"},
            {"number": "../End", "capability": "work"},
            {"number": "End", "capability": "../work"},
            {"number": "End", "capability": "work", "isRoot": "yes"},
        )
        for invalid in cases:
            with self.subTest(stage=invalid):
                with self.assertRaises(PlanError):
                    self._compile([self._stage("Start", isRoot=True), invalid])
        with self.assertRaises(PlanError):
            self._compile([
                self._stage("Start", isRoot=True),
                {"number": "Hidden", "capability": "../work",
                 "when": {"field": "enabled", "truthy": True}},
            ])

    def test_duplicate_stage_numbers_fail_before_conditions(self) -> None:
        with self.assertRaisesRegex(PlanError, "duplicate_stage"):
            self._compile([
                self._stage("Same", isRoot=True),
                self._stage("Same", when={"field": "enabled", "truthy": True}),
            ])

    def test_malformed_references_are_rejected_for_both_reference_fields(self) -> None:
        for field in ("dependsOn", "inputsFrom"):
            for invalid in ([None], [True], [{}], [{"stage": []}], [["Start"]]):
                with self.subTest(field=field, reference=invalid):
                    with self.assertRaisesRegex(PlanError, "invalid_reference"):
                        self._compile([
                            self._stage("Start"),
                            self._stage("End", **{field: invalid}, isRoot=True),
                        ])

    def test_self_references_and_cycles_fail_before_condition_pruning(self) -> None:
        for field in ("dependsOn", "inputsFrom"):
            with self.subTest(self_reference=field):
                with self.assertRaisesRegex(PlanError, "self_reference"):
                    self._compile([self._stage("Self", **{field: "Self"}, isRoot=True)])
        for closing_field in ("dependsOn", "inputsFrom"):
            with self.subTest(cycle=closing_field):
                with self.assertRaisesRegex(PlanError, "stage_cycle"):
                    self._compile([
                        self._stage("A", dependsOn="B", isRoot=True),
                        self._stage("B", **{closing_field: "A"},
                                    when={"field": "enabled", "truthy": True}),
                    ])

    def test_string_and_named_object_reference_forms_remain_supported(self) -> None:
        for reference in ("Start", {"stage": "Start"}, {"number": "Start"}, {"task": "Start"}):
            with self.subTest(reference=reference):
                plan = self._compile([
                    self._stage("End", dependsOn=reference, isRoot=True),
                    self._stage("Start"),
                ])
                self.assertEqual(plan["tasks"][-1]["depends_on"], ["REQ.Start"])
        for reference in (1, {"stage": 1}, {"number": 1}, {"task": 1}):
            with self.subTest(numeric_reference=reference):
                plan = self._compile([
                    {"number": 2, "capability": "work", "dependsOn": reference, "isRoot": True},
                    {"number": 1, "capability": "work"},
                ])
                self.assertEqual(plan["tasks"][-1]["depends_on"], ["REQ.1"])

    def test_inputs_from_binds_a_transitive_ancestor_without_adding_a_scheduling_edge(self) -> None:
        plan = self._compile([
            self._stage("End", dependsOn="Middle", inputsFrom={"task": "Start"},
                        inputMap={"Start": "raw", "Middle": "processed"}, isRoot=True),
            self._stage("Middle", dependsOn="Start"),
            self._stage("Start"),
        ])
        end = plan["tasks"][-1]
        self.assertEqual(end["depends_on"], ["REQ.Middle"])
        self.assertEqual(end["inputs"], [
            {"task_id": "REQ.Middle", "as": "processed"},
            {"task_id": "REQ.Start", "as": "raw"},
        ])

    def test_inputs_from_cannot_read_an_emitted_source_outside_dependency_ancestry(self) -> None:
        with self.assertRaisesRegex(PlanError, "input_without_dependency"):
            self._compile([
                self._stage("Final", dependsOn=["End", "Ungated"], isRoot=True),
                self._stage("End", dependsOn="Required", inputsFrom="Ungated"),
                self._stage("Required"),
                self._stage("Ungated"),
            ])

    def test_declared_but_omitted_dependencies_and_inputs_preserve_conditional_pruning(self) -> None:
        stages = [
            self._stage("End", dependsOn=["Start", "AfterOptional"],
                        inputsFrom="Optional", isRoot=True),
            self._stage("AfterOptional", dependsOn="Optional"),
            self._stage("Optional", dependsOn="Start",
                        when={"field": "enabled", "truthy": True}),
            self._stage("Start"),
        ]
        disabled = self._compile(stages, {"id": "REQ", "enabled": False})
        self.assertEqual([task["id"] for task in disabled["tasks"]], ["REQ.Start", "REQ.End"])
        self.assertEqual(disabled["tasks"][-1]["depends_on"], ["REQ.Start"])
        self.assertEqual(disabled["tasks"][-1]["inputs"], [{"task_id": "REQ.Start", "as": "work"}])
        enabled = self._compile(stages, {"id": "REQ", "enabled": True})
        self.assertEqual(len(enabled["tasks"]), 4)
        self.assertEqual(enabled["tasks"][-1]["id"], "REQ.End")

    def test_mutually_exclusive_roots_are_resolved_after_conditions(self) -> None:
        stages = [
            self._stage("Left", dependsOn="Start", isRoot=True,
                        when={"field": "route", "equals": "left"}),
            self._stage("Right", dependsOn="Start", isRoot=True,
                        when={"field": "route", "equals": "right"}),
            self._stage("Start"),
        ]
        for route in ("left", "right"):
            with self.subTest(route=route):
                plan = self._compile(stages, {"id": "REQ", "route": route})
                roots = [task["id"] for task in plan["tasks"] if task.get("is_root")]
                self.assertEqual(roots, [f"REQ.{route.title()}"])
                self.assertEqual(len(plan["tasks"]), 2)
        fallback = self._compile(stages, {"id": "REQ", "route": "neither"})
        self.assertEqual([task["id"] for task in fallback["tasks"]], ["REQ.Start"])
        self.assertTrue(fallback["tasks"][0]["is_root"])
        self.assertEqual(fallback["warnings"], [{"code": "root_auto_selected", "taskId": "REQ.Start"}])

    def test_automatic_root_requires_one_unique_terminal(self) -> None:
        plan = self._compile([
            self._stage("End", dependsOn="Start"), self._stage("Start"),
        ])
        self.assertEqual(plan["warnings"], [{"code": "root_auto_selected", "taskId": "REQ.End"}])
        self.assertTrue(plan["tasks"][-1]["is_root"])
        with self.assertRaisesRegex(PlanError, "ambiguous_roots"):
            self._compile([self._stage("Left"), self._stage("Right")])

    def test_explicit_root_must_be_unique_and_cover_every_emitted_task(self) -> None:
        with self.assertRaisesRegex(PlanError, "multiple_roots"):
            self._compile([
                self._stage("Start", isRoot=True),
                self._stage("End", dependsOn="Start", isRoot=True),
            ])
        for stages in (
            [self._stage("Root", isRoot=True), self._stage("Disconnected")],
            [self._stage("Root", isRoot=True), self._stage("Later", dependsOn="Root")],
        ):
            with self.subTest(stages=stages):
                with self.assertRaisesRegex(PlanError, "root_does_not_cover_tasks"):
                    self._compile(stages)

    def test_fanout_needs_a_join_when_multiple_terminal_variants_are_emitted(self) -> None:
        stage = self._stage("Work", fanout=["locale"], isRoot=True)
        single = self._compile([stage], {"id": "REQ", "locales": ["only"]})
        self.assertEqual(single["tasks"][0]["id"], "REQ.Work.only")
        self.assertTrue(single["tasks"][0]["is_root"])
        with self.assertRaisesRegex(PlanError, "multiple_roots"):
            self._compile([stage], {"id": "REQ", "locales": ["a", "b"]})
        with self.assertRaisesRegex(PlanError, "ambiguous_roots"):
            self._compile([self._stage("Work", fanout=["locale"])],
                          {"id": "REQ", "locales": ["a", "b"]})
        joined = self._compile([
            self._stage("Join", dependsOn="Work", isRoot=True),
            self._stage("Work", fanout=["locale"]),
        ], {"id": "REQ", "locales": ["a", "b"]})
        self.assertEqual(joined["tasks"][-1]["depends_on"], ["REQ.Work.a", "REQ.Work.b"])
        self.assertEqual([task["id"] for task in joined["tasks"] if task.get("is_root")], ["REQ.Join"])

    def test_already_ordered_output_and_caller_inputs_remain_unchanged(self) -> None:
        profile = {
            "id": "demo", "defaults": {"options": {"checks": ["basic"]}},
            "stages": [
                {"number": "T1", "capability": "collect", "stage": "collect_input",
                 "outputAs": "raw", "taskFields": {"outputContract": "record.v1", "priority": 3}},
                {"number": "T2", "capability": "check", "stage": "check_output",
                 "dependsOn": ["T1"], "inputMap": {"T1": "evidence"}, "isRoot": True,
                 "taskFields": {"outputContract": "validated.v1"}},
            ],
        }
        request = {"id": "REQ", "details": {"values": [1, 2]}}
        original = copy.deepcopy(request)
        self._write(self.profile_path, profile)
        saved_profile = self.profile_path.read_bytes()
        plan = compile_request(self.workspace, request)
        product = {**original, "domainId": "demo", "pipelineId": "demo",
                   "options": {"checks": ["basic"]}}
        common = {"queue_contract": "queue.v2", "req_id": "REQ", "product": product,
                  "domainId": "demo", "pipelineId": "demo"}
        self.assertEqual(plan["tasks"], [
            {**common, "id": "REQ.T1", "capability": "collect", "stage": "collect_input",
             "depends_on": [], "inputs": [], "outputContract": "record.v1", "priority": 3},
            {**common, "id": "REQ.T2", "capability": "check", "stage": "check_output",
             "depends_on": ["REQ.T1"], "inputs": [{"task_id": "REQ.T1", "as": "evidence"}],
             "outputContract": "validated.v1", "is_root": True},
        ])
        self.assertEqual(plan["warnings"], [])
        self.assertEqual(request, original)
        self.assertEqual(self.profile_path.read_bytes(), saved_profile)
        plan["tasks"][0]["product"]["details"]["values"].append(9)
        self.assertEqual(request, original)
        self.assertEqual(plan["tasks"][1]["product"]["details"]["values"], [1, 2])

    def test_failed_fanout_branch_propagates_through_compiled_join_to_request_final(self) -> None:
        plan = self._compile([
            self._stage("Join", capability="join", dependsOn="Work", isRoot=True),
            self._stage("Work", capability="work", dependsOn="Seed", fanout=["locale"]),
            self._stage("Seed", capability="seed"),
        ], {"id": "FLOW", "locales": ["a", "b"]})
        for task in plan["tasks"]:
            self._write(self.data / "tasks" / "pending" / f"{task['id']}.json", task)
        paths = OrchestratorPaths(self.root, self.data)
        orchestrator = Orchestrator(paths, HookRunner(None, paths))
        orchestrator.tick()
        seed = claim_task(self.workspace, "seed", "seed-worker", task_id="FLOW.Seed")
        self.assertEqual(seed.status, "claimed")
        complete_claim(self.workspace, seed, {"seed": 1})
        orchestrator.tick()
        failed = claim_task(self.workspace, "work", "first-worker", task_id="FLOW.Work.a")
        passed = claim_task(self.workspace, "work", "second-worker", task_id="FLOW.Work.b")
        self.assertEqual(failed.status, "claimed")
        self.assertEqual(passed.status, "claimed")
        fail_claim(self.workspace, failed, "synthetic_branch_failure")
        complete_claim(self.workspace, passed, {"ok": True})
        orchestrator.tick()
        root_result = self._read(self.data / "results" / "FLOW.Join.json")
        root_task = self._read(self.data / "tasks" / "done" / "FLOW.Join.json")
        validate_terminal_result_binding(root_task, root_result, expected_task_id="FLOW.Join")
        self.assertEqual(root_result["status"], "failed")
        self.assertEqual(root_result["error"], "dep_failed:FLOW.Work.a")
        self.assertEqual(self._read(self.data / "results" / "FLOW.final.json")["status"], "failed")
        self.assertEqual(RequestStatusReader(self.data).project("FLOW").state, "failed")
        self.assertFalse((self.data / "tasks" / "ready-join" / "FLOW.Join.json").exists())


def run_pipeline_selftest() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(PipelineCompilerTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(run_pipeline_selftest())
