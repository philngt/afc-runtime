from __future__ import annotations

import argparse
import sys

from wo_runtime.orchestrator import core as orchestrator_core
from wo_runtime.queue import commands as queue_commands
from wo_runtime.runtime import fleet_plan, resolve, selftest as runtime_selftest, start, supervisor
from wo_runtime.testing import run_synthetic_selftest
from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, ensure_data_root, load_workspace


def _workspace_parent() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    return parser


def _add_queue(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    workspace = _workspace_parent()
    queue = subparsers.add_parser("queue", help="File queue commands")
    queue_sub = queue.add_subparsers(dest="queue_command")

    claim = queue_sub.add_parser("claim", parents=[workspace], help="Claim one ready task")
    claim.add_argument("capability", nargs="?")
    claim.add_argument("worker_id", nargs="?")
    claim.set_defaults(func=queue_commands.claim)

    complete = queue_sub.add_parser("complete", parents=[workspace], help="Complete a claimed task")
    complete.add_argument("task_id", nargs="?")
    complete.add_argument("json_result_value", nargs="?")
    complete.add_argument("worker_id", nargs="?", default="unknown")
    complete.set_defaults(func=queue_commands.complete)

    fail = queue_sub.add_parser("fail", parents=[workspace], help="Fail a claimed task")
    fail.add_argument("task_id", nargs="?")
    fail.add_argument("error", nargs="?")
    fail.add_argument("worker_id", nargs="?", default="unknown")
    fail.add_argument("recovery_json", nargs="?", default="")
    fail.add_argument("artifact_type", nargs="?", default="")
    fail.set_defaults(func=queue_commands.fail)

    promote = queue_sub.add_parser("promote", parents=[workspace], help="Promote a task to ready")
    promote.add_argument("task_json_path", nargs="?")
    promote.set_defaults(func=queue_commands.promote)

    status = queue_sub.add_parser("status", parents=[workspace], help="Print queue status")
    status.set_defaults(func=queue_commands.status)


def _not_implemented(name: str):
    def _run(_args: argparse.Namespace) -> int:
        print(f"wo-runtime: '{name}' is not implemented in this extraction phase", file=sys.stderr)
        return 2

    return _run


def _dispatch_runtime(argv: list[str]) -> int:
    if not argv:
        print("usage: wo-runtime runtime {supervisor,resolve,start,fleet-plan,selftest} ...", file=sys.stderr)
        return 2
    command, rest = argv[0], argv[1:]
    if command == "supervisor":
        return supervisor.run(supervisor.parse_args(rest))
    if command == "resolve":
        resolve.main(rest)
        return 0
    if command == "start":
        return start.main(rest)
    if command == "fleet-plan":
        return fleet_plan.main(rest)
    if command == "selftest":
        return runtime_selftest.main(rest)
    print(f"wo-runtime: unknown runtime command '{command}'", file=sys.stderr)
    return 2


def _dispatch_orchestrator(argv: list[str]) -> int:
    if not argv:
        print("usage: wo-runtime orchestrator {tick,daemon} ...", file=sys.stderr)
        return 2
    command, rest = argv[0], argv[1:]
    if command == "tick":
        return orchestrator_core.tick_main(rest)
    if command == "daemon":
        return orchestrator_core.daemon_main(rest)
    print(f"wo-runtime: unknown orchestrator command '{command}'", file=sys.stderr)
    return 2


def build_parser() -> argparse.ArgumentParser:
    workspace = _workspace_parent()
    parser = argparse.ArgumentParser(prog="wo-runtime")
    subparsers = parser.add_subparsers(dest="command")

    _add_queue(subparsers)

    ensure = subparsers.add_parser("ensure-data-root", parents=[workspace], help="Prepare runtime data root")
    ensure.set_defaults(func=lambda args: _ensure_data_root(args))

    runtime = subparsers.add_parser("runtime", help="Runtime adapter commands")
    runtime_sub = runtime.add_subparsers(dest="runtime_command")
    for name in ("supervisor", "resolve", "start", "fleet-plan", "selftest"):
        child = runtime_sub.add_parser(name, parents=[workspace])
        child.set_defaults(func=_not_implemented(f"runtime {name}"))

    orchestrator = subparsers.add_parser("orchestrator", help="Generic orchestrator commands")
    orchestrator_sub = orchestrator.add_subparsers(dest="orchestrator_command")
    for name in ("tick", "daemon"):
        child = orchestrator_sub.add_parser(name, parents=[workspace])
        child.set_defaults(func=_not_implemented(f"orchestrator {name}"))

    selftest = subparsers.add_parser("selftest", parents=[workspace], help="Run runtime synthetic selftests")
    selftest.add_argument("--synthetic-workspace", action="store_true")
    selftest.set_defaults(func=lambda _args: run_synthetic_selftest())

    return parser


def _ensure_data_root(args: argparse.Namespace) -> int:
    workspace = load_workspace(args.workspace_root, args.workspace_config)
    ensure_data_root(workspace)
    print(f"[data-root] runtime data root: {workspace.data_root_raw}")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["runtime"]:
        return _dispatch_runtime(argv[1:])
    if argv[:1] == ["orchestrator"]:
        return _dispatch_orchestrator(argv[1:])
    parser = build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 2
    return int(func(args) or 0)
