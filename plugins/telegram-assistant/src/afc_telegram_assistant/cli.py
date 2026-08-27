from __future__ import annotations

import argparse
from collections.abc import Sequence

from . import __version__
from .service import GatewayContext, run_gateway_specification


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="afc-telegram-assistant",
        description="Optional Telegram ingress plugin for AFC workflow assistants.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(dest="command")
    run = subparsers.add_parser(
        "run",
        help="run one explicitly configured workflow gateway adapter",
    )
    run.add_argument(
        "--workspace-root",
        required=True,
        help="single workspace bound to this plugin process",
    )
    run.add_argument(
        "--factory",
        required=True,
        help="host gateway factory in package.module:callable form",
    )
    run.add_argument(
        "--once",
        action="store_true",
        help="perform one host-defined polling cycle and exit",
    )
    run.add_argument(
        "host_arguments",
        nargs=argparse.REMAINDER,
        help="host adapter arguments after --",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command != "run":
        parser.print_help()
        return 0
    host_arguments = list(args.host_arguments)
    if host_arguments[:1] == ["--"]:
        host_arguments.pop(0)
    context = GatewayContext.create(
        args.workspace_root,
        once=args.once,
        host_arguments=host_arguments,
    )
    return run_gateway_specification(args.factory, context)
