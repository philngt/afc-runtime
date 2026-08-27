from __future__ import annotations

import signal
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from afc_telegram_assistant.service import (
    GatewayContext,
    GatewayFactoryError,
    load_gateway_factory,
    run_gateway,
)


class _Gateway:
    def __init__(self, result: int | None = None) -> None:
        self.result = result
        self.once_values: list[bool] = []
        self.shutdown_calls = 0

    def run(self, *, once: bool = False) -> int | None:
        self.once_values.append(once)
        return self.result

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class GatewayServiceTests(unittest.TestCase):
    def test_context_requires_an_existing_single_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            context = GatewayContext.create(
                directory,
                once=True,
                host_arguments=("--mode", "ask"),
            )
        self.assertTrue(context.once)
        self.assertEqual(context.host_arguments, ("--mode", "ask"))
        with self.assertRaisesRegex(ValueError, "does not exist"):
            GatewayContext.create("/path/that/does/not/exist")

    def test_runner_forwards_once_and_always_shuts_down(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            context = GatewayContext.create(directory, once=True)
            gateway = _Gateway(result=7)

            result = run_gateway(lambda _: gateway, context)

        self.assertEqual(result, 7)
        self.assertEqual(gateway.once_values, [True])
        self.assertEqual(gateway.shutdown_calls, 1)

    def test_sigterm_requests_clean_shutdown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            context = GatewayContext.create(directory)
            gateway = _Gateway()

            def interrupted(*, once: bool = False) -> None:
                del once
                handler = signal.getsignal(signal.SIGTERM)
                assert callable(handler)
                handler(signal.SIGTERM, None)

            gateway.run = interrupted  # type: ignore[method-assign]
            result = run_gateway(lambda _: gateway, context)

        self.assertEqual(result, 0)
        self.assertEqual(gateway.shutdown_calls, 1)

    def test_factory_loader_is_explicit_and_adds_only_workspace_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            expected = lambda _: _Gateway()  # noqa: E731
            module = SimpleNamespace(create_gateway=expected)
            with mock.patch(
                "afc_telegram_assistant.service.importlib.import_module",
                return_value=module,
            ) as importer:
                factory = load_gateway_factory("host.adapter:create_gateway", root)

        self.assertIs(factory, expected)
        importer.assert_called_once_with("host.adapter")
        with self.assertRaises(GatewayFactoryError):
            load_gateway_factory("host.adapter", Path.cwd())


if __name__ == "__main__":
    unittest.main()
