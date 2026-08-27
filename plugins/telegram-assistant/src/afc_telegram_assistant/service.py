from __future__ import annotations

import importlib
import re
import signal
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Protocol, runtime_checkable


_FACTORY_PATTERN = re.compile(
    r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$"
)


@dataclass(frozen=True)
class GatewayContext:
    """Explicit host input for one plugin process bound to one workspace."""

    workspace_root: Path
    once: bool = False
    host_arguments: tuple[str, ...] = ()

    @classmethod
    def create(
        cls,
        workspace_root: Path | str,
        *,
        once: bool = False,
        host_arguments: Sequence[str] = (),
    ) -> "GatewayContext":
        root = Path(workspace_root).expanduser().resolve()
        if not root.is_dir():
            raise ValueError(f"Telegram assistant workspace does not exist: {root}")
        return cls(root, once=once, host_arguments=tuple(host_arguments))


@runtime_checkable
class GatewayService(Protocol):
    def run(self, *, once: bool = False) -> int | None: ...

    def shutdown(self) -> None: ...


GatewayFactory = Callable[[GatewayContext], GatewayService]


class GatewayFactoryError(RuntimeError):
    pass


class _StopRequested(BaseException):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


def load_gateway_factory(specification: str, workspace_root: Path) -> GatewayFactory:
    """Load one explicitly named host adapter; no plugin auto-discovery occurs."""
    if not _FACTORY_PATTERN.fullmatch(specification):
        raise GatewayFactoryError(
            "gateway factory must use the form package.module:callable"
        )
    source_root = workspace_root / "src"
    source_value = str(source_root)
    if source_root.is_dir() and source_value not in sys.path:
        sys.path.insert(0, source_value)
    module_name, attribute_path = specification.split(":", 1)
    try:
        value: object = importlib.import_module(module_name)
        for attribute in attribute_path.split("."):
            value = getattr(value, attribute)
    except (ImportError, AttributeError) as exc:
        raise GatewayFactoryError(
            f"cannot load Telegram assistant gateway factory {specification}: {exc}"
        ) from exc
    if not callable(value):
        raise GatewayFactoryError(
            f"Telegram assistant gateway factory is not callable: {specification}"
        )
    return value  # type: ignore[return-value]


def run_gateway(factory: GatewayFactory, context: GatewayContext) -> int:
    """Run one host gateway with deterministic shutdown on TERM or Ctrl+C."""
    gateway = factory(context)
    if not isinstance(gateway, GatewayService):
        raise GatewayFactoryError(
            "gateway factory result must provide run(*, once=...) and shutdown()"
        )
    previous_handlers: dict[signal.Signals, signal.Handlers] = {}

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        raise _StopRequested(signum)

    if threading.current_thread() is threading.main_thread():
        for name in ("SIGTERM", "SIGINT"):
            current_signal = getattr(signal, name, None)
            if current_signal is None:
                continue
            previous_handlers[current_signal] = signal.getsignal(current_signal)
            signal.signal(current_signal, request_stop)
    try:
        result = gateway.run(once=context.once)
        return int(result or 0)
    except _StopRequested as stop:
        return 130 if stop.signum == getattr(signal, "SIGINT", -1) else 0
    finally:
        try:
            gateway.shutdown()
        finally:
            for current_signal, handler in previous_handlers.items():
                signal.signal(current_signal, handler)


def run_gateway_specification(
    specification: str,
    context: GatewayContext,
) -> int:
    factory = load_gateway_factory(specification, context.workspace_root)
    return run_gateway(factory, context)
