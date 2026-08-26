from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class AssistantCapabilities:
    native_session_resume: bool = False
    structured_progress: bool = False
    usage_metrics: bool = False
    image_input: bool = False
    workspace_write: bool = False
    output_files: bool = False
    model_selection: bool = False
    reasoning_control: bool = False


@dataclass(frozen=True)
class AssistantAttachment:
    """Normalized untrusted input passed from a transport to a backend."""

    id: str
    kind: str
    original_name: str
    media_type: str
    size_bytes: int
    sha256: str
    path: Path
    extracted_text: str | None = None

    @property
    def is_image(self) -> bool:
        return self.kind == "image"

    def prompt_record(self) -> Mapping[str, Any]:
        value: dict[str, Any] = {
            "id": self.id,
            "name": self.original_name,
            "kind": self.kind,
            "mediaType": self.media_type,
            "bytes": self.size_bytes,
            "sha256": self.sha256,
        }
        if self.extracted_text is not None:
            value["content"] = self.extracted_text
        elif self.is_image:
            value["content"] = "Attached as a visual image input."
        return value


@dataclass(frozen=True, init=False)
class AssistantTurnResult:
    runtime_session_id: str
    response: str
    trace_path: str
    usage: Mapping[str, Any] | None
    runtime_id: str

    def __init__(
        self,
        runtime_session_id: str | None = None,
        response: str = "",
        trace_path: str = "",
        usage: Mapping[str, Any] | None = None,
        runtime_id: str = "assistant",
        *,
        thread_id: str | None = None,
    ) -> None:
        resolved_session_id = runtime_session_id or thread_id
        if not resolved_session_id:
            raise ValueError("assistant result requires a runtime session id")
        object.__setattr__(self, "runtime_session_id", resolved_session_id)
        object.__setattr__(self, "response", response)
        object.__setattr__(self, "trace_path", trace_path)
        object.__setattr__(self, "usage", usage)
        object.__setattr__(self, "runtime_id", runtime_id)

    @property
    def thread_id(self) -> str:
        """Compatibility alias for an earlier Codex-only contract."""
        return self.runtime_session_id


class AssistantSessionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        trace_path: str | None = None,
        runtime_session_id: str | None = None,
        thread_id: str | None = None,
        usage: Mapping[str, Any] | None = None,
        runtime_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.trace_path = trace_path
        self.runtime_session_id = runtime_session_id or thread_id
        self.thread_id = self.runtime_session_id
        self.usage = usage
        self.runtime_id = runtime_id


class AssistantTurnCancelled(AssistantSessionError):
    pass


class AssistantBackend(Protocol):
    runtime_id: str
    capabilities: AssistantCapabilities

    @property
    def available(self) -> bool: ...

    def run(
        self,
        prompt: str,
        session_id: str | None,
        turn_id: str,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
        permission_mode: str | None = None,
        attachments: Sequence[AssistantAttachment] = (),
        on_event: Callable[[Mapping[str, Any]], None] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> AssistantTurnResult: ...
