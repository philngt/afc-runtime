"""Immutable and atomic artifact primitives shared by workflow instances."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Collection, Mapping


_SAFE_SEGMENT = re.compile(r"[^a-zA-Z0-9._-]+")


class ArtifactError(RuntimeError):
    """An artifact cannot be safely persisted or replayed."""


class ArtifactConflictError(ArtifactError):
    """An immutable destination already contains different content."""


@dataclass(frozen=True)
class ArtifactRef:
    relative_path: str
    sha256: str
    byte_count: int


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _stage_bytes(destination: Path, payload: bytes) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=f".{uuid.uuid4().hex}.tmp",
        dir=destination.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
    return temporary_name


def atomic_write_bytes(path: Path | str, payload: bytes) -> None:
    destination = Path(path)
    temporary_name = _stage_bytes(destination, payload)
    try:
        os.replace(temporary_name, destination)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _atomic_create_bytes(path: Path, payload: bytes) -> bool:
    """Publish bytes without replacing a concurrent writer."""

    temporary_name = _stage_bytes(path, payload)
    try:
        try:
            os.link(temporary_name, path)
        except FileExistsError:
            return False
        return True
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def atomic_write_text(path: Path | str, value: str, *, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, value.encode(encoding))


def atomic_write_json(path: Path | str, value: Any, *, pretty: bool = False) -> None:
    if pretty:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
    else:
        payload = canonical_json_bytes(value)
    atomic_write_bytes(path, payload + b"\n")


def idempotent_write_json(
    path: Path | str,
    payload: Mapping[str, Any],
    *,
    ignore_keys: Collection[str] = (),
    pretty: bool = True,
) -> bool:
    """Write once, or accept an equivalent replay.

    Returns ``True`` when a new file was committed and ``False`` for an
    equivalent replay. Existing malformed or conflicting content is never
    overwritten.
    """

    destination = Path(path)
    document = dict(payload)
    ignored = set(ignore_keys)

    def accept_existing() -> bool:
        try:
            existing = json.loads(destination.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ArtifactConflictError(
                f"refusing to overwrite invalid artifact: {destination}"
            ) from exc
        if not isinstance(existing, Mapping):
            raise ArtifactConflictError(
                f"refusing to overwrite non-object artifact: {destination}"
            )
        comparable_existing = {
            key: value for key, value in existing.items() if key not in ignored
        }
        comparable_payload = {
            key: value for key, value in document.items() if key not in ignored
        }
        if comparable_existing != comparable_payload:
            raise ArtifactConflictError(
                f"refusing to overwrite conflicting artifact: {destination}"
            )
        return False

    if destination.exists():
        return accept_existing()
    encoded = (
        json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        if pretty
        else canonical_json_bytes(document)
    ) + b"\n"
    if _atomic_create_bytes(destination, encoded):
        return True
    return accept_existing()


class ContentAddressedStore:
    """Immutable content-addressed store grouped by source and capture date."""

    def __init__(self, root: Path | str):
        self.root = Path(root)

    def write_bytes(
        self,
        source_type: str,
        payload: bytes,
        *,
        extension: str,
        captured_at: datetime | None = None,
    ) -> ArtifactRef:
        captured_at = captured_at or datetime.now(timezone.utc)
        if captured_at.tzinfo is None:
            captured_at = captured_at.replace(tzinfo=timezone.utc)
        digest = hashlib.sha256(payload).hexdigest()
        safe_source = _SAFE_SEGMENT.sub("-", source_type).strip("-") or "source"
        safe_ext = _SAFE_SEGMENT.sub("", extension.lstrip(".")) or "bin"
        relative = Path(safe_source) / captured_at.strftime("%Y/%m/%d") / f"{digest}.{safe_ext}"
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            try:
                existing = destination.read_bytes()
            except OSError as exc:
                raise ArtifactError(f"could not verify artifact: {destination}") from exc
            if len(existing) != len(payload) or hashlib.sha256(existing).hexdigest() != digest:
                raise ArtifactConflictError(f"content-addressed artifact is corrupt: {destination}")
        else:
            atomic_write_bytes(destination, payload)
        return ArtifactRef(relative.as_posix(), digest, len(payload))

    def write_json(
        self,
        source_type: str,
        value: Any,
        *,
        captured_at: datetime | None = None,
    ) -> ArtifactRef:
        return self.write_bytes(
            source_type,
            canonical_json_bytes(value),
            extension="json",
            captured_at=captured_at,
        )
