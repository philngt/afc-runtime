from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from wo_runtime.core.validation import safe_file, validate_identifier


_ORPHAN_LOCK_GRACE_SECONDS = 900.0


class QueueBusyError(RuntimeError):
    pass


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _stale_lock(path: Path, stale_seconds: float) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        pid = int(payload.get("pid") or 0)
        created = float(payload.get("created") or 0.0)
    except (OSError, ValueError, TypeError, AttributeError, OverflowError):
        pid, created = 0, 0.0
    if pid <= 0:
        # O_EXCL precedes the metadata write. A crash in that interval must
        # remain recoverable even when a caller disables live-lock age expiry.
        # Use mtime and a grace period so a publisher still writing metadata
        # is not mistaken for an abandoned ownerless lock.
        stale_seconds = min(stale_seconds, _ORPHAN_LOCK_GRACE_SECONDS)
        created = 0.0
    try:
        age = time.time() - created if created else time.time() - path.stat().st_mtime
    except FileNotFoundError:
        return True
    return age > stale_seconds or (pid > 0 and not _pid_alive(pid))


@contextmanager
def task_lock(
    data_root: Path,
    task_id: str,
    *,
    purpose: str,
    stale_seconds: float = 900.0,
) -> Iterator[Path]:
    safe_id = validate_identifier(task_id, "task.id")
    lock_dir = data_root / ".locks" / "queue"
    lock_dir.mkdir(parents=True, exist_ok=True)
    path = safe_file(lock_dir, safe_id, suffix=".lock", field="task.id")
    payload = json.dumps(
        {"pid": os.getpid(), "created": time.time(), "purpose": purpose},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    for attempt in range(2):
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            break
        except FileExistsError as exc:
            if attempt == 0 and _stale_lock(path, stale_seconds):
                try:
                    path.unlink()
                except OSError:
                    pass
                continue
            raise QueueBusyError(f"task_locked:{safe_id}") from exc
    else:  # pragma: no cover - loop either breaks or raises
        raise QueueBusyError(f"task_locked:{safe_id}")
    try:
        os.write(fd, payload)
        os.close(fd)
        fd = -1
        yield path
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
