from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_WORKSPACE_CONFIG = "runtime/workspace.json"
DEFAULT_DATA_ROOT_ENV = "PIPELINE_DATA_ROOT"
DEFAULT_DATA_ROOT = ".run/data"
RUNTIME_DIRS = ("inbox", "tasks", "results", "assets", "prompts", "tg", "events")


@dataclass(frozen=True)
class Workspace:
    root: Path
    config_path: Path
    config: dict[str, Any]
    data_root_raw: str
    data_root: Path


def load_workspace(
    workspace_root: str | os.PathLike[str] | None = None,
    workspace_config: str | os.PathLike[str] | None = None,
) -> Workspace:
    """Resolve instance workspace settings without inferring from the submodule path."""
    initial_root = Path(workspace_root or os.getcwd()).resolve()
    config_rel = Path(workspace_config or DEFAULT_WORKSPACE_CONFIG)
    config_path = config_rel if config_rel.is_absolute() else initial_root / config_rel
    config: dict[str, Any] = {}
    if config_path.exists():
        config = json.loads(config_path.read_text(encoding="utf-8"))

    configured_root = config.get("workspaceRoot")
    if configured_root:
        configured = Path(str(configured_root))
        root = configured if configured.is_absolute() else (initial_root / configured)
        root = root.resolve()
        config_path = config_rel if config_rel.is_absolute() else root / config_rel
    else:
        root = initial_root

    data_env = str(config.get("dataRootEnv") or DEFAULT_DATA_ROOT_ENV)
    data_root_raw = os.environ.get(data_env) or str(config.get("defaultDataRoot") or DEFAULT_DATA_ROOT)
    data_root = Path(data_root_raw)
    return Workspace(
        root=root,
        config_path=config_path,
        config=config,
        data_root_raw=data_root_raw,
        data_root=data_root,
    )


def chdir_workspace(workspace: Workspace) -> None:
    os.chdir(workspace.root)


def ensure_data_root(workspace: Workspace) -> None:
    """Move legacy root-level runtime dirs under the configured data root."""
    chdir_workspace(workspace)
    workspace.data_root.mkdir(parents=True, exist_ok=True)
    for name in RUNTIME_DIRS:
        source = Path(name)
        target = workspace.data_root / name
        if source.is_symlink():
            source.unlink()
            target.mkdir(parents=True, exist_ok=True)
            continue
        if source.exists():
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source), str(target))
            else:
                target.mkdir(parents=True, exist_ok=True)
                for child in list(source.iterdir()):
                    shutil.move(str(child), str(target / child.name))
                source.rmdir()
        else:
            target.mkdir(parents=True, exist_ok=True)


def load_and_prepare(
    workspace_root: str | os.PathLike[str] | None = None,
    workspace_config: str | os.PathLike[str] | None = None,
) -> Workspace:
    workspace = load_workspace(workspace_root, workspace_config)
    ensure_data_root(workspace)
    return workspace
