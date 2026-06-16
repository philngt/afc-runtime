from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from wo_runtime.core.domain import current_workspace, domain_id_from_context, domain_resource_path, workspace_path
from wo_runtime.workspace import Workspace


RUNTIME_CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts"


def _json_name(value: str) -> str:
    name = str(value).strip()
    return name if name.endswith(".json") else f"{name}.json"


def _load_index(path: Path) -> dict[str, Path]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    raw_entries: Any = data.get("contracts") if isinstance(data, dict) else {}
    entries: dict[str, Any] = raw_entries if isinstance(raw_entries, dict) else {}
    if isinstance(raw_entries, list):
        entries = {}
        for item in raw_entries:
            if isinstance(item, dict) and item.get("id"):
                entries[str(item["id"])] = item
    out: dict[str, Path] = {}
    for contract_id, value in entries.items():
        if isinstance(value, dict):
            raw_ref = value.get("path") or value.get("file") or value.get("ref")
        else:
            raw_ref = value
        if not raw_ref:
            continue
        ref = Path(str(raw_ref))
        resolved = ref if ref.is_absolute() else path.parent / ref
        out[str(contract_id)] = resolved
        out[resolved.name] = resolved
    return out


def _runtime_candidates(contract_id_or_name: str) -> list[Path]:
    name = _json_name(contract_id_or_name)
    candidates = [RUNTIME_CONTRACTS_DIR / name]
    if str(contract_id_or_name).endswith(".json"):
        candidates.append(RUNTIME_CONTRACTS_DIR / str(contract_id_or_name))
    return candidates


def contracts_dir(workspace: Workspace | None = None) -> Path:
    return workspace_path(workspace, "contractsDir", "contracts")


def _indexed_candidate(index_path: Path, contract_id_or_name: str) -> Path | None:
    entries = _load_index(index_path)
    name = _json_name(contract_id_or_name)
    return entries.get(contract_id_or_name) or entries.get(name)


def _workspace_candidates(workspace: Workspace, contract_id_or_name: str) -> list[Path]:
    base = contracts_dir(workspace)
    candidate = _indexed_candidate(base / "contract-index.json", contract_id_or_name)
    out = [candidate] if candidate is not None else []
    out.append(base / _json_name(contract_id_or_name))
    return out


def domain_contracts_dir(domain_id: str, workspace: Workspace | None = None) -> Path:
    return domain_resource_path(domain_id, "contractsDir", workspace=workspace, legacy=Path("domains") / domain_id / "contracts")


def _domain_candidates(workspace: Workspace, domain_id: str, contract_id_or_name: str) -> list[Path]:
    base = domain_contracts_dir(domain_id, workspace)
    candidate = _indexed_candidate(base / "contract-index.json", contract_id_or_name)
    out = [candidate] if candidate is not None else []
    out.append(base / _json_name(contract_id_or_name))
    return out


def contract_path(
    contract_id_or_name: str,
    context: dict[str, Any] | None = None,
    *,
    workspace: Workspace | None = None,
    domain_id: str | None = None,
    must_exist: bool = True,
) -> Path:
    ws = current_workspace(workspace)
    resolved_domain = domain_id or domain_id_from_context(context, workspace=ws)
    candidates = [
        *_workspace_candidates(ws, contract_id_or_name),
        *_domain_candidates(ws, str(resolved_domain), contract_id_or_name),
        *_runtime_candidates(contract_id_or_name),
    ]
    seen: set[Path] = set()
    unique = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        unique.append(candidate)
        if candidate.exists():
            return candidate
    if must_exist:
        searched = ", ".join(str(path) for path in unique)
        raise FileNotFoundError(f"contract not found: {contract_id_or_name}; searched: {searched}")
    return unique[0]


def load_contract(
    contract_id_or_name: str,
    context: dict[str, Any] | None = None,
    *,
    workspace: Workspace | None = None,
    domain_id: str | None = None,
) -> dict[str, Any]:
    path = contract_path(contract_id_or_name, context, workspace=workspace, domain_id=domain_id)
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}
