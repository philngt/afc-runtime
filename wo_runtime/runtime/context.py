"""Runtime-scoped context prepend.

Reads `descriptor["context"]` (optional) and produces a string that should be
prepended to the role's payload before the runtime CLI consumes it.

Schema (declared by `runtime/runtimes/<name>.json`):

    "context": {
      "prepend_files": ["<rel/path>"],            # optional global context
      "prepend_files_by_role": {                 # optional role context
        "<role>": ["<rel/path/to/file>", ...]
      },
      "prepend_files_by_product_kind": {          # optional task-aware context
        "<product-kind>": {
          "*": ["<rel/path>"],
          "<role>": ["<rel/path>"]
        }
      }
    }

If neither key matches, prepend is empty (no-op). If a declared file is missing
on disk, raises SystemExit -- a typo in the descriptor should fail loudly rather
than silently dropping the intended context.

Each file is wrapped with a marker pair so the agent can see boundaries:

    [CONTEXT: <relpath>]
    <body>
    [/CONTEXT: <relpath>]

Used by `supervisor.py` (headless exec), `resolve.py` (interactive), and runtime
hooks. Callers with a queue trigger path should pass it so product-specific
context can be selected from the task/request JSON. Interactive manual launches
have no trigger and therefore receive only global/role context.
"""
from __future__ import annotations

from typing import Any
from pathlib import Path

from wo_runtime.core.context_packs import ensure_context_pack, resolve_ref
from wo_runtime.workspace import load_workspace


def _as_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item]
    return []


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _load_trigger_context(root: Path, trigger_path: str | Path | None) -> dict[str, Any]:
    if not trigger_path:
        return {}
    path = Path(trigger_path)
    if not path.is_absolute():
        path = root / path
    try:
        import sys

        sys.path.insert(0, str(root))
        from scripts.framework.product_kind import load_context_file, flatten_product_context

        return flatten_product_context(load_context_file(path))
    except Exception:
        return {}


def infer_product_kind(context: dict[str, Any] | None = None) -> str:
    try:
        from scripts.framework.product_kind import infer_product_kind as shared_infer_product_kind

        return shared_infer_product_kind(context)
    except Exception:
        context = context or {}
        explicit = str(context.get("productKind") or context.get("productType") or context.get("kind") or "")
        return explicit.casefold().replace("_", "-") or "generic"


def _product_kind_files(ctx: dict[str, Any], role: str, product_kind: str) -> list[str]:
    by_kind = ctx.get("prepend_files_by_product_kind")
    if not isinstance(by_kind, dict) or not product_kind:
        return []
    spec = by_kind.get(product_kind)
    if spec is None:
        return []
    if isinstance(spec, list):
        return _as_list(spec)
    if not isinstance(spec, dict):
        return []
    files: list[str] = []
    files.extend(_as_list(spec.get("*")))
    files.extend(_as_list(spec.get(role)))
    return files


def _context_materialized_enabled(ctx: dict[str, Any]) -> bool:
    materialized = ctx.get("materialized")
    if isinstance(materialized, dict):
        return bool(materialized.get("enabled", False))
    return bool(materialized)


def _trigger_value(trigger_context: dict[str, Any], key: str) -> str:
    value = trigger_context.get(key)
    if value:
        return str(value)
    raw = trigger_context.get("_raw") if isinstance(trigger_context.get("_raw"), dict) else {}
    product = raw.get("product") if isinstance(raw.get("product"), dict) else {}
    source = product.get("source") if isinstance(product.get("source"), dict) else {}
    return str(raw.get(key) or product.get(key) or source.get(key) or "")


def resolve_context_files(
    descriptor: dict,
    role: str,
    root: Path,
    trigger_path: str | Path | None = None,
) -> tuple[list[str], dict[str, str]]:
    ctx = descriptor.get("context")
    if not ctx:
        return [], {}
    trigger_context = _load_trigger_context(root, trigger_path)
    product_kind = infer_product_kind(trigger_context) if trigger_context else ""
    files: list[str] = []
    files.extend(_as_list(ctx.get("prepend_files")))
    by_role = ctx.get("prepend_files_by_role")
    if isinstance(by_role, dict):
        files.extend(_as_list(by_role.get(role)))
    files.extend(_product_kind_files(ctx, role, product_kind))
    meta = {
        "productKind": product_kind,
        "domainId": _trigger_value(trigger_context, "domainId"),
        "pipelineId": _trigger_value(trigger_context, "pipelineId"),
    }
    return _dedupe(files), meta


def resolve_context_prepend(
    descriptor: dict,
    role: str,
    root: Path,
    trigger_path: str | Path | None = None,
) -> str:
    ctx = descriptor.get("context")
    if not ctx:
        return ""
    files, meta = resolve_context_files(descriptor, role, root, trigger_path=trigger_path)
    if not files:
        return ""
    if _context_materialized_enabled(ctx):
        workspace = load_workspace(root)
        pack = ensure_context_pack(
            workspace,
            runtime_name=str(descriptor.get("name") or ""),
            role=role,
            domain_id=meta.get("domainId") or "",
            pipeline_id=meta.get("pipelineId") or "",
            product_kind_id=meta.get("productKind") or "",
            source_refs=files,
            pack_kind="runtime-prepend",
        )
        compiled = resolve_ref(workspace, str(pack.get("compiledRef") or ""))
        if not compiled.exists():
            raise SystemExit(f"[context] missing materialized context pack: {pack.get('compiledRef')}")
        header = (
            f"[CONTEXT-PACK: {pack.get('ref', '')}]\n"
            f"kind: {pack.get('kind', '')}\n"
            f"packKey: {pack.get('packKey', '')}\n"
            f"sourceHash: {pack.get('sourceHash', '')}\n"
            f"status: {pack.get('status', '')}\n"
            "[/CONTEXT-PACK]"
        )
        return header + "\n\n" + compiled.read_text(encoding="utf-8").strip() + "\n\n"
    parts = []
    for rel in files:
        fp = root / rel
        if not fp.exists():
            raise SystemExit(
                f"[context] missing prepend file: {rel} "
                f"(declared in runtime/runtimes/{descriptor.get('name', '?')}.json)"
            )
        body = fp.read_text(encoding="utf-8").strip()
        parts.append(f"[CONTEXT: {rel}]\n{body}\n[/CONTEXT: {rel}]")
    return "\n\n".join(parts) + "\n\n"
