#!/usr/bin/env python
"""Generic runtime-adapter wiring self-test (no model calls, no network).

For every runtime descriptor in runtime/runtimes/ (or --runtime NAME), verifies:
  1. descriptor shape (name, bin, kinds; supervisor => exec block);
  2. interactive resolve.py emits BIN + role_env + ARGs (binary stubbed);
  3. headless supervisor (if kind "supervisor") schedules work, honours exclude (dry-run);
  4. Stop hook (if "hook" declared) continues on work and idles/excludes correctly.

Adding a runtime = drop runtime/runtimes/<name>.json; this test then covers it too.
"""
from __future__ import annotations
import argparse, json, os, re, shutil, subprocess, sys, tempfile
from pathlib import Path

from wo_runtime.runtime.context import resolve_context_prepend
from wo_runtime.workspace import DEFAULT_WORKSPACE_CONFIG, load_workspace

RUNTIME_ROOT = Path(__file__).resolve().parents[2]
ROOT = Path.cwd()
CLI = RUNTIME_ROOT / "bin" / "wo-runtime"
PY = sys.executable
PASS = 0

SYNTH_ROLES = {
    "wk": {"watch": "tasks/ready-wk", "kickoff": "/wk", "prompt": "do wk {worker_id}", "worker_id": "wk-1"},
    "gw": {"watch": ["gwin", "resgw"], "kickoff": "/gw", "prompt": "do gw",
           "trigger_match": {"gwin": "*.json", "resgw": "*.final.json"},
           "exclude_if_sibling_exists": "sent/{name}"},
}


def check(cond: bool, msg: str) -> None:
    global PASS
    if not cond:
        raise AssertionError(msg)
    PASS += 1
    print(f"  PASS: {msg}")


def run(cmd, cwd=None, env=None):
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)


def with_runtime_pythonpath(env: dict[str, str] | None = None) -> dict[str, str]:
    out = dict(env or os.environ)
    out["PYTHONPATH"] = str(RUNTIME_ROOT) + (os.pathsep + out["PYTHONPATH"] if out.get("PYTHONPATH") else "")
    return out


def hook_message(protocol: str, res) -> str:
    if protocol == "stdout-json-block":
        try:
            return json.loads(res.stdout)["reason"]
        except Exception:
            return ""
    return res.stderr  # stderr-exit2


def hook_continues(protocol: str, res) -> bool:
    if protocol == "stdout-json-block":
        try:
            return json.loads(res.stdout).get("decision") == "block"
        except Exception:
            return False
    return res.returncode == 2


def test_runtime(name: str) -> None:
    print(f"== runtime: {name} ==")
    desc = json.loads((ROOT / "runtime" / "runtimes" / f"{name}.json").read_text(encoding="utf-8"))
    check(desc.get("name") == name, f"{name}: descriptor name matches file")
    check(isinstance(desc.get("bin"), list) and desc["bin"], f"{name}: has bin candidates")
    check(isinstance(desc.get("kinds"), list) and desc["kinds"], f"{name}: declares kinds")
    if "supervisor" in desc["kinds"]:
        check("exec" in desc, f"{name}: supervisor kind has exec block")

    # 1b. context prepend (optional): files exist, roles known, helper produces marker
    if "context" in desc:
        ctx = desc["context"]
        by_role = ctx.get("prepend_files_by_role", {}) or {}
        by_kind = ctx.get("prepend_files_by_product_kind", {}) or {}
        all_files = set(ctx.get("prepend_files", []) or [])
        for files in by_role.values():
            all_files.update(files)
        for kind, spec in by_kind.items():
            check(isinstance(kind, str) and bool(kind), f"{name}: context product kind is named")
            if isinstance(spec, dict):
                for role, files in spec.items():
                    if role != "*":
                        real_roles = json.loads((ROOT / "runtime" / "roles.json").read_text(encoding="utf-8"))
                        check(role in real_roles, f"{name}: context.by_product_kind['{kind}']['{role}'] is a known role")
                    all_files.update(files)
            else:
                all_files.update(spec if isinstance(spec, list) else [])
        for rel in sorted(all_files):
            check((ROOT / rel).exists(), f"{name}: context file exists: {rel}")
        if by_role:
            real_roles = json.loads((ROOT / "runtime" / "roles.json").read_text(encoding="utf-8"))
            for role in by_role:
                check(role in real_roles, f"{name}: context.by_role['{role}'] is a known role")
            sample = next(iter(by_role))
            out = resolve_context_prepend(desc, sample, ROOT)
            check("[CONTEXT:" in out and "[/CONTEXT:" in out,
                  f"{name}: prepend for role '{sample}' includes CONTEXT markers")
            if by_kind:
                with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as f:
                    json.dump({
                        "domainId": "kids-printable-pdf",
                        "product": {"productType": "activity_calendar", "title": "365 Screen-Free Activities"}
                    }, f)
                    activity_calendar_trigger = f.name
                try:
                    activity_out = resolve_context_prepend(desc, "worker-content", ROOT, activity_calendar_trigger)
                    check("alphabet-tracing-workbook" not in activity_out,
                          f"{name}: activity-calendar context does not inject alphabet skill")
                finally:
                    try:
                        os.unlink(activity_calendar_trigger)
                    except OSError:
                        pass
                if "alphabet-tracing" in by_kind:
                    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as f:
                        json.dump({
                            "domainId": "kids-printable-pdf",
                            "product": {"productType": "alphabet_tracing", "title": "Alphabet Tracing Workbook"}
                        }, f)
                        alphabet_trigger = f.name
                    try:
                        alphabet_out = resolve_context_prepend(desc, "worker-content", ROOT, alphabet_trigger)
                        check("alphabet-tracing-workbook" in alphabet_out,
                              f"{name}: alphabet-tracing context injects alphabet skill")
                    finally:
                        try:
                            os.unlink(alphabet_trigger)
                        except OSError:
                            pass

    # 2. interactive resolve -- only meaningful if descriptor declares an interactive block
    if desc.get("interactive"):
        roles = json.loads((ROOT / "runtime" / "roles.json").read_text(encoding="utf-8"))
        sample = next(iter(roles))
        env = os.environ.copy()
        if desc.get("bin_env"):
            env[desc["bin_env"]] = "stub-bin"
        res = run([PY, str(CLI), "runtime", "resolve", "--workspace-root", str(ROOT),
                   "--runtime", name, "--role", sample], env=env)
        check(res.returncode == 0, f"{name}: resolve.py succeeds for role '{sample}'")
        check("BIN\tstub-bin" in res.stdout, f"{name}: resolve emits stubbed BIN")
        role_env_name = (desc.get("role_env") or {}).get("role")
        if role_env_name:
            check(f"ENV\t{role_env_name}\t{sample}" in res.stdout, f"{name}: resolve sets {role_env_name}")

    # Daemon kinds: verify daemon_script path exists
    if "daemon" in desc.get("kinds", []):
        ds = desc.get("daemon_script")
        if ds:
            check((ROOT / ds).exists(), f"{name}: daemon_script {ds} exists")

    with tempfile.TemporaryDirectory(prefix=f"rt-{name}-") as d:
        sb = Path(d)
        (sb / "runtime" / "runtimes").mkdir(parents=True)
        (sb / "runtime" / "roles.json").write_text(json.dumps(SYNTH_ROLES), encoding="utf-8")
        shutil.copy2(ROOT / "runtime" / "runtimes" / f"{name}.json", sb / "runtime" / "runtimes" / f"{name}.json")
        for p in ("tasks/ready-wk", "gwin", "resgw", "sent"):
            (sb / p).mkdir(parents=True, exist_ok=True)

        # 3. supervisor dry-run
        if "supervisor" in desc["kinds"]:
            def sup_run(role):
                return run([PY, str(CLI), "runtime", "supervisor", "--workspace-root", str(sb), "--runtime", name,
                            "--role", role, "--once", "--dry-run", "--bin", "stub-bin"])
            (sb / "tasks/ready-wk" / "t.json").write_text("{}", encoding="utf-8")
            r = sup_run("wk")
            check("would run:" in r.stdout and "stub-bin" in r.stdout, f"{name}: supervisor schedules queued work")
            # exclude: gw final WITH sibling => not scheduled
            (sb / "resgw" / "x.final.json").write_text("{}", encoding="utf-8")
            (sb / "sent" / "x.json").write_text("{}", encoding="utf-8")
            check("would run:" not in sup_run("gw").stdout, f"{name}: supervisor skips excluded gateway final")
            (sb / "sent" / "x.json").unlink()
            check("would run:" in sup_run("gw").stdout, f"{name}: supervisor schedules gateway final once sibling gone")
            # concurrency: 2 ready files + max-inflight 2 => 2 launches in one scan
            (sb / "tasks/ready-wk" / "t2.json").write_text("{}", encoding="utf-8")
            rc = run([PY, str(CLI), "runtime", "supervisor", "--workspace-root", str(sb),
                      "--runtime", name, "--role", "wk",
                      "--once", "--dry-run", "--bin", "stub-bin", "--max-inflight", "2"])
            check(rc.stdout.count("would run:") >= 2, f"{name}: supervisor runs N concurrent passes (max-inflight)")

        # 4. hook behaviour
        hook = desc.get("hook")
        if hook:
            src = ROOT / hook["path"]
            dst = sb / hook["path"]
            dst.parent.mkdir(parents=True, exist_ok=True)
            text = src.read_text(encoding="utf-8")
            if hook.get("fast_patch"):
                old, new = hook["fast_patch"]
                text = text.replace(old, new)
            dst.write_text(text, encoding="utf-8")
            proto = hook["protocol"]
            role_var = (desc.get("role_env") or {}).get("role", "ROLE")

            henv = with_runtime_pythonpath(); henv[role_var] = "wk"
            if hook.get("timeout_env"):
                henv[hook["timeout_env"]] = "0"
            (sb / "tasks/ready-wk" / "w.json").write_text("{}", encoding="utf-8")
            rw = run([PY, str(dst)], cwd=sb, env=henv)
            check(hook_continues(proto, rw), f"{name}: hook continues session on work")
            check("Work detected" in hook_message(proto, rw), f"{name}: hook reports work detected")

            # If descriptor wires `context`, verify the Stop-hook continuation prompt
            # carries the prepended skill body too -- the wake path must not silently
            # skip context that the headless/interactive paths inject.
            if "context" in desc:
                desc_path = sb / "runtime" / "runtimes" / f"{name}.json"
                sandbox_desc = json.loads(desc_path.read_text(encoding="utf-8"))
                skill_rel = "skills/_selftest/probe.md"
                (sb / "skills" / "_selftest").mkdir(parents=True, exist_ok=True)
                (sb / skill_rel).write_text("# selftest skill probe\nbody-marker-XYZ\n", encoding="utf-8")
                sandbox_desc["context"] = {"prepend_files_by_role": {"wk": [skill_rel]}}
                desc_path.write_text(json.dumps(sandbox_desc), encoding="utf-8")
                # Reuse the staged hook + henv; the trigger is still in tasks/ready-wk.
                # `scripts/runtime/_context.py` must be available in the sandbox for the
                # hook to import it -- copy it next to the sandbox script dir.
                src_ctx = ROOT / "scripts" / "runtime" / "_context.py"
                if src_ctx.exists():
                    dst_ctx = sb / "scripts" / "runtime" / "_context.py"
                    dst_ctx.parent.mkdir(parents=True, exist_ok=True)
                    dst_ctx.write_text(src_ctx.read_text(encoding="utf-8"), encoding="utf-8")
                rc = run([PY, str(dst)], cwd=sb, env=henv)
                check("[CONTEXT: " + skill_rel + "]" in hook_message(proto, rc),
                      f"{name}: Stop-hook continuation prompt includes prepended context")

            # idle/exclude needs the fast path (timeout_env or fast_patch)
            if hook.get("timeout_env") or hook.get("fast_patch"):
                genv = with_runtime_pythonpath(); genv[role_var] = "gw"
                if hook.get("timeout_env"):
                    genv[hook["timeout_env"]] = "0"
                # Clean gw watch dirs so leftovers from the supervisor test don't leak in.
                for sub in ("resgw", "gwin", "sent"):
                    for f in (sb / sub).glob("*"):
                        f.unlink()
                (sb / "resgw" / "y.final.json").write_text("{}", encoding="utf-8")
                (sb / "sent" / "y.json").write_text("{}", encoding="utf-8")
                rg = run([PY, str(dst)], cwd=sb, env=genv)
                check("Work detected" not in hook_message(proto, rg),
                      f"{name}: hook excludes replied gateway final (idle/heartbeat)")


def main(argv: list[str] | None = None) -> int:
    global ROOT
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime")
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--workspace-config", default=DEFAULT_WORKSPACE_CONFIG)
    args = parser.parse_args(argv)
    ROOT = load_workspace(args.workspace_root, args.workspace_config).root
    target = args.runtime
    names = [target] if target else sorted(p.stem for p in (ROOT / "runtime" / "runtimes").glob("*.json"))
    for n in names:
        test_runtime(n)
    print(f"[runtime-selftest] OK ({PASS} checks across {len(names)} runtime(s): {', '.join(names)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
