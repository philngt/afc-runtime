# Host-Owned Structured Output

Headless queue workers can opt into `exec.result_protocol: "json-file"`.
The child produces one JSON result; the supervisor owns the claim, validation,
and terminal commit. Use this when a provider CLI or instance adapter accepts
an output schema and writes its final response to a file.

An absent `result_protocol`, or the explicit value `"worker"`, preserves the
existing receipt-bound worker protocol. Interactive sessions, planners, and
gateway roles continue to use their existing descriptors.
Use the headless supervisor for this automated path; existing interactive hooks
remain available.

## Descriptor contract

Put descriptors in the instance's `runtime/runtimes/` directory. This generic
example assumes an instance-supplied `workflow-worker` binary with these flags:

```json
{
  "name": "structured-worker",
  "bin": ["workflow-worker"],
  "kinds": ["supervisor"],
  "exec": {
    "result_protocol": "json-file",
    "args": [
      "--schema", "{output_schema}",
      "--context", "{task_context}",
      "--output", "{output_path}"
    ],
    "payload": "none"
  }
}
```

`exec.args` must contain both `{output_schema}` and `{output_path}`. The host
substitutes absolute paths. Pass `{task_context}` in the arguments when there
is no prompt payload. Alternatively, select a role field such as
`"payload": "prompt"` with `"payload_position": "last"`; the host adds the
schema, context, and output paths to that prompt. Other payload positions are
unsupported in this protocol.

Use this descriptor only for roles watching queue tasks, selecting them with
`--role`. A task must declare `outputContract`. Queue-free startup probes
(`--startup-probe` and `--startup-probe-only`) are unsupported for `json-file`
and are rejected before claiming work.

`--dry-run` is a read-only command preview. It leaves `{output_schema}`,
`{output_path}`, and `{task_context}` symbolic, claims no work, and creates no
invocation files. It does not validate per-task prerequisites or demonstrate
live provider/schema compatibility.

## Invocation files and ownership

For each claim, the supervisor resolves the declared output contract and
dependency results strictly. It creates a fresh directory beneath:

```text
<dataRoot>/runtime-invocations/<task-id>/attempt-<N>-<random>/
```

The directory contains a schema file and a task-context file. The schema is the
resolved contract without provider-specific relaxation. The context contains
`task` metadata with `claim` and `_claim` removed, plus `inputs` containing the
resolved dependency results. Missing, malformed, or failed dependencies prevent
the child from starting.

`{output_path}` points to `candidate.json`, which does not exist when the child
starts. The child writes only the result value required by `outputContract`,
not a queue result envelope or a claim receipt. Standard output remains a log;
the host does not extract JSON from prose or Markdown fences.
The candidate must be a regular, non-symlink UTF-8 file of at most 16 MiB.
Duplicate object keys and non-finite numbers are rejected.

The prompt instructs the child to perform one pass and leave queue completion
to the host. Bound claim receipt environment variables are not passed to the
child. This is an ownership protocol for a trusted subprocess, not a sandbox:
the child still shares host filesystem access. Instance tools, permissions,
provider credentials, and external side effects remain instance concerns.

## Completion and failures

Only an invocation that exits with code zero, does not time out, and writes a
valid candidate can succeed. The supervisor submits that candidate through
`complete_claim`; existing lease/attempt fencing, declared-contract validation,
and the instance result-validation hook still apply before terminal writes.

| Outcome | Host behavior |
| --- | --- |
| Valid JSON, valid contract, active claim, accepted hook | Commit through the existing queue boundary. |
| Missing file, malformed JSON, or rejected result | Requeue the owned claim with an incremented attempt; `--once` returns nonzero. |
| Nonzero child exit without a quota signal, or any timeout | Ignore the candidate and requeue the owned claim with an incremented attempt. |
| Nonzero child exit with a quota log signal | Keep existing quota backoff; ignore the candidate and requeue without incrementing the attempt, unless it timed out. |
| Preparation fails, such as a missing contract or dependency | Do not start the child; requeue without incrementing the attempt and block this trigger until supervisor restart; `--once` returns nonzero. |

A stale claim cannot be rescued by a valid file. Each invocation uses a new
directory, so a prior attempt's candidate is not reused. Retention and cleanup
of invocation files are an operational choice for the instance.
For a preparation failure, fix its cause and restart the supervisor to retry.
Quota wording in a log alone does not reject an otherwise valid zero-exit result.

Native structured output improves format adherence; it does not prove that
content is correct or that delivery occurred. Keep domain checks and real
delivery receipts in instance validators. Provider schema support varies: if
the provider rejects a contract, the invocation must fail rather than silently
drop constraints or fall back to free-form text. The runtime itself enforces
the documented [JSON Schema subset](PRIMITIVES.md#declared-output-contracts),
not every JSON Schema keyword. Check contracts against both boundaries.

## Instance example: Codex CLI

This is optional instance configuration, not a provider dependency of the core.
The [Codex non-interactive guide](https://developers.openai.com/codex/non-interactive-mode)
documents `--output-schema` and `-o` for a schema-constrained final response:

```json
{
  "name": "codex-structured",
  "bin": ["codex"],
  "kinds": ["supervisor"],
  "exec": {
    "result_protocol": "json-file",
    "args": ["exec", "--output-schema", "{output_schema}", "-o", "{output_path}"],
    "payload": "prompt",
    "payload_position": "last"
  }
}
```

Pin and verify the CLI version in the instance. Configure its model, credentials,
permissions, and sandbox policy there. A CLI accepting these flags does not
establish that every contract is supported or that a live provider call passed.

## Migrate one queue role

1. Declare and register an `outputContract` for every task handled by the role.
2. Add an opt-in descriptor and select it for that role in the fleet.
3. Update the role prompt and any injected guidance: describe the work and its
   result, and remove instructions to claim, complete, fail, or move queue files.
4. Keep planner/gateway and other queue-free roles on their existing descriptor.
5. Verify fixtures, failed dependencies, missing/invalid output, stale claims,
   retry isolation, nonzero exits, and timeouts before a live smoke run.

From an instance checkout, with a prepared fixture task ready for `worker`:

```bash
vendor/woafc-runtime/bin/wo-runtime selftest
vendor/woafc-runtime/bin/wo-runtime runtime selftest --workspace-root "$PWD"
vendor/woafc-runtime/bin/wo-runtime runtime supervisor \
  --workspace-root "$PWD" --runtime structured-worker --role worker --once
```

The supervisor command executes the selected binary; use a deterministic fixture
adapter for offline verification. Then separately test the real provider and,
when applicable, real delivery. Runtime selftests verify synthetic behavior and
adapter wiring; they do not prove provider availability, native schema support,
semantic quality, or external delivery success.
