# Runtime Evolution: Reproducible Work and Verifiable Results

## Direction

Develop AFC as a domain-neutral runtime that can explain which inputs and
execution rules produced an artifact, which checks assessed it, and whether the
result is still eligible for downstream use. Preserve the existing file queue,
one-pass workers, provider adapters, and thin-instance ownership model.

The current delivery is **Phase 1: validated request-plan publication** below.
Execution snapshots, verification bindings, rework routing, and adaptive plans
are subsequent proposals, not capabilities delivered by this change.

## Existing foundation

AFC already provides:

- `queue.v2` claims, leases, receipt fencing, and idempotent terminal commits;
- strict dependency-result resolution for receipt-bound workers;
- declared `outputContract` validation before successful terminal publication;
- data-driven pipelines, fanout, input aliases, and generic context assembly;
- materialized context packs with source hashes and task input references;
- atomic writes, immutable writes, and verified content-addressed storage;
- asset manifests, audit reports, result hooks, and neutral status projection.

These remain the implementation foundation. A schema-valid output proves only
the properties checked by its schema and hooks. A content digest identifies
bytes; it does not establish the truth or quality of their contents.

See [Core](CORE.md), [Queue](QUEUE.md), and [Primitives](PRIMITIVES.md) for the
current contracts and compatibility behavior.

## Ownership

| Layer | Responsibility |
| --- | --- |
| Runtime core | Queue correctness, graph validation, publication, generic reference and contract checks, reusable state projection |
| Instance data and handlers | Workflow topology, role guidance, source policy, quality criteria, review decisions, approval and delivery policy |
| Runtime adapter | Launching one bounded invocation with its configured context and process budget |
| Optional plugin | External integrations that depend on runtime contracts and remain explicitly enabled |

Product skills and role catalogs belong in an instance or template repository.
Core must not import them or infer product policy from a role name. A declared
capability describes routing; any authorization still comes from the instance
and its host boundary.

## Ideas carried forward

| Reference | Useful idea | AFC adaptation |
| --- | --- | --- |
| Academic Research Skills | Durable handoffs, provenance, claim-to-evidence links, explicit verification limits | Bind artifacts and assessments to exact inputs; retain unresolved checks in typed records |
| Agency Agents | Clear role deliverables, acceptance criteria, actionable review feedback, bounded correction | Instance-owned role contracts and review/rework stages with explicit evidence |
| Decepticon | Pinned workflows, objective dependencies, version checks, fresh task context, evidence invalidation | Strengthen the existing planner and context boundary before adding dynamic planning |

The references motivate independent runtime design. Their domain taxonomies,
prompts, scoring rules, deployment stacks, and mandatory workflow sequences are
not AFC runtime requirements. Primary sources are linked at the end.

## Phase 1: Validate and publish a complete request plan

**Outcome:** the scheduler sees a complete validated plan, and a planning retry
cannot silently replace work that already belongs to the request.

**Placement:** generic planner, queue publication boundary, and synthetic tests.
Pipeline topology and stage meanings remain instance data.

Validate the entire compiled graph before making it executable. In addition to
duplicate IDs, missing dependencies, self-dependencies, and root validation,
detect cycles across multiple tasks. Invalid input must fail before any part of
that request becomes schedulable.

Treat publication as a request-level operation. A crash between individual file
writes must not expose a partially published plan to workers. Distinguish an
equivalent retry from a conflicting plan for the same request. Preserve task
envelopes that have already moved through the queue; publication is not a way to
reset claimed or terminal work.

Acceptance scenarios:

- a valid fanout/join plan becomes schedulable as a complete request;
- a multi-node cycle, missing dependency, or invalid root is rejected;
- interrupted publication exposes no executable partial request;
- an equivalent retry is idempotent, including after publication;
- conflicting reuse of a request ID fails without overwriting existing work;
- queue receipts, deadlines, and terminal fencing retain their behavior.

## Phase 2: Freeze execution rules per request

**Outcome:** a resumed request uses the same pipeline, output schemas, and
declared context that governed its initial execution.

**Placement:** a generic immutable execution manifest and resolvers in core;
the instance declares the resources and non-secret configuration to bind.

An exact runtime commit and a content-hashed context pack already exist as
building blocks. Extend them with a per-request manifest that binds the request,
resolved pipeline, contract bytes, and required context references. Pin the
resolved content, including inherited pipeline definitions, rather than only a
mutable file path or contract ID. Do not include credentials in a manifest.

Task execution and terminal validation must resolve through the same manifest.
A changed schema must not silently change what an in-flight task can commit.
New requests may select updated configuration; an existing request needs an
explicit migration or a new run to adopt it.

Acceptance scenarios:

- editing a pipeline, output schema, or guidance file does not alter a pinned run;
- missing or corrupt pinned content produces a specific execution error;
- replay reads the same manifest and input digests;
- legacy tasks retain documented behavior through an explicit compatibility path.

## Phase 3: Bind evidence and verification to artifacts

**Outcome:** downstream release checks can verify that an assessment applies to
the exact artifact being consumed.

**Placement:** reusable reference validation in core; criterion definitions,
semantic checks, approval rules, and delivery gates in instance contracts and
handlers. Use existing result hooks before introducing another gate engine.

Extend existing artifact and audit records with explicit links between producer
task/attempt, input digests, execution manifest, assessed output digest, checker
execution, criterion version, and evidence references. Keep these records
immutable. Do not infer a successful assessment from a filename or free-text
summary.

Execution success and semantic acceptance are separate facts. A reviewer may
successfully complete its task with a `needs_changes` verdict. That is different
from a reviewer process failing to run. The delivery handler checks the verdict
and required evidence before any external side effect. The finalizer uses the
same assessment and the destination's real receipt; a finalization-only gate is
too late if delivery has already happened. See
[Workflow Authoring](WORKFLOW-AUTHORING.md).

Acceptance scenarios:

- a passing review of artifact A cannot approve changed artifact B;
- missing evidence, digest mismatch, or an unperformed required check blocks release;
- a valid review that requests changes is retained as a successful review execution;
- distinct reviewer roles are recorded without claiming automatic model independence;
- instance quality decisions cannot bypass queue fencing or generic output validation.

## Phase 4: Bounded correction with durable handoffs

**Outcome:** review feedback produces focused follow-up work while preserving
the history and cost of every attempt.

**Placement:** role guidance, review criteria, correction budgets, and routing in
the instance; reusable task/reference plumbing stays in core.

Define a small role contract for accepted inputs, responsibility, output
contract, evidence, and escalation. Include unresolved issues and relevant
decisions in handoffs. Reuse AFC's one-pass worker and context-pack boundaries;
the next invocation should recover its task from durable records.

A correction creates new work linked to the reviewed artifact and feedback.
It does not edit a completed terminal record. Begin with a bounded pipeline or
linked follow-up requests. Let the instance choose iteration and cost limits,
and record an explicit disposition when the budget is exhausted.

Acceptance scenarios:

- feedback names failed criteria, supporting evidence, and the correction target;
- previous outputs and verdicts remain available after correction;
- correction stops at the configured limit with an actionable disposition;
- finalization consumes the assessment of the selected output revision.

## Phase 5: Adaptive plans and evidence invalidation

**Outcome:** workflows that need discovery can change future work without losing
revision history or silently invalidating active claims.

**Placement:** generic revision checks and eligibility projection in core;
plan proposals, decision policy, and domain evidence interpretation in instances.

Introduce this only after a real workflow demonstrates that static pipelines
and bounded follow-up work are insufficient. A proposal should name its expected
plan revision and explicit delta. Validate the resulting graph before commit;
reject stale proposals and protect claimed or terminal work.

Evidence invalidation changes eligibility for reuse or release, with a reason
and affected references. Completed terminal commits remain historical execution
facts. Preserve them and schedule successor work where needed. A new graph or
status projection must not become a second authority for queue ownership.

Acceptance scenarios include competing revisions, an invalidated upstream
artifact, affected downstream work, and replay after a partial failure.

## Scope and sequencing

Finish each phase with synthetic failure/replay coverage and a representative
thin instance before expanding the abstraction. Core tests establish runtime
invariants; instance tests establish product quality and real delivery behavior.
Record the commands and evidence actually exercised.

A graph database, large agent catalog, automatic model routing, and a second
orchestration framework are not prerequisites. Consider them only when measured
instance needs justify the additional dependencies and operational cost.

## Primary references

- ARS: [handoff schemas](https://github.com/Imbad0202/academic-research-skills/blob/main/shared/handoff_schemas.md), [claim verification](https://github.com/Imbad0202/academic-research-skills/blob/main/academic-pipeline/references/claim_verification_protocol.md), [passport reset boundary](https://github.com/Imbad0202/academic-research-skills/blob/main/academic-pipeline/references/passport_as_reset_boundary.md).
- Agency Agents: [role authoring](https://github.com/msitarzewski/agency-agents/blob/main/CONTRIBUTING.md), [handoff and review templates](https://github.com/msitarzewski/agency-agents/blob/main/strategy/coordination/handoff-templates.md).
- Decepticon: [extension boundaries and workflow pins](https://github.com/BitterSecurity/Decepticon/blob/main/docs/design/extension-contract.md), [versioned objective graph](https://github.com/BitterSecurity/Decepticon/blob/main/docs/red-team/opplan-dag.md).

This document uses original wording and does not import upstream prompts or
schemas. Consult the source license before reusing their implementation or text:
[ARS](https://github.com/Imbad0202/academic-research-skills/blob/main/LICENSE),
[Agency Agents](https://github.com/msitarzewski/agency-agents/blob/main/LICENSE),
and [Decepticon](https://github.com/BitterSecurity/Decepticon/blob/main/LICENSE).
