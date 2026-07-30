# M3 Basic Flow and Start-Control Contract

**State:** Accepted

**Scope:** Acyclic sequential and conditional flow, deterministic deciders,
durable traversal, start limits, and allow-start-if-complete.

This document is the canonical owner for the M3 flow slice. Advanced repeat,
split/parallel flow, nested flows/jobs, remote nodes, and fork/savepoint
behavior remain assigned to later milestones.

## M3 graph subset

An M3 `JobDefinition` has one entry node and a finite acyclic graph containing:

- tasklet or chunk `StepNode`;
- typed `DecisionNode`;
- `Complete`, `Fail`, or `Stop` terminal nodes;
- directed transition edges selected by a bounded exit pattern.

Every node and component has a stable logical ID of 1 to 128 UTF-8 bytes using
the existing validated-token rules. Display names may change without changing
logical identity. Runtime and database IDs are never graph IDs.

The M3 limits are 1,024 nodes, 4,096 edges, 64 outgoing edges per node, and a
64 KiB canonical manifest. Compilation rejects cycles, unreachable nodes,
duplicate IDs, missing terminals or transitions, unsupported node kinds,
ambiguous patterns, and a graph whose bounds are exceeded.

## Exit outcomes and transition matching

Flow matches the bounded `ExitStatus` code, not `BatchStatus`. An exit pattern
contains literal characters plus `*` for zero or more characters and `?` for
exactly one character. Patterns are 1 to 64 UTF-8 bytes and may not contain
control characters or surrounding whitespace.

Compilation orders outgoing transitions by:

1. more literal characters;
2. fewer wildcards;
3. longer UTF-8 byte length.

Two patterns from one node that can match the same value with equal specificity
are ambiguous and rejected. Registration order never breaks a tie. If no edge
matches a produced outcome, the job fails with `UnmappedExitOutcome`; it does
not select an arbitrary default.

A convenience sequential edge compiles to exact `FAILED` leading to a fail
terminal and a less-specific `*` leading to the next step. Custom successful
exit codes therefore continue, while failure terminates. Explicit conditional
edges must cover every intended outcome. Terminals alter only the job outcome;
they do not rewrite the source step's status or exit code.

## Deciders

A decider receives an immutable, bounded `DecisionInput` containing:

- job instance/execution identity and attempt;
- the current plan fingerprint and decision-node ID;
- the preceding durable step logical ID, status, exit code, and counters when
  present;
- read-only typed parameters and committed job/step contexts through their
  existing sensitivity-aware accessors.

It returns one validated exit outcome or a typed failure. A decider has no
transaction port, may not mutate repository state, and must not perform an
external effect. Applications declare a revision token and are responsible for
determinism from the supplied durable input. Parameters and contexts are never
copied into a flow-decision record or diagnostic.

The runtime hashes a canonical projection of non-secret decision identity and
durable input versions. It invokes the decider, matches the result, and commits
the result and selected target as one decision record before starting the
target. A crash before that commit may invoke the decider again; a crash after
it reuses the record and does not invoke the decider again.

Decider error or panic fails the job with a typed, redacted category. It
creates no successful transition record and cannot be retried in M3.

## Durable traversal

Every selected transition in a format-2 plan is recorded append-only with:

- job execution and monotonically increasing sequence;
- plan fingerprint and source node logical ID;
- optional source step execution;
- transition kind (`StepExit`, `Decider`, or `CompletedStepReuse`);
- observed bounded outcome and input digest;
- selected target node or terminal;
- optional prior decision reused during restart;
- facade-clock timestamp.

The repository validates that source, outcome, and target exist in the exact
persisted plan. The unique `(job_execution_id, sequence)` and
`(job_execution_id, source_node_id)` constraints are valid because M3 graphs
are acyclic. A stale version, duplicate source visit, plan mismatch, or invalid
target rolls back the decision.

A step terminal update commits before its outgoing transition. If a process
stops between those commits, restart derives the same transition from the
durable step result and appends it. A committed decision is never replaced.
Telemetry is emitted after decision commit and is not traversal authority.

## Restart traversal

A restart creates a new job execution. It walks from the entry node using the
latest durable state for the same job instance and logical IDs:

- a failed or stopped step is started as a new step execution when its start
  limit permits;
- a completed step with `allow_start_if_complete = false` is not invoked; its
  committed exit outcome is reused and the reuse is recorded in the new
  execution;
- a completed step with `allow_start_if_complete = true` starts again when the
  restarted path reaches it;
- a prior committed decider decision is reused when node ID, plan fingerprint,
  and input digest match;
- a changed durable input caused by an explicitly rerun step permits a new
  decision, recorded as a new path rather than silently replacing history.

Missing, corrupt, or conflicting traversal history fails closed before the next
node starts. Definition upgrade uses the accepted explicit directed mapping;
names, graph similarity, or a decider returning the old value do not imply
compatibility.

## Start controls

`StartLimit` is a nonzero `u32` maximum number of step executions that may
enter `STARTING` for one `(job_instance, step_logical_id)`. The default is
`u32::MAX`, matching an effectively unrestricted step while remaining a finite
typed value.

The repository checks the count and creates the next step execution in one
transaction. Failed before-listener calls and failures before user work still
consume a start because the step entered `STARTING`. A concurrent loser
receives `StartLimitExceeded` or an optimistic conflict and does no user work.

`allow_start_if_complete` defaults to false. It affects only a restartable
failed or stopped job instance whose path reaches the step; it does not permit
an ordinary relaunch of a completed or abandoned job instance.

Both controls are restart-relevant manifest input. Reducing a limit below
historical starts cannot make an existing definition compatible without an
explicit upgrade edge.

## Manifest format 2 and format-1 compatibility

Format 2 extends the accepted canonical JSON manifest with:

- definition and stable node/component logical IDs;
- entry node, sorted normalized nodes, edges, patterns, and terminals;
- step kind, component revisions, checkpoint/context schemas, and delivery
  requirements;
- fault policies and authoritative listener revisions;
- decider revisions and durable input-contract version;
- start controls and relevant finite resource bounds.

Map keys use canonical UTF-8 byte order. Nodes sort by logical ID. Edges sort
by source ID, computed specificity, pattern bytes, and target ID. Integers use
canonical JSON decimal form; floats, duplicate keys, secrets, endpoints,
executable code, item values, and private state are forbidden. SHA-256 over
the exact canonical bytes remains the definition fingerprint.

A format-2 runtime must continue to read format 1. Existing
`TaskletJob`/`TaskletStep` and `ChunkJob`/`ChunkStep` wrappers lower a recognized
format-1 one-step manifest into an in-memory compatibility plan while retaining
the original manifest bytes, fingerprint, and repository writes. The synthetic
entry/step/terminal nodes use a fixed framework-owned derivation from the
validated job and step names. Compatibility lowering emits no durable flow
decision row.

New general definitions emit format 2. Converting a persisted format-1
definition to format 2 changes its fingerprint and requires an explicit direct
compatibility edge, even when the graph is mechanically one step. No database
migration rewrites old manifest bytes. A format-1 runtime rejects format 2;
format-2 readers reject unknown newer versions, malformed canonical JSON, an
invalid digest, or out-of-bound graphs.

## Wrapper equivalence gate

Production lowering is enabled only after the same one-step tasklet and chunk
fixtures produce equal normalized:

- lifecycle statuses, attempts, exit outcomes, counters, checkpoints, and
  contexts;
- listener order and typed failures;
- business effects and transaction boundaries;
- repository commands and durable rows;
- stop, panic, known rollback, and unknown-commit outcomes.

Plan compilation diagnostics and non-authoritative telemetry may add
plan-specific observations, but the compatibility lowering cannot add a flow
decision row, change a fingerprint, or reorder an existing callback.

## Schema and security impact

Schema version 2 adds `step_logical_id` to step execution history and an
append-only flow-decision table. Existing rows backfill logical ID from the
validated step name. Decision records contain IDs, codes, digests, versions,
and timestamps only. Parameters, contexts, decider private state, credentials,
item values, and user error text are prohibited.

Migration requires quiescence and backup/restore rollback. Old binaries reject
schema version 2, so mixed v1/v2 writers are not supported.

## Required evidence

Implementation issues must provide:

- invalid-graph property tests and bounded compilation tests;
- format-1 reader and format-2 canonical golden/fuzz vectors;
- wrapper/plan trace and repository-write equivalence;
- exact, wildcard, specificity, ambiguity, and unmapped-exit conformance;
- decider success/error/panic, input-digest, crash-before/after-commit, and
  restart-reuse tests;
- start-limit concurrency and allow-start-if-complete restart tests;
- schema-v1-to-v2, corruption, newer-version, backup, and restore evidence;
- diagnostics and telemetry redaction tests.
