# Execution, Restart, and Transaction Semantics

**State:** Accepted

**Open extension:** Distributed protocol semantics remain proposed under
RFC-0009. Compiled-plan and delivery-capability directions are accepted.

This document defines the canonical correctness contract. Precise Rust APIs,
database tables, and wire formats belong to their focused architecture
documents.

## Core terms

| Term | Meaning |
| --- | --- |
| Job definition | Immutable named graph plus parameter, policy, component, and capability declarations |
| Definition revision | Bounded application-owned audit label |
| Definition fingerprint | Framework hash of the canonical restart-relevant manifest |
| Compiled execution plan | Validated normalized executable form of a definition |
| Job parameters | Typed launch inputs marked identifying or non-identifying |
| Job instance | Logical occurrence selected by definition/job identity and canonical identifying parameters |
| Job execution | One attempt to run a job instance |
| Step execution | One attempt to run a step in a job execution |
| Execution lineage | Auditable relationship among restart, compatible upgrade, fork, savepoint, and migration |
| Execution context | Bounded, versioned, durable restart state scoped to job, step, or component |
| Checkpoint | Last durably committed restart position, context, and associated counters |
| Batch status | Framework lifecycle state |
| Exit status | User-visible bounded result used for flow decisions |
| Lease/fencing token | Distributed ownership proof and monotonically changing stale-writer guard |

Parameter identity uses canonical typed values, never display strings or map
insertion order. Secrets and unbounded payloads do not belong in parameters,
contexts, definitions, protocol messages, or diagnostics.

## Definition identity and restart

[ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md)
is binding: every execution references an immutable revision, canonical
manifest, and fingerprint. Same revision with different fingerprint is
definition drift.

A restart:

- creates new job and step execution attempts rather than mutating old attempts;
- uses the last valid committed checkpoint;
- requires the same fingerprint or one explicit directed compatibility edge;
- never infers compatibility from names, revision ordering, deserialization,
  or semantic-version syntax;
- atomically records any context upgrade and the new execution;
- preserves the guarantee and meaning of already committed effects.

[RFC-0004](../rfcs/0004-compiled-execution-plan.md) and
[ADR-0005](../architecture/decisions/0005-compiled-execution-plan.md) accept
`Strict`, `Compatible`, and lineage-preserving `Fork`. A
disabled-by-default audited `Force` mode remains unavailable until separately
approved. ADR-0004 behavior remains authoritative during staged lowering.

## Lifecycle rules

The status vocabulary is `STARTING`, `STARTED`, `STOPPING`, `STOPPED`,
`FAILED`, `COMPLETED`, `ABANDONED`, and `UNKNOWN`.

| From | Normally allowed to | Rule |
| --- | --- | --- |
| STARTING | STARTED, STOPPING, FAILED, UNKNOWN | Failure before user work is recorded |
| STARTED | STOPPING, STOPPED, FAILED, COMPLETED, UNKNOWN | Completion requires all required nodes |
| STOPPING | STOPPED, FAILED, UNKNOWN | Stop is cooperative and bounded |
| STOPPED | new STARTING attempt, ABANDONED | Restart creates new attempts |
| FAILED | new STARTING attempt, ABANDONED | Only when restart policy and definition permit |
| COMPLETED | — | Terminal for the instance/definition policy |
| ABANDONED | — | Terminal and intentionally not restartable |
| UNKNOWN | FAILED, ABANDONED | Explicit audited recovery decision required |

Repositories reject illegal transitions, stale versions, and stale fencing
tokens. Exit status cannot forge lifecycle state.

## Launch, stop, abandon, recover, and fork

- At most one job instance exists for one logical job identity and canonical
  identifying-parameter set.
- Concurrent launches are serialized by the repository.
- A completed or abandoned instance rejects another ordinary launch.
- An apparently running execution is not automatically stolen.
- Stop halts new intake, handles in-flight work by the step's declared policy,
  commits or rolls back, persists state, and completes within a configured
  deadline or reports incomplete drain.
- Abandon and recovery are guarded, versioned, idempotent where defined, and
  audited with actor, reason, evidence, prior state, and resulting state.
- A fork starts a new lineage and does not claim to resume the source
  execution.

M4 makes these rules executable through a portable operator service. Every
mutating operator action carries a caller-supplied operation ID, an observed
optimistic version, a bounded actor reference, and, where destructive, a
bounded reason code. Replaying an operation ID returns the recorded outcome;
reusing it for a different request is rejected. An ambiguous operator commit is
an explicit unknown outcome that the caller resolves by replay, never by
guessing. Read, lifecycle, and destructive actions are separately
authorizable by the deployment while core guards remain unconditional.

Query surfaces are bounded and keyset paginated over immutable ordering keys,
and every projection is redacted. The exact M4 query, cursor, idempotency,
guard, audit, and retention rules are owned by the
[operator, explorer, and retention contract](../architecture/operator-and-explorer-services.md).

## Tasklet and chunk restart points

A tasklet may expose a versioned contribution/checkpoint only at a documented
successful boundary. A chunk step resumes from the last committed checkpoint,
not from items read or processed only in memory.

The canonical chunk attempt is read, process, write, prepare state/counters,
commit, acknowledge, and continue/close. A failure before commit leaves the
previous checkpoint authoritative. A failure after a proven commit does not
replay that chunk. An ambiguous commit becomes `UNKNOWN` pending durable
inspection.

Item-stream open/update/close order, component state schema/version, and
checkpoint ownership are explicit. A restartable step cannot contain required
state that is neither reconstructible nor checkpointed.

## Transaction and delivery semantics

For an enlisted same-resource PostgreSQL writer, one transaction contains:

1. business writes for the chunk;
2. affected durable counters;
3. context and checkpoint;
4. optimistic-lock version;
5. relevant outbox/effect record when that mode is selected.

All commit or roll back together. Telemetry and acknowledgement occur after
commit and are not correctness authorities.

Cross-resource modes are:

- atomic same resource;
- transactional message;
- outbox;
- inbox/deduplication;
- idempotent external effect/effect journal;
- at-least-once;
- best effort with manual reconciliation.

These capability names and plan-time validation are accepted by
[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md) and
[ADR-0006](../architecture/decisions/0006-repository-capability-model.md). No
generic exactly-once guarantee exists across arbitrary resources. Every adapter
states acknowledgement, offset, commit, redelivery, and unknown-outcome
behavior.

## Retry, skip, rollback, and repeat

- Retry classification receives stable phase/category data, never error text.
  M3 durably reserves each re-invocation after a known rollback, so restart
  cannot exceed the configured retry budget.
- Backoff uses injected monotonic time, deterministic finite arithmetic, and
  cooperative cancellation. A stop during backoff consumes an already durable
  reservation and invokes no component.
- One aggregate skip limit applies across separately durable read, process, and
  write counts. The skip after the limit fails. A skip and its callback become
  authoritative only with the accepting chunk commit.
- No-rollback is capability-scoped. It cannot silently discard an item,
  preserve an ambiguous effect, or advance a checkpoint beyond the selected
  delivery guarantee.
- Completion/repeat policies are bounded. Adaptive decisions are persisted
  when needed for deterministic restart.

The exact M3 policy, listener, durable-state, and fingerprint rules are owned
by the [fault-tolerance contract](../architecture/fault-tolerance.md).

## Basic flow and start controls

M3 sequential and conditional flow is a finite acyclic compiled graph.
Transitions match bounded step exit outcomes by deterministic specificity; an
ambiguous definition is rejected and an unmatched outcome fails rather than
choosing registration order.

The selected transition or decider result commits before the target starts.
Restart reuses a matching committed decision and a previously completed step
unless `allow_start_if_complete` explicitly permits re-execution. Step start
limits are checked atomically across the job instance and stable logical step
ID. Entering `STARTING` consumes one start even if no user work follows.

The exact M3 graph, manifest, decision, restart, and start-control rules are
owned by the [basic-flow contract](../architecture/basic-flow.md).

## Listener and event semantics

Authoritative listeners/interceptors are distinct from telemetry events.
Before callbacks run in registration order; after callbacks run in reverse
order. Step callbacks nest inside job callbacks; chunk/item callbacks nest
inside the step.

A before failure prevents its body. An after failure cannot undo already
committed chunks; the original outcome and every listener failure remain
available as redacted diagnostic context. Panic is classified at the same
boundary as an error. Exact deviations from Spring Batch are ledgered.

Telemetry observes committed decisions and may be duplicated or lost; it never
changes execution state.

M3 read, process, write, retry, and skip callbacks follow the ordering and
commit-relative rules in the
[fault-tolerance contract](../architecture/fault-tolerance.md). Listener
failure and panic are not themselves retryable or skippable in the M3 slice.

## Local concurrency

Concurrency is bounded. The engine owns a structured task tree, propagates
cancellation, joins children, and never relies on detached work. Mutable state
is not shared without an explicit merge rule. Ordered readers/writers and
commit barriers preserve their declared ordering. Resource pools, queues,
in-flight chunks, memory, and blocking threads have finite budgets.

M4 adds a bounded split node and a bounded partitioned step node. Their
partition identity is durable, the partitioner runs at most once per
partitioned step execution, completed partitions are not rerun on restart, and
aggregation is deterministic in key order rather than completion order. An
`UNKNOWN` child makes its parent `UNKNOWN`. Running the same plan with a single
branch or worker is the canonical sequential execution and must produce
identical normalized observations. The exact subset, budgets, and equivalence
rules are owned by the
[M4 local-scale contract](../architecture/local-scale.md).

## Shutdown, stale detection, and recovery

Graceful shutdown stops intake, propagates cancellation, applies the step's
declared in-flight chunk policy, joins owned children, persists the resulting
state, and only then flushes telemetry and closes the repository. A missed
join deadline is reported with the count of unjoined work; it never fabricates
a terminal status. An ambiguous in-flight commit remains `UNKNOWN`.

Stale detection compares durable inactivity against repository server time and
a per-process ownership token. It produces evidence and a proposal; it never
rewrites status, never expires an owner, and confers no takeover authority.
Recovery binds an evidence digest and an observed version, permits only
`FAILED` or `ABANDONED`, and never infers whether an ambiguous external effect
committed. The exact ordering, deadlines, evidence, clock rules, and signal
matrix are owned by the
[M4 shutdown and recovery contract](../architecture/shutdown-and-recovery.md).

## Distributed ownership and equivalence

Distributed semantics remain proposed under
[RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md):

- assignments, ownership, checkpoints, and completion are durable repository
  state;
- leases expire and reassignment increments a fencing token;
- stale workers cannot commit progress or results;
- commands/results assume duplicate, delay, reordering, and redelivery;
- transport acknowledgement is never execution authority;
- completed partitions remain complete across restart;
- embedded, local, and distributed execution of the same plan produces
  equivalent normalized lifecycle/restart observations.

## Time, cancellation, panic, and crash

Persisted timestamps use UTC wall-clock instants. Deadlines, leases, timeouts,
and backoff use monotonic time and account for clock skew where distributed.

Stop is cooperative. Blocking work has a documented cancellation limit and a
bounded adapter. User panic becomes a typed framework-owned failure. Forced
termination may leave apparently running work. Recovery never guesses whether
an ambiguous external effect committed.
