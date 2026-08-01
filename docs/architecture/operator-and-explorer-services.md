# M4 Operator, Explorer, and Retention Contract

**State:** Accepted

**Governing decisions:**
[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md),
[RFC-0008](../rfcs/0008-core-and-control-plane-boundary.md),
[ADR-0006](decisions/0006-repository-capability-model.md), and
[ADR-0007](decisions/0007-control-plane-boundary.md)

This document is the canonical contract for the bounded M4 `JobExplorer`,
`JobOperator`, and retention services. The
[repository and transaction model](repository-and-transaction-model.md) owns
the port boundaries and capability model; this document fixes the observable
M4 behavior those ports must implement.

It authorizes no hosted API, identity system, scheduler, or remote execution.
The [control-plane boundary](../operations/control-plane-boundary.md) remains
binding.

## Shared request model

Every service call carries a bounded, framework-validated request envelope.

| Field | Rule |
| --- | --- |
| `ActorRef` | Deployment-supplied opaque reference, 1 to 128 bytes, `[A-Za-z0-9._:@-]`, required for every mutating action, never a credential |
| `ReasonCode` | Bounded closed-set code, at most 64 bytes, required for `abandon`, `recover`, and retention application |
| `OperationId` | Caller-supplied idempotency key, 1 to 64 bytes, `[A-Za-z0-9._:-]`, required for every mutating action |
| `RequestDigest` | Framework SHA-256 of the canonical request, computed from action, target identity, expected version, and bounded arguments |
| `ExpectedVersion` | Observed optimistic-lock version of the target execution, required for every lifecycle mutation |

The core validates the envelope; it does not authenticate or authorize the
actor. Free-form operator text, credentials, endpoints, parameter values, and
context payloads are not part of any request or audit record.

Actions declare one authorization class so a deployment can grant them
separately:

| Class | Actions |
| --- | --- |
| `Read` | every `JobExplorer` query and every retention plan |
| `Lifecycle` | `launch`, `restart`, `stop` |
| `Destructive` | `abandon`, `recover`, `hold`, `release_hold`, `apply_purge` |

## `JobExplorer`

### Query set

The M4 explorer owns exactly these named queries. Each maps to a named
PostgreSQL query in the
[physical metadata model](postgres-physical-metadata-model.md) and uses an
existing or newly published index.

| Query | Result |
| --- | --- |
| `list_job_names` | Registered job names in byte order |
| `list_instances` | Instances of one job name, newest identity first |
| `list_executions` | Executions of one instance, newest attempt first |
| `get_execution` | One execution projection |
| `list_step_executions` | Step executions of one job execution |
| `list_unresolved_executions` | Non-terminal executions older than a supplied bounded age |
| `list_recovery_decisions` | Recovery decisions of one job execution |
| `list_flow_decisions` | Flow decisions of one job execution in sequence order |
| `list_step_partitions` | Partitions of one partitioned step execution |
| `list_operator_requests` | Audited operator requests for one job execution |

Aggregation, arbitrary predicates, joins across job names, full-text search,
and ordering by caller-supplied columns are not available. There is no
unbounded list, no `count(*)` over full history, and no query that filters by
parameter, context, or checkpoint content.

### Pagination and cursor consistency

Pagination is keyset only. Offsets and page numbers do not exist.

- Page size is `1..=500`, default `50`.
- Ordering keys are immutable columns only: identity `id`, `(attempt, id)`, or
  `(sequence, id)`. Mutable columns such as `status` or `updated_at` may be
  filtered but never define the sort order.
- The first page captures an exclusive identity ceiling. Every later page in
  the traversal carries that ceiling, so a traversal observes a fixed row set.

The cursor is an opaque token and not a documented format. Its bounded
encoding is:

- 1-byte cursor format version, currently `1`;
- query discriminant;
- the immutable ordering key tuple;
- the captured identity ceiling;
- an 8-byte binding derived from the canonical query identity, including its
  filters and page size;
- a 32-byte SHA-256 checksum over all preceding fields.

The checksum covers integrity and the binding covers identity, so a damaged
token and a token reused against another query are separable rather than
indistinguishable.

The token is at most 256 bytes and carries no parameter, context, item,
credential, or user-supplied text. A cursor presented to a different query,
different filters, or a different page size is rejected as
`CursorQueryMismatch`. A malformed, oversized, or checksum-failing cursor is
rejected as `CursorInvalid`. Cursors are not authenticated and confer no
authority.

The consistency guarantee is exact for identity and best effort for mutable
filters:

- a row that existed at traversal start and satisfies the filter throughout
  the traversal is returned exactly once;
- a row created after traversal start is never returned by that traversal;
- a row whose filtered column changes mid-traversal may be present or absent
  in a later page, because each page evaluates its filter at read time;
- no page repeats a row of the same traversal.

Each page is one statement under the adapter's ordinary read committed
isolation. Cross-page snapshot isolation is explicitly not provided and must
not be claimed.

### Projections and redaction

Every explorer projection is redacted by construction. Returnable values are:

- job, step, and logical node names, and definition revisions;
- opaque instance, execution, step-execution, partition, and decision IDs;
- attempt ordinals, sequences, statuses, exit codes, and reason codes;
- numeric counters, optimistic versions, and byte sizes;
- UTC timestamps;
- hex-encoded 32-byte digests, including instance key, manifest, plan
  fingerprint, and evidence digests;
- framework-owned failure category plus opaque failure ID;
- parameter names with their type tag and identifying flag;
- presence, format, schema, schema version, and size of context, checkpoint,
  fault-state, and partition-context envelopes.

Parameter values, context payloads, checkpoint payloads, fault-state payloads,
item data, user error text, SQL, connection details, credentials, decider or
policy private state, and transition patterns are never returned. A projection
that cannot be produced without one of them fails rather than degrading.

### Query bounds

- Page size at most `500` rows; total encoded response at most `256 KiB`.
- Each query executes under the configured `StatementTimeout`; exceeding it is
  a typed `Timeout`, never a partial page.
- `list_unresolved_executions` requires an explicit age bound of at least one
  minute and is limited to the same page size.
- An adapter that cannot supply keyset pagination rejects the explorer with a
  typed `UnsupportedCapability`; it does not emulate cursors by scanning.

## `JobOperator`

### Idempotent request identity

Every mutating action commits an `ob_operator_request` row in the same
transaction as its effect. The row is unique on `(action, operation_id)` and
records the request digest, actor reference, reason code, prior status, result
status, observed execution version, outcome class, and facade-clock timestamp.

A rejected action is audited without an effect, so it commits its row in a
separate transaction that stages no lifecycle change. Both target references
are therefore optional: a launch rejected before its instance exists names
neither an instance nor an execution, and the operation identifier and request
digest remain the audit correlation.

Replay behavior is fixed:

- the same `(action, operation_id)` with the same `RequestDigest` returns the
  recorded outcome without repeating the effect;
- the same `(action, operation_id)` with a different digest is rejected as
  `OperationIdConflict`;
- a different `operation_id` for an equivalent request is a new request and is
  guarded only by the action's own lifecycle rules.

Idempotency is therefore explicit and durable. It is never inferred from
timing, request similarity, or client retry behavior.

### Action guards

| Action | Preconditions | Effect | Idempotency |
| --- | --- | --- | --- |
| `launch` | Definition resolves to one manifest and fingerprint; instance is absent or has no unresolved execution; instance is not `COMPLETED` or `ABANDONED` | Creates instance when required and one `STARTING` execution | By operation ID |
| `restart` | Prior latest execution is `FAILED` or `STOPPED`; fingerprint is `Strict` or has exactly one directed `Compatible` edge; start controls permit the attempt | Creates a new execution attempt from the committed checkpoint | By operation ID |
| `stop` | Execution is `STARTING` or `STARTED` | Durably records the stop request and moves the execution to `STOPPING` when this process owns it | Repeat request on a `STOPPING` or terminal execution succeeds and changes nothing |
| `abandon` | Execution is `STOPPED` or `FAILED`, or is `UNKNOWN` with an applied recovery decision | Terminal `ABANDONED` | Repeat on an already `ABANDONED` execution succeeds and changes nothing |
| `recover` | Execution is `UNKNOWN` or a stale candidate with valid evidence | Appends one recovery decision and applies `FAILED` or `ABANDONED` | Only for the same observed execution version |

`restart` never targets an `UNKNOWN` execution. `recover` resolves it first.
Neither `stop` nor `abandon` may skip a state in the accepted lifecycle table
in [execution semantics](../compatibility/execution-semantics.md).

Stop is durable rather than in-process only. The operator writes
`stop_requested_at` and `stop_requested_by` under compare-and-swap. The owning
runtime observes the request at the next chunk-commit boundary and at least
once per configured `StopPollInterval`, default `1 s`, bounded
`100 ms..=60 s`. A stop request against an execution whose owner is gone does
not transition the execution; stale detection and explicit recovery own that
case.

### Optimistic conflict and unknown outcome

Every mutation supplies `ExpectedVersion` and updates under
`WHERE version = expected`. An affected-row count other than one is a typed
`OptimisticConflict` carrying the current version. The service performs no
internal retry loop; the caller re-reads and re-decides.

An ambiguous commit response makes the outcome `OperationOutcomeUnknown`. The
service does not guess, does not re-issue the statement on a suspect
connection, and does not mark the operator request completed. The caller
resolves the ambiguity by replaying the same `operation_id`, which either
returns the recorded outcome or re-attempts the effect exactly once.

### Audit

`ob_operator_request` is the audit record for every mutating action, including
rejected ones, whose row records the rejection class instead of a result
status. Recovery additionally appends `ob_recovery_decision` as accepted in
M2. Retention appends `ob_retention_action`.

Audit rows are append-only, bounded, and contain no free text. External audit
systems correlate through the opaque actor reference, operation ID, and
evidence digest.

### Authorization boundary

The core enforces lifecycle, version, definition, checkpoint, idempotency, and
bounds. The deployment authenticates the caller and authorizes the action's
class before invoking the service. Core services never accept a credential,
never consult an identity provider, and never treat a supplied actor reference
as proof of authorization. Removing deployment authorization does not weaken a
core guard.

## Retention primitives

The M4 slice is deliberately smaller than the M8 portability program. It adds
holds and a guarded, bounded purge of terminal history for the PostgreSQL
reference adapter only.

### Holds

A hold is placed on a job instance, records actor, reason code, and
facade-clock timestamp, and is released explicitly. A held instance rejects
purge planning and purge application. A hold protects history only: it does not
block launch, restart, or any other lifecycle action, and the `launch` guard
above therefore does not consult it.

### Eligibility

An execution is purge eligible only when all of the following hold:

- its status is `COMPLETED`, `FAILED`, `STOPPED`, or `ABANDONED`;
- no execution of the same instance is `STARTING`, `STARTED`, `STOPPING`, or
  `UNKNOWN`;
- its `updated_at` is older than the requested minimum age, which is at least
  `1 h` and defaults to `30 d`;
- its instance carries no active hold.

Definitions, definition upgrade edges, and the schema-version row are never
purged in M4. Purging never targets a running, stopping, or ambiguous
execution.

### Plan and apply

Purge is two phase and target guarded.

1. `plan_purge` accepts a job name, terminal-status set, minimum age, and a
   batch bound of at most `1000` executions. It returns the bounded candidate
   identities, per-table row counts, and a 32-byte plan digest computed over
   the canonical candidate list and the observed execution versions.
2. `apply_purge` accepts that digest, the operation ID, actor, and reason. It
   re-validates eligibility and versions inside one transaction. Any candidate
   that changed produces a typed `RetentionPlanStale` rejection and deletes
   nothing.

Application deletes within one instance-owned order and one transaction per
batch: flow decisions, recovery decisions, operator requests, step partitions,
step executions, job executions, and finally job instances that retain no
execution. A surviving flow decision that cites a purged decision as its reused
provenance has that citation cleared first, because the evidence it names no
longer exists. A retention audit row outlives the instance it protected, so its
instance reference is cleared rather than cascading the row away. Interrupting a run leaves completed batches durable; re-running is
safe because a new plan observes the remaining candidates. Replaying the same
operation ID returns the recorded outcome instead of deleting again.

`ob_retention_action` records the action, operation ID, actor, reason, plan
digest, per-table deleted counts, batch bound, and outcome.

### Privilege separation

Purge requires the operator-writer role plus the narrowly granted delete
privileges recorded in
[persistence and migrations](../operations/persistence-and-migrations.md). The
runtime role cannot purge. The operator-reader role can plan but not apply.

### M4 boundary

M4 retention does not provide archive packages, export or import, checksum
verification of exported data, scheduled or automatic purge, retention policy
storage, cross-adapter portability, or partial-row redaction. Those remain M8
scope under [RFC-0007](../rfcs/0007-repository-services-and-capabilities.md).

## Evidence

Production implementation requires:

- unit and property tests for cursor encoding, checksum rejection, query
  mismatch, page bounds, and traversal exactly-once behavior;
- redaction tests asserting that every projection excludes the prohibited
  value classes, including a compile-fail test at the facade boundary;
- PostgreSQL integration tests proving each named query uses its named index
  on realistic history;
- idempotency tests for replayed operation IDs, digest conflicts, and
  concurrent duplicate requests;
- optimistic-conflict, unknown-commit, and guard-rejection tests for every
  action in the guard table;
- destructive-action tests proving hold, eligibility, plan staleness,
  ordering, batch bounds, interruption, and audit;
- least-privilege tests proving the runtime role cannot purge and the
  operator-reader role cannot apply.
