# Observability Contract

**State:** Accepted

Observability describes execution; it is never the source of truth for
correctness. Durable metadata remains authoritative.

## Correlation model

Events use opaque stable identifiers:

- job name;
- job instance ID;
- job execution ID and attempt;
- step name;
- step execution ID and attempt;
- optional chunk sequence;
- framework version and schema version.

Raw identifying parameter values, item keys, execution-context values, SQL, and
credentials are excluded by default. Applications may attach reviewed,
low-cardinality safe attributes.

## Structured events

The initial catalog includes:

- launch requested, accepted, rejected;
- job/step starting, started, stopping, stopped, failed, completed, abandoned;
- chunk started, committed, rolled back;
- retry scheduled/exhausted and item skipped;
- checkpoint loaded/committed;
- repository conflict/transient failure;
- shutdown requested/deadline exceeded;
- recovery proposed/applied/rejected;
- migration started/completed/failed.

Each event defines severity, safe fields, emission timing relative to commit,
and whether duplicate emission is possible after crash.

### M1 executable-kernel events

The M1 launcher emits `launch.accepted`, job/step `starting`, `started`,
`stopping`, `stopped`, `failed`, and `completed`, plus typed listener-failure
events. Lifecycle state events are emitted after the corresponding in-memory
repository commit. Listener-failure events are emitted after classification and
before the enclosing final-state commit. Event delivery is best effort; sink
failure or panic cannot change execution metadata or outcome.

Every M1 event carries validated job and step names, opaque instance/job/step
execution IDs, and nonzero job/step attempt ordinals. Failure events may add
only the framework-owned failure category and opaque failure ID.

The stable diagnostic projections are:

- formatted event/log output: event name, severity, bounded correlation,
  lifecycle status, and optional redacted failure summary;
- span fields: the same reviewed correlation and outcome fields;
- metric-label candidates: event name, component, and lifecycle status only.

Parameters, contexts, records, credentials, user error text, identifiers, and
job/step names are excluded from metric labels. Exporter integration remains an
M4 concern.

### M2 deterministic chunk events

The M2 chunk runtime emits `chunk.started`, `chunk.committed`,
`chunk.rolled_back`, and `chunk.unknown`. Each event carries the existing
validated job/instance/execution/step correlation plus a nonzero chunk-attempt
sequence. The sequence is a span/event field, not a metric label.

`chunk.committed` is emitted only after the transaction port returns a commit
receipt. `chunk.rolled_back` is emitted only after rollback succeeds.
`chunk.unknown` means the adapter could not determine whether commit reached
durable storage; the enclosing step and job become `UNKNOWN` rather than
guessing. Completion callbacks and after-listeners run after the committed
event and cannot undo it. Event sink failure or panic remains isolated from
execution correctness.

### M3 fault-tolerance and flow events

M3 adds:

- `retry.reserved`, after the retry reservation commits;
- `retry.backoff_started` and `retry.backoff_cancelled`;
- `retry.exhausted`, after exhaustion is durably classified;
- `item.skipped`, only after the accepting chunk commits;
- `fault.rollback_committed` and `fault.no_rollback_committed`;
- `flow.step_result_committed`, after terminal step lifecycle commit;
- `flow.decision_committed`, after result and target commit;
- `flow.completed_step_reused`;
- `step.start_limit_exceeded`.

The chunk runtime emits the retry, skip, and rollback events above. The flow
runtime emits its four events through a separate `FlowEventSink` after the
named repository decision. Sink failure or panic is isolated and cannot alter
execution state or outcome.

Safe fields are fault phase, stable failure category, retry ordinal, configured
limit class, backoff duration, skip phase, aggregate numeric counts, source
node kind, target kind, and existing opaque execution correlation. Events may
include logical node IDs in spans/logs under the same bounded-name policy as
step names. They do not include item/record identifiers, error text,
parameters, contexts, retry-key/input digests, policy private state, decider
private state, or transition patterns.

Job/step/node names, retry ordinals, IDs, and exit codes are not metric labels.
Metric candidates remain bounded enums such as phase, category, outcome, and
event name. A crash may lose or duplicate telemetry after a durable decision;
repository counters and flow-decision rows remain authoritative.

### M4 operations, shutdown, and local-scale events

M4 publishes the event catalog as versioned documentation. The telemetry
schema version is `1`, is carried on every emitted event, and changes only
through the documented compatibility policy. Adding an event or a safe field
is a minor change; removing or repurposing either is a breaking change.

Every catalog entry fixes its name, severity, timing relative to the governing
commit, safe fields, and whether a crash may duplicate or lose it. M4 adds:

- `operator.request_accepted`, `operator.request_rejected`, and
  `operator.request_completed`, emitted after the operator request row commits
  with its effect;
- `explorer.page_served`, debug level, after the page read returns;
- `shutdown.requested`, `shutdown.intake_stopped`, `shutdown.drain_completed`,
  and `shutdown.deadline_exceeded`;
- `stale.detected`, after evidence is gathered and before any proposal is
  returned;
- `recovery.proposed`, `recovery.applied`, and `recovery.rejected`, with the
  applied event emitted after the decision and lifecycle change commit;
- `retention.planned`, `retention.applied`, and `retention.rejected`;
- `split.branch_started` and `split.branch_completed`;
- `partition.plan_committed`, `partition.assigned`, `partition.completed`, and
  `partition.aggregated`, with the aggregated event emitted after the parent
  step's terminal commit;
- `telemetry.export_dropped`, emitted at most once per throttling window.

Safe fields for these events are the existing opaque execution correlation
plus the operator action, authorization class, outcome class, rejection class,
reason code, opaque operation ID, opaque actor reference, drain result, counts
of unjoined tasks, elapsed-inactivity duration class, evidence-digest presence,
per-table deleted counts, branch and partition ordinals, worker count, and
aggregate status.

Operator actions, recovery decisions, retention actions, partitions, and
shutdown outcomes remain authoritative only in durable metadata. A crash may
lose or duplicate any of these events. Operator request rows, recovery
decisions, retention actions, partition rows, and execution status remain the
sole authorities.

## Logs

- Logs use stable event names plus human-readable messages.
- Expected user/configuration failures are not logged as framework panics.
- Error chains are captured only after redaction.
- High-volume item/chunk events are sampled or debug-level by default.
- A logging failure cannot fail or commit a batch transaction.

## Metrics

Metric names, units, and label sets are versioned documentation. Candidate
families cover:

- active and completed job/step executions;
- duration distributions;
- read, process, write, filter, skip, retry, commit, and rollback counts;
- repository operation duration/conflicts/errors;
- queue depth and configured/active concurrency;
- recovery and shutdown outcomes.

The schema-version-1 metric catalog is closed as follows. A unit or label-key
change is a schema compatibility change; adapters may rename only in their own
documented mapping layer.

| Family | Unit | Complete label keys |
| --- | --- | --- |
| `oxide_batch_active_executions` | executions | `status`, `job`, `step` |
| `oxide_batch_completed_executions_total` | executions | `status`, `job`, `step` |
| `oxide_batch_execution_duration_seconds` | seconds | `status`, `job`, `step` |
| `oxide_batch_item_operations_total` | items | `event` |
| `oxide_batch_repository_operation_duration_seconds` | seconds | `event` |
| `oxide_batch_repository_conflicts_total` | events | `event` |
| `oxide_batch_repository_errors_total` | events | `event` |
| `oxide_batch_queue_depth_records` | records | `event` |
| `oxide_batch_concurrency_configured_workers` | workers | `event` |
| `oxide_batch_concurrency_active_workers` | workers | `event` |
| `oxide_batch_operator_requests_total` | events | `action`, `authorization`, `outcome` |
| `oxide_batch_execution_events_total` | events | `event`, `status`, `job`, `step` |
| `oxide_batch_recovery_outcomes_total` | events | `outcome`, `action` |
| `oxide_batch_shutdown_outcomes_total` | events | `status` |
| `oxide_batch_telemetry_export_dropped_total` | events | `reason` |

IDs, job parameters, exception messages, item types, and arbitrary user strings
are forbidden metric labels. Job/step names require an explicit cardinality
budget.

### M4 label-cardinality budget

The budget is enforced by the framework, not left to deployment discipline.

- Every metric family declares its complete label set in the versioned
  catalog. A label absent from the catalog cannot be attached.
- Bounded enum labels are the default: phase, category, outcome, action,
  authorization class, lifecycle status, event name, and node kind.
- Total distinct label-value combinations per family are budgeted at `200`.
  Reaching the budget maps further values to the reserved value `__other__`
  and increments a dropped-cardinality counter; it never allocates unbounded
  series.
- Job and step names are labels only when name labelling is explicitly enabled
  with a configured allowlist of at most `50` names. Names outside the
  allowlist map to `__other__`.
- Opaque IDs, operation IDs, actor references, partition keys, cursor tokens,
  reason text, and digests are never metric labels under any configuration.

## Traces

The schema-version-1 span catalog and required direct parents are:

| Span | Direct parent | Complete reviewed field keys |
| --- | --- | --- |
| `job.execution` | root | `job.name`, `job.instance.id`, `job.execution.id`, `job.attempt`, `status`, `failure.category`, `failure.id` |
| `step.execution` | `job.execution` | job fields plus `step.name`, `step.execution.id`, `step.attempt`, `status`, `failure.category`, `failure.id` |
| `chunk.attempt` | `step.execution` | `job.execution.id`, `step.execution.id`, `chunk.sequence`, `status`, `failure.category`, `failure.id` |
| `item.read`, `item.process`, `item.write` | `chunk.attempt` | execution IDs, `chunk.sequence`, `outcome`, `failure.category`, `failure.id` |
| `repository.commit` | `chunk.attempt` | the `chunk.attempt` field set |
| `retry` | `step.execution` | execution IDs, `retry.ordinal`, `outcome`, `failure.category`, `failure.id` |
| `backoff` | `retry` | execution IDs, `retry.ordinal`, `backoff.duration_class`, `outcome` |

This produces the fixed hierarchy:

```text
job execution
└── step execution
    ├── chunk attempt
    │   ├── read/process/write
    │   └── repository commit
    └── retry/backoff
```

Span status follows documented lifecycle outcomes. Sampling must not change
execution. The adapter-neutral status classes are `unset`, `ok`, `error`,
`cancelled`, and `unknown`. Parameters, context, checkpoints, credentials,
endpoints, SQL, user error text, item identifiers, and retry keys are forbidden
span fields. Trace context propagation to external writers is opt-in and
cannot carry sensitive batch context.

## Export

The framework emits `tracing`-compatible structured diagnostics. An optional
OpenTelemetry adapter maps the stable OxideBatch event model to SDK/exporter
types. Export queues are bounded and flush behavior during shutdown has a
separate deadline from batch correctness.

### M4 exporter bounds and failure isolation

- The exporter owns one bounded queue of `64..=65536` records, default `1024`.
- A full queue drops the newest record, increments a dropped-record counter,
  and emits `telemetry.export_dropped` at most once per throttling window,
  bounded `1 s..=1 h`, default `60 s`. The queue never applies backpressure to
  execution.
- Exporter construction, encoding, transport, and shutdown failures are
  isolated. An exporter failure or panic cannot fail a step, roll back a
  transaction, change a status, or extend a correctness deadline.
- The exporter runs on tasks owned by the runtime that created it. There is no
  detached task and no process-global exporter.
- Flush uses the separate `TelemetryFlushDeadline` in
  [shutdown and stale-recovery](../architecture/shutdown-and-recovery.md).
  Missing it reports the dropped count and never changes the durable outcome.
- No SDK, exporter, protocol, or transport type appears in a public OxideBatch
  API, error, or diagnostic.

Telemetry overhead is measured with export enabled and disabled on the same
workloads, and reported with the environment and correctness result required by
the [performance plan](../engineering/performance-plan.md).

## Diagnostic bundles

`diagnostics bundle` produces a bounded, redacted incident package for one
named execution. Its contents are fixed:

- a manifest with the bundle format version, framework version, telemetry
  schema version, metadata schema version, manifest format version, creation
  instant, and a checksum over every included file;
- effective configuration with resolved sources and redacted values;
- the repository capability descriptor and schema status;
- explorer projections for the named execution, its step executions,
  partitions, flow decisions, recovery decisions, and operator requests;
- the last retained events for that execution, bounded to `200` records by
  default;
- host resource summary limited to CPU count, available memory class, and
  platform identity.

A bundle excludes parameters, contexts, checkpoints, fault-state payloads,
item data, credentials, endpoints, SQL, user error text, and environment
variable values. Total bundle size is bounded to `4 MiB`; a bound that removes
content records the omission in the manifest. File names are deterministic so
two bundles of the same execution are comparable. A bundle is diagnostic
evidence and never an authority for correctness.
