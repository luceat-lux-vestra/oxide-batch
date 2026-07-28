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

IDs, job parameters, exception messages, item types, and arbitrary user strings
are forbidden metric labels. Job/step names require an explicit cardinality
budget.

## Traces

Proposed span hierarchy:

```text
job execution
└── step execution
    ├── chunk attempt
    │   ├── read/process/write
    │   └── repository commit
    └── retry/backoff
```

Span status follows documented lifecycle outcomes. Sampling must not change
execution. Trace context propagation to external writers is opt-in and cannot
carry sensitive batch context.

## Export

The framework emits `tracing`-compatible structured diagnostics. An optional
OpenTelemetry adapter maps the stable OxideBatch event model to SDK/exporter
types. Export queues are bounded and flush behavior during shutdown has a
separate deadline from batch correctness.
