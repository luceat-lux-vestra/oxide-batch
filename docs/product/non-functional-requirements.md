# Non-Functional Requirements

**State:** Accepted

NFRs are framework-level requirements. End-to-end service levels also depend on
the user job, database, infrastructure, and external systems.

## Priority order

When requirements conflict, use this order:

1. metadata and restart correctness;
2. security and sensitive-data protection;
3. bounded resource use and recoverability;
4. operability and diagnosability;
5. compatibility;
6. performance and ergonomics.

## Correctness and durability

- No committed checkpoint may refer to uncommitted transactional business data.
- No completed chunk may be silently lost from execution counters.
- Duplicate job-instance creation must be prevented under concurrent launch.
- Stale writes must be rejected through versioning or equivalent serialization.
- Recovery must never guess that an ambiguous external side effect succeeded.
- All durability claims must identify the transaction/resource scope.

## Reliability and recovery

- Every framework lifecycle phase has a deterministic crash/restart expectation.
- Stop and shutdown are bounded by configurable deadlines and report incomplete
  cleanup.
- Repository operations define timeouts and transient/permanent classifications.
- Retry storms are bounded by attempts, duration, backoff, and cancellation.
- Metadata backup/restore and migration recovery are exercised before 1.0.

Proposed measurement gates:

- M2: 100% pass rate for the named crash matrix over repeated CI runs;
- M4: a 24-hour soak without unbounded memory, task, connection, or handle
  growth;
- M5: restore and restart the reference workload within its documented recovery
  exercise target.

## Performance and scalability

- Memory is bounded in terms of chunk size, item size, and configured
  concurrency; no unbounded internal work queue is allowed.
- Backpressure propagates from writers and repository operations to readers.
- User-facing timeouts are monotonic and cancellation-aware.
- Framework overhead is measured separately from item business logic and I/O.
- Scaling claims state hardware, PostgreSQL version, dataset, configuration,
  warmup, sample count, and variance.

Numeric throughput/latency budgets are set after the M1/M2 benchmark harness
exists. Before then, architecture is judged by asymptotic bounds and measured
spike evidence rather than invented throughput numbers.

## Security and privacy

- Job parameters, execution context, item data, credentials, and exception
  payloads are sensitive by default.
- No sensitive value appears in telemetry without an explicit safe wrapper or
  allowlist.
- Serialized input is bounded and validated before allocation or use.
- SQL uses bound parameters; identifiers assembled dynamically are allowlisted.
- Release identity uses short-lived credentials and reviewed workflows.

## Compatibility and maintainability

- All supported behavior has a versioned contract and named conformance test.
- Public features are additive; enabling one feature must not silently change
  unrelated behavior.
- The supported MSRV builds every public crate and documented feature.
- Public errors are classified structurally; callers never need string parsing.
- Metadata and serialized context declare a version and failure mode for newer
  unsupported data.

## Observability

- Each job instance, job execution, and step execution has stable correlation
  identifiers.
- Logs, metrics, and traces agree on lifecycle outcome and attempt identity.
- Metric labels are bounded; item values and raw job parameters are forbidden.
- Telemetry failure cannot corrupt or decide batch execution state.
- Operator-facing errors include a stable code, safe message, and diagnostic
  source chain where available.

## Portability and supportability

- The core public API is tested on every supported operating system.
- Durable integration is tested on every supported PostgreSQL major version.
- Shutdown, filesystem, signal, and process-exit differences are documented per
  platform.
- Unsupported combinations fail clearly instead of degrading silently.

## Accessibility and usability

- CLI output has a machine-readable mode and meaningful exit codes.
- Color is never the sole carrier of status.
- Documentation examples are copyable, compiled, and version-matched.
- Default behavior is safe; dangerous recovery and destructive maintenance
  operations require explicit intent.
