# M4 Shutdown and Stale-Recovery Contract

**State:** Accepted

**Governing decisions:**
[RFC-0006](../rfcs/0006-runtime-neutral-core-tokio-engine.md),
[ADR-0002](decisions/0002-execution-model.md), and
[ADR-0006](decisions/0006-repository-capability-model.md)

This document is the canonical contract for graceful shutdown, cancellation
ownership, in-flight commit policy, durable terminal outcomes, stale detection,
and explicit recovery in M4. It refines, and does not replace, the lifecycle
and unknown-commit rules in
[execution semantics](../compatibility/execution-semantics.md).

M4 has no lease, fencing token, heartbeat, or cross-host ownership. Every rule
below is single-host and cannot be reinterpreted as a distributed guarantee
while [RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md) remains
proposed.

## Shutdown request sources

A shutdown request enters the runtime through an application-owned
`ShutdownSignal` handle. The library installs no process signal handler
implicitly and owns no process-global state.

| Source | Meaning |
| --- | --- |
| Explicit API request | Graceful shutdown of the owning runtime |
| Application-installed `SIGINT`/`SIGTERM` handler | Graceful shutdown, opt-in |
| Second request before the deadline | Escalation: stop waiting for the join deadline and report the drain result immediately |
| `SIGKILL`, host loss, or power failure | Not a shutdown; a crash resolved by stale detection |

Escalation never forces process exit from inside the library, never cancels an
in-flight database commit, and never fabricates a terminal status.

A durable operator `stop` request recorded by the
[operator service](operator-and-explorer-services.md) is a per-execution stop,
not a process shutdown. Process shutdown implies a stop of every execution the
process owns.

## Ordering

Shutdown is a fixed sequence. A later phase never starts before its
predecessor reports.

1. **Stop intake.** Reject new launches with a typed `ShuttingDown` error,
   start no new step, begin no new chunk, and assign no new partition or
   branch.
2. **Propagate cancellation.** Signal the owned task tree cooperatively from
   the root outward.
3. **Apply the in-flight policy.** Each open chunk resolves through its
   declared policy below.
4. **Join owned children.** Await every owned task until the join deadline.
5. **Persist the outcome.** Commit the durable terminal or non-terminal state
   established by phases 3 and 4.
6. **Flush telemetry.** Flush exporters under a separate deadline that is not
   a correctness bound.
7. **Close the repository.** Close the pool under the existing
   `PoolCloseTimeout`.

Telemetry flush and pool close cannot change a durable execution outcome. A
failure in phase 6 or 7 is reported and does not alter phase 5.

## In-flight policy

Each step declares one policy in its definition. It is a definition-class
value and participates in the fingerprint.

| Policy | Behavior |
| --- | --- |
| `FinishChunk` (default) | The open chunk completes read, process, write, and commit, then the step stops at the chunk boundary |
| `RollbackChunk` | The open chunk rolls back; the previous checkpoint stays authoritative |

Neither policy may abandon an open transaction whose commit outcome is
unresolved. If the commit response is ambiguous, the step and its job execution
become `UNKNOWN` exactly as in the accepted M2 rule, the physical connection is
discarded, and shutdown continues without guessing.

A tasklet without a documented checkpoint boundary always uses cooperative
cancellation and its recorded contribution rule; it is never interrupted
mid-transaction.

## Deadlines

| Value | Bounds and default | Missed-deadline behavior |
| --- | --- | --- |
| `ShutdownDeadline` | `1 s..=1 h`, default `30 s` | Total budget for phases 1 to 5 |
| `TaskJoinDeadline` | `1 s..=1 h`, default equals `ShutdownDeadline` | Unjoined tasks are counted and reported |
| `StopPollInterval` | `100 ms..=60 s`, default `1 s` | Upper bound on observing a durable stop request |
| `TelemetryFlushDeadline` | `100 ms..=60 s`, default `5 s` | Dropped events are counted; correctness is unaffected |
| `PoolCloseTimeout` | existing `1 ms..=5 min`, default `30 s` | Incomplete close is reported |

`TaskJoinDeadline` may not exceed `ShutdownDeadline`. A telemetry flush
deadline is never counted against `ShutdownDeadline`.

Missing a deadline is reported, never guessed. When phase 4 cannot join every
owned child, the runtime:

- leaves the job execution in its last durable status, normally `STOPPING`;
- returns a typed `DrainIncomplete` result carrying the count of unjoined
  tasks and the phases they were in;
- emits `shutdown.deadline_exceeded`;
- does not write `STOPPED`, `FAILED`, or `COMPLETED` for work it did not
  observe.

## Durable terminal outcomes

| Drain result | Durable job-execution state |
| --- | --- |
| Every owned child joined, no ambiguous commit | `STOPPED` with the recorded exit code |
| Every owned child joined, at least one ambiguous commit | `UNKNOWN` |
| Join deadline missed | Unchanged, normally `STOPPING`, plus `DrainIncomplete` |
| Shutdown requested after the job already reached a terminal state | Unchanged |

A step that stopped at a committed chunk boundary records `STOPPED` with its
committed counters and checkpoint. Its parent job records `STOPPED` only after
every owned step reports.

## Stale detection

Stale detection produces evidence and a proposal. It never rewrites status.

An execution is a stale candidate when all of the following hold:

- its status is `STARTING`, `STARTED`, or `STOPPING`;
- the elapsed interval between its durable `updated_at` and the repository
  server time exceeds `StaleThreshold`, bounded `1 min..=24 h`, default
  `15 min`;
- its recorded `owner_token` is absent or differs from the current process
  token.

`owner_token` is a per-process random 16-byte value written when the process
takes ownership of an execution. It proves that a non-terminal execution is
not owned by the inspecting process. It is not a lease: it does not expire, it
grants nothing, and it never authorizes takeover.

### Clock rules

Elapsed staleness is computed from repository server time against the durable
`updated_at`. The inspecting process's wall clock is never the authority.

- The runtime reads server time and its own monotonic clock in the same
  bounded window and records the observed offset.
- An offset larger than `MaxClockSkew`, bounded `100 ms..=60 s`, default
  `5 s`, or a negative elapsed interval, produces `ClockEvidenceUnusable` and
  no proposal.
- Backwards movement of server time between observations invalidates the
  evidence rather than shortening the threshold.

## Recovery

Recovery is explicit, evidence bound, and audited. It is the only path that
resolves `UNKNOWN` or a stale candidate.

### Proposal

`propose_recovery(job_execution_id)` returns a bounded, redacted evidence
record and its 32-byte digest:

- execution identity, status, attempt, and optimistic version;
- owner-token presence and whether it matches the current process;
- durable `updated_at`, elapsed inactivity, and the server-time observation;
- the latest durable step execution, its status, and its committed checkpoint
  presence, format, schema, and size;
- whether the last durable marker is an unknown commit;
- whether any completed partition or committed flow decision exists;
- whether the definition declares an ambiguous external effect.

The digest covers the canonical durable decision evidence and the observed
execution version. It includes `updated_at`, but deliberately excludes the
advancing server-time observation and its derived inactivity, wall-clock
offset, and monotonic-window values. Those values gate whether a proposal can
be produced, while excluding them lets a stateless client regenerate the same
digest exactly when no durable evidence changed. Evidence contains no
parameter, context, checkpoint, item, error text, credential, endpoint, or SQL
value.

### Application

`apply_recovery` requires the evidence digest, the observed execution version,
the actor reference, and a reason code. It appends one `ob_recovery_decision`
row and applies the lifecycle change in one transaction, exactly as accepted
in M2.

- Permitted results are `FAILED` and `ABANDONED` only.
- A digest or version mismatch is rejected as `RecoveryEvidenceStale` and
  changes nothing.
- An execution whose last durable marker is an unknown commit may become
  `FAILED` only with the `UNKNOWN_EFFECT` reason code, which records that the
  application must confirm the external effect before restart. It may
  otherwise become `ABANDONED`.
- Recovery never infers whether an ambiguous external effect committed, never
  advances a checkpoint, never adjusts a counter, and never reuses telemetry as
  evidence.

A recovered `FAILED` execution becomes restartable under the ordinary restart
rules. A recovered `ABANDONED` execution is terminal.

## Process signal and kill matrix

Every row is required evidence for the M4 exit gate.

| Event | Expected durable state | Operator action |
| --- | --- | --- |
| `SIGINT`/`SIGTERM` during a chunk, `FinishChunk` | Chunk committed, step and job `STOPPED` | Restart |
| `SIGINT`/`SIGTERM` during a chunk, `RollbackChunk` | Chunk rolled back, previous checkpoint authoritative, `STOPPED` | Restart |
| Second signal before the join deadline | Last durable state plus `DrainIncomplete` | Inspect, then recover if stale |
| `SIGKILL` before chunk commit | Execution left non-terminal, previous checkpoint authoritative | Stale detection, then recover to `FAILED` |
| `SIGKILL` after a proven chunk commit | Committed chunk durable, execution left non-terminal | Stale detection, then recover to `FAILED`; the committed chunk is not replayed |
| Ambiguous commit then shutdown | `UNKNOWN` | Inspect durable state, then recover |
| Panic inside an owned child task | Typed framework failure for that child; parent joins and records the step failure | Restart or abandon |
| Panic inside a listener | Classified at the listener boundary as accepted in M3 | Restart |
| Host loss or power failure | Whatever committed before the failure | Stale detection, then recover |
| Repository unreachable during phase 5 | Last durable state, typed repository failure reported | Retry shutdown reporting, then recover if stale |

Forced termination is always a crash result. It is never evidence of low
cancellation latency and never satisfies a graceful-shutdown claim.

## Evidence

Production implementation requires:

- deterministic unit tests for phase ordering, intake rejection, escalation,
  and deadline arithmetic with injected clocks;
- cancellation tests measuring request-to-intake-stop and
  request-to-durable-terminal latency separately for async, blocking, and
  transaction phases;
- structured-ownership tests proving no detached task survives shutdown and
  every owned child is joined or counted;
- in-flight policy tests for `FinishChunk`, `RollbackChunk`, and the ambiguous
  commit path;
- separate-process kill tests covering every row of the signal matrix on
  PostgreSQL 15 and 18;
- stale-detection tests for threshold, owner-token mismatch, unusable clock
  evidence, and the absence of automatic status rewriting;
- recovery tests for digest staleness, version conflict, permitted results,
  the `UNKNOWN_EFFECT` reason, and audited append-plus-transition atomicity;
- soak evidence that repeated shutdown cycles leak no task, connection, or
  handle.
