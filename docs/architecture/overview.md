# Architecture Overview

**State:** Accepted

## Dependency rule

Dependencies point inward toward stable domain contracts:

```text
oxide-batch (facade)
    ├── runtime ────────┐
    ├── postgres ───────┼──> repository contracts ──> core domain
    ├── observability ──┤
    ├── test ───────────┘
    └── cli ──> facade/application services
```

The core domain must not depend on Tokio, SQLx, PostgreSQL, Clap, or an
OpenTelemetry SDK. Integration crates implement core ports. The facade exposes
only a curated supported API and prevents workspace structure from becoming a
compatibility promise.

## Proposed components

| Component | Responsibility | Initial publication |
| --- | --- | --- |
| `oxide-batch-core` | Domain values, state rules, execution contracts | Internal |
| `oxide-batch-runtime` | Job/step orchestration and policy execution | Internal |
| `oxide-batch-repository` | Repository and transaction ports | Internal |
| `oxide-batch-postgres` | PostgreSQL adapter and migrations | Public candidate |
| `oxide-batch-observability` | Stable events and telemetry adapters | Internal initially |
| `oxide-batch-test` | Conformance fixtures and user test utilities | Public candidate |
| `oxide-batch-cli` | Operator commands | Public candidate/binary |
| `oxide-batch` | Curated facade and default assembly | Public |

Create a crate only when its dependency boundary is exercised. Internal crates
use `publish = false` until their external support obligation is approved.

## Correctness boundaries

- The repository is the authority for execution identity and lifecycle state.
- Runtime state is reconstructible from durable metadata plus job definition.
- A checkpoint is advanced only with its associated successful commit.
- Exactly-once effects are not promised for arbitrary external resources.
- Blocking user work must not silently occupy asynchronous executor threads.
- Cancellation is cooperative and is persisted as an explicit lifecycle event.
- Telemetry observes decisions; it does not determine correctness.

## Architecture spikes required before M1/M2

1. **Async public contract:** compare native async traits, boxed futures, and a
   synchronous core with async adapters for object safety and cancellation.
2. **Transaction enlistment:** prove that item writes and checkpoint metadata
   can share a PostgreSQL transaction without leaking SQLx into core contracts.
3. **Crash matrix:** terminate a process at each chunk phase and record replay,
   counters, context, and status after recovery.
4. **Optimistic locking:** race duplicate launches and concurrent execution
   updates against real PostgreSQL.
5. **Execution-context evolution:** read old serialized contexts after type and
   application version changes.

Spike code is disposable. Its conclusion and evidence are permanent ADR input.
