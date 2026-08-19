# Architecture Overview

**State:** Accepted

**Open proposals:** The distributed worker protocol remains gated by
RFC-0009. The static/erased hot-path decision was accepted as RFC-0005 and is
recorded as [ADR-0008](decisions/0008-item-component-contract.md); its
production migration boundary is frozen by the
[M6 design-gate evidence](../project/m6-design-gate-evidence.md) and lands in
[#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143).

This document shows current and accepted target structure. Focused documents
own detailed contracts. Elements governed by RFC-0009 remain proposed and are
not implementation authority.

## Dependency rule

Dependencies point inward toward stable domain and plan contracts:

```text
public facade / application assembly
          |
          +--> CLI and operator/explorer interfaces
          +--> integration and observability adapters
          +--> local/distributed execution engines
                         |
                compiled execution plan
                         |
        item model + repository/transaction ports
                         |
          core domain, identities, and state rules
```

Core, plan, item, and repository contracts do not expose Tokio, SQLx,
PostgreSQL, Clap, broker clients, web frameworks, or OpenTelemetry SDK types.
Adapters depend inward. The facade curates supported APIs so workspace layout
does not become an accidental compatibility promise.

## Current state

The current M1/M2 code is intentionally concentrated in the public
`oxide-batch` crate while boundaries are proven. It provides core domain,
in-memory and PostgreSQL repository behavior, tasklet execution, initial item
contracts, diagnostics, and test support. Tokio is the accepted initial
runtime, PostgreSQL/SQLx is the accepted durable adapter, and the public facade
owns all exposed types.

The accepted current decisions are:

- [workspace and facade](decisions/0001-workspace-and-facade.md);
- [async execution model](decisions/0002-execution-model.md);
- [repository capability model](decisions/0006-repository-capability-model.md),
  retaining PostgreSQL as the current reference adapter;
- [definition identity and restart](decisions/0004-job-definition-restart-compatibility.md).

No target below silently supersedes them.

## Target layers

| Layer | Responsibility | Canonical detail |
| --- | --- | --- |
| Public facade | ergonomic builders, curated re-exports, defaults, application assembly | [API guidelines](../api/design-guidelines.md) |
| Core domain/definitions | identities, parameters, statuses, state rules, immutable definitions | [Execution plan](execution-plan.md) |
| Compiled plan | graph normalization, capability validation, manifest, fingerprint, lowering | [Execution plan](execution-plan.md) |
| Execution engine | lifecycle orchestration, policies, structured concurrency, local runtime | [Execution semantics](../compatibility/execution-semantics.md) |
| Item model | reader/processor/writer/stream, chunks, checkpoints, composition | [Item-processing model](item-processing-model.md) |
| Repository ports/adapters | commands, queries, operator service, registry, retention, transactions | [Repository and transaction model](repository-and-transaction-model.md) |
| Capability/delivery model | same-resource, messaging, outbox/inbox, idempotency, at-least-once | [Repository model](repository-and-transaction-model.md) |
| Integrations | databases, files, object stores, HTTP, brokers, custom adapters | [Integration model](integration-model.md) |
| Observability | stable events, metrics, traces, redaction, exporter adapters | [Observability contract](../operations/observability-contract.md) |
| Test/conformance | user test kit, adapter contracts, Spring differential and failure fixtures | [Conformance strategy](../compatibility/conformance-strategy.md) |
| Distributed protocol | coordinator/worker, remote nodes, leases, fencing, protocol compatibility | [Distributed execution](distributed-execution.md) |
| Operator/explorer | portable launch/query/stop/recovery/retention semantics, CLI | [Control-plane boundary](../operations/control-plane-boundary.md) |
| External control plane | hosting, UI, auth/RBAC, scheduler, fleet, Kubernetes, SaaS | [Control-plane boundary](../operations/control-plane-boundary.md) |

## Static hot path and erased boundary

The accepted native item path is generic and monomorphizable, with no
item-per-item boxed future or trait-object lookup. Erased adapters
(`BoxedReader`/`BoxedProcessor`/`BoxedWriter`) are used for heterogeneous
plans, dynamic registration, ergonomic facade boundaries, and out-of-process
protocols. Allocation is limited to step, chunk, or registry boundaries where
a handle is explicitly constructed.

This changes the consequences of accepted ADR-0002 and is decided by
[RFC-0005](../rfcs/0005-static-and-erased-components.md), accepted
2026-08-03 and recorded as
[ADR-0008](decisions/0008-item-component-contract.md), which supersedes
ADR-0002 in part — the item component representation only. Production code
still carries the ADR-0002 boxed item traits until
[#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143) migrates
them under the boundary the
[M6 design-gate evidence](../project/m6-design-gate-evidence.md) freezes;
item listeners keep the ADR-0002 boxed form for M6 regardless of that
migration.

## Runtime boundary

Tokio remains the explicit default engine and no hidden global runtime is
created. The accepted target makes core/item/repository contracts
runtime-neutral and keeps spawn, cancellation, timer, and blocking integration
inside the engine boundary. OxideBatch does not invent a general runtime
abstraction.

This refinement is accepted by
[RFC-0006](../rfcs/0006-runtime-neutral-core-tokio-engine.md).

## Target workspace forecast

Potential boundaries include facade, core, plan, engine, item, repository
contracts and adapters, transaction, flow/fault-tolerance, distributed
protocol, observability, test, migration, integrations, and CLI crates.
Extraction is incremental and behavior-preserving. No crate is created or
published solely to reserve a name.

[RFC-0003](../rfcs/0003-target-workspace-boundaries.md) owns the accepted
staging direction and
[crate publishing policy](../governance/crate-publishing.md) owns publication.

## Correctness boundaries

- The repository is authoritative for execution identity, lifecycle, durable
  ownership, and checkpoint.
- Runtime state is reconstructible from durable metadata, an exact definition
  manifest/fingerprint, and declared external resources.
- Checkpoint advances only with its associated successful commit/delivery
  contract.
- Exactly-once effects are not promised across arbitrary resources.
- Blocking work, queues, concurrency, connections, and memory are bounded.
- Cancellation is cooperative and durable.
- Telemetry and transports observe/carry decisions but never determine them.
- A stale optimistic version or distributed fencing token cannot commit.

## Extension boundary

High-performance extensions use static linking and traits. Heterogeneous Rust
components use registered erased adapters. Dynamic/language-neutral extensions
use a versioned out-of-process protocol; untrusted components require an
explicit WASI sandbox. A stable Rust dynamic-library ABI is not promised.

## Architecture validation

Before a target layer is implemented, evidence must cover dependency
direction, facade compatibility, plan validation, static/erased equivalence,
allocation/performance budgets, transaction capability rejection, restart
definition drift, adapter contracts, and local/distributed trace equivalence as
applicable.
