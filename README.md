# OxideBatch

[![CI](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Reliable, restartable batch processing for Rust.**

OxideBatch is a Rust-native batch-processing framework inspired by Spring Batch. It focuses on durable execution state, explicit transaction boundaries, restartability, fault tolerance, and operational evidence rather than treating batch jobs as one-shot background functions.

> **Current release:** `0.6.0` (`v0.6.0`, published 2026-08-31). OxideBatch is still a pre-1.0 project. M6 completes the current item-processing and public test-kit scope, but the project does not claim GA, enterprise readiness, or full Spring Batch parity yet.

## Why OxideBatch

OxideBatch is designed around batch workloads that must answer questions such as:

- What was committed before the process died?
- Can the job restart without duplicating already committed work?
- Which definition produced this execution?
- Where are retry, skip, rollback, and stop boundaries?
- Can operators inspect or recover durable execution state safely?
- Can compatibility claims be backed by repeatable evidence rather than implementation alone?

The project therefore treats job metadata, checkpoints, execution identity, restart semantics, and failure recovery as first-class framework concerns.

## Quick start

Add the public facade crate:

```toml
[dependencies]
oxide-batch = "0.6.0"
tokio = { version = "1", features = ["rt", "macros"] }
```

For PostgreSQL-backed durable metadata:

```toml
oxide-batch = { version = "0.6.0", features = ["postgres"] }
```

`oxide-batch` is the supported application entry point. The internal workspace crates are implementation details and carry no independent stability promise.

### Run the smallest complete job

The repository includes a tested single-tasklet example:

```console
cargo run -p oxide-batch --example first_job
```

A job follows the same basic shape whether it uses the in-memory reference repository or PostgreSQL:

1. construct a clock, ID generator, and job repository;
2. implement a `Tasklet`, or `ItemReader` / `ItemProcessor` / `ItemWriter` for chunk work;
3. define the job and its explicit component/definition revisions;
4. launch it through `JobLauncher`;
5. inspect the resulting launch/execution outcome.

See the [developer guide](docs/guides/developer-guide.md) for the complete path from dependency declaration to a working PostgreSQL-backed job.

## Core capabilities

- **Durable metadata and restartability** — job/step execution state, checkpoints, execution identity, and definition-drift protection.
- **Chunk-oriented processing** — explicit item reader/processor/writer contracts with atomic chunk transaction boundaries.
- **Fault tolerance** — typed retry, skip, rollback, backoff, and durable fault state.
- **Flow execution** — compiled multi-step flow graphs, conditional transitions, decisions, and start controls.
- **Operational recovery** — guarded inspection, stale-execution recovery, retention, diagnostics, and operator tooling.
- **Observability** — bounded structured logs, metrics, traces, listeners, and diagnostic bundles.
- **Local scale** — bounded tasklet parallel splits and local partition execution.
- **Application test support** — public `oxide-batch-test` utilities for exercising supported application-facing contracts.

## PostgreSQL

OxideBatch separates migration and runtime identities and fails closed when repository schema expectations are not met.

```rust,no_run
use std::sync::Arc;
use oxide_batch::{PostgresConfig, PostgresJobRepository, PostgresMigrator, SystemClock};

# async fn setup() -> Result<(), Box<dyn std::error::Error>> {
let migrator = PostgresConfig::new(std::env::var("MIGRATOR_DATABASE_URL")?)?;
PostgresMigrator::migrate(&migrator).await?;

let runtime = PostgresConfig::new(std::env::var("RUNTIME_DATABASE_URL")?)?;
let repository = PostgresJobRepository::connect(runtime, Arc::new(SystemClock)).await?;
# repository.close().await?;
# Ok(())
# }
```

Production role separation, TLS, pool/timeout configuration, migrations, and startup behavior are documented in [PostgreSQL setup](docs/operations/postgres-setup.md).

## Execution model

```text
Job definition
     │
     ▼
Compiled execution plan
     │
     ▼
JobLauncher ──────► JobRepository
     │                  │
     ▼                  ▼
step / chunk       durable metadata
execution          checkpoints / state
     │                  │
     └──── restart / recovery ────┘
```

Multi-step jobs use a compiled `FlowGraph`. Chunk-oriented steps commit business work together with their framework checkpoint/context/counters through the framework transaction boundary.

## Current status

Completed implementation milestones:

- **M0 — Foundation**
- **M1 — Executable Kernel**
- **M2 — Durable Chunk and Restart**
- **M3 — Fault Tolerance and Flow**
- **M4 — Operations and Local Scale**
- **M5 — Embedded Core Production Preview** (`0.5.0`)
- **M6 — Complete Item Processing and User Test Kit** (`0.6.0`)

The detailed exit evidence, compatibility ledger, failure campaigns, and milestone records intentionally live under `docs/` rather than in this landing page.

Start with:

- [Documentation index](docs/README.md)
- [Developer guide](docs/guides/developer-guide.md)
- [Production preview guide](docs/guides/production-preview.md)
- [Crash, restart, and recovery](docs/operations/crash-restart-and-recovery.md)
- [Capacity and resource budgets](docs/operations/capacity-and-resource-budgets.md)
- [Roadmap](docs/roadmap.md)

## Repository layout

```text
crates/
├── oxide-batch/            Public facade crate
├── oxide-batch-core/       Internal domain model and durable values
├── oxide-batch-plan/       Internal flow graph / plan compiler
├── oxide-batch-repository/ Internal metadata ports and durable values
├── oxide-batch-cli/        Public operator CLI
└── oxide-batch-test/       Public application-facing test kit
spikes/                     Reproducible architecture/performance evidence
xtask/                      Repository development commands
```

The shipped `oxide-batch` command is a guarded repository operator, not a standalone Rust job-definition loader. Launching or restarting application work requires a host application that supplies its compiled definition catalog.

## Compatibility and release claims

OxideBatch separates **implemented** behavior from **verified** behavior. A feature is not promoted to a release-backed compatibility claim merely because code or a test exists; compatibility promotion is governed separately by the project's evidence ledger.

`0.6.0` should therefore be read as a published M6 pre-1.0 release, not as a claim of full Spring Batch compatibility or production GA.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request. Security vulnerabilities must be reported according to [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
