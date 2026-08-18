# M5 Developer Guide

**State:** Accepted

**Applies to:** OxideBatch `0.5.0`, the M5 Embedded Core Production Preview

An executable path from a dependency declaration to a working embedded job.
This guide links to canonical contracts rather than restating them; see
[documentation strategy](../documentation/strategy.md).

## 1. Add the dependency

```toml
[dependencies]
oxide-batch = { version = "0.5.0", features = ["postgres"] }
tokio = { version = "1", features = ["rt", "macros"] }
```

`oxide-batch` is the only crate this release supports as an entry point.
`oxide-batch-core`, `oxide-batch-plan`, and `oxide-batch-repository` are
published only because the facade depends on them; they carry no stability
promise and every supported item they hold is re-exported from `oxide-batch`
under a stable path (`crates/oxide-batch/src/lib.rs:284-333`). Import only
from `oxide_batch::*`. Drop `features = ["postgres"]` to build against the
in-memory reference repository only.

## 2. Run the smallest complete job

[`crates/oxide-batch/examples/first_job.rs`](../../crates/oxide-batch/examples/first_job.rs)
is a tested, runnable single-tasklet job against the in-memory repository:

```console
cargo run -p oxide-batch --example first_job
```

It shows the shape every job follows:

1. construct a `Clock`, an `IdGenerator`, and a repository
   (`InMemoryJobRepository::new`, or `PostgresJobRepository::connect` — see
   step 4);
2. implement `Tasklet` (or, for chunked work, `ItemReader`/`ItemProcessor`/
   `ItemWriter`) for your unit of work;
3. build the job definition (`TaskletJob::new`, or a `FlowGraph` compiled to a
   `CompiledExecutionPlan` for multi-step jobs — see step 3) with an explicit
   `DefinitionRevision` and `ComponentRevision`;
4. construct `JobLauncher::new(&repository, &clock, &ids)` and call
   `launcher.launch(&job, &parameters, &stop_token).await`;
5. inspect the returned `LaunchReport`/`TaskletExecutionOutcome`.

The facade's own rustdoc at
[`crates/oxide-batch/src/lib.rs`](../../crates/oxide-batch/src/lib.rs) has
further runnable, tested examples: typed job parameters and instance keys
(lines 7-25), reading a definition manifest back (lines 105-124), compiling a
multi-step `FlowGraph` (lines 131-157), exit-pattern matching (lines 162-172),
and constructing a `FaultPolicy` and deciding a retry (lines 177-219). Every
one of them is a doctest that runs in CI on every change.

## 3. Multi-step jobs and restart identity

A job with more than one step is declared as a `FlowGraph` of `FlowNode`
values joined by `ExitPattern` transitions and compiled once into an
immutable `CompiledExecutionPlan`, which owns the canonical manifest and
[definition fingerprint](production-preview.md#restart-and-definition-drift).
See [execution-plan architecture](../architecture/execution-plan.md) for the
model and the compile-time validation it performs, and
[basic flow](../architecture/basic-flow.md) for sequential/conditional
transitions, deciders, and start controls.

Chunked steps implement `ItemReader<I>`, `ItemProcessor<I, O>`, and
`ItemWriter<O>` and commit atomically with their checkpoint, context, and
counters through `ChunkTransaction`. See
[item-processing model](../architecture/item-processing-model.md).

## 4. Move to PostgreSQL

Apply the schema-3 migrations with the migrator identity, then connect with
the runtime identity:

```rust,no_run
use std::sync::Arc;
use oxide_batch::{PostgresConfig, PostgresJobRepository, PostgresMigrator, SystemClock};

# async fn setup() -> Result<(), Box<dyn std::error::Error>> {
let migrator_config = PostgresConfig::new(std::env::var("MIGRATOR_DATABASE_URL")?)?;
PostgresMigrator::migrate(&migrator_config).await?;

let runtime_config = PostgresConfig::new(std::env::var("RUNTIME_DATABASE_URL")?)?;
let repository = PostgresJobRepository::connect(runtime_config, Arc::new(SystemClock)).await?;
# repository.close().await?;
# Ok(())
# }
```

Full role separation, TLS (`verify-full` in production), pool/timeout
configuration, and fail-closed startup behavior on a mismatched schema version
are in [PostgreSQL setup](../operations/postgres-setup.md). A worked
PostgreSQL-backed local-partition job is
[`crates/oxide-batch/examples/postgres_local_partition.rs`](../../crates/oxide-batch/examples/postgres_local_partition.rs).

## 5. Add fault tolerance

`ChunkStep::with_fault_runtime` installs a `FaultRuntime` built from a
`FaultPolicy` (classifier, retry limit, retry-state limit, skip limit,
backoff policy). See the fault-policy doctest at
`crates/oxide-batch/src/lib.rs:177-219` and
[M3 fault-tolerance contract](../architecture/fault-tolerance.md) for the
complete retry/skip/rollback/backoff semantics, including the crash-recovery
boundaries in [crash, restart, and recovery](../operations/crash-restart-and-recovery.md#fault-tolerance-state-after-a-crash).

## 6. Add listeners and telemetry

`JobExecutionListener`, `StepExecutionListener`, and the M3
`ItemListenerSet` (read/process/write/retry/skip) observe deterministic
boundaries without becoming lifecycle authority; see
[the observability contract](../operations/observability-contract.md) for the
structured event schema, span hierarchy, and redaction rules those callbacks
and `LifecycleEventSink` participate in.

## 7. Embed the operator CLI

`oxide-batch-cli`'s library crate is embeddable: a host application registers
its compiled job definitions in its own `DefinitionCatalog` so that `launch`
and `execution restart` can be authorized against the job's real
`DefinitionIdentity`. Without a host-supplied catalog, the shipped
`oxide-batch` binary serves every other command and reports a guard
rejection for those two. See the [operator guide](operator-guide.md) and the
[operator CLI contract](../operations/operator-cli.md#delivery-boundary) for
why this boundary exists.

## What the facade will not expose

The curated `oxide-batch` surface is a reviewed, closed disclosure boundary:
no async-runtime, database-driver, telemetry-SDK, credential,
deployment-authorization, or sensitive-payload type appears in a public
signature, field, or `Debug` output, and no user-supplied error text is
disclosed. See the
[M5 preview surface and disclosure gate](../api/design-guidelines.md#m5-preview-surface-and-disclosure-gate)
for the complete rule set this release was reviewed against.

## Next

- [Operator guide](operator-guide.md) for inspecting and recovering the jobs
  you launch.
- [Upgrade and rollback guide](upgrade-and-rollback.md) before your first
  schema upgrade.
- [Limitations](limitations.md) for what is not yet covered.
