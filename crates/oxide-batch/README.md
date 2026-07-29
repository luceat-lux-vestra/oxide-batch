# oxide-batch

The public facade crate for [OxideBatch](https://github.com/luceat-lux-vestra/oxide-batch),
a restart-oriented batch processing framework for Rust inspired by Spring Batch.

> This crate contains the completed M1 executable kernel and the first M2
> PostgreSQL metadata adapter, deterministic M2 chunk orchestration, and atomic
> enlisted PostgreSQL chunk commits. Durable restart and recovery selection are
> still in progress, so it is not yet a production-ready batch runtime.

The facade owns validated job and step names, opaque instance/execution IDs,
typed and value-redacted job parameters, canonical job-instance keys, lifecycle
metadata, counters, exit statuses, redacted failure summaries, runtime-neutral
repository ports, an in-memory reference repository, and async tasklet
execution with cooperative stopping. Job and step listeners nest
deterministically around tasklet work, and facade-owned lifecycle events expose
only reviewed correlation and failure fields.

Job and step execution snapshots enforce the accepted lifecycle through
database-agnostic optimistic versions. Exit status can be enriched independently
of batch status, and restarting a stopped or failed record always creates a
distinct `STARTING` attempt.

Repository operations are staged in an explicit unit of work and become
visible only after commit. The in-memory implementation uses optimistic
snapshot commits, reports a typed conflict when another unit commits first, and
accepts injected clock and ID sources instead of reading hidden global state.
It is intended for deterministic kernel tests and process-local execution; it
is not durable across restarts.

With the optional `postgres` feature, `PostgresJobRepository` implements the
same repository contract over the immutable OxideBatch schema. Runtime startup
verifies schema version 1 but never applies migrations; deployments call
`PostgresMigrator` separately with a migrator identity. Production defaults
require certificate and hostname validation, while plaintext transport is an
explicit local-test choice. Connection strings, certificate paths, parameters,
contexts, SQL text, and bound values are excluded from facade diagnostics.

```toml
[dependencies]
oxide-batch = { version = "0.1.0-alpha.1", features = ["postgres"] }
```

```rust,no_run
use std::sync::Arc;

use oxide_batch::{PostgresConfig, PostgresJobRepository, PostgresMigrator, SystemClock};

# async fn configure() -> Result<(), Box<dyn std::error::Error>> {
let migrator = PostgresConfig::new(std::env::var("MIGRATOR_DATABASE_URL")?)?;
PostgresMigrator::migrate(&migrator).await?;

let runtime = PostgresConfig::new(std::env::var("RUNTIME_DATABASE_URL")?)?;
let repository = PostgresJobRepository::connect(runtime, Arc::new(SystemClock)).await?;
# repository.close().await?;
# Ok(())
# }
```

The migrator and runtime connection strings should identify different
least-privilege roles in production. The adapter remains behind the public
repository and unit-of-work ports; SQLx types are never part of those APIs.

`JobLauncher` executes on an application-owned async runtime. The public
`Tasklet` trait returns an OxideBatch-owned boxed future, so implementations can
borrow call-scoped parameters and stop state without exposing Tokio types.
Synchronous work must use `BlockingTaskletAdapter`, which applies an explicit
nonzero concurrency bound and awaits already-running work before reporting a
late stop.

Before-listeners run in registration order and after-listeners run in reverse
order. A before failure prevents the associated user body. An after failure
cannot undo completed user work; `LaunchReport` retains the provisional outcome
and every redacted listener failure. Event sinks observe committed lifecycle
states and cannot fail an execution, even if a sink panics.

## M2 component contracts

The facade now owns runtime-neutral `ItemReader`, `ItemProcessor`, `ItemWriter`,
and `ChunkCompletion` traits. Each asynchronous call may borrow component and
call-scoped state without exposing an executor. Typed outcomes keep normal
end-of-input, filtered items, cooperative stop, component failure, and
post-commit acknowledgement separate.

Durable PostgreSQL writers receive a call-scoped `BusinessTransaction` port.
Statements use separately bound, value-redacted facade types; SQLx pools,
connections, transactions, rows, and errors remain adapter-internal.

`Checkpoint` and `ExecutionContext` are bounded, versioned, schema-aware JSON
envelopes. Application codecs receive JSON object bytes rather than Serde
types, own explicit old-version upgrade paths, and produce only redacted failure
classifications. The default envelope limit is 64 KiB and depth 16, with hard
ceilings aligned to the accepted metadata model.

## M2 chunk execution

`ChunkStep` executes bounded read/process/filter/write attempts and publishes
only counts returned by a successful `ChunkTransaction` commit. `ChunkJob` and
`JobLauncher::launch_chunk` reuse the existing repository-backed job and step
lifecycle. Stop before commit rolls the attempt back; stop or completion
failure after commit cannot undo the committed chunk.

Chunk listeners nest in registration/reverse-registration order. Component and
listener panic payloads are discarded, and an ambiguous transaction response
becomes `UNKNOWN` rather than being inferred. Correlated `chunk.started`,
`chunk.committed`, `chunk.rolled_back`, and `chunk.unknown` events include a
nonzero chunk sequence.

With the `postgres` feature, `PostgresChunkTransactionManager` binds a launched
chunk to its job/step execution identity. A `PostgresChunkStateProvider`
produces the checkpoint and context for each commit from the prior durable
counters and current checked counts. The adapter lends the writer one
`BusinessTransaction`, then commits its writes, checkpoint, context, cumulative
counters, injected-clock update time, and optimistic step version on the same
connection. A failed CAS rolls everything back; a failed `COMMIT`
acknowledgement discards the connection and reports `UNKNOWN`.

Managers that do not enlist the writer retain the documented at-least-once
boundary and cannot claim PostgreSQL same-resource atomicity. Durable restart
selection and audited recovery remain the next M2 workstream.

## First in-memory job

The checked-in example defines a tasklet using only facade-owned batch types.
The application supplies the async executor and explicitly constructs the
process-local repository, clock, and identifier source:

```console
cargo run -p oxide-batch --example first_job
```

It prints the tasklet observation and the final persisted job status. The
in-memory repository is intentionally non-durable; use this M1 example for
learning, local execution, and deterministic tests rather than restart
guarantees across process failure.
