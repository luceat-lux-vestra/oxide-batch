# oxide-batch

The public facade crate for [OxideBatch](https://github.com/luceat-lux-vestra/oxide-batch),
an enterprise-ready batch processing framework for Rust inspired by Spring Batch.

> This crate contains the completed M1 executable kernel. Its domain model,
> process-local repository, and single-step tasklet launcher are available
> while M2 durable chunk and restart work proceeds, but it is not yet a
> production-ready batch runtime.

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
