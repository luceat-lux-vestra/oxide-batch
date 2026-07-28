# oxide-batch

The public facade crate for [OxideBatch](https://github.com/luceat-lux-vestra/oxide-batch),
an enterprise-ready batch processing framework for Rust inspired by Spring Batch.

> This crate is implementing the M1 executable kernel. Its domain model and
> process-local repository are available, but it does not yet expose a
> production-ready batch runtime.

The facade owns validated job and step names, opaque instance/execution IDs,
typed and value-redacted job parameters, canonical job-instance keys, lifecycle
metadata, counters, exit statuses, redacted failure summaries, runtime-neutral
repository ports, and an in-memory reference repository. Runtime APIs will be
added behind this supported public entry point as M1 progresses.

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
