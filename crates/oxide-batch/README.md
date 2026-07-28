# oxide-batch

The public facade crate for [OxideBatch](https://github.com/luceat-lux-vestra/oxide-batch),
an enterprise-ready batch processing framework for Rust inspired by Spring Batch.

> This crate is implementing the M1 executable kernel. Its domain model is
> available, but it does not yet expose a production-ready batch runtime.

The facade owns validated job and step names, opaque instance/execution IDs,
typed and value-redacted job parameters, canonical job-instance keys, lifecycle
metadata, counters, exit statuses, and redacted failure summaries. Runtime and
repository APIs will be added behind this supported public entry point as M1
progresses.

Job and step execution snapshots enforce the accepted lifecycle through
database-agnostic optimistic versions. Exit status can be enriched independently
of batch status, and restarting a stopped or failed record always creates a
distinct `STARTING` attempt.
