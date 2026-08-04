# oxide-batch-plan

**This crate is OxideBatch implementation detail. Use
[`oxide-batch`](https://crates.io/crates/oxide-batch) instead.**

It exists on crates.io only because the published `oxide-batch` facade depends
on it. Its API carries no stability promise: items may be added, changed, or
removed in any release, without a deprecation period, and without a changelog
entry of their own. It has no supported-configuration matrix, no compatibility
ledger row, and no independent release cadence; every version tracks the facade
version that consumes it.

Everything this crate exports that OxideBatch supports is re-exported from
`oxide-batch` under a stable path. Depend on the facade.

The publication rule is
[ADR-0010](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/architecture/decisions/0010-extracted-crate-publication.md),
and the boundary it holds is the
[staged crate-extraction contract](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/architecture/crate-extraction.md).

## Contents

The immutable flow graph of step, decision, split, join, and partitioned-step
nodes with its exit-pattern transitions, the compiled execution plan those
graphs lower into, graph normalization, the structural validation the accepted
basic-flow contract names, and the canonical restart-relevant manifest
projection whose SHA-256 digest — taken in `oxide-batch-core` — is the
definition fingerprint.

The crate depends on no async runtime, database driver, command-line framework,
telemetry SDK, broker client, or web framework, and on no OxideBatch crate
other than `oxide-batch-core`. The flow engine that executes a compiled plan,
the metadata ports that persist its decisions, and the runtime live above it.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
