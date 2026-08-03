# oxide-batch-core

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

Domain identities, typed job parameters, statuses and exit statuses, execution
records and their lifecycle rules, bounded versioned execution-context and
checkpoint state, chunk sizing and counting values, and restart-relevant
definition identity with its canonical manifest encoding.

The crate depends on no async runtime, database driver, command-line framework,
telemetry SDK, broker client, or web framework, and on no other OxideBatch
crate.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
