# OxideBatch

[![CI](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

OxideBatch is an early-stage Rust-native framework for reliable, restartable
batch processing, inspired by Spring Batch.

> [!IMPORTANT]
> OxideBatch has completed the M1 executable kernel, M2 durable PostgreSQL
> chunk/restart gate, and M3 fault-tolerance and finite-flow implementation
> gate. Compatibility rows remain unreleased `Implemented` or `Partial`
> evidence, and no production-ready runtime has been released.

## Project goals

- Predictable job and step execution semantics
- Durable metadata and restartability
- Chunk-oriented processing with explicit transaction boundaries
- Typed retry, skip, and failure policies
- Operational visibility through structured logs, metrics, and traces
- A conformance suite for documented Spring Batch-compatible behavior

## Repository layout

OxideBatch is a Cargo workspace. `oxide-batch` is the public facade crate.
Additional crates will be introduced only when their boundaries and public
support commitments are clear.

```text
crates/
└── oxide-batch/    Public facade crate
spikes/
└── m0-architecture/ Reproducible, non-published architecture evidence
xtask/              Repository development commands (not published)
```

See [crate publishing policy](docs/governance/crate-publishing.md) for the
planned multi-crate strategy.

## Status

**M0 — Foundation**, **M1 — Executable Kernel**, **M2 — Durable Chunk and
Restart**, **M3 — Fault Tolerance and Flow**, and **M4 — Operations and Local
Scale** are complete implementation milestones. M2 includes the PostgreSQL
schema and repository, atomic enlisted chunks, definition-guarded restart,
audited recovery, and separate-process pre/post-commit crash evidence recorded
in the [M2 exit record](docs/project/m2-exit-evidence.md). M3 adds typed bounded
retry/skip/rollback, deterministic listener boundaries, schema-2 durable fault
state, finite compiled flow, durable decisions and start controls, and
process-kill restart evidence recorded in the
[M3 exit record](docs/project/m3-exit-evidence.md). M4 adds guarded
operator/explorer and retention services, the operator CLI and configuration
diagnostics, graceful shutdown and stale recovery, bounded telemetry and
diagnostic bundles, and the tasklet-only bounded parallel-split and
local-partition runtime, with the PostgreSQL process-kill, resource-bound,
cancellation-latency, telemetry-overhead, and soak evidence recorded in the
[M4 exit record](docs/project/m4-exit-evidence.md) and the derived
[capacity guidance](docs/operations/capacity-and-resource-budgets.md). Every M4
row remains implemented or partial rather than released and verified.

**M5 — Embedded Core Production Preview** is the active milestone. It
stabilizes the delivered M0-M4 embedded scope rather than adding batch
capability, and its decision gates, workstreams, and exit criteria are recorded
in the [M5 kickoff gate](docs/project/m5-kickoff-gate.md). M5 is the first
milestone that may promote advertised embedded-kernel ledger rows to verified,
and only against a named released version.

The shipped `oxide-batch` command is a guarded repository operator, not a
standalone Rust job-definition loader. It can inspect and recover durable
partition metadata; launching or restarting application work requires a host
application that embeds `oxide-batch-cli` and supplies its own compiled
`DefinitionCatalog`. Run `oxide-batch --help` for that boundary and the command
grammar entry point.

Start with the [documentation index](docs/README.md) and
[continuous delivery roadmap](docs/roadmap.md). The M5-M14 full-parity program
is accepted; the static/erased component architecture and distributed worker
protocol remain gated by RFC-0005 and RFC-0009.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.
Security vulnerabilities must be reported according to
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
