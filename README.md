# OxideBatch

[![CI](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

OxideBatch is an early-stage Rust-native framework for reliable, restartable
batch processing, inspired by Spring Batch.

> [!IMPORTANT]
> OxideBatch `0.6.0` is the published first M6 release (tag `v0.6.0`,
> published 2026-08-31; see
> [post-publish verification](docs/release/evidence/v0.6.0-post-publish.md)).
> It remains a `0.x` pre-1.0 release: M6 adds the item-processing surface and
> public `oxide-batch-test`, but does not claim M7-M14 completion, GA,
> enterprise readiness, or full Spring Batch parity. M6 campaign evidence
> alone is not release-backed `Verified` evidence; promoting a compatibility
> row to `Verified` is a separate governance decision this release does not
> make automatically.

## Project goals

- Predictable job and step execution semantics
- Durable metadata and restartability
- Chunk-oriented processing with explicit transaction boundaries
- Typed retry, skip, and failure policies
- Operational visibility through structured logs, metrics, and traces
- A conformance suite for documented Spring Batch-compatible behavior

## Repository layout

OxideBatch is a Cargo workspace. `oxide-batch` is the public facade crate and
the only supported entry point. The three crates below it are implementation
detail with no stability promise: they are published only because the facade
depends on them, and every item the facade supports is re-exported from
`oxide-batch` under a stable path.

```text
crates/
├── oxide-batch/            Public facade crate
├── oxide-batch-core/       Internal: domain model and durable values
├── oxide-batch-plan/       Internal: flow graphs and the plan compiler
├── oxide-batch-repository/ Internal: metadata ports and their durable values
├── oxide-batch-cli/        Public operator CLI, released in lockstep
└── oxide-batch-test/       Public application-facing test kit
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

**M5 — Embedded Core Production Preview** is complete and released as
`oxide-batch` `0.5.0`. It stabilizes the delivered M0-M4 embedded scope rather
than adding batch capability, and its decision gates, workstreams, and exit
criteria are recorded in the [M5 kickoff gate](docs/project/m5-kickoff-gate.md)
and [M5 exit evidence](docs/project/m5-exit-evidence.md). M5 is the first
milestone that may promote advertised embedded-kernel ledger rows to
`Verified`, and `0.5.0`'s release promoted `28` of the `29` advertised rows.

The shipped `oxide-batch` command is a guarded repository operator, not a
standalone Rust job-definition loader. It can inspect and recover durable
partition metadata; launching or restarting application work requires a host
application that embeds `oxide-batch-cli` and supplies its own compiled
`DefinitionCatalog`. Run `oxide-batch --help` for that boundary and the command
grammar entry point.

**M6 — Complete Item Processing and User Test Kit** is implementation-complete
and published as `0.6.0` (tag `v0.6.0`, 2026-08-31). Its component campaigns,
test-kit boundary, and explicit non-parity limits are recorded in the
[M6 exit evidence](docs/project/m6-exit-evidence.md). Publication does not by
itself promote M6 ledger rows to released `Verified` and does not imply
M7-M14 work.

Release preparation, including the first-publication bootstrap boundary for
`oxide-batch-test`, is tracked in the [release checklist](docs/release/release-checklist.md).

Start with the [documentation index](docs/README.md) and
[continuous delivery roadmap](docs/roadmap.md). The M5-M14 full-parity program
is accepted; the static/erased component architecture is accepted
(RFC-0005/ADR-0008) and its implementation is M6 scope, while the distributed
worker protocol remains gated by RFC-0009.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.
Security vulnerabilities must be reported according to
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
