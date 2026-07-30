# OxideBatch

[![CI](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

OxideBatch is an early-stage Rust-native framework for reliable, restartable
batch processing, inspired by Spring Batch.

> [!IMPORTANT]
> OxideBatch has completed the M1 executable kernel and M2 durable PostgreSQL
> chunk/restart implementation gate. M3 fault tolerance and flow is active at
> its kickoff/design gate; no M3 runtime capability is claimed yet.
> Compatibility rows remain unreleased `Implemented` evidence, and no
> production-ready runtime has been released.

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

**M0 — Foundation**, **M1 — Executable Kernel**, and
**M2 — Durable Chunk and Restart** are complete. M2 includes the PostgreSQL
schema and repository, atomic enlisted chunks, definition-guarded restart,
audited recovery, and separate-process pre/post-commit crash evidence recorded
in the [M2 exit record](docs/project/m2-exit-evidence.md). **M3 — Fault
Tolerance and Flow** is active under its
[kickoff gate](docs/project/m3-kickoff-gate.md); implementation remains behind
the named policy, persistence, listener, compiled-plan, and flow decisions.

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
