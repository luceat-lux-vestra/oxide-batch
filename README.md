# OxideBatch

[![CI](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/luceat-lux-vestra/oxide-batch/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

OxideBatch is an enterprise-ready batch processing framework for Rust, inspired
by Spring Batch and designed around idiomatic Rust.

> [!IMPORTANT]
> OxideBatch has completed its M1 executable kernel. M2 development is adding
> durable chunk processing, restart, and PostgreSQL metadata; no
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

**M0 — Foundation** and **M1 — Executable Kernel** are complete.
**M2 — Durable Chunk and Restart** is active: chunk processing, PostgreSQL
metadata, transaction/checkpoint boundaries, and restart after process failure
are being delivered through the
[M2 kickoff gate](docs/project/m2-kickoff-gate.md).

Start with the [documentation index](docs/README.md) and
[M0–M5 delivery roadmap](docs/roadmap.md). Accepted decisions and deferred
later-milestone gates are recorded before implementation depends on them.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening an issue or pull request.
Security vulnerabilities must be reported according to
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
