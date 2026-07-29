# Changelog

All notable changes to OxideBatch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Facade-owned M1 domain types for job and step names, opaque execution
  identifiers, typed identifying parameters, canonical job-instance keys,
  lifecycle metadata, counters, exit statuses, and redacted failure summaries.
- Deterministic M1 integration-test support for clocks, IDs, randomness,
  backoff, bounded waits, event diagnostics, fixture provenance, redaction
  sentinels, conformance IDs, and reusable repository contracts.
- Deterministic job and step lifecycle transitions with typed optimistic-version
  conflicts, terminal-state enforcement, separate exit-status enrichment, and
  fresh execution attempts for restart.
- Async-first single-step tasklet execution with persisted lifecycle outcomes,
  cooperative stopping, panic classification, and an explicitly bounded
  blocking adapter.
- Deterministic job and step listeners, commit-aligned lifecycle events,
  execution-attempt correlation, and value-redacted log, span, metric-label,
  and listener-failure diagnostics.
- A runnable first in-memory job, facade-boundary compile-fail tests, and M1
  executable-kernel conformance and exit evidence.
- An active M2 kickoff gate with dependency-ordered workstreams, PostgreSQL
  15–18 verification targets, and durable chunk/restart exit criteria.
- M0 implementation-readiness plan, M0–M5 roadmap, decision records, and
  product, compatibility, architecture, engineering, security, operations, and
  release policy set.
- Dedicated MSRV and supply-chain CI checks.
- Repository `cargo xtask` commands for development checks and package
  verification.

## [0.1.0-alpha.1] - 2026-07-29

### Added

- Initial project governance, repository policy, and CI foundation.
- Public `oxide-batch` facade crate metadata and pre-alpha documentation.

[Unreleased]: https://github.com/luceat-lux-vestra/oxide-batch/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/luceat-lux-vestra/oxide-batch/releases/tag/v0.1.0-alpha.1
