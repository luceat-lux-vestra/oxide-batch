# Changelog

All notable changes to OxideBatch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Facade-owned M1 domain types for job and step names, opaque execution
  identifiers, typed identifying parameters, canonical job-instance keys,
  lifecycle metadata, counters, exit statuses, and redacted failure summaries.
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
