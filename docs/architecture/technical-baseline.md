# Technical Baseline

**State:** Proposed

**Decision needed:** approve after the named architecture spikes

Exact dependency versions are selected and locked when a crate is introduced.
The project starts from the latest stable release compatible with the accepted
MSRV, then records a concrete Cargo SemVer requirement and lockfile resolution.
Floating `latest`, wildcard, and unbounded requirements are not used.

| Concern | Proposed choice | Constraint or rationale |
| --- | --- | --- |
| Language | Rust 2024; stable 1.97.1 development/release; MSRV 1.95 | Accepted; stable-only required CI |
| Async runtime | Tokio 1.x | Mature I/O, scheduling, cancellation primitives; isolate from core |
| Database | PostgreSQL first | Strong transactional and locking behavior; only 1.0 repository |
| SQL access | SQLx with Tokio and Rustls features | Async PostgreSQL, compile-time/query tooling, migrations |
| Serialization | Serde; versioned JSON initially | Human-inspectable execution context; evolution rules required |
| IDs | UUID with framework-defined generation policy | No database sequence dependency in domain contracts |
| Time | `time` crate plus injected clock | Deterministic tests; persist UTC instants |
| Errors | typed domain errors; `thiserror` internally | Stable classification without string matching |
| Diagnostics | `tracing` facade | Structured events and span context |
| Telemetry export | optional OpenTelemetry adapter | Vendor-neutral; no SDK dependency in core |
| CLI | Clap | Typed operator command surface |
| Integration tests | Testcontainers plus real PostgreSQL | Transaction/locking semantics cannot be mocked |
| Property tests | Proptest where state-space matters | State transitions and retry/skip boundaries |
| Benchmarks | Criterion or purpose-built harness | Record workload and environment with results |
| Supply chain | `cargo-deny`, RustSec audit, pinned CI actions | License, advisory, source, and workflow controls |

## Deliberate deferrals

- a web server or management API framework;
- a generic plugin ABI or dynamic loading;
- Kafka/message-broker selection;
- a configuration framework beyond the CLI and application-owned assembly;
- database abstraction across PostgreSQL, MySQL, and SQLite;
- distributed consensus or leader election.

## Dependency acceptance

A production dependency needs:

- a clear owner and use site;
- license compatibility with Apache-2.0 distribution;
- acceptable maintenance and security posture;
- controlled features and minimal transitive surface;
- MSRV compatibility;
- a replacement/removal plan for dependencies exposed in public types.

OpenTelemetry's Rust traces, metrics, and logs are currently documented as beta,
so telemetry contracts must be owned by OxideBatch and adapters must remain
optional:

- [OpenTelemetry Rust status](https://opentelemetry.io/docs/languages/rust/)
- [Tokio runtime](https://docs.rs/tokio/latest/tokio/runtime/)
- [SQLx](https://docs.rs/sqlx/latest/sqlx/)
