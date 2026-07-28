# Technology Evaluation

**State:** Proposed

Selection is provisional until the required spike passes. Exact versions and
features are recorded in Cargo manifests and lockfile when introduced.

## Candidate version snapshot

Checked against crates.io metadata on 2026-07-29. These are the latest stable
releases evaluated today, not dependencies already approved or added. Refresh
the snapshot immediately before each dependency-admission pull request.

| Candidate | Stable version | Declared MSRV |
| --- | --- | --- |
| Tokio | 1.53.1 | 1.71 |
| SQLx | 0.9.0 | 1.94 |
| Serde | 1.0.229 | 1.56 |
| serde_json | 1.0.151 | 1.71 |
| uuid | 1.24.0 | 1.85 |
| time | 0.3.54 | 1.88 |
| thiserror | 2.0.19 | 1.71 |
| tracing | 0.1.44 | 1.65 |
| OpenTelemetry API | 0.32.0 | 1.75 |
| OpenTelemetry SDK | 0.32.1 | 1.75 |
| Clap | 4.6.4 | 1.85 |
| Testcontainers | 0.27.3 | 1.88 |
| Proptest | 1.11.0 | 1.85 |
| Criterion | 0.8.2 | 1.86 |
| secrecy | 0.10.3 | 1.60 |

All listed candidates fit the accepted Rust 1.95 MSRV. A listed version is not
permission to add the dependency: the owning ADR/issue, required features,
license, transitive graph, and public API impact still need review.

## Evaluation criteria

In descending importance:

1. correctness and transaction/cancellation semantics;
2. MSRV and platform support;
3. maintenance/security/license posture;
4. public API leakage and replacement cost;
5. failure diagnostics and testability;
6. ecosystem interoperability;
7. performance and compile-time cost;
8. contributor familiarity and ergonomics.

## Runtime

| Candidate | Strengths | Risks | Position |
| --- | --- | --- | --- |
| Tokio | Broad async ecosystem, scheduling/I/O/time primitives, SQLx support | Runtime coupling, blocking misuse | Recommended for initial runtime |
| async-std/smol | Alternative ecosystem and simpler pieces | Smaller integration target, larger support matrix | Not initial target |
| Synchronous core only | Simple object safety and embedding | Harder cancellation/concurrent I/O composition | Spike comparator |
| Runtime-neutral futures | Lower named dependency coupling | Still needs executor; trait ergonomics/lifetimes | Spike comparator |

Acceptance evidence: object-safe user components, scoped transactions,
cancellation, shutdown, panic isolation, blocking adapter, and no hidden global
runtime.

## PostgreSQL access

| Candidate | Strengths | Risks | Position |
| --- | --- | --- | --- |
| SQLx | Async, PostgreSQL, pools, migrations, typed/checked queries | Lifetimes/types can leak; feature/runtime coupling | Recommended |
| tokio-postgres | Focused driver, lower abstraction | More pool/migration/query infrastructure to own | Viable fallback |
| Diesel | Mature typed query/schema ecosystem | Async model and transaction ergonomics need separate evaluation | Not initial recommendation |
| ORM layer | Faster CRUD modeling | Metadata operations need explicit SQL/locking control | Rejected for metadata core |

Acceptance evidence: atomic writer/checkpoint transaction, duplicate launch
serialization, optimistic conflicts, migration behavior, cancellation during
queries, TLS, and pool shutdown.

## Serialization and durable context

| Candidate | Strengths | Risks | Position |
| --- | --- | --- | --- |
| Versioned JSON via Serde | Inspectable, broad tooling, evolvable objects | Size, number/type ambiguity, schema discipline | Recommended initially |
| CBOR/MessagePack | Compact, typed binary values | Inspection/tooling and canonicalization complexity | Defer |
| Postcard/bincode | Efficient Rust-centric representation | Type/layout evolution and cross-version risk | Reject for default durable format |
| User-owned opaque bytes | Maximum flexibility | Framework cannot validate/evolve/inspect | Possible explicit adapter later |

Acceptance evidence: size/depth limits, unknown/missing fields, version upgrade,
backward-read fixtures, corrupted data diagnostics, and sensitive-data rules.

## Domain utilities

| Concern | Candidate | Position |
| --- | --- | --- |
| Serialization | Serde | Recommended |
| JSON | `serde_json` | Recommended initial format |
| IDs | `uuid` | Recommended with framework-owned generation abstraction |
| Time | `time` | Recommended with injected clock and UTC persistence |
| Error derive | `thiserror` | Recommended internally and for owned public errors |
| Generic application reports | `anyhow`/`miette` | CLI/examples only, not core public errors |
| Secrets | `secrecy` or owned redacted wrapper | Evaluate before configuration implementation |

## Configuration and CLI

| Concern | Candidate | Position |
| --- | --- | --- |
| Typed CLI | Clap | Recommended for first-party operator binary |
| Library configuration | Owned typed builders | Required |
| File/env merging | Custom thin loader or established config crate | Defer until use cases require files |
| Human config format | TOML/YAML/JSON | No selection in M0 |

The framework does not require an application to adopt the first-party CLI or a
specific file format.

## Diagnostics and telemetry

| Candidate | Position |
| --- | --- |
| `tracing` events/spans | Recommended diagnostic facade |
| `tracing-subscriber` | Application/CLI assembly, not core |
| OpenTelemetry Rust adapter | Optional integration behind a separate boundary |
| Prometheus-specific exporter | Application/adapter responsibility initially |

OpenTelemetry Rust components are currently documented as beta, so
OxideBatch-owned event names and attributes—not SDK types—form the stability
contract.

## Testing and analysis

| Purpose | Candidate | Adoption gate |
| --- | --- | --- |
| Property testing | Proptest | M1 state/parameter logic |
| Compile-fail API tests | trybuild or rustdoc compile-fail | First public trait API |
| PostgreSQL containers | Testcontainers for Rust or CI service containers | Repository spike |
| Test runner | cargo-nextest | When CI partitioning/reporting helps |
| Feature matrix | cargo-hack | First optional feature |
| Coverage | cargo-llvm-cov | First substantive code |
| API SemVer | cargo-semver-checks | First public runtime release |
| Concurrency model | Loom | If custom synchronization is introduced |
| Fuzzing | cargo-fuzz/libFuzzer | First untrusted parser/serialized format |
| Benchmarks | Criterion plus end-to-end harness | M1/M2 |

## Supply chain and release

| Purpose | Candidate/standard | Position |
| --- | --- | --- |
| Dependency policy | cargo-deny and RustSec advisories | Required before M1 |
| Dependency PR review | GitHub dependency review | Active |
| Registry authentication | crates.io Trusted Publishing/OIDC | Active |
| SBOM | CycloneDX or SPDX | Select before first beta |
| Provenance | GitHub artifact attestations/SLSA-compatible evidence | Select before 1.0 RC |
| Release orchestration | GitHub Actions plus Rust `xtask` verification | Proposed |

## Rejection rule

A tool is not selected merely because it is popular. A failed spike records the
constraint and fallback. Replacing a tool exposed in public API, durable data,
or release evidence requires an ADR and migration plan.
