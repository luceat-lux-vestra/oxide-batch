# Agent Instructions

## Mission and non-negotiable direction

OxideBatch is an idiomatic Rust framework for reliable, restartable, observable
batch workloads. Its long-term target is complete, evidence-backed coverage of
the pinned Spring Batch 6.x feature population and observable execution
semantics, delivered through a Rust-native API and engine.

The project must become production-ready on the strength of executable
evidence, operational clarity, and supportable contracts. It should ultimately
surpass Spring Batch where Rust and a new architecture permit stronger
distributed execution, verifiable state, reproducibility, resource control,
data efficiency, and operational transparency.

This direction does not mean copying Java APIs, Spring dependency injection,
exception hierarchies, or the Spring metadata schema. Prefer Rust's type
system, ownership, explicit capabilities, deterministic construction, typed
errors, and structured concurrency. Treat “production-ready,” “compatible,”
“high performance,” and “beyond Spring” as evidence claims, never as design
adjectives.

## Authority and required reading

For any non-trivial work, start with `docs/README.md` and follow its reading
path for the affected subsystem. Before changing architecture, compatibility,
roadmap, public APIs, repository semantics, durable metadata, or distributed
behavior, read in order:

1. `docs/README.md`;
2. `docs/project/post-m5-full-parity-strategy.md`;
3. `docs/product/vision-and-scope.md`;
4. `docs/roadmap.md`;
5. `docs/compatibility/spring-batch.md`;
6. `docs/compatibility/conformance-matrix.md`;
7. `docs/architecture/overview.md`;
8. the focused canonical document for the subsystem;
9. relevant accepted RFCs/ADRs and milestone gates.

Chat, issue, pull-request, example, or session instructions never override
accepted repository documents. Follow the precedence in
`docs/documentation/strategy.md`. When authoritative documents conflict, stop
dependent implementation and produce an RFC/ADR or documentation correction.
Never infer that a proposed document, merged experiment, roadmap entry, or
passing test is accepted.

The post-M5 strategy is an umbrella rationale and map, not the sole normative
owner of detailed behavior. RFC-0005 is accepted and recorded as ADR-0008;
follow its accepted item-component boundary and the M6 migration handoff that
implements it. RFC-0009 remains proposed and is not implementation authority
until accepted.

## Change gates

Before implementation, classify the change's impact on:

- observable behavior and Spring Batch compatibility;
- public API and feature combinations;
- lifecycle, restart, checkpoint, transaction, and delivery semantics;
- durable schemas, codecs, migrations, retention, and recovery;
- dependency direction, runtime boundaries, and distributed protocols;
- security, sensitive data, operator actions, and telemetry;
- resource bounds, performance, support matrices, and release claims.

Confirm that the work item meets the definition of ready in
`docs/project/development-process.md`. A change to an accepted contract requires
its RFC/ADR or superseding decision before dependent implementation. When the
decision is missing, restrict work to analysis, an explicit spike, or the
required decision/documentation correction.

Do not create packages, abstractions, feature flags, compatibility shims, or
extension points only to reserve a future design. Add them when a proven
dependency boundary, user-visible capability, or support obligation requires
them.

## Rust and architecture bar

Engineering choices follow the canonical design philosophy in
[`docs/engineering/standards.md`](docs/engineering/standards.md#design-philosophy).

- Preserve correctness, restart, transaction, delivery, and ordering semantics
  before optimizing ergonomics or throughput.
- Keep core domain, plan, item, and repository contracts independent of Tokio,
  SQLx, database clients, broker clients, CLI/web frameworks, and telemetry
  SDK types. Adapters depend inward.
- Use validated types and constructors so invalid states are unrepresentable
  where evolution remains practical. Prefer enums and typed categories over
  booleans, strings, reflection-like lookup, or service locators.
- Make ownership, cancellation safety, thread safety, retry, ordering,
  reentrancy, transaction participation, and delivery guarantees explicit at
  public and extension boundaries.
- Keep blocking work, queues, pages, retries, caches, buffers, connections,
  tasks, and in-flight work bounded. Do not create detached tasks or hidden
  process-global runtimes.
- Use static, zero-cost Rust mechanisms on hot paths when the governing design
  is accepted and measurements justify them. Limit erasure and allocation to
  deliberate composition boundaries; do not preempt a proposed RFC.
- Treat expected failure as typed `Result` data. Do not classify policy from
  error strings or use panic as ordinary control flow.
- `unsafe`, unchecked assumptions, `unwrap`, `expect`, and `panic!` in
  production code require the exceptions and evidence defined by accepted
  engineering policy.
- New dependencies, macros, and code generation must justify maintenance,
  MSRV, compile-time, supply-chain, diagnostic, and feature-graph cost.
- Public APIs must be idiomatic Rust: follow the Rust API Guidelines, implement
  standard traits consistently, provide useful rustdoc, and include runnable
  examples for meaningful use.

## Compatibility and beyond-parity discipline

The compatibility ledger is the only denominator for parity. For every affected
Spring capability:

- identify or add the ledger row before claiming coverage;
- preserve the pinned reference behavior as normalized observable scenarios;
- classify the result as an exact equivalent, reviewed Rust-native equivalent,
  documented divergence, unsupported disposition, or not-applicable rationale;
- link executable conformance and failure evidence;
- keep `Implemented` distinct from released, fully evidenced `Verified`.

Never silently omit a Spring feature because its Java mechanism is unsuitable
for Rust. Map the user/operator need to an idiomatic native equivalent or record
the reviewed difference. Never describe OxideBatch as Java source, binary,
configuration, Bean-container, or shared-schema compatible.

Beyond-parity capabilities must not weaken canonical behavior. Each
differentiator needs an approved boundary plus benchmark, failure, replay,
resource-limit, or operational evidence as applicable. An optimization must
retain a canonical deterministic fallback and equivalent observable semantics.

Database, file, object-store, HTTP, and messaging adapters must expose
resource-native capabilities and limitations. Do not hide distinct transaction,
acknowledgement, redelivery, ordering, rebalance, or consistency models behind
a fictitious universal abstraction. Never make blanket exactly-once claims
across arbitrary resources.

## Production-readiness bar

Code existence is not readiness. Apply the relevant milestone gate and require,
as applicable:

- deterministic unit, property/state-machine, compile-fail, contract,
  integration, conformance, and failure-injection tests;
- crash/restart evidence at lifecycle and commit boundaries;
- versioned schema and codec migration, upgrade/rollback, corruption, backup,
  restore, retention, and reconciliation behavior;
- bounded-resource, cancellation, load, soak, leak, and recovery evidence;
- supported-version matrices for Rust, platforms, databases, brokers,
  transports, schemas, and protocols;
- redaction, least-privilege, destructive-operation, dependency, provenance,
  and supply-chain review;
- stable operator behavior, diagnostics, metrics, traces, runbooks, limitations,
  and rollback procedures;
- SemVer and feature-combination review for public releases.

Tests use injected clocks, deterministic IDs/backoff, seeded generators, and
bounded eventual assertions. A retried or quarantined flaky test remains a
failure signal. Coverage percentage alone never substitutes for named
lifecycle, transaction, restart, recovery, and compatibility scenarios.

Performance work follows `docs/engineering/performance-plan.md`. Measure like
for like, record the environment and correctness result, retain reproducible
raw evidence, and never trade away semantics or bounded resource behavior for a
faster number.

## Work and review discipline

- Preserve unrelated user changes and keep each change focused.
- Prefer one writer for a code area. When subagents are available, use them for
  bounded, independent, read-heavy exploration or specialized review; avoid
  parallel write-heavy work on overlapping files.
- Review in this order: correctness/data/restart, security/destructive behavior,
  compatibility/public API, operations/observability, tests/failure evidence,
  then maintainability/style.
- Record decisions, limits, migrations, failure semantics, and residual risks
  in canonical repository documents. No implementation-critical fact may live
  only in chat, an issue, or a pull-request description.
- Implementation changes update affected feature-ledger rows, evidence links,
  public documentation/examples, migration or runbook material, changelog, and
  milestone evidence when applicable.

## Validation and definition of done

Run the narrowest relevant checks while iterating, then the full affected gate.
The ordinary local baseline is:

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Run affected PostgreSQL, migration, feature-matrix, conformance, crash,
security, performance, or soak checks when the change touches those contracts.
Do not claim a check passed unless it was run successfully; report skipped
checks and the reason.

A task is done only when implementation, tests, documentation, ledger/evidence,
and operational consequences agree; required CI is green; public limitations
are explicit; and the repository can explain the change without access to the
originating conversation.
