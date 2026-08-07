# Rust API Design Guidelines

**State:** Accepted

The [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) are the
default reference. OxideBatch adds the following framework-specific rules.

## Public boundary

- `oxide-batch` is the curated entry point; a workspace crate is not public
  merely because Cargo can build it.
- Public types belong to OxideBatch unless interoperability requires exposing a
  standard/ecosystem type.
- Tokio, SQLx, Serde, tracing, and OpenTelemetry types do not appear in core
  public signatures without an accepted ADR.
- Sealed traits are used only when downstream implementation would violate an
  invariant, not to avoid designing an extension point.
- Every extension trait documents thread-safety, cancellation, retry, ordering,
  reentrancy, and transaction expectations.

## Naming and types

- Use Spring Batch domain terms where semantics match; use a distinct name when
  they do not.
- Prefer validated newtypes for job names, step names, parameters, chunk sizes,
  limits, and identifiers.
- Prefer enums over boolean argument pairs and strings over ad hoc status codes.
- Implement standard traits consistently: `Debug`, `Display`, `Error`,
  conversions, equality, hashing, iteration, and borrowing where meaningful.
- `Debug` output for potentially sensitive types is redacted by default.
- Constructors establish invariants; invalid states should be unrepresentable
  where this does not make evolution brittle.

## Ownership and async behavior

- Public futures document cancellation safety and whether dropping them leaves
  durable work in progress.
- Components crossing task boundaries are `Send`; `Sync` is required only when
  concurrent shared access is part of the contract.
- Borrowed transaction/resource scopes must not require unsafe lifetime
  extension or hidden global state.
- Blocking and CPU-bound work use explicit adapters with bounded concurrency.
- The framework does not implicitly create or retain a process-global runtime.

## Builders and configuration

- Required values are constructor inputs or validated before execution.
- Builders return a structured validation error containing all safe-to-report
  problems when practical.
- Defaults are documented, conservative, and stable within a compatible release.
- Configuration precedence is deterministic and visible in diagnostics without
  exposing secret values.
- New optional configuration should not change unrelated existing behavior.

## Errors

Public failures have a stable category:

- invalid definition or configuration;
- duplicate/completed/not-restartable execution;
- illegal or conflicting lifecycle transition;
- transient repository/infrastructure failure;
- permanent repository/infrastructure failure;
- user component failure;
- cancellation/stop/deadline;
- serialization/version incompatibility;
- framework invariant violation.

Error strings are for people and are not stable APIs. Source errors are retained
where safe. Sensitive item/context/parameter values are never embedded in error
messages. Panics represent framework bugs or violated internal invariants and
are not normal control flow.

## Feature flags

- Features are additive and use positive capability names.
- Default features provide the recommended supported experience but remain
  small enough for a library facade.
- No feature silently selects credentials, a network endpoint, or a global
  runtime.
- Every documented feature combination is tested; unsupported combinations
  produce an intentional compile error when feasible.
- Removing or repurposing a public feature is a compatibility change.

## Documentation and examples

- Crate-level docs explain purpose, guarantees, limits, and a minimal complete
  example.
- Public items include examples when the example communicates meaningful use.
- Examples avoid `unwrap` in framework code; concise executable docs may use it
  only when the reason is obvious and lint-scoped.
- Safety, restart, transaction, and data-loss implications use explicit
  sections rather than being buried in prose.

## M5 preview surface and disclosure gate

The M5 Production Preview makes a bounded public-API claim. These rules are
closed for the preview and govern the facade review that precedes it.

**Curated surface.** `oxide-batch` is the only crate whose API the preview
claims. Extracted implementation crates stay `publish = false`, and their paths
are not compatibility promises even when Cargo can build them.

**Prohibited disclosure classes.** No public signature, public field, public
associated type, trait bound, error variant, `Debug` output, or rustdoc
example may expose:

- an async-runtime type, handle, or executor;
- a database driver, connection, pool, row, or SQL fragment type;
- a telemetry-SDK, exporter, or tracing-subscriber type;
- a credential, secret, token, certificate, or connection string;
- a deployment authorization or actor-identity implementation type;
- a sensitive payload: a parameter value, execution context, checkpoint
  payload, or item value;
- user-supplied error text.

Redaction failures in `Debug` or `Display` are treated as disclosure, not as
cosmetic defects.

**Pre-1.0 evolution.** The preview surface remains governed by pre-1.0 policy:
an incompatible change may occur in a minor release and must be called out in
the changelog. The preview creates no project-wide stability promise and does
not shorten the M14 gate.

**M6-M12 non-blocking requirement.** The facade review MUST record, per target
boundary, why the delivered surface admits the later milestone without a
breaking change or with an explicitly accepted one. The boundaries reviewed are
the M6 item and test-kit surface, the M7 flow, registry, and scope surface, the
M8 repository-portability surface, the M9 integration surface, the M10 and M11
concurrency and distributed surfaces, and the M12 migration surface. A boundary
the current surface would block is a finding, not a note.

**Review evidence.** The review runs the compatibility checklist below, plus a
rustdoc leakage inspection over the complete public surface, the public API
snapshot, and compile-fail tests for each prohibited disclosure class that can
be expressed as a type error. The leakage inspection is `cargo xtask surface`,
and the review it produced is recorded in the
[M5 facade and API review evidence](../project/m5-facade-api-review-evidence.md).

## Compatibility review

Before releasing a public API:

- check naming and standard-trait conventions;
- test default, minimal, and all supported features;
- inspect rustdoc for dependency leakage;
- run SemVer compatibility analysis against the previous release;
- review object safety and downstream implementation ergonomics;
- confirm that status, error, and configuration changes match the behavioral
  contract.
