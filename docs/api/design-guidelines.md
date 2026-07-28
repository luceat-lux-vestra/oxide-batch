# Rust API Design Guidelines

**State:** Proposed

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

## Compatibility review

Before releasing a public API:

- check naming and standard-trait conventions;
- test default, minimal, and all supported features;
- inspect rustdoc for dependency leakage;
- run SemVer compatibility analysis against the previous release;
- review object safety and downstream implementation ergonomics;
- confirm that status, error, and configuration changes match the behavioral
  contract.
