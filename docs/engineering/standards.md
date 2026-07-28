# Engineering Standards

**State:** Accepted

## Rust and API design

- Rust 2024 edition and the workspace MSRV are mandatory.
- `unsafe` is forbidden unless a dedicated ADR defines the invariant, audit,
  and tests.
- Production code does not use `unwrap`, `expect`, or `panic!` without a
  reviewed exception.
- Public types and errors use domain language consistently with the
  compatibility glossary.
- Public APIs avoid exposing optional implementation dependencies.
- All public items have rustdoc and runnable examples where practical.
- Time, IDs, randomness, retries, and cancellation are injectable or
  deterministic in tests.

## Formatting and linting

`rustfmt` is authoritative. Clippy runs for all targets and features with
warnings denied. Repository lint configuration is inherited by every workspace
crate. Exceptions are narrow, documented at the use site, and reviewed.

## Change design

An issue is required before implementation. Use an RFC or ADR for:

- public API or compatibility changes;
- status, restart, checkpoint, or transaction semantics;
- metadata schema changes;
- new production dependencies or cross-crate dependency direction;
- security boundaries and operator behavior.

## Review requirements

Every behavior change includes:

- tests at the lowest useful layer;
- failure-path and restart implications;
- compatibility-matrix impact;
- telemetry and sensitive-data impact;
- migration and release-note impact when applicable.

## Definition of done

- acceptance criteria and tests pass;
- formatting, lint, documentation, dependency, and license gates pass;
- public behavior and limitations are documented;
- no unresolved P0/P1 issue is introduced;
- ADR/RFC and compatibility links are updated;
- generated artifacts are reproducible and secrets are absent.
