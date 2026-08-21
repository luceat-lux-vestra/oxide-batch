# oxide-batch-test

The application-facing test kit for [`oxide-batch`](https://crates.io/crates/oxide-batch).

`oxide-batch-test` is a dedicated public crate, not a module re-exported from
the `oxide-batch` facade: it has its own dependency and resource boundary
(deterministic clock/ID sources, failure/panic/cooperative-stop injection,
repository fixtures, a restart harness) independent of the production
runtime, per the M6 design gate's
[Gate G decision](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/project/m6-design-gate-evidence.md#gate-g--oxide-batch-test-boundary).

- `oxide-batch` never depends on this crate.
- The `oxide-batch` facade never re-exports it.
- It consumes only `oxide-batch`'s public contracts, never a private
  implementation type.
- It leaks no `SQLx`, Tokio runtime-handle, or other database-driver concrete
  type in its public API.
- Its MSRV and release cadence track `oxide-batch`; it makes no independent
  stability promise while both are pre-1.0.

See the crate documentation for the full harness catalog: deterministic
clock/ID sources, a scoped-component fixture, a single-step harness, a
full-job harness, failure/panic/cooperative-stop injection, a restart
harness, and repository fixtures (embedded and, behind the `postgres`
feature, `PostgreSQL`).
