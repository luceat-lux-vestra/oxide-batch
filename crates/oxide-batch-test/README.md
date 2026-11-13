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
- Workspace facade integration tests compile the same certification source through an internal test-only path; external adapters consume the public `oxide_batch_test::certification` API.
- It consumes only `oxide-batch`'s public contracts, never a private
  implementation type.
- It leaks no `SQLx`, Tokio runtime-handle, or other database-driver concrete
  type in its public API.
- Its MSRV and release cadence track `oxide-batch`; it makes no independent
  stability promise while both are pre-1.0.

See the crate documentation for the full harness catalog: reusable adapter
certification contracts, deterministic clock/ID sources, a scoped-component
fixture, a single-step harness, a full-job harness,
failure/panic/cooperative-stop injection, a restart harness, and repository
fixtures (embedded and, behind the `postgres` feature, `PostgreSQL`).

The `certification` module is a contract test kit, not a support badge. Its
runners are async and executor-neutral: the consuming adapter test chooses and
owns its executor. The portable core repository contract is separate from optional definition,
partition, explorer, operator/control, and retention contract groups. The
`run_full_*` aggregates exist for reference adapters that claim all represented
capabilities; they are not the minimum portable contract.

An adapter that passes every applicable contract group still needs the versioned
migration, crash/unknown-outcome, resource-bound, security, and capability-specific
evidence required by the OxideBatch support matrix before it can be advertised
as supported.
