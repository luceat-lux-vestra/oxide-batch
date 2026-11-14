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
owns its executor.

Repository certification has two additive portable levels while M8 adapters
migrate:

- `run_core_repository_contract` is the primitive/bootstrap subset for
  identity, execution graph, lifecycle CAS, completion guards, and rollback;
- `run_runnable_core_repository_contract` composes that subset and proves an
  ordinary one-step tasklet `FlowLauncher`, exact definition-bound strict
  restart, logical flow-step/start-limit authority, and durable decision reuse
  while optional capability declarations are masked to zero.

Optional definition-registry, partition, explorer, operator/control, and
retention contract groups remain separate. The `run_full_*` aggregates exist
for reference adapters that claim all represented capabilities; they are not
the minimum portable contract.

A runnable-core pass still does not certify checkpoint/context payload restart
or operational support. An adapter that passes every applicable contract group
also needs the versioned migration, crash/unknown-outcome, resource-bound,
security, backup/restore, stateful restart, and capability-specific evidence
required by the OxideBatch support matrix before it can be advertised as
supported.
