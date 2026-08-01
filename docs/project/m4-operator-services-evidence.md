# M4 Operator, Explorer, and Retention Service Evidence

**State:** Complete on merge

**Issue:** [#76](https://github.com/luceat-lux-vestra/oxide-batch/issues/76)

**Date:** 2026-08-01

This record maps the second M4 workstream's exit criteria to portable services,
the PostgreSQL schema-3 adapter, and executable evidence. It does not claim
that the operator CLI, graceful shutdown, stale detection, telemetry export, or
bounded local scale exists. Those remain owned by issues
[#77](https://github.com/luceat-lux-vestra/oxide-batch/issues/77),
[#78](https://github.com/luceat-lux-vestra/oxide-batch/issues/78),
[#79](https://github.com/luceat-lux-vestra/oxide-batch/issues/79), and
[#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80). No ledger row
becomes released `Verified` because of this workstream.

| Exit criterion | Evidence |
| --- | --- |
| Bounded list, query, and response paths | `PageSize` accepts only `1..=500`, `JobExplorer` requests one statement per page, truncates a page that would exceed the 256 KiB encoded bound, and rejects an unresolved-execution age below one minute. `keyset_traversal_returns_each_row_once` and `page_and_response_bounds_are_enforced` run on both backends. |
| Stable cursor behavior | The opaque token carries a format version, query discriminant, immutable ordering key, captured identity ceiling, an 8-byte query binding, and a checksum. `cursor_rejects_a_different_query_or_filter` proves a different filter or page size is `CursorQueryMismatch`; `corrupt_cursor_checksum_is_rejected` proves a damaged token is `CursorInvalid`; `rows_created_after_traversal_start_are_not_returned` proves the ceiling holds. |
| Redacted projections | Every projection returns names, opaque identifiers, ordinals, statuses, counters, versions, timestamps, digests, framework failure categories, parameter descriptors, and envelope descriptions only. `projection_excludes_parameter_and_context_values` asserts a sentinel parameter value appears in no projection or `Debug` rendering, and the existing `trybuild` fixtures still reject runtime, database, and serializer re-exports. |
| Duplicate idempotency keys cannot duplicate an effect | `ob_operator_request` is unique on `(action, operation_id)` and commits with its effect. `replayed_operation_id_returns_the_recorded_outcome` proves a replay returns the recorded row and creates no second execution; `operation_id_reuse_with_a_different_digest_is_rejected` proves a reused key with a different canonical request is `OperationIdConflict`. |
| Guards preserve lifecycle, definition, audit, and unknown-outcome rules | `abandon_requires_a_stopped_failed_or_recovered_execution`, `repeat_abandon_changes_nothing`, `abandoned_execution_rejects_restart`, `stop_on_a_stopping_or_terminal_execution_changes_nothing`, `stale_expected_version_loses_the_compare_and_swap`, `operator_request_and_effect_commit_together`, and `rejected_action_is_audited_without_an_effect` run on both backends. `ambiguous_operator_commit_reports_unknown_outcome` proves an ambiguous commit returns `OperationOutcomeUnknown` and records no completed request. |
| Guarded, audited retention | `held_instance_is_never_purged`, `running_stopping_or_unknown_execution_is_never_purged`, `stale_plan_digest_rejects_apply_without_deleting`, `purge_deletes_in_instance_owned_order_within_batch_bounds`, and `interrupted_purge_leaves_completed_batches_durable` run on both backends. |
| Separated PostgreSQL privileges | Not satisfied by this workstream. The [schema-3 migration guide](../operations/migrations/0003-operations-and-local-scale.md) specifies the runtime, operator-reader, and operator-writer grants, and the [setup guide](../operations/postgres-setup.md) records them for deployments. The design-gate fixture still runs every PostgreSQL suite as the runtime role, which cleans up its own rows with `DELETE`, so narrowing that role requires the schema-3 release fixture the M4 exit workstream owns. |
| Compatibility adapters preserve facade behavior | The M1 through M3 repository, chunk, fault, flow, and PostgreSQL suites pass unchanged. `RecoveryDecision` gains an opaque identity, and the canonical instance-key digest moves to `JobInstanceKey::digest` with byte-identical version-1 encoding, which its released golden vector still asserts. |
| Public APIs expose no implementation types | The services use facade types, `BoxFuture`, and the standard library. `ExplorerRepository` is the only new adapter port, and no SQLx, Tokio, credential, SQL, parameter, context, or deployment-authorization type appears in a signature, projection, or audit record. |

## Named scenarios satisfied by this workstream

`REPO-EXPLORE-001` gains `keyset_traversal_returns_each_row_once`,
`rows_created_after_traversal_start_are_not_returned`,
`cursor_rejects_a_different_query_or_filter`,
`corrupt_cursor_checksum_is_rejected`, `page_and_response_bounds_are_enforced`,
and `projection_excludes_parameter_and_context_values`.

`REPO-OPERATOR-001` gains `replayed_operation_id_returns_the_recorded_outcome`,
`operation_id_reuse_with_a_different_digest_is_rejected`,
`stale_expected_version_loses_the_compare_and_swap`,
`ambiguous_operator_commit_reports_unknown_outcome`,
`operator_request_and_effect_commit_together`, and
`rejected_action_is_audited_without_an_effect`.

`REPO-RETENTION-001` gains `held_instance_is_never_purged`,
`running_stopping_or_unknown_execution_is_never_purged`,
`stale_plan_digest_rejects_apply_without_deleting`,
`purge_deletes_in_instance_owned_order_within_batch_bounds`, and
`interrupted_purge_leaves_completed_batches_durable`. `runtime_role_cannot_purge`
is not satisfied: the shared contract exercises purge semantics through the
configured test identity, and proving that the runtime role cannot purge needs
the schema-3 release fixture rather than a Rust test.

`LIFE-ABANDON-001` gains
`abandon_requires_a_stopped_failed_or_recovered_execution`,
`repeat_abandon_changes_nothing`, `abandon_records_actor_reason_and_prior_state`
as part of the first case, and `abandoned_execution_rejects_restart`.

`LIFE-STOP-001` gains `stop_on_a_stopping_or_terminal_execution_changes_nothing`
only. The durable stop request is recorded by this workstream, but the owning
runtime does not yet observe it, so the remaining stop scenarios stay with
issue #78.

`META-UPGRADE-001` scenarios remain owned by the M4 exit workstream, because
this workstream adds the schema-3 migration but does not run the PostgreSQL
matrix upgrade and restore evidence, and does not narrow the schema-3 role
grants. The design-gate fixture now expects schema version 3 and rejects
version 4, which is the only fixture change this workstream makes.

## Documentation corrections

Two accepted statements could not both be implemented, so this workstream
corrects them rather than choosing silently:

- the `launch` guard listed a held instance as a rejection while the retention
  section states that a hold blocks purge only. The guard row now matches the
  retention rule, and a hold never blocks a lifecycle action;
- `ob_operator_request` required a job execution while every rejected action had
  to be audited. A launch rejected before its instance exists has neither
  reference, so both foreign keys are now optional and the operation identifier
  with its request digest remains the audit correlation.

The cursor encoding is also restated: the checksum covers the token body and a
separate 8-byte query binding covers identity, because a single checksum over
both cannot distinguish `CursorInvalid` from `CursorQueryMismatch`, which the
same contract requires.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

PostgreSQL service, migration, and least-privilege evidence runs in CI on
PostgreSQL 15 and 18 with `OXIDEBATCH_POSTGRES_TEST_URL`,
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and the design-gate fixture. It is not
reproducible on a workstation without a PostgreSQL service.

## Boundary handed to implementation

Issue #77 may build the CLI over `JobExplorer`, `JobOperator`, and
`RetentionService` without adding a query, mutation, or audit field. Issue #78
owns the runtime side of the durable stop request, ownership tokens, stale
evidence, and evidence-bound recovery; the schema-3 columns and the
`SHUTDOWN_INCOMPLETE` and `STALE_RECOVERED` failure categories exist but are
never written by this workstream. Issue #80 owns `ob_step_partition` writes,
manifest format 3, and aggregation; the table, its projection, and its bounded
query exist and return no row until then.
