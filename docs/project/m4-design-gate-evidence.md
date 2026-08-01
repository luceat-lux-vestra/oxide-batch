# M4 Operations and Local-Scale Design-Gate Evidence

**State:** Complete on merge

**Issue:** [#75](https://github.com/luceat-lux-vestra/oxide-batch/issues/75)

**Date:** 2026-08-01

This record closes the decisions required before M4 production implementation.
It authorizes the dependency order below; it does not claim that the operator,
explorer, retention, CLI, shutdown, telemetry, or local-scale capabilities are
implemented. No row moves toward released `Verified` status because of this
document.

## Closed decision gates

| Gate | Canonical evidence |
| --- | --- |
| Operator and explorer services | The [operator, explorer, and retention contract](../architecture/operator-and-explorer-services.md) fixes the closed query set, keyset-only pagination, cursor encoding and rejection, traversal consistency, redacted projections, query bounds, the request envelope, operation-ID idempotency, action guards, optimistic conflicts, unknown outcomes, audit records, and the authorization-class boundary. |
| CLI and configuration | The [operator CLI and configuration contract](../operations/operator-cli.md) fixes the closed command grammar, global options, per-value configuration precedence, strict validation, secret handling, the versioned JSON output schema, output bounds and truncation, the stable exit categories, confirmation and non-interactive safeguards, dry-run scope, and broken-output behavior. |
| Shutdown and stale recovery | The [shutdown and stale-recovery contract](../architecture/shutdown-and-recovery.md) fixes shutdown sources, the seven-phase ordering, the in-flight chunk policy, every deadline and its missed-deadline behavior, durable terminal outcomes, owner-token and server-time stale evidence, clock-skew rules, recovery proposal and application, and the process-signal/kill matrix. |
| Telemetry and diagnostics | The [observability contract](../operations/observability-contract.md) adds telemetry schema version `1`, the M4 event catalog with commit-relative timing and safe fields, the enforced label-cardinality budget, bounded exporter queues with a drop-newest policy, failure isolation, the separate flush deadline, and the bounded redacted diagnostic bundle. |
| Initial retention slice | The operator, explorer, and retention contract plus [persistence and migrations](../operations/persistence-and-migrations.md) fix holds, eligibility, the two-phase plan/apply digest guard, deletion order, batch bounds, interruption and replay behavior, privilege separation, and the boundary before M8 archive/purge portability. |
| Bounded local scale | The [bounded local-scale contract](../architecture/local-scale.md) fixes the split and partitioned-step subset, assignment identity, single-invocation partitioning, durable restart state, deterministic aggregation, structured ownership and cancellation, thread-safety validation, finite budgets, sequential-fallback equivalence, and manifest format 3. |
| Evidence and support bounds | This record's scenario table, the [schema-3 migration](../operations/migrations/0003-operations-and-local-scale.md) verification list, and the M4 additions to the [performance plan](../engineering/performance-plan.md) name the required conformance, crash/restart, destructive-action, security, cardinality, cancellation, load, soak, and PostgreSQL matrix evidence. |

## Impact classification

| Area | M4 decision |
| --- | --- |
| Observable compatibility | Spring-like launch, stop, abandon, recover, inspection, parallel steps, and local partitioning. Keyset-only pagination, mandatory operation IDs, closed CLI grammar, own exit categories, and evidence-bound recovery are recorded divergences. |
| Public API | Adds bounded explorer queries and cursors, an operator request envelope with authorization classes, retention hold and plan/apply primitives, shutdown and stale/recovery types, split and partitioned-step plan nodes, and telemetry catalog types. All use the current `BoxFuture` component boundary. |
| Restart and transactions | Operator request and effect commit together; the partition plan commits before any worker starts; partition results are per-row compare-and-swap; aggregation commits with the parent terminal update; each purge batch is one transaction. An ambiguous commit remains `UNKNOWN` in every path. |
| Durable data | PostgreSQL schema 3 adds execution ownership and stop columns, one instance hold, `ob_operator_request`, `ob_retention_action`, and `ob_step_partition`. Manifest format 3 adds split and partitioned-step nodes; formats 1 and 2 remain byte-identical. |
| Security | Requests carry a deployment-supplied opaque actor reference and closed-set reason codes, never credentials or free text. Read, lifecycle, and destructive classes are separately authorizable. Projections, telemetry, CLI output, and bundles exclude parameters, contexts, checkpoints, items, SQL, endpoints, credentials, and user error text. |
| Telemetry | The catalog is versioned, commit-relative, and observational only. Labels obey an enforced per-family budget, names require an allowlist, and exporter queues are bounded with counted drops. No telemetry value is ever an authority. |
| Migration | Schema 2 to 3 is quiesced, transactional, and backfill-free; schema-2 runtimes reject schema 3; rollback is verified backup restore; purge has no reverse operation. |
| Resources | Branches `2..=8`, branch length `1..=8`, partitions `1..=1024`, partition context `4 KiB`, workers `1..=64`, page size `1..=500`, response `256 KiB`, cursor `256 bytes`, exporter queue `64..=65536`, bundle `4 MiB`, purge batch `1000`, and validated pool capacity. |

No decision uses RFC-0005's proposed native/erased hot path or RFC-0009's
proposed worker protocol. M4 retains the accepted ADR-0002 boxed component
boundary, and the recorded `owner_token` is ownership evidence rather than a
lease.

## Named implementation scenarios

| Ledger row | Scenario IDs required by dependent issues |
| --- | --- |
| `REPO-EXPLORE-001` | `keyset_traversal_returns_each_row_once`, `rows_created_after_traversal_start_are_not_returned`, `cursor_rejects_a_different_query_or_filter`, `corrupt_cursor_checksum_is_rejected`, `page_and_response_bounds_are_enforced`, `projection_excludes_parameter_and_context_values` |
| `REPO-OPERATOR-001` | `replayed_operation_id_returns_the_recorded_outcome`, `operation_id_reuse_with_a_different_digest_is_rejected`, `stale_expected_version_loses_the_compare_and_swap`, `ambiguous_operator_commit_reports_unknown_outcome`, `operator_request_and_effect_commit_together`, `rejected_action_is_audited_without_an_effect` |
| `LIFE-STOP-001` | `durable_stop_request_is_observed_at_the_next_chunk_boundary`, `stop_on_a_stopping_or_terminal_execution_changes_nothing`, `shutdown_stops_intake_before_cancelling_children`, `finish_chunk_policy_commits_the_open_chunk_then_stops`, `rollback_chunk_policy_preserves_the_previous_checkpoint`, `missed_join_deadline_reports_drain_incomplete_without_a_terminal_status` |
| `LIFE-ABANDON-001` | `abandon_requires_a_stopped_failed_or_recovered_execution`, `repeat_abandon_changes_nothing`, `abandon_records_actor_reason_and_prior_state`, `abandoned_execution_rejects_restart` |
| `LIFE-RECOVER-001` | `stale_candidate_requires_server_time_evidence`, `owner_token_mismatch_is_required_for_a_proposal`, `unusable_clock_evidence_produces_no_proposal`, `stale_detection_never_rewrites_status`, `recovery_digest_or_version_mismatch_is_rejected`, `unknown_commit_recovers_only_with_the_unknown_effect_reason` |
| `REPO-RETENTION-001` | `held_instance_is_never_purged`, `running_stopping_or_unknown_execution_is_never_purged`, `stale_plan_digest_rejects_apply_without_deleting`, `purge_deletes_in_instance_owned_order_within_batch_bounds`, `interrupted_purge_leaves_completed_batches_durable`, `runtime_role_cannot_purge` |
| `OPS-CLI-001` | `precedence_resolves_per_value`, `unknown_option_or_configuration_key_fails`, `every_exit_category_is_returned_by_its_named_case`, `destructive_command_without_yes_exits_confirmation_required`, `dry_run_makes_no_durable_change`, `broken_stdout_stops_output_and_repeats_no_mutation`, `json_output_matches_the_published_schema_and_redaction_rules` |
| `OBS-EXEC-001` | `m4_events_match_the_published_catalog_and_schema_version`, `operator_and_recovery_events_follow_their_durable_commit`, `diagnostic_bundle_excludes_every_prohibited_value_class`, `diagnostic_bundle_respects_its_size_bound_and_records_omissions` |
| `OBS-METRICS-001` | `metric_labels_stay_within_the_family_cardinality_budget`, `unallowlisted_names_map_to_other`, `full_exporter_queue_drops_newest_and_counts`, `exporter_failure_cannot_change_execution_state`, `telemetry_flush_deadline_is_separate_from_shutdown` |
| `SCALE-PARSTEP-001` | `split_outside_the_accepted_subset_is_rejected`, `parent_joins_every_branch_before_aggregating`, `branch_aggregation_is_deterministic_in_declared_order`, `unknown_branch_makes_the_parent_unknown`, `single_branch_budget_matches_the_sequential_observations` |
| `SCALE-LOCALPART-001` | `partition_plan_commits_before_any_worker_starts`, `partitioner_is_not_reinvoked_on_restart`, `completed_partition_is_not_rerun`, `duplicate_partition_key_is_rejected`, `aggregation_is_deterministic_in_partition_key_order`, `stale_partition_writer_loses_the_compare_and_swap`, `insufficient_pool_capacity_fails_launch` |
| `META-UPGRADE-001` | `schema2_upgrades_to_schema3`, `schema3_migration_performs_no_backfill`, `schema2_runtime_rejects_schema3`, `schema3_backup_restores_schema2` |
| `LIFE-DEFINITION-001` | `format2_manifest_is_unchanged_by_schema3`, `format3_manifest_has_a_golden_fingerprint`, `format2_to_format3_requires_a_direct_upgrade`, `budget_change_that_alters_assignment_identity_changes_the_fingerprint` |

Required evidence classes are unit/property, PostgreSQL integration, named
conformance, crash/failure injection, destructive-action, security and
least-privilege, cardinality, cancellation, migration/restore, and
bounded-resource tests as indicated by each ledger profile. Documentation names
are acceptance targets, not evidence links until the tests exist and pass.

## Dependency handoff

- Issue [#76](https://github.com/luceat-lux-vestra/oxide-batch/issues/76) may
  implement the bounded explorer, operator, and retention services, their
  PostgreSQL contracts, and schema 3.
- Issue [#79](https://github.com/luceat-lux-vestra/oxide-batch/issues/79) may
  implement the telemetry catalog, cardinality budget, exporter bounds, and
  diagnostic bundle independently of #76.
- Issue [#77](https://github.com/luceat-lux-vestra/oxide-batch/issues/77)
  follows #76 and implements the CLI and configuration diagnostics over the
  portable services.
- Issue [#78](https://github.com/luceat-lux-vestra/oxide-batch/issues/78)
  follows #76 and implements shutdown, stale detection, and recovery using the
  accepted lifecycle and service contracts.
- Issue [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)
  follows #78 and implements the bounded local-scale subset, manifest format 3,
  and sequential-fallback equivalence.
- Issue [#81](https://github.com/luceat-lux-vestra/oxide-batch/issues/81)
  follows all implementation work and owns the M4 exit evidence.

Any implementation need that changes these observable rules, manifest identity,
schema meaning, transaction boundary, authorization boundary, or resource bound
requires a documentation correction and, when it changes an accepted contract,
a superseding RFC or ADR.
