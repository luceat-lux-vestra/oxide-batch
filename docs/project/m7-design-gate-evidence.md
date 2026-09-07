# M7 Advanced Flow, Scope, Repeat, and Composition Design-Gate Evidence

**State:** Complete on merge

**Issue:** [#194](https://github.com/luceat-lux-vestra/oxide-batch/issues/194)

**Date:** 2026-09-07

**Authorization baseline:** `main`
`255b113044e08fd5a9390ef3d9d804ff720f6ba7`

This closes the six design gates frozen by the
[M7 kickoff gate](m7-kickoff-gate.md). Binding sources are
[ADR-0013](../architecture/decisions/0013-m7-advanced-flow-scope-repeat-and-evolution.md)
and the
[M7 architecture contract](../architecture/m7-advanced-flow-scope-repeat-and-evolution.md).

This is design authorization only: no product behavior, ledger promotion, or
repository migration is implemented here. #195-#199 remain blocked until this
change passes exact-head review, merges, passes post-main verification, and #194
is semantically accepted/closed.

## Closed gates and owners

| Gate | Frozen decision | Owner |
| --- | --- | --- |
| A — advanced flow/composition | finite acyclic nested/split/nested-job graph; owned split join; deterministic nested-job parameter binding and terminal propagation; registered custom leaf under one engine; stable identity; deterministic aggregation; global bounds plus depth 8 | #195 |
| B — scope/late binding | job/step execution-attempt scope; deterministic factory reuse/reverse cleanup; structured typed source selector only; no ambient env/time/random/credential/I/O or semantic free-form evaluator; bounded source provenance with no derived resolved-value digest | #196 |
| C — repeat/interceptors | explicit repeat rather than graph cycles; durable ordinal/continue decision; M6 retry/skip/rollback remains inner engine; deterministic interceptor pairing and primary/secondary failure authority; bounded state/depth | #197 |
| D — registry/evolution | immutable CAS/idempotent registry; exact default; one direct compatible edge with total injective source mapping and atomic deterministic transform; fail-closed drift/newer/corrupt/missing-artifact behavior; fork is new lineage | #198 |
| E — application/operator | deterministic pure incrementer over an explicitly supplied prior parameter set; non-restartable/start-control semantics; explicit strict/compatible/fork actions; M4 CAS/idempotency/audit/redaction inherited; closed explorer query families | #199 |
| F — cross-cutting | manifest format 4 retaining the 64 KiB ceiling; PostgreSQL schema 5 over current schema-4 baseline; immutable manifest 1-3; ordered schema 1-4 to 5 migration/restore; structured cancellation; PG15/18 crash/restart matrix | #195-#200; #200 aggregates |

Any dependent implementation that needs behavior outside these decisions stops
and opens a bounded M7 design correction rather than deciding locally.

## Impact classification

| Area | Decision / proof obligation |
| --- | --- |
| Public API | Only typed M7 families named by the contract, including structured late-bound selectors. No SQLx, runtime ownership, credentials, executable locations, serializer/driver diagnostics, deployment authorization types, or mandatory free-form expression engine crosses the facade. |
| Durable data | Manifest 4 and repository schema 5. Current schema 4 is M6 component-state baseline and is preserved. Earlier manifest bytes never change. |
| Restart | Branch/join/nested-child parameter/link state, scope/repeat state, registry selection, compatible transform, and fork lineage use committed repository state only. |
| Definition identity | ADR-0009 remains exact: restart-relevant policy/schema/handler/mapping identities enter; capacity/throughput/credentials/telemetry do not. |
| Failure/recovery | Unknown commit stays unknown; no log/memory inference or blind retry; process loss fabricates no callback, child completion, migration success, upgrade, or fork completion. Interceptor unwind cannot replace an earlier primary failure. |
| Security | No credential/ambient late binding and no new resolved-value digest; projections/diagnostics carry only allowed bounded metadata/digests; deployment owns auth/RBAC. |
| Resource | Existing plan/split and 64 KiB manifest bounds; composition/repeat depth 8; finite scope/interceptor counts; M4 explorer limits retained. |
| Compatibility | Manifest 1-3 readable/immutable; pre-format-4 runtime rejects format 4. Repository schema 1-4 reaches 5 by ordered chain; pre-schema-5 runtime rejects 5 before writes. |
| Scope boundary | No M8 repository portability, M9 integration certification, M10 performance scheduler, M11 remote execution, M12 migration tooling, hosted control plane, or generic distributed exactly-once. |

## Manifest and repository migration matrix

| Source | Target | Rule |
| --- | --- | --- |
| persisted manifest 1/2/3 | same format | preserved byte-for-byte under existing reader |
| older definition meaning | manifest 4 definition | never rewritten by DB migration; changed identity follows ADR-0004 strict/direct-compatible selection |
| new M7 definition | manifest 4 | canonical bytes remain at most 64 KiB; only after owner implementation/evidence exists |
| repository schema 1 | schema 5 | ordered shipped chain `1 -> 2 -> 3 -> 4 -> 5` |
| repository schema 2 | schema 5 | ordered shipped chain `2 -> 3 -> 4 -> 5` |
| repository schema 3 | schema 5 | ordered shipped chain `3 -> 4 -> 5` |
| current repository schema 4 | schema 5 | direct final M7 migration `4 -> 5` |
| schema 5 opened by pre-schema-5 runtime | none | typed newer-schema rejection before write |
| successful schema-5 operational rollback | restored source backup | restore-based only; no down-migration claim |

## Reviewer-specified adversarial scenarios

These IDs are obligations for the dependent owners; this design PR does not
claim the tests already exist.

### #195 — advanced flow/composition

- `m7_flow_rejects_structural_back_edge_even_when_predicate_claims_termination`
- `m7_split_branch_cannot_escape_to_foreign_join_branch_or_outer_graph`
- `m7_nested_expansion_respects_global_node_transition_split_and_depth_bounds`
- `m7_custom_leaf_cannot_own_second_executor_repository_lifecycle_or_detached_work`
- `m7_split_result_is_identical_across_branch_completion_orders`
- `m7_split_primary_failure_uses_severity_then_declared_branch_order`
- `m7_nested_job_parameter_mapping_is_committed_before_child_user_work`
- `m7_nested_job_restart_reuses_committed_parameter_identity_and_child_link`
- `m7_nested_job_unknown_failed_stopped_completed_outcomes_propagate_exactly`
- `m7_process_kill_before_and_after_branch_join_child_decisions_is_equivalent`

### #196 — scope/late binding

- `m7_job_scope_is_new_for_each_job_execution_attempt`
- `m7_step_scope_is_new_for_each_step_execution_attempt`
- `m7_partial_factory_failure_cleans_dependencies_in_reverse_order`
- `m7_scope_cleanup_covers_success_failure_stop_cancel_and_contained_panic`
- `m7_late_binding_rejects_environment_clock_random_network_and_credentials`
- `m7_structured_selector_is_typed_bounded_and_has_no_mandatory_freeform_evaluator`
- `m7_optional_selector_parser_adds_no_source_function_or_runtime_semantic`
- `m7_restart_resolution_uses_referenced_committed_authoritative_source_only`
- `m7_scope_resolution_persists_no_copy_or_new_digest_of_resolved_value`
- `m7_scope_dependency_cycle_and_depth_overflow_fail_before_user_work`
- `m7_scope_projections_never_expose_resolved_values`

### #197 — repeat/interceptors

- `m7_repeat_is_not_representable_as_a_flow_back_edge`
- `m7_rollback_replays_the_same_repeat_iteration_ordinal`
- `m7_continue_decision_commits_before_next_iteration_starts`
- `m7_retry_inside_iteration_does_not_reenter_repeat_before`
- `m7_interceptor_before_after_order_is_outer_in_inner_out`
- `m7_after_pairs_only_successfully_entered_interceptors`
- `m7_before_failure_prevents_body_and_remains_primary`
- `m7_after_failure_becomes_primary_only_when_body_succeeded`
- `m7_secondary_unwind_failure_does_not_replace_primary_failure`
- `m7_interceptor_failure_never_recursively_enters_item_retry_skip_loop`
- `m7_process_kill_fabricates_no_after_and_does_not_advance_iteration`
- `m7_repeat_interceptor_and_nesting_bounds_fail_closed`

### #198 — registry/evolution

- `m7_registry_identical_registration_is_idempotent`
- `m7_registry_same_revision_different_fingerprint_is_definition_drift`
- `m7_registry_concurrent_conflict_has_one_winner`
- `m7_missing_or_mismatched_assembly_rejects_before_lifecycle_write`
- `m7_compatible_restart_requires_one_direct_nontransitive_edge`
- `m7_upgrade_maps_every_durable_source_node_injectively_without_silent_drop`
- `m7_target_only_upgrade_nodes_start_from_declared_initial_state`
- `m7_upgrade_failure_rolls_back_state_and_execution_creation`
- `m7_upgrade_unknown_commit_resolves_without_blind_duplicate_transform`
- `m7_fork_creates_new_lineage_without_copying_lifecycle_counters_by_default`
- `m7_fork_target_instance_collision_uses_normal_launch_guard`
- `m7_newer_corrupt_or_ambiguous_definition_state_fails_closed`

### #199 — application/operator/explorer

- `m7_parameter_incrementer_is_deterministic_and_has_no_ambient_inputs`
- `m7_incrementer_uses_explicitly_supplied_prior_parameters_not_repository_lookup`
- `m7_incrementer_same_instance_result_does_not_spin_or_invent_parameters`
- `m7_nonrestartable_job_rejects_restart_before_user_work`
- `m7_nested_repeat_flow_does_not_reset_start_limit_accounting`
- `m7_operator_replay_same_operation_and_digest_returns_recorded_outcome`
- `m7_operator_same_operation_different_digest_is_conflict`
- `m7_restart_audit_names_strict_or_exact_compatible_edge`
- `m7_fork_audit_and_projection_never_present_fork_as_restart`
- `m7_explorer_new_queries_remain_keyset_bounded_and_payload_redacted`
- `m7_facade_contains_no_database_runtime_credential_or_executable_location_type`

### #200 — cross-cutting exit evidence

- `m7_manifest4_golden_bytes_are_deterministic_across_repeated_compilation`
- `m7_manifest4_over_64k_is_rejected_even_below_node_transition_ceilings`
- `m7_restart_relevant_change_changes_the_fingerprint`
- `m7_throughput_only_budget_change_does_not_change_the_fingerprint`
- `m7_manifest1_2_3_bytes_are_never_rewritten`
- `m7_preformat4_runtime_rejects_manifest4`
- `m7_schema1_2_3_4_upgrade_to_schema5_on_postgres15_and_postgres18`
- `m7_preschema5_runtime_rejects_schema5_before_write`
- `m7_failed_schema5_migration_leaves_source_schema_recoverable`
- `m7_schema5_restore_returns_source_schema_and_prior_normalized_rows`
- `m7_pg15_and_pg18_process_kill_matrix_executes_without_green_by_skip`
- `m7_resource_campaign_proves_declared_bounds_without_unbounded_growth`
- `m7_redaction_sweep_finds_no_parameter_context_checkpoint_credential_or_selector_value`

## Non-delegable proof obligations

- CI green is necessary but insufficient.
- Unsupported/newer/corrupt paths without tests are gate failures, not assumed
  fail-closed behavior.
- A crash campaign that does not prove its kill point was reached is not crash
  evidence.
- Migration evidence must use populated prior-version fixtures; empty DB-only
  migration is insufficient for semantic preservation.
- Facade evidence must include public snapshot/rustdoc and diagnostic-redaction
  paths, not import resolution alone.
- UNKNOWN / UNVERIFIED / INSUFFICIENT EVIDENCE remains FAIL.

## Dependency handoff

After exact-final-HEAD review, merge, post-main verification, and #194 closure:

1. #195-#199 may become ready only if fresh dependency readback shows #194 was
   their final live blocker.
2. #200 stays blocked until all delivery owners complete.
3. #192 stays open through the M7 exit gate.

Any implementation need that changes this record requires a reviewed design
correction or superseding ADR before dependent production code may merge.
