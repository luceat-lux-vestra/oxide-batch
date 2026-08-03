# M5 Embedded Core Production Preview Design-Gate Evidence

**State:** Complete on merge

**Issue:** [#97](https://github.com/luceat-lux-vestra/oxide-batch/issues/97)

**Date:** 2026-08-03

This record closes the nine decision gates that the
[M5 kickoff gate](m5-kickoff-gate.md) requires before dependent M5
implementation. It authorizes the dependency order below; it does not stabilize
the plan or fingerprint, extract any crate, move any durable format, change the
facade, run any campaign, or promote any ledger row. No row moves toward
released `Verified` status because of this document.

## Closed decision gates

| Gate | Decision | Canonical evidence |
| --- | --- | --- |
| Compiled plan and definition fingerprint | Stabilize the delivered M2-M4 subset: a canonical restart-relevant manifest with an explicit exclusion list, deterministic fingerprint bytes, direct one-way format edges, and fail-closed drift rejection before any lifecycle write | The [M5 stabilization slice](../architecture/execution-plan.md#m5-stabilization-slice) of the execution-plan architecture |
| Static and erased components | Continued deferral of [RFC-0005](../rfcs/0005-static-and-erased-components.md); M5 retains the ADR-0002 boxed boundary | The [M5 gate outcome](../rfcs/0005-static-and-erased-components.md#m5-gate-outcome) recorded in the RFC |
| Staged crate extraction | Three authorized stages — core, repository contracts, and plan — with forbidden-dependency rules, facade and API equivalence, durable invariance, packaging checks, measurements, and per-stage reversal; every other boundary deferred past M5 | The [staged crate-extraction contract](../architecture/crate-extraction.md) |
| Context codec and external state | Retain the versioned JSON envelope at framework format `1` with separate application schema versioning, bounded size and depth, typed value-redacted failures, and restore-based rollback; move no durable format in M5 | The [M5 codec and capability direction](../architecture/repository-and-transaction-model.md#m5-context-codec-and-transaction-capability-direction) |
| Transaction capability direction | Capabilities are declared in the versioned descriptor, an undeclared requirement fails with a typed rejection rather than a degraded guarantee, the borrowed adapter-owned transaction path is unchanged, and only durable-meaning capabilities participate in the fingerprint | The same M5 codec and capability direction section |
| Public facade and API review | The curated `oxide-batch` surface is the only preview claim, seven disclosure classes are prohibited, pre-1.0 evolution continues to apply, and the review must record a per-boundary M6-M12 non-blocking argument | The [M5 preview surface and disclosure gate](../api/design-guidelines.md#m5-preview-surface-and-disclosure-gate) |
| Ledger disposition and promotion | Every M0-M4 row has a reviewed disposition; `29` advertised embedded-kernel rows are the only promotion candidates, `13` rows stay `Partial` and are published as limitations, and `39` `Planned` plus `2` `Unknown` rows remain visible | The [M5 disposition and promotion set](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set) |
| Preview support, upgrade, and release bounds | A `0.x` single-host preview on Linux x86_64 GNU with PostgreSQL 15-18, `verify-full` TLS, schema 3, MSRV 1.95, direct upgrade from schemas 1 and 2, and restore-based rollback | The [M5 production-preview support bounds](../release/support-matrix.md#m5-production-preview-support-bounds) |
| Evidence campaigns | Nine named campaigns — reference workload, performance, resource bounds, crash and restore, upgrade, security, soak, cancellation, and extraction — each with retained reproducible raw evidence | The [M5 production-preview campaigns](../engineering/performance-plan.md#m5-production-preview-campaigns) |

## Static and erased component decision

This is the only gate whose outcome was genuinely open, and it closes as
**continued deferral** rather than approval.

RFC-0005's own approval gate requires a reproducible spike and measurements for
both the native and erased paths, reviewed public ergonomics, and a superseding
ADR that updates ADR-0002's allocation consequence. That spike has not run. M5
additionally stabilizes the fingerprint path and repackages crates, and doing
that on top of a changing item hot path would invalidate the equivalence
evidence both workstreams depend on.

The [M5 kickoff gate](m5-kickoff-gate.md) records that continued deferral
satisfies the roadmap dependency by the recorded decision rather than by an
approval, so this closure does not deadlock the milestone. M5 exits on the
boxed boundary. The decision is revisited at M6 kickoff, where the spike,
measurements, and superseding ADR become prerequisites for item-model work.

## Impact classification

| Area | M5 decision |
| --- | --- |
| Observable compatibility | Unchanged. M5 adds no node kind, manifest format, restart mode, schema table, CLI command, capability, or extension point. |
| Public API | The curated facade is enumerated and bounded by seven prohibited disclosure classes; extraction re-exports keep every supported import path resolving to the same item. Extracted crates stay `publish = false`. |
| Restart and transactions | Unchanged boundaries. Drift detection is fail-closed before any lifecycle write; the borrowed adapter-owned transaction path, atomic checkpoint, and unknown-outcome semantics are preserved exactly. |
| Durable data | No format moves in M5. Formats 1, 2, and 3 keep their bytes and golden vectors; schema 3 is the preview schema, with direct upgrade from 1 and 2 and restore-based rollback. |
| Packaging | Three authorized extraction stages, enforced forbidden-dependency and cycle checks, and per-stage reversal that requires no migration because no stage may alter durable state. |
| Security | Preview supports `verify-full` TLS only in production mode, with least-privilege separation across migration, runtime, explorer, operator, and retention roles and a redaction sweep across errors, telemetry, CLI output, and bundles. |
| Ledger claims | `29` advertised rows are the sole promotion candidates and require a named released version plus every required evidence link; `13` `Partial` and `41` remaining rows stay visible and prevent any parity claim. |
| Support | Single host, embedded, Linux x86_64 GNU, PostgreSQL 15-18, MSRV 1.95, pre-1.0 latest-line support only. No 1.0, GA, or enterprise-readiness claim. |
| Performance | Nine campaigns with retained raw evidence; M4 budgets remain provisional. P-002 is explicitly excluded and moves to M6 with RFC-0005. |

No decision uses RFC-0005's proposed native hot path or RFC-0009's proposed
worker protocol. M5 retains the accepted ADR-0002 boxed component boundary and
adds no remote, distributed, or multi-host semantics.

## Ledger disposition review

The population reviewed is `83` rows: `0` `Verified`, `29` `Implemented`, `13`
`Partial`, `39` `Planned`, and `2` `Unknown`. The complete disposition, the
named advertised set, and the promotion rules are recorded in the
[ledger](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set).

The review also corrected two ledger rows, `SCALE-PARSTEP-001` and
`SCALE-LOCALPART-001`, which carried twelve cells against a thirteen-column
header and therefore had no reviewable owner. Their canonical owner is now
recorded and their notes occupy the notes column.

Two promotion gaps are named rather than assumed closed: `META-CONTEXT-001`
links an architecture spike rather than codec migration tests, and the
least-privilege separation that `REPO-RETENTION-001` depends on must run on the
released schema-3 fixture. Both close inside the campaigns below.

## Named campaign scenarios

| Workstream | Scenario IDs required by the dependent issue |
| --- | --- |
| Plan and fingerprint | `unchanged_definition_recompiles_to_the_same_fingerprint`, `restart_relevant_change_changes_the_fingerprint`, `display_name_or_storage_key_change_does_not_change_the_fingerprint`, `throughput_only_budget_change_does_not_change_the_fingerprint`, `fingerprint_mismatch_without_an_edge_rejects_restart_before_any_write`, `revision_rebound_to_a_new_fingerprint_is_drift`, `newer_manifest_format_is_rejected`, `format1_and_format2_bytes_are_never_rewritten` |
| Crate extraction | `facade_import_paths_resolve_unchanged_after_each_stage`, `public_api_snapshot_is_unchanged_by_extraction`, `forbidden_dependency_check_fails_the_build_on_violation`, `workspace_has_no_dependency_cycle`, `golden_fingerprints_are_unchanged_by_extraction`, `normalized_repository_write_traces_are_unchanged_by_extraction`, `package_dry_run_succeeds_for_every_workspace_crate` |
| Codec and capability | `older_recorded_schema_version_upgrades_through_one_directed_chain`, `newer_recorded_schema_version_is_rejected`, `oversized_or_over_deep_payload_is_a_known_not_committed_outcome`, `corrupt_payload_never_advances_a_checkpoint`, `undeclared_capability_requirement_is_rejected_with_a_typed_error`, `borrowed_transaction_preserves_atomic_checkpoint_and_unknown_outcome`, `durable_meaning_capability_change_changes_the_fingerprint`, `throughput_capability_change_does_not_change_the_fingerprint` |
| Facade review | `facade_exposes_no_runtime_database_or_telemetry_sdk_type`, `debug_output_redacts_every_sensitive_payload_class`, `rustdoc_surface_contains_no_leaked_implementation_type`, `public_api_snapshot_matches_the_reviewed_preview_surface` |
| Evidence campaigns | `full_embedded_conformance_suite_passes_on_the_accepted_scope`, `process_kill_at_each_commit_phase_recovers_without_a_forged_status`, `schema1_and_schema2_upgrade_directly_to_schema3`, `schema2_runtime_rejects_schema3`, `schema3_backup_restores_the_prior_schema`, `verify_full_tls_is_required_in_the_supported_mode`, `least_privilege_role_cannot_exceed_its_class`, `redaction_sweep_finds_no_prohibited_value_class`, `declared_ceilings_hold_under_stress_with_backpressure`, `soak_reports_no_task_connection_handle_or_memory_growth` |

Required evidence classes are unit/property, PostgreSQL integration, named
conformance, crash/failure injection, security and least-privilege,
migration/restore, bounded-resource, and performance tests as indicated by each
ledger profile. Documentation names are acceptance targets, not evidence links,
until the tests exist and pass.

## Dependency handoff

- Issue [#98](https://github.com/luceat-lux-vestra/oxide-batch/issues/98) may
  stabilize the compiled plan and definition fingerprint at the boundary fixed
  above.
- Issue [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99) may
  begin extraction stages 1 and 2 under the extraction contract; stage 3 waits
  for #98 to land.
- Issue [#100](https://github.com/luceat-lux-vestra/oxide-batch/issues/100) may
  apply the codec and capability direction; it moves no durable format without
  its own migration and rollback evidence.
- Issue [#101](https://github.com/luceat-lux-vestra/oxide-batch/issues/101)
  follows #99 and #100 and reviews the delivered facade against the disclosure
  rules and the M6-M12 argument.
- Issue [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102)
  follows the implementation streams and runs the nine campaigns. The
  conformance, crash, upgrade, security, and resource campaigns do not depend
  on #101.
- Issue [#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103)
  follows all implementation and evidence work and owns the preview
  documentation, the ledger promotions, and the M5 exit record.

Any implementation need that changes these observable rules, manifest identity,
schema meaning, transaction boundary, disclosure boundary, support bound, or
promotion rule requires a documentation correction and, when it changes an
accepted contract, a superseding RFC or ADR.
