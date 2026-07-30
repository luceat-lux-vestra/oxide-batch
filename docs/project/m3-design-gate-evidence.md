# M3 Fault-Tolerance and Flow Design-Gate Evidence

**State:** Complete on merge

**Issue:** [#59](https://github.com/luceat-lux-vestra/oxide-batch/issues/59)

**Date:** 2026-07-31

This record closes the decisions required before M3 production implementation.
It authorizes the dependency order below; it does not claim that the public
policies, schema 2, compiled plan, or flow runtime are implemented.

## Closed decision gates

| Gate | Canonical evidence |
| --- | --- |
| Fault policy and error taxonomy | [Fault-tolerance contract](../architecture/fault-tolerance.md) fixes stable phase/category inputs, unambiguous rules, retry/skip limits, exhaustion, no-rollback restrictions, fingerprint impact, and fail-closed categories. |
| Durable policy state and schema | The fault-tolerance contract and [schema-2 migration](../operations/migrations/0002-fault-tolerance-and-flow.md) define retry reservation, atomic skip state, counters, checksummed bounded state, restart inheritance, corruption, newer-version, upgrade, and restore behavior. |
| Listener and interceptor slice | The fault-tolerance contract fixes read/process/write/retry/skip taxonomy, nesting, order, callback timing, failure/panic aggregation, authoritative outcome, and redaction. |
| One-step compiled-plan lowering | [Basic-flow contract](../architecture/basic-flow.md) fixes logical IDs, format-1 compatibility lowering, format-2 canonicalization, fingerprint rules, manifest migration, bounds, and normalized trace/repository equivalence. |
| Basic flow and durable decisions | The basic-flow contract fixes the acyclic M3 graph, exit matching/specificity, decider input and persistence, restart traversal, start limits, and allow-start-if-complete. |
| Backoff, cancellation, and telemetry | The fault-tolerance contract and [observability contract](../operations/observability-contract.md) fix monotonic schedules, stop points, retained state bounds, commit-relative event timing, safe fields, and metric cardinality. |

## Impact classification

| Area | M3 decision |
| --- | --- |
| Observable compatibility | Spring-like aggregate skip limit, per-item retry, exit-pattern flow, deciders, and start controls; typed error categories and capability-scoped no-rollback are recorded divergences. |
| Public API | Adds validated policy, classifier, listener, graph, decider, logical-ID, and start-control values using the current `BoxFuture` boundary. |
| Restart and transactions | Retry reservations survive restart; skip state commits with the chunk; decisions commit before target start; unknown commit never retries; stale CAS loses. |
| Durable data | PostgreSQL schema 2 adds logical IDs, fault counters/state, and append-only flow decisions. Definition manifest format 2 is independent and format 1 remains readable. |
| Security | Policy/decision state and diagnostics exclude items, errors, parameters, contexts, credentials, endpoints, SQL, and private component/decider state. |
| Telemetry | New events are post-decision observers with bounded enums and numeric fields; identifiers and user strings are not metric labels. |
| Migration | Schema 1 to 2 is quiesced and transactional; old runtimes reject version 2; rollback is verified backup restore; old manifest bytes are not rewritten. |
| Resources | Retry state retains at most 256 unresolved keys per M3 step; retries, backoff, nodes, edges, outgoing transitions, manifest, and state sizes have finite bounds. |

No decision uses RFC-0005's proposed native/erased hot path. M3 retains the
accepted ADR-0002 boxed public component boundary.

## Named implementation scenarios

| Ledger row | Scenario IDs required by dependent issues |
| --- | --- |
| `FT-RETRY-001` | `retryable_failure_succeeds_within_limit`, `retry_exhaustion_uses_initial_plus_reserved_retries`, `crash_before_reservation_replays_initial_call`, `retry_reservation_survives_restart`, `stale_retry_reservation_loses_cas` |
| `FT-BACKOFF-001` | `backoff_uses_injected_monotonic_time`, `stop_during_backoff_consumes_reservation_without_reinvoke`, `backoff_arithmetic_is_capped` |
| `FT-SKIP-001` | `skip_limit_is_shared_across_phases`, `next_skip_after_limit_fails`, `skip_count_commits_with_chunk`, `write_skip_requires_located_known_rollback` |
| `FT-ROLLBACK-001` | `retry_rolls_back_before_reinvoke`, `commit_safe_skip_requires_capability`, `crash_before_commit_replays_chunk`, `unknown_commit_is_never_retried` |
| `LISTENER-ITEM-001` | `item_listeners_nest_and_reverse_after_order`, `item_error_precedes_policy_decision`, `skip_listener_effect_commits_once_with_skip`, `listener_failure_rolls_back_and_redacts` |
| `FLOW-SEQUENCE-001` | `exit_status_selects_most_specific_transition`, `ambiguous_transition_is_rejected`, `unmapped_exit_fails_job`, `committed_transition_survives_restart` |
| `FLOW-DECIDER-001` | `decider_result_and_target_commit_together`, `committed_decider_is_not_reinvoked`, `decider_input_change_records_new_path`, `decider_panic_is_redacted_failure` |
| `STEP-STARTLIMIT-001` | `start_limit_is_atomic_per_instance_and_logical_step`, `failed_start_consumes_limit`, `completed_step_is_skipped_by_default`, `allow_start_if_complete_reruns_on_restart_path` |
| `LIFE-DEFINITION-001` | `format1_wrapper_lowers_without_identity_change`, `format2_manifest_has_golden_fingerprint`, `format1_to_format2_requires_direct_upgrade`, `newer_manifest_is_rejected` |
| `META-UPGRADE-001` | `schema1_upgrades_to_schema2`, `schema2_corruption_fails_closed`, `schema1_runtime_rejects_schema2`, `schema2_backup_restores_schema1` |

Required evidence classes are unit/property, PostgreSQL integration, named
conformance, crash/failure injection, migration/restore, and bounded-resource
tests as indicated by each ledger profile. Documentation names are acceptance
targets, not evidence links until the tests exist and pass.

## Dependency handoff

- Issue #60 may implement runtime-neutral failure, policy, backoff, and
  listener contracts.
- Issue #63 may implement format-1 compatibility lowering, format-2 values,
  compilation, and one-step equivalence independently.
- Issue #61 follows #60 and integrates policy execution.
- Issue #62 follows #61 and promotes schema 2 plus PostgreSQL durability.
- Issue #64 follows #63 and uses the accepted schema-2 flow-decision contract.
- Issue #65 follows all implementation work and owns M3 exit evidence.

Any implementation need that changes these observable rules, manifest identity,
schema meaning, or transaction boundary requires a documentation correction
and, when it changes an accepted contract, a superseding RFC or ADR.
