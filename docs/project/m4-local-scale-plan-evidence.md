# M4 Bounded Local-Scale Plan Evidence

**State:** Partial implementation (unreleased)

**Issue:** [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)

**Date:** 2026-08-03

This record covers the first reviewable implementation slice of the accepted
M4 bounded local-scale contract: manifest format 3, the exact split and
partitioned-step declaration subset, finite plan budgets, and canonical
definition identity. Later evidence records the implemented execution slices;
this plan record alone does not claim that either scale ledger row is complete.

## Implemented boundary

- Manifest format 3 adds bounded split, structural join, and local
  partitioned-step declarations while formats 1 and 2 remain readable and
  byte-identical.
- A split has `2..=8` declared-order branches, each containing `1..=8`
  ordinary M3 tasklet or chunk steps, and owns exactly one structural join.
  A split cannot be the entry, cannot have an explicit outgoing transition,
  and its join cannot be entered by another edge or owned by another split.
- A partitioned-step declaration records its ordinary worker definition,
  deterministic partitioner and aggregation revisions, `1..=1024` partition
  count, start controls, sibling failure policy, and finite launch budgets.
- Embedded branch and worker logical IDs participate in global uniqueness
  validation. This prevents a runtime from silently aliasing independent
  child work to one durable identity.
- Branch concurrency is `1..=8`, partition-worker concurrency is `1..=64`,
  and repository pool capacity must cover the larger active-child budget plus
  one parent connection. Zero, over-limit, and contradictory values fail with
  typed plan errors.
- Local-scale declarations are canonical manifest members and therefore change
  the SHA-256 definition fingerprint. Builder declaration order does not change
  canonical bytes.
- **Corrected during M5.** This slice also made the concurrency and connection
  budgets manifest members, which
  [ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md)
  later removed: they bound throughput and select no durable state, so hashing
  them blocked restart after ordinary resource tuning. The partition count,
  partitioner, and aggregation identity, which do select assignment and
  aggregate meaning, are unaffected. See the
  [M5 plan and fingerprint evidence](m5-plan-fingerprint-evidence.md).

The current `FlowLauncher` accepts the tasklet-only bounded parallel-split and
local-partition runtimes recorded in the
[parallel-split evidence](m4-parallel-split-evidence.md) and
[local-partition evidence](m4-local-partition-runtime-evidence.md). Format-3
plans with unbound components or partition workers outside the tasklet-only M4
slice still fail closed. The supporting
[durable partition repository evidence](m4-partition-repository-evidence.md)
records the schema-3 plan and result transaction boundary.

## Named executable evidence

| Scenario | Evidence |
| --- | --- |
| Exact subset rejection | [`split_outside_the_accepted_subset_is_rejected`](../../crates/oxide-batch/tests/local_scale_plan.rs) |
| Branch and join isolation | [`empty_branch_and_external_join_entry_fail_closed`](../../crates/oxide-batch/tests/local_scale_plan.rs) |
| Embedded identity uniqueness | [`embedded_step_identity_cannot_alias_a_top_level_node`](../../crates/oxide-batch/tests/local_scale_plan.rs) |
| Finite budgets and pool capacity | [`zero_unbounded_and_contradictory_budgets_are_rejected`](../../crates/oxide-batch/tests/local_scale_plan.rs) |
| Format-3 golden identity | [`format3_manifest_has_a_golden_fingerprint`](../../crates/oxide-batch/tests/local_scale_plan.rs) |
| Canonical declaration ordering | [`declaration_order_does_not_change_format3_identity`](../../crates/oxide-batch/tests/local_scale_plan.rs) |
| Assignment identity | [`partition_count_changes_the_format3_fingerprint`](../../crates/oxide-batch/tests/local_scale_plan.rs), which replaced this slice's `assignment_budget_changes_the_format3_fingerprint` under ADR-0009 |
| Existing format-2 golden bytes | [`format2_manifest_has_golden_bytes_and_fingerprint`](../../crates/oxide-batch/tests/plan_manifest.rs) |
| Newer-reader rejection | [`a_newer_manifest_is_rejected_rather_than_guessed`](../../crates/oxide-batch/tests/plan_manifest.rs) |

## Remaining issue #80 boundary

The tasklet-only execution slice is implemented, but chunk-worker composition
and the final M4 process-kill, resource, cancellation, PostgreSQL-matrix, and
soak-evidence judgment remain outside this plan record.

`SCALE-PARSTEP-001` and `SCALE-LOCALPART-001` therefore remain unreleased
`Partial`, not `Implemented` or `Verified`. RFC-0009 remains proposed, and this
slice adds no transport, lease, fencing, or remote-worker behavior.
