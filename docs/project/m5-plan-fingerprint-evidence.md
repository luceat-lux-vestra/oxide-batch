# M5 Plan and Definition-Fingerprint Stabilization Evidence

**State:** Complete on merge

**Issue:** [#98](https://github.com/luceat-lux-vestra/oxide-batch/issues/98)

**Date:** 2026-08-03

This record delivers the plan-and-fingerprint workstream the
[M5 kickoff gate](m5-kickoff-gate.md) orders second and the
[M5 design-gate evidence](m5-design-gate-evidence.md) authorizes. It stabilizes
the delivered M2-M4 manifest and fingerprint path. It adds no node kind, no
manifest format, no restart mode, no schema table, and no public extension
point, and it promotes no ledger row.

## What changed

The gate fixed the canonical manifest as exactly the values that select or
reinterpret durable state, with an explicit exclusion list. Auditing the
delivered projection against that list found two classes of excluded value
inside the canonical bytes.

| Finding | Where | Why it is excluded | Consequence if kept |
| --- | --- | --- | --- |
| Framework capacity bounds (`max_nodes`, `max_transitions`, `max_outgoing_transitions`, `max_pattern_bytes`, and the format-3 split, branch, partition, and worker maxima) | The `bounds` member of manifest formats 2 and 3 | They are compile-time constants of the running build, not values of the definition. No reader consumed the member; `DefinitionManifest` validates a graph against the constants the running build carries | Raising any bound in a later release changes every fingerprint the framework has produced, so every persisted definition becomes drift and every restart fails closed after a routine upgrade |
| `repository_pool_size`, `max_parallel_branches`, `max_partition_workers` | The `budget` member of format-3 split and partitioned-step nodes | The partitioner receives the partition count and never the worker count, aggregation orders by `partition_key` and declared branch order, and the accepted sequential fallback equivalence requires a budget change to leave every normalized durable observation unchanged | An operator who retunes a pool or a worker count after a crash cannot restart until the tuning is reverted |

[ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md)
records the decision: both classes leave the projection, format identifiers and
canonical encoding rules do not change, and the format-2 and format-3 golden
vectors are re-pinned exactly once. Format 1 carries neither class and is
unchanged byte for byte.

## Re-pinned vectors

No released version emitted the prior bytes: the workspace is
`0.1.0-alpha.1` and no crate has been published, so the re-pin migrates
nothing. Both digests are recorded here so the change is auditable.

| Vector | Prior fingerprint | Current fingerprint |
| --- | --- | --- |
| Format 2, `LIFE-DEFINITION-001` two-step golden manifest | `75305aa3ce1ca2e2f8952e139bb3ee4f308787b547c765da93f8fb5add5e5057` | `c0ea69669657cb8ec425801588a1f042608d8785333ad7d38d8a1f7ed5d8557f` |
| Format 3, bounded local-scale plan | `022df67b0163557ae0cd13c3522db7bc1b697f69eb8e6c1cb4725a19290cf3a9` | `f5ee7c2d6923411c8c068b6c2770b95575256833bddaed1be9c3893324c541a9` |

The format-2 canonical bytes are committed at
`crates/oxide-batch/tests/fixtures/LIFE-DEFINITION-001/format2-two-step.manifest.json`
and are compared byte for byte, not only by digest.

## Named scenarios

Every scenario the design gate names for this workstream is executable and
passing. The suite is
`crates/oxide-batch/tests/plan_fingerprint.rs` unless another file is named.

| Scenario | Evidence |
| --- | --- |
| `unchanged_definition_recompiles_to_the_same_fingerprint` | Two independently built maximal plans produce identical canonical bytes and digests, and eight repeated compilations produce one digest, so nothing depends on allocation, iteration order, or a clock. `declaration_order_does_not_change_the_fingerprint` (`plan.rs`) and `declaration_order_does_not_change_format3_identity` (`local_scale_plan.rs`) cover declaration order |
| `restart_relevant_change_changes_the_fingerprint` | Seven single-value mutations — job name, logical node and step ID, component revision, chunk size, start limit, partition count, partitioner identity — each change the digest and none collide. A second scenario covers checkpoint schema version, context schema version, delivery mode, and in-flight policy. `restart_relevant_values_change_the_fingerprint` (`plan.rs`) and `partition_count_changes_the_format3_fingerprint` (`local_scale_plan.rs`) retain their M3 and M4 coverage |
| `display_name_or_storage_key_change_does_not_change_the_fingerprint` | The same definition persisted into two repositories with different identifier seeds and clocks yields one digest, and the canonical bytes contain no storage table, adapter key, execution identifier, pool, timestamp, or telemetry token |
| `throughput_only_budget_change_does_not_change_the_fingerprint` | Retuning branch concurrency, worker count, and both connection budgets — together and one knob at a time — leaves the canonical bytes identical. `worker_budget_does_not_change_the_format3_fingerprint` (`local_scale_plan.rs`) replaces the M4 test that asserted the opposite |
| `fingerprint_mismatch_without_an_edge_rejects_restart_before_any_write` | A failed instance rejects a differently fingerprinted definition with `IncompatibleDefinition`, and the execution rows, versions, and statuses read identical before and after the rejected attempt |
| `revision_rebound_to_a_new_fingerprint_is_drift` | One revision rebound to different restart-relevant values is rejected as `DefinitionDrift` rather than as an incompatible definition, and creates no execution |
| `newer_manifest_format_is_rejected` | A format-4 manifest is rejected with `UnsupportedFormat { format: 4, supported: 3 }` by both `read` and `read_verified`, so a matching digest cannot admit a format the runtime cannot interpret |
| `format1_and_format2_bytes_are_never_rewritten` | A format-1 identity is compared against its exact committed byte string and re-reads verified; a format-2 plan compiled by a runtime that understands format 3 keeps its own format and gains no format-3 member. `a_format1_wrapper_lowers_without_changing_its_identity` and `moving_a_definition_to_format2_requires_a_direct_upgrade_edge` (`plan_manifest.rs`) retain the lowering and edge coverage |

## Member allowlist

`canonical_manifest_contains_only_allowlisted_members` walks every object key of
a maximal format-3 manifest — chunk and tasklet steps, start controls, a
listener, a decision, a split with two branches, a join, and a partitioned step
— and fails on any member outside the ADR-0009 input set. It also asserts that
the projection is non-trivial, so the allowlist cannot pass by describing an
empty manifest.

The projection is hand-written per node kind, which is how both excluded classes
entered it. The allowlist moves the rule from review into the suite: a member
added to the projection fails the build until the input set is amended by a
superseding ADR.

## Unchanged behavior

- Persisted bytes: no stored manifest is rewritten, and no schema, migration,
  table, or column changes.
- Transaction boundaries and lifecycle writes: unchanged. Drift and
  incompatibility are still decided before any write, which the two rejection
  scenarios assert by reading durable state before and after.
- Normalized traces: the eleven `plan_equivalence` wrapper-trace goldens pass
  unchanged, because format-1 identity is untouched.
- Public API: `SplitBudget` and `PartitionBudget` keep their constructors,
  accessors, validation, and typed rejections. Only the manifest projection
  narrowed.
- Full embedded conformance across the accepted M0-M4 scope passes.

## Delivered subset boundary

M5 stabilizes fingerprinting, drift detection, and restart identity over the
delivered M2-M4 subset. General compiled-plan restart, the M7
`DefinitionRegistry` as a public service, schema-transforming upgrade edges, and
`Fork` lineage remain M7. Declared transaction capabilities that change durable
meaning enter the fingerprint with issue
[#100](https://github.com/luceat-lux-vestra/oxide-batch/issues/100) under the
same input-set rule, which is why `capabilities` is absent from the allowlist
today. M5 keeps the accepted ADR-0002 boxed component boundary and does not
implement the ADR-0008 item contract.

## Validation

Run locally on macOS arm64 with the pinned toolchain. Every command below
passed. `OXIDEBATCH_POSTGRES_TEST_URL` is not set locally, so the PostgreSQL
suites reported themselves skipped rather than passing; they run in CI on the
supported matrix and are release-blocking there.

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

The PostgreSQL definition-drift and compatibility-edge contracts run in CI
against the supported PostgreSQL majors; they exercise the same identity values
and are unaffected by the projection change, because a digest is compared rather
than parsed.

## Residual risk

- The allowlist enumerates member names, not their placement. A member name
  reused in a new position passes it. Placement is covered by the golden vectors
  for the shapes they pin, and a new node kind requires a manifest format
  decision that reviews the projection.
- Pre-release deployments holding format-2 or format-3 manifests see drift after
  this change. The
  [support matrix](../release/support-matrix.md#m5-production-preview-support-bounds)
  states the expectation and the two recovery paths.
