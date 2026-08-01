# M3 Compiled-Plan Lowering Evidence

**State:** Complete on merge

**Issue:** [#63](https://github.com/luceat-lux-vestra/oxide-batch/issues/63)

**Date:** 2026-07-31

This record maps the fifth M3 workstream's exit criteria to the compiled
execution plan. It implements the immutable flow graph, bounded compilation,
canonical manifest format 2, and the format-1 compatibility lowering the
[basic-flow contract](../architecture/basic-flow.md) and
[execution-plan architecture](../architecture/execution-plan.md) accept, and it
routes one-step `TaskletJob` and `ChunkJob` execution through that plan without
changing a single observable line of their behavior.

It does not claim durable flow traversal, decider execution, start-limit
enforcement, or `allow_start_if_complete` behavior. Decision nodes and start
controls are compiled and fingerprinted here because manifest format 2 is
immutable; issue
[#64](https://github.com/luceat-lux-vestra/oxide-batch/issues/64) owns their
runtime and the `ob_flow_decision` queries.

## Model

`FlowGraph` declares an entry node, tasklet or chunk `StepNode` values, typed
`DecisionNode` values, and transitions selected by a bounded `ExitPattern`.
`FlowGraph::compile` normalizes and validates the graph into an immutable
`CompiledExecutionPlan` that owns the definition identity.

A transition target is `FlowTarget::Node` or `FlowTarget::Terminal`. Terminals
live in their own namespace rather than carrying a `NodeId`, so a framework
terminal can never collide with an application step name and the durable record
`#64` writes can distinguish a node target from a terminal without a reserved
identifier prefix.

Transitions are ordered by descending `PatternSpecificity`: more literal
characters, then fewer wildcards, then a longer UTF-8 byte length. Two patterns
leaving one node that are equally specific *and* can match one common value are
rejected as ambiguous. Equally specific disjoint patterns are accepted and
ordered by pattern bytes and target, which is a canonical-encoding decision
only: no exit outcome can reach two of them.

## Exit criteria

| Exit criterion | Evidence |
| --- | --- |
| One-step tasklet and chunk definitions compile deterministically with stable logical IDs and bounded diagnostics | `a_format1_wrapper_lowers_without_changing_its_identity` and `the_compatibility_plan_routes_every_framework_exit_code` show `TaskletJob` and `ChunkJob` lowering into a one-node plan whose logical ID is the validated step name. `declaration_order_does_not_change_the_fingerprint` shows compilation is order-independent. `PlanError` carries node identifiers, patterns, and bounds only; `plan_diagnostics_do_not_print_manifest_bytes` shows the identity still redacts its manifest. |
| Canonical manifest golden vectors, bounds, fuzz/property coverage, old-manifest reading, and migration behavior pass | `format2_manifest_has_golden_bytes_and_fingerprint` pins the exact canonical bytes in `tests/fixtures/LIFE-DEFINITION-001/format2-two-step.manifest.json` and their SHA-256. `a_format2_runtime_still_reads_format1_manifests`, `a_newer_manifest_is_rejected_rather_than_guessed`, `malformed_and_non_canonical_manifests_fail_closed`, and `an_altered_manifest_does_not_match_its_fingerprint` cover the reader. `pattern_matching_agrees_with_an_exhaustive_reference` and `pattern_intersection_agrees_with_an_exhaustive_reference` exhaustively compare every pattern of up to three characters over `{A,B,*,?}` against an independent reference matcher. `structural_errors_are_rejected_before_execution`, `one_node_cannot_exceed_the_outgoing_transition_bound`, and `exit_patterns_are_bounded_and_printable` cover invalid graphs and bounds. |
| Existing wrappers and lowered plans have normalized lifecycle, event, repository-write, stop, panic, and restart equivalence | `tests/plan_equivalence.rs` records every repository command, every lifecycle event, and the final durable rows of eleven one-step scenarios and compares them with reviewed golden traces. Ten remain byte-for-byte traces captured from the pre-lowering wrapper implementation at commit `1fea043`; the M3 exit gate deliberately updates the failed-chunk terminal `rollback_count` from zero to one for the accepted lifecycle fix and pins the corrected value. The scenarios cover tasklet completion, failure, panic, unknown commit, stop before start, stop during execution, a before-step listener failure, restart after failure, and chunk completion, failure, and unknown commit. |
| Strict and directed-compatible restart remain fail closed; `Fork` is not implemented and `Force` remains rejected | `moving_a_definition_to_format2_requires_a_direct_upgrade_edge` shows a mechanically equivalent one-step format-2 plan producing a different fingerprint, a restart attempt rejected with `IncompatibleDefinition`, and acceptance only after an explicit `DefinitionUpgrade` is registered. No `Fork` or `Force` mode exists: RFC-0004 leaves fork identity and input-snapshot requirements unresolved, so this workstream does not implement it. |
| The implementation does not adopt RFC-0005's proposed static/erased public API | The plan records component *revisions*, not component values. `StepComponents` holds `ComponentRevision` and `ChunkComponentRevisions`, and execution still runs through the accepted ADR-0002 boxed `Tasklet`, `ItemReader`, `ItemProcessor`, and `ItemWriter` contracts. |

## Named scenarios satisfied by this workstream

| Ledger row | Scenario | Evidence |
| --- | --- | --- |
| `LIFE-DEFINITION-001` | `format1_wrapper_lowers_without_identity_change` | `a_format1_wrapper_lowers_without_changing_its_identity` plus the eleven golden equivalence traces |
| `LIFE-DEFINITION-001` | `format2_manifest_has_golden_fingerprint` | `format2_manifest_has_golden_bytes_and_fingerprint` |
| `LIFE-DEFINITION-001` | `format1_to_format2_requires_direct_upgrade` | `moving_a_definition_to_format2_requires_a_direct_upgrade_edge` |
| `LIFE-DEFINITION-001` | `newer_manifest_is_rejected` | `a_newer_manifest_is_rejected_rather_than_guessed` |

`FLOW-SEQUENCE-001` and `FLOW-DECIDER-001` remain `Planned`. Compilation-time
halves of `exit_status_selects_most_specific_transition` and
`ambiguous_transition_is_rejected` are covered by
`exit_status_selects_the_most_specific_transition` and
`equally_specific_overlapping_patterns_are_rejected`, but neither row may move
until #64 executes a durable multi-step traversal.

That limitation was subsequently closed for the finite M3 slice by the
[M3 durable flow runtime evidence](m3-flow-runtime-evidence.md). The rows are
now `Partial`, not `Verified`, because advanced M7 flow and release evidence
remain outstanding.

## Deliberate decisions recorded here

- **An unknown commit outcome never reaches the graph.** The accepted M3
  terminal set is `Complete`, `Fail`, and `Stop`. Routing `UNKNOWN` through it
  would either fail closed as `UnmappedExitOutcome` — silently converting an
  ambiguous durable outcome into a decided failure — or require an unknown
  terminal the contract does not have. The launcher therefore persists `UNKNOWN`
  before consulting the plan, and the compatibility plan declares no `UNKNOWN`
  transition. `the_compatibility_plan_routes_every_framework_exit_code` pins
  both halves of that rule.
- **The compatibility plan is authoritative for the job's terminal status,
  not for the step's outcome detail.** `JobLauncher` selects the job's
  `BatchStatus` and `ExitStatus` from the terminal the plan reaches for the
  step's persisted exit code, while `LaunchReport::outcome` keeps the finer
  `TaskletFailure` classification a terminal cannot express. A future graph in
  which a failed step reaches a `Complete` terminal therefore already produces
  the right job status.
- **The compatibility plan maps the three framework exit codes exactly** rather
  than reusing the `FAILED`/`*` sequential convenience edge. A `*` fallback
  would have completed a stopped job, which is a behavior change the
  wrapper-equivalence gate forbids.
- **Terminals are a target kind, not nodes with reserved identifiers.** A
  reserved `NodeId` prefix would have made an existing job whose step is named
  with that prefix fail to lower, which lowering must never do.
- **The canonical manifest omits computed specificity.** Specificity is derived
  from the pattern, and storing it would let a stored value disagree with the
  rule that computes it. Edges are *sorted* by it, so a change to the rule still
  changes the fingerprint.
- **`DefinitionIdentity::canonical_manifest` is now public.** The manifest
  records names, logical identifiers, revisions, schema versions, and bounded
  policy values, and the contract forbids parameters, contexts, item values,
  credentials, endpoints, and private state in it. Operators need to inspect and
  archive exactly the bytes a fingerprint covers, and golden-vector evidence
  needs the same bytes.
- **The PostgreSQL adapter now accepts manifest format 1 and 2** through one
  shared `check_manifest_format` predicate instead of hard-coding format 1.
  Nothing emits a format-2 identity into a repository yet, because this
  workstream adds no launcher for a general plan; the predicate is what keeps
  the runtime's declared support and its adapter from drifting.

## Residual limitations

- `CompiledExecutionPlan` compiles and fingerprints decision nodes and start
  controls, but no runtime reads them. A plan with more than one node cannot be
  launched: `JobLauncher` returns `LaunchError::UnsupportedPlan`.
- The wrapper-equivalence goldens are recorded against the in-memory
  repository. PostgreSQL equivalence rests on the unchanged repository command
  sequence the goldens pin plus the existing PostgreSQL suites, not on a
  separate durable trace.
- Manifest format 2 has no persisted deployment, so its migration evidence is
  the reader, the fingerprint change, and the required upgrade edge rather than
  a database rehearsal.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Regenerating a golden trace is a deliberate, reviewable fixture change:

```console
OXIDEBATCH_UPDATE_TRACE_GOLDEN=1 cargo test -p oxide-batch --test plan_equivalence
```
