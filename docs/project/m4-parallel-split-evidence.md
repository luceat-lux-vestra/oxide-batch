# M4 Bounded Parallel-Split Runtime Evidence

**State:** Partial implementation (unreleased)

**Issues:** [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)
and [#81](https://github.com/luceat-lux-vestra/oxide-batch/issues/81)

**Date:** 2026-08-03

This record covers the executable runtime slice of the accepted M4 bounded
local-scale contract: launch-scoped tasklet factories for split branches,
finite concurrent polling, owned child draining, deterministic status/exit
aggregation, durable join decisions, and completed-branch reuse. It does not
claim complete parallel-step or partition execution.

Issue #80 delivered the runtime; issue #81 added the branch-concurrency
ceiling, launch-time pool revalidation, panic conversion, `DrainSiblings`,
parent-stop, completion-order, sequential-fallback, repeated-drain, and
PostgreSQL process-kill evidence recorded below.

## Implemented boundary

- `FlowJob` accepts manifest format 3 when every embedded tasklet branch step
  has a `TaskletStepFactory`. The factory closure is `Send + Sync`, declares
  its expected step name, constructs one launch-scoped component instance, and
  is invoked before durable launch. A panic or name mismatch fails before a job
  execution is created.
- `FlowLauncher` polls at most `MaxParallelBranches` branch futures at once.
  Branch steps remain sequential, every child future is retained by the parent,
  and all results are drained before the split publishes an outcome. No
  detached task or process-global runtime is created.
- `CancelSiblings` requests cooperative cancellation after a failed or
  `UNKNOWN` child; `DrainSiblings` continues polling siblings. Parent/process
  stop propagates to the same child token. Both policies still join every
  branch.
- Results are sorted back into declared branch order before aggregation.
  Status severity is `UNKNOWN > FAILED > STOPPED > COMPLETED`; the first branch
  at that severity supplies the exit status and failure classification.
- A completed logical branch step is loaded from durable state and not invoked
  again on restart. The join input digest covers the plan fingerprint, join,
  declared branch order, terminal status/exit, and every durable child-step
  observation.
- The selected join transition is appended as `SPLIT_AGGREGATE`. In-memory and
  PostgreSQL validation accept it only for a format-3 structural join with a
  manifest-declared matching target. A reused join decision must match the
  exact prior digest and observation.
- The unreleased schema-3 migration extends the schema-2 flow-decision check
  constraint with `SPLIT_AGGREGATE` without rewriting existing decisions.
- An `UNKNOWN` branch makes the job attempt `UNKNOWN` without selecting a
  downstream transition. A stopped aggregate likewise ends the attempt as
  `STOPPED`.

## Named executable evidence

| Scenario | Evidence |
| --- | --- |
| Owned bounded concurrency and join | [`parent_joins_every_branch_before_aggregating`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Declared-order exit aggregation | [`branch_aggregation_is_deterministic_in_declared_order`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Completed child reuse on restart | [`completed_branch_is_reused_on_restart`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Unknown propagation | [`unknown_branch_makes_the_parent_unknown`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Cancel-siblings join | [`cancel_siblings_still_joins_every_branch`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Drain-siblings reaches every boundary | [`drain_siblings_lets_every_branch_reach_its_boundary`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Branch panic to durable failure | [`branch_panic_is_durable_failure`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Branch-concurrency ceiling and drained scope | [`branch_concurrency_never_exceeds_manifest_bound`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Repository connection ceiling | [`repository_capacity_is_revalidated_before_launch`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Parent stop cancels and joins | [`parent_stop_cancels_and_joins_active_branches`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Sequential-fallback equivalence | [`concurrency_one_matches_parallel_durable_observations`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Completion-order independent aggregate | [`completion_order_does_not_change_failed_aggregate`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| Repeated task-scope drain | [`repeated_split_runs_leave_no_active_branch`](../../crates/oxide-batch/tests/local_split_runtime.rs) |
| PostgreSQL process kill and completed-branch reuse | [`committed_branch_is_reused_after_process_kill_and_recovery`](../../crates/oxide-batch/tests/postgres_local_split_crash_recovery.rs) |

The PostgreSQL process-kill target runs on PostgreSQL 15 and 18 in CI. It exits
the worker process inside the second branch after the first branch committed
and before the `SPLIT_AGGREGATE` join decision exists, then proves that an
audited recovery plus restart reuses the committed branch attempt, runs only
the incomplete branch, and appends the join decision.

## Normalized equivalence and digest boundary

`concurrency_one_matches_parallel_durable_observations` runs the same job name,
plan shape, inputs, and injected clock against two isolated repositories and
compares durable job status/exit status, every step row's status, exit status,
and counters, and every flow decision's sequence, source, kind, observed
outcome, and target. Step rows are compared in logical-name order rather than
insertion order, matching the contract's per-child ordering rule.

The decision `input_digest` is excluded from that one comparison because
`MaxParallelBranches` is restart-relevant and participates in the plan
fingerprint, so the two runs legitimately record different digests.
`repeated_split_runs_leave_no_active_branch` closes that gap by comparing 32
runs of an identical plan, including every digest, byte for byte. Every run
behind both fixtures also asserts that peak branch occupancy stayed within the
configured ceiling and that no branch remained active when the launch returned,
so a repeated run cannot leak an owned child.

Peak occupancy alone is an upper bound, not proof of real concurrency;
`branch_concurrency_never_exceeds_manifest_bound` supplies that separately by
holding four branches against a two-permit barrier and observing exactly two
active branches.

## Reviewed dispositions

- **Aggregate counter publication.** A split has no parent step row. Each
  branch step owns its own durable counters, and the join publishes a decision
  rather than a summed execution. The sequential-fallback and repeated-drain
  fixtures therefore compare per-branch counters directly. No aggregate counter
  row is introduced, because no accepted contract requires one at this
  boundary.
- **`ConcurrentUse` capability.** The contract requires compilation to reject a
  component instance shared across concurrent children. The M4 split API makes
  that state unrepresentable instead: `with_split_tasklet_factory` binds one
  launch-scoped factory per branch step, and each branch step is a distinct
  plan node, so no instance is reachable from two branches. A declaration
  carrying no decision would only reserve future design, so none is added here.
  The capability becomes necessary when a later milestone admits a component
  reachable from more than one child.

## Remaining boundary

- chunk-step factories inside split branches, which the local-scale contract
  permits for partition workers but which no accepted M4 gate requires for
  split branches;
- bounded load, cancellation-latency, memory/connection/queue ceiling,
  telemetry-overhead, and soak/leak reports required by the
  [performance plan](../engineering/performance-plan.md) M4 section, which the
  M4 exit gate owns.

`SCALE-PARSTEP-001` therefore remains unreleased `Partial`. No remote
execution, transport, lease, fencing, work stealing, local chunking, or
RFC-0005 static hot path was added.
