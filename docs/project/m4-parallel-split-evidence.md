# M4 Bounded Parallel-Split Runtime Evidence

**State:** Partial implementation (unreleased)

**Issue:** [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)

**Date:** 2026-08-03

This record covers the first executable runtime slice of the accepted M4
bounded local-scale contract: launch-scoped tasklet factories for split
branches, finite concurrent polling, owned child draining, deterministic
status/exit aggregation, durable join decisions, and completed-branch reuse.
It does not claim complete parallel-step or partition execution.

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

## Remaining issue #80 boundary

- chunk-step factories inside split branches and explicit shared-component
  `ConcurrentUse` validation;
- aggregate counter publication, focused cancellation/panic/sibling-policy
  fixtures, sequential-fallback comparison, PostgreSQL process-kill coverage,
  and task/memory/connection/soak ceilings;
- runtime use of the committed partition plan, bounded partition workers,
  completed-result reuse, `UNKNOWN` blocking, and atomic parent aggregation.

`SCALE-PARSTEP-001` therefore remains unreleased `Partial`, and
`SCALE-LOCALPART-001` is unchanged by this runtime slice. No remote execution,
transport, lease, fencing, work stealing, local chunking, or RFC-0005 static
hot path was added.
