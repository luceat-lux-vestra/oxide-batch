# M4 Bounded Local-Scale Contract

**State:** Accepted

**Governing decisions:**
[RFC-0004](../rfcs/0004-compiled-execution-plan.md),
[ADR-0002](decisions/0002-execution-model.md), and
[ADR-0005](decisions/0005-compiled-execution-plan.md)

This document is the canonical contract for the M4 parallel-step and local
partition subset. It fixes the exact graph subset, assignment identity,
durable restart state, deterministic aggregation, resource budgets, and
sequential-fallback equivalence that dependent implementation may rely on.

The subset is deliberately narrower than M7 general flow, M10 performance
architecture, and M11 distributed execution. Nothing here depends on the
proposed [RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md) worker
protocol, and nothing here may be described as a distributed guarantee.

## Graph subset

M4 extends the accepted acyclic M3 graph in
[basic flow](basic-flow.md) with exactly two node kinds.

### Split node

- A split node has `2..=8` branches and exactly one join node.
- Each branch is a linear sequence of `1..=8` M3 step nodes.
- A branch contains no decision node, no nested split, no partitioned step,
  and no terminal node. Only the join node leaves the split.
- The join node has no transition logic of its own beyond the aggregated
  outcome rule below.
- A split node may not be the entry node.

### Partitioned step node

- A partitioned step node declares one worker step definition, one
  partitioner, and one aggregation rule.
- The worker step definition is an ordinary M3 tasklet or chunk step.
- A partitioned step contains no split, no nested partitioned step, and no
  decision node.
- Partition count is `1..=1024`.

Compilation rejects any other combination, any cycle, any branch that
re-enters another branch, and any budget that is zero, contradictory, or
unbounded. Diagnostics remain bounded and carry no user data.

## Local assignment identity

Partition identity is `(job_execution_id, step_logical_id, partition_key)`.

- `partition_key` is a bounded UTF-8 name of `1..=128` bytes, compared byte
  for byte, produced by the partitioner.
- Keys are unique within one partitioned step execution; a duplicate key is a
  typed compilation-time or plan-time failure, never a silent merge.
- Branch identity inside a split is the branch's first logical step ID, which
  is already unique in the plan.

This identity keeps a future-compatible meaning for remote assignment, but M4
correctness depends only on local uniqueness and durable rows. There is no
lease, fencing token, heartbeat, worker registration, or transport envelope.

## Partitioning determinism and durable restart state

The partitioner runs at most once per partitioned step execution.

1. On first execution the partitioner receives the plan fingerprint, the job
   instance identity, the logical step ID, and the configured partition count.
   It must be deterministic for that input.
2. Its output is a bounded set of `(partition_key, partition_context)` pairs.
   Each partition context is a versioned envelope of at most `4 KiB` with a
   32-byte checksum and carries no credential, endpoint, SQL, or unbounded
   payload.
3. The complete partition plan commits in one transaction before any worker
   starts, as `ob_step_partition` rows with status `STARTING`.
4. On restart the persisted plan is reused. The partitioner is never
   re-invoked, so a non-deterministic partitioner cannot change identity or
   work assignment after a crash.

Each partition row carries its own status, exit code, counters, optimistic
version, worker step-execution reference, and partition context. A partition
that reached `COMPLETED` is not rerun on restart. A partition that is
`FAILED`, `STOPPED`, or `STARTING` without a durable result is retried as a
new worker attempt under the ordinary M3 start controls. A partition that is
`UNKNOWN` blocks restart of the parent step until recovery resolves it.

## Deterministic aggregation

Aggregation never depends on completion order, thread scheduling, or wall
clock.

- Partition results aggregate in `partition_key` byte order. Branch results
  aggregate in the plan's declared branch order.
- Counters sum in that fixed order.
- The aggregate batch status is the most severe child status under the fixed
  total order `UNKNOWN > FAILED > STOPPED > COMPLETED`.
- The aggregate exit status is the exit status of the first child, in the same
  fixed order, whose batch status equals the aggregate batch status.
- Any `UNKNOWN` child makes the parent step `UNKNOWN`. The parent never
  resolves ambiguity on a child's behalf.
- Aggregation commits with the parent step's terminal lifecycle update, after
  every child result is durable.

A partial aggregate is never published. If the parent cannot observe every
child result, it records the drain outcome from
[shutdown and recovery](shutdown-and-recovery.md) instead of aggregating.

## Structured ownership and cancellation

- The parent step owns one task per branch or partition worker. There are no
  detached tasks and no process-global runtime.
- Cancellation propagates from the parent to every child. Children stop
  cooperatively under the same in-flight chunk policy as a sequential step.
- The parent joins every child before writing its terminal state or reporting
  `DrainIncomplete`.
- A child panic is classified as a typed framework failure of that child. It
  does not unwind into the parent, does not cancel sibling children implicitly
  unless the declared failure policy says so, and never escapes the runtime.
- The declared failure policy is `CancelSiblings` (default) or
  `DrainSiblings`. Both join every child; they differ only in whether siblings
  are cancelled immediately or allowed to reach their next boundary.

## Component thread-safety validation

Concurrency is validated, never assumed.

- Every component used inside a branch or partition worker must be `Send` and
  `'static`.
- Each partition worker and each branch receives components constructed by a
  factory, so instances are not shared by default.
- Sharing one component instance across children requires an explicit
  `ConcurrentUse` capability declaration. Compilation rejects a shared
  component that lacks it, as already required by the
  [execution-plan architecture](execution-plan.md).
- M4 keeps the accepted ADR-0002 boxed component boundary. It does not
  introduce the static hot path proposed by
  [RFC-0005](../rfcs/0005-static-and-erased-components.md) and does not use
  local scale to preempt M6 or M10.

## Resource budgets

Every budget is finite, validated at compilation, and re-validated at launch.

| Budget | Bounds and default |
| --- | --- |
| `MaxParallelBranches` | `1..=8`, default equals the branch count |
| `MaxPartitionWorkers` | `1..=64`, default `4` |
| Partition count | `1..=1024` |
| Partition context size | at most `4 KiB` per partition |
| Aggregation buffer | at most one bounded result record per partition or branch |
| In-flight chunks per parent step | at most one per active child |
| Repository connections | `MaxPartitionWorkers + 1` or `MaxParallelBranches + 1`, whichever applies |

Launch validates that the configured `PoolSize` can supply the required
connections and otherwise fails with a typed `InsufficientPoolCapacity`. A
budget is never silently reduced, and concurrency never exceeds the validated
budget.

There is no work stealing, dynamic repartitioning, adaptive sizing, unbounded
queue, or implicit blocking-thread growth. Blocking components continue to use
the accepted bounded blocking adapter.

## Sequential fallback equivalence

Setting `MaxParallelBranches = 1` and `MaxPartitionWorkers = 1` produces the
canonical sequential execution. The contract is that concurrent and sequential
runs of the same plan, inputs, and injected clocks produce identical
normalized observations:

- the same durable rows for job execution, step executions, partitions, flow
  decisions, and counters;
- the same aggregate batch status, exit status, and summed counters;
- the same checkpoint contents at every committed boundary;
- the same callback set, with ordering compared per child rather than
  globally.

Only telemetry interleaving and wall-clock durations may differ. Any other
divergence is a defect in the concurrent path, never an accepted difference.

## Manifest and schema impact

- Manifest format 3 adds the split node, the join node, the partitioned step
  node, the partitioner and aggregation identity, and the budgets above.
  Budgets that change assignment identity or aggregate meaning are
  restart-relevant and participate in the fingerprint.
- Formats 1 and 2 remain readable and their bytes are never rewritten. Moving
  a persisted definition to format 3 requires one direct compatibility edge.
- Schema 3 adds `ob_step_partition` as specified in the
  [physical metadata model](postgres-physical-metadata-model.md) and the
  [schema-3 migration](../operations/migrations/0003-operations-and-local-scale.md).

## Scope boundary

M4 local scale excludes remote steps, remote partitioning, remote chunking,
worker registration, transports, leases, fencing, multi-threaded item
processing inside one chunk, local chunking, dynamic partitioning, work
stealing, adaptive optimization, nested splits, splits inside partition
workers, and columnar fast paths. Those remain M7, M10, and M11 scope.

## Evidence

Production implementation requires:

- invalid-graph and budget-rejection tests for every rejected combination;
- deterministic aggregation tests, including seeded permutations of child
  completion order that must produce identical aggregates;
- durable restart tests proving completed partitions are not rerun, the
  partitioner is not re-invoked, and an `UNKNOWN` partition blocks restart;
- crash tests at partition-plan commit, worker commit, and aggregation commit
  boundaries on PostgreSQL 15 and 18;
- cancellation and ownership tests proving every child is joined and no task
  is detached, including panic and `DrainSiblings` cases;
- compile-fail tests for non-`Send` components and shared components without
  `ConcurrentUse`;
- sequential-fallback equivalence tests comparing normalized durable state and
  callback traces;
- bounded-resource evidence for connections, memory, queue depth, and task
  count at `1`, `10`, and the largest configured worker count.
