# First Vertical Slice

**State:** Proposed  
**Target milestone:** M1 design, M2 durable implementation

## Scenario

Import a deterministic list of input records, transform each record, and write
results to PostgreSQL in chunks. Deliberately terminate the worker during the
second chunk, inspect execution metadata, restart with the same identifying
parameters, and complete without replaying the first committed chunk.

## Why this slice

It crosses the framework's highest-risk boundaries: domain identity, lifecycle
state, user component contracts, transaction enlistment, execution context,
checkpointing, crash recovery, operator visibility, and telemetry.

## Acceptance criteria

1. The first launch creates one job instance, job execution, and step execution.
2. Reusing the same identifying parameters selects the same job instance.
3. Each retry or restart attempt has a distinct execution identity.
4. A committed chunk advances its checkpoint and counters atomically.
5. A crash before commit replays only work allowed by the documented delivery
   guarantee.
6. A restart resumes from the latest committed checkpoint.
7. Launching an already completed instance is rejected.
8. Concurrent launch attempts cannot create duplicate job instances.
9. Status, exit status, counts, timestamps, and failure summaries are
   inspectable without exposing record contents.
10. Structured events identify the job, instance, execution, step, and attempt.

## Required failure injection

- reader failure before an item is returned;
- processor failure for a chosen item;
- writer failure before and during database interaction;
- process termination immediately before and after commit;
- metadata optimistic-lock conflict;
- listener failure;
- stop request during a chunk;
- database disconnect during commit.

## Non-goals

Retry/skip policy breadth, conditional flow, distributed execution, and maximum
throughput are not required in the first slice. The slice establishes
correctness boundaries before those features are layered on.
