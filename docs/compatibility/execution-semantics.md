# Execution, Restart, and Transaction Semantics

**State:** Proposed

**Decision needed:** approve invariants before runtime implementation

This document defines the minimum correctness contract. Precise Rust APIs and
database tables are implementation details unless separately documented.

## Core terms

| Term | Meaning |
| --- | --- |
| Job definition | Named, versioned arrangement of steps and flow |
| Job parameters | Typed launch inputs, each marked identifying or non-identifying |
| Job instance | Logical job occurrence identified by job name and canonical identifying parameters |
| Job execution | One attempt to run a job instance |
| Step execution | One attempt to run a step within a job execution |
| Execution context | Versioned, bounded, durable restart state scoped to a job or step execution |
| Batch status | Framework lifecycle state |
| Exit status | User-visible result used for flow decisions and process exit mapping |
| Checkpoint | Last durably committed restart position and associated counters/context |

Parameter identity is computed from canonical typed values, never display
strings or insertion order. Secrets and large payloads do not belong in job
parameters or execution context.

## Lifecycle rules

The initial status vocabulary is `STARTING`, `STARTED`, `STOPPING`, `STOPPED`,
`FAILED`, `COMPLETED`, `ABANDONED`, and `UNKNOWN`.

| From | Normally allowed to | Notes |
| --- | --- | --- |
| STARTING | STARTED, STOPPING, FAILED, UNKNOWN | Failure before user work is recorded |
| STARTED | STOPPING, STOPPED, FAILED, COMPLETED, UNKNOWN | Completion requires all required steps |
| STOPPING | STOPPED, FAILED, UNKNOWN | Stop is cooperative, not instantaneous |
| STOPPED | STARTING, ABANDONED | Restart creates new execution attempts |
| FAILED | STARTING, ABANDONED | Only when restart is permitted |
| COMPLETED | — | Terminal and not restartable |
| ABANDONED | — | Terminal and intentionally not restartable |
| UNKNOWN | FAILED, ABANDONED | Requires an explicit recovery decision |

Repository methods reject illegal transitions and stale versions. A new restart
does not mutate an old attempt into a running attempt; it creates new job and
step execution records linked to the same job instance.

Exit status is not a substitute for batch status. Listener or application code
may enrich exit status but cannot forge a lifecycle transition.

## Launch and restart

- at most one job instance exists for a job name and identifying-parameter set;
- concurrent launch requests are serialized by the repository;
- a completed or abandoned instance rejects another launch;
- failed and stopped instances may restart only when the definition permits it;
- an apparently running execution is never automatically stolen;
- recovery of `UNKNOWN` or orphaned `STARTED` work is an explicit, audited
  operator decision;
- restart uses the latest valid checkpoint for the selected step and definition
  compatibility rules.

Definition evolution and restart compatibility require a separate versioning
decision before M2.

## Chunk transaction boundary

For a transactional PostgreSQL writer, one successful chunk transaction
contains:

1. all business writes for the chunk;
2. step/job counters affected by that chunk;
3. the new execution context and checkpoint;
4. the optimistic-lock version update.

Acknowledgement and telemetry occur after commit and are not correctness
authorities. A failure before commit rolls back all four groups. A process may
die after commit but before observing success; recovery reads durable metadata.

For external or non-transactional writers, OxideBatch cannot guarantee
exactly-once effects. The contract is at-least-once delivery with application
idempotency, deduplication, or an outbox/inbox pattern documented by the
adapter. The selected guarantee is visible in configuration and diagnostics.

## Retry and skip

- retry repeats a failed operation within a bounded policy;
- skip records an item as intentionally not completed and increments a durable
  counter at the corresponding commit boundary;
- policy classification uses typed error categories, not error-message text;
- attempts and limits remain deterministic after restart;
- retry does not weaken the chunk transaction boundary;
- backoff is cancellable and testable with an injected clock.

## Cancellation, panic, and crash

- stop requests are cooperative and durable;
- blocking user code receives a documented cancellation limitation;
- a user panic is isolated at the framework boundary and classified as failure;
- forced process termination may leave an execution apparently running;
- the repository never guesses whether external side effects committed;
- recovery requires evidence and an operator decision when state is ambiguous.

## Concurrency

Optimistic locking protects execution updates. Bounded concurrency must preserve
per-step ordering only where the reader/writer contract requires it. Parallel
work must not share mutable execution context without an explicit merge rule.

No distributed lease or cross-host ownership protocol is promised for 1.0.
