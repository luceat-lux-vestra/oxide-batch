# M3 Fault-Tolerance and Listener Contract

**State:** Accepted

**Scope:** The bounded, single-threaded M3 chunk runtime on the current
ADR-0002 boxed component boundary.

This document is the canonical owner for M3 retry, backoff, skip,
rollback/no-rollback, stable failure classification, and item/retry/skip
listener behavior. The complete component model is decided
([RFC-0005](../rfcs/0005-static-and-erased-components.md), accepted
2026-08-03 and recorded as
[ADR-0008](decisions/0008-item-component-contract.md)) but not yet
implemented in production; that migration is M6 scope, starting with
[#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143).

## Compatibility baseline and deliberate differences

The Spring Batch 6.0.4 reference behavior used by this slice is:

- retry is configured per item and bounded by an explicit retry limit;
- one skip limit applies across read, process, and write skips, while the three
  phase counts remain distinct;
- the skip after the configured limit fails the step;
- item error callbacks are distinct from callbacks that confirm a skip;
- a committed skip callback occurs once for the skipped item;
- rollback/no-rollback is classified separately from exception text.

OxideBatch uses stable failure categories and phases instead of Java exception
class hierarchies. It also narrows no-rollback behavior: no-rollback may not
silently discard an item, preserve an ambiguous effect, or bypass the selected
delivery mode. These API and resource-model differences remain visible in the
feature ledger.

## Stable failure input

Policy receives a framework-owned `FaultDescriptor`, never a user error string
or erased error type. The descriptor contains only:

- `FaultPhase`: `Read`, `Process`, `Write`, `Transaction`, `Checkpoint`,
  `Listener`, or `Backoff`;
- the existing `FailureCategory`, extended without renaming current variants
  by `OptimisticConflict`, `Timeout`, `UnsupportedCapability`, and
  `UnknownCommit`;
- an opaque failure ID;
- the current retry ordinal and committed skip counts;
- whether a transaction is open and the declared delivery mode.

Categories are non-exhaustive public enums. Component adapters translate their
own typed errors at the boundary. Classification never examines `Display`,
`Debug`, source-chain text, SQL, item values, parameters, or context values.

`InvalidDefinition`, `DuplicateExecution`, `IllegalTransition`, `Cancelled`,
`Serialization`, `Invariant`, `UnsupportedCapability`, and `UnknownCommit` are
not retryable or skippable in M3. Listener phase is also never policy-eligible.
`UnknownCommit` always produces `UNKNOWN`; cancellation/stop follows the stop
contract; invariant and unsupported states fail closed.

Custom classifiers declare a bounded revision token. The token and the ordered
phase/category rules are definition-fingerprint input. Compilation rejects
duplicate or overlapping rules whose outcome would depend on registration
order. An unmatched fault means `FailAndRollback`.

## Retry and exhaustion

`RetryLimit` is the maximum number of re-invocations after the initial
component call. Zero disables retry. The M3 representation accepts
`0..=65_535`; a larger value is invalid definition input.

For one retry key, the observable sequence is:

1. invoke the component once;
2. on a retryable known failure, roll back any open transaction;
3. reserve the next retry ordinal durably;
4. emit the authoritative retry callback;
5. wait for the configured cancellable backoff;
6. invoke the component again.

When `retry_limit = N`, at most `N + 1` component calls occur without a process
crash. A reserved retry is consumed even if the process stops after reservation
and before reinvocation. Restart therefore never invokes more than `N` durably
reserved retries for one key; it may invoke fewer. The initial call can replay
after a process crash before its failure and retry decision become durable,
just as other pre-checkpoint item work can replay. Its external effects remain
subject to the declared delivery mode.

A retry key is a framework SHA-256 digest over the definition fingerprint,
step logical ID, failure phase, committed checkpoint identity, and stable
reader/output ordinal supplied by the component contract. It contains no item
value. `RetryStateLimit` bounds unresolved keys to `1..=256` per step; the
definition must choose a value and compilation rejects a larger or zero bound.
Keys sort by digest in durable state. Exhausting this capacity fails before
reserving another retry. A new key starts a new budget; an identical key
resumes the persisted ordinal.

Successful calls leave their durable keys available until the chunk commits,
because the uncommitted work may replay. A successful or skipped chunk clears
all resolved keys in the same commit that advances the checkpoint. Exhaustion
is persisted before `retry.exhausted`, preserves the last typed category, and
evaluates skip only when the skip policy explicitly accepts the same
phase/category. Otherwise the step fails.

## Backoff and stop

M3 supports deterministic `None`, `Fixed`, and integer `Exponential` backoff:

- delays are monotonic durations in `0..=24 h`;
- exponential backoff uses a nonzero integer multiplier and a finite maximum;
- the delay for retry ordinal `r` is derived only from the fingerprinted
  policy and `r`, with checked arithmetic capped at the configured maximum;
- no jitter or wall-clock input is permitted in M3;
- the test and runtime boundary injects the monotonic sleeper.

The runtime checks stop before reservation, before waiting, while waiting, and
before reinvocation. Stop during backoff cancels the wait, leaves the durable
reservation consumed, and produces `STOPPED` without another component call.
No task or timer is detached.

## Skip classification and counts

`SkipLimit` is the maximum aggregate number of committed read, process, and
write skips for one step in one job instance. It accepts `0..=u64::MAX`.
The limit check uses the inherited durable totals across restart. A limit of
`N` permits exactly `N` committed skips; the next skippable failure fails the
step.

The three phase counts remain separate and their checked sum is the aggregate
limit input. A skip becomes authoritative only when its checkpoint, context,
business writes, retry-key removal, skip callback work, counters, and
optimistic version commit. Rollback leaves all of them unchanged.

Phase-specific safety requirements are:

- a read skip requires the reader to prove that its checkpoint moved past
  exactly one failed input; otherwise repeated failure at the same position
  fails the step;
- a process skip identifies one already-read input and removes only its output;
- a write skip requires a known-rolled-back writer result identifying exactly
  one output ordinal. An unlocated, partially visible, or ambiguous write
  cannot be skipped.

The policy and durable state store phase, category, ordinals, and digests only.
They do not store failed items, record keys, error messages, or source chains.

## Rollback and no-rollback

The default for every fault is rollback. Retry always starts after a known
rollback. `UnknownCommit` is never rolled back, retried, skipped, or guessed.

M3 exposes a typed `RollbackDisposition` with `Rollback` and
`CommitSafeSkip`. `CommitSafeSkip` is valid only when all of these are true:

- the skip classifier accepts the same fault;
- the phase is read or process;
- no writer or external effect for the failed item has started;
- the reader proves forward checkpoint progress;
- the selected transaction/delivery capability can commit the remaining
  successful items and the skip atomically.

Compilation rejects a statically impossible combination. A runtime capability
mismatch fails before user work. Write, transaction, checkpoint, listener, and
unknown-commit failures always roll back or become `UNKNOWN`.

`CommitSafeSkip` still increments a skip count and invokes skip listeners. It
does not reproduce Spring's callback-free ignored exception behavior; this
deliberate divergence prevents silent data omission.

`rollback_count` counts framework rollback decisions that have a durable
acknowledgement, not every database abort caused by process death. A retry
reservation increments it in the same metadata transaction; a terminal known
rollback increments it with the terminal step update. A crash before either
decision is durable may replay the open chunk without adding a count.
`no_rollback_count` increments only in the commit that accepts a
`CommitSafeSkip`. No inferred counter changes for an unknown commit outcome.

## Item, retry, and skip listeners

M3 adds typed async listener families for read, process, write, retry, and
skip. They use the accepted boxed future representation and do not decide
classification by returning strings.

The nesting for one item is:

```text
chunk before
  read before -> reader -> read after/on-error
  process before -> processor -> process after/on-error
  write before -> writer -> write after/on-error
  retry scope and backoff, when selected
  skip callback immediately before the accepting commit, when selected
chunk after
```

Before callbacks run in registration order. Successful after callbacks and
error callbacks run in reverse order. Retry `before_retry` callbacks run in
registration order before backoff; retry completion/exhaustion callbacks run
in reverse order. Skip callbacks run in registration order immediately before
commit. The enclosing order remains job, step, chunk, item/retry/skip.

An item error callback runs for every failed component invocation, including
one later retried successfully. A skip callback runs at most once in one chunk
attempt, immediately before the accepting commit. Known rollback or crash may
cause another invocation on replay; only work enlisted in the accepting
transaction has exactly one committed effect. A non-enlisted skip-listener
effect follows its declared at-least-once or idempotent delivery mode.

These listeners are authoritative interceptors:

- a before failure prevents its component call;
- an after/error/retry/skip failure prevents an uncommitted chunk from
  committing;
- listener failures are not themselves retryable or skippable in M3;
- all already-entered reverse callbacks run so failures can be aggregated;
- a panic is caught and classified exactly like a listener error;
- the original component outcome and every listener failure remain available
  through opaque failure IDs and redacted categories.

Telemetry sinks are separate non-authoritative observers. Their failure or
panic never affects policy, transaction, or lifecycle outcome.

## Durable state and atomicity

Schema version 2 adds distinct read/process/write retry and skip counters,
`no_rollback_count`, and a bounded checksummed fault-state envelope to each
step execution. Restart copies the latest committed totals and unresolved
fault state to the new step attempt.

A retry reservation is a small metadata-only compare-and-swap transaction
performed after rollback and before backoff. It increments the phase retry and
rollback counters and the matching fault-state ordinal. A successful or
skipped chunk clears resolved fault state in the enlisted chunk transaction.
Concurrent or stale versions fail rather than spending the same ordinal twice.

Fault state format 1 is canonical JSON, at most 64 KiB, with:

- one schema ID and positive schema version;
- up to 256 unresolved retry-key digests sorted by digest;
- each key's phase, stable category, reserved retry ordinal, and policy
  revision;
- the prior committed checkpoint digest;
- a SHA-256 checksum over canonical bytes.

Unknown format/schema versions, checksum mismatch, invalid enum values,
counter overflow, an ordinal above the configured limit, or a retry key that
does not match the checkpoint is corruption or unsupported-version failure.
No component work begins.

## Fingerprint and API impact

The manifest includes retry and skip limits, retry-state capacity, backoff kind
and numeric values, phase/category classifier rules and revision, rollback
dispositions, listener logical IDs and revisions in registration order, and
fault-state schema version. Any change to an authoritative value changes the
fingerprint. Telemetry sink/exporter changes do not.

Public contracts expose only validated OxideBatch types and `BoxFuture`.
Runtime, database, serializer, timer, and telemetry SDK types remain private.

## Required evidence

Implementation issues must provide:

- unit/property tests for limits, rule ambiguity, checked counters, retry keys,
  and backoff arithmetic;
- listener order, panic, aggregation, and redaction tests;
- deterministic stop-before/during/after-backoff tests;
- read/process/write retry, exhaustion, skip, and no-rollback conformance;
- PostgreSQL atomic skip/counter/state and retry-reservation CAS tests;
- crash tests before/after reservation, rollback, skip callback, and commit;
- schema-v1-to-v2, corruption, newer-version, backup, and restore evidence;
- bounded-state and no-item-value disclosure tests.

The runtime-neutral policy, classifier, backoff, and listener contracts of this
document are implemented and evidenced by the
[M3 fault-tolerance and listener contract evidence](../project/m3-fault-contract-evidence.md).
Their integration into deterministic chunk execution — retry replay after a
known rollback, reservation ordering, stop points, phase skip classification,
capability-scoped no-rollback, item/retry/skip callbacks, and the post-decision
events — is evidenced by the
[M3 fault-tolerance runtime evidence](../project/m3-fault-runtime-evidence.md).

A retry ends the chunk attempt: the runtime rolls back, reserves the ordinal,
runs the retry scope, and replays the chunk from its in-memory buffer of
already-read inputs. Only components that have not yet succeeded are
re-invoked, so a stateful reader never rewinds.

Durable reservation, schema 2, and restart inheritance are implemented and
evidenced by the
[M3 PostgreSQL fault-durability evidence](../project/m3-postgres-fault-durability-evidence.md).
Manifest fingerprint input remains owned by the compiled-plan workstream, so the
ledger rows stay `Implemented` rather than released `Verified` until a named
release satisfies the compatibility contract's full evidence profile.
