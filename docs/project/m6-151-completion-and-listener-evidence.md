# M6 Item-Level Fault and Listener Ergonomics Evidence

**State:** Complete on merge; pending independent strict merge-gate review

**Issue:** [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151)

This record maps issue #151's exit criteria to production types and
deterministic test evidence. It completes the item/component-facing M6
surface for `FT-RETRY-001`, `FT-BACKOFF-001`, `FT-SKIP-001`,
`FT-ROLLBACK-001`, `REPEAT-POLICY-001`, and `LISTENER-ITEM-001` without
reimplementing the M3 fault-tolerance engine. It does not touch flow-level
retry/skip (M7), the M7 repeat-context/interceptor architecture, or Gate F's
KEEP decision (preserved, not revisited).

## Completion-policy implementation (`REPEAT-POLICY-001`)

`crates/oxide-batch/src/completion.rs` adds a `CompletionPolicy` trait and
four implementations, none of which replace the existing `ChunkSize` ceiling
every chunk already enforces -- `ChunkStep::with_completion_policy` installs
an *additional*, earlier-stopping decision; without one, completion is
exactly the pre-#151 `ChunkSize`-only behavior.

- **`ItemCountCompletionPolicy`** -- the `ChunkSize` bound expressed as a
  policy, so it composes inside `CompositeCompletionPolicy`.
- **`TimeCompletionPolicy`** -- a bounded (`ChunkTimeThreshold`, `1 ms..=24 h`)
  duration against an injected `oxide_batch::Clock`. No direct wall-clock read
  appears in runtime logic; `oxide_batch_test::ManualClock` (or the file-local
  `support/clock.rs` `ManualClock`) substitutes deterministically in tests.
  The enclosing `ChunkSize` ceiling still bounds buffering while the
  threshold has not elapsed.
- **`CompositeCompletionPolicy`** -- bounded in both member count
  (`MAX_COMPOSITE_MEMBERS = 32`) and nesting depth (`MAX_COMPOSITE_DEPTH = 8`,
  counting the outermost composite as depth `1`), combined via
  `CompositeMode::Any` (OR) or `::All` (AND). Both bounds are validated
  eagerly at construction -- via `CompletionPolicy::composite_depth`, a
  `#[doc(hidden)]` protocol method every leaf policy defaults to `0` and only
  `CompositeCompletionPolicy` overrides -- so a too-deep tree, direct or
  indirect, is rejected with `CompletionPolicyError::CompositeTooDeep` before
  it exists, never discovered by runtime recursion.
- **`AdaptiveCompletionPolicy`** -- a bounded (`AdaptiveBounds`) policy whose
  confirmed target chunk size adjusts toward an observed `target_duration`.
  Its authoritative decision is persisted through the existing `ItemStream`
  open/update/close contract -- the same commit-boundary mechanism
  `#144`/`#150` already built -- registered under the same
  `ComponentStreamIdentity` as both the step's completion policy and one of
  its `ItemStream`s, via a single `ChunkStep::with_adaptive_completion_policy(Arc<AdaptiveCompletionPolicy>)`
  call. No second persistence path is introduced, and no second way to
  (mis)register the two roles onto different instances exists: the method
  takes one `Arc`, derives the identity and the `StreamStateContract` from
  the policy itself (the contract's codec is this policy's own private
  implementation detail, so a caller could never reconstruct a matching one
  by hand), and wires a private `ItemStream`-delegating newtype around the
  same `Arc` -- never a public `impl ItemStream for Arc<T>` blanket, which
  would still let two *different* instances be registered by mistake.

### Speculative vs. confirmed state (post-review correction)

An independent strict merge-gate review of PR #179 found that the original
`AdaptiveCompletionPolicy::update()` mutated the same `confirmed` field
`CompletionPolicy::is_complete` reads, *before* the chunk it was computed for
had committed. A rollback therefore left that speculative value looking
authoritative for the rest of the process, and a replayed attempt recomputed
its candidate from an already-corrupted baseline instead of the true last
commit.

The corrected design splits `AdaptiveInterior` into `confirmed` (mutated only
by `ItemStream::open`, restoring the last *durable* commit, and by the new
`CompletionPolicy::end_chunk` hook below) and `pending` (written by `update`,
read by nothing else). `CompletionPolicy` gained
`end_chunk(&self, outcome: ChunkAttemptOutcome)` -- called exactly once per
chunk attempt, after its terminal outcome is known and always before the next
attempt's `begin_chunk` -- with a no-op default correct for every stateless
policy. `AdaptiveCompletionPolicy::end_chunk` promotes `pending` into
`confirmed` only on `ChunkAttemptOutcome::Committed`; every other outcome
discards it, and `begin_chunk` discards it defensively too, so `confirmed`
can never reflect work this process has not itself observed commit.

`update` is now pure with respect to `confirmed`: it only reads the baseline
and this attempt's freshly observed duration, so a replayed attempt (rolled
back and retried without an intervening commit) recomputes the identical
candidate from the same unmodified baseline -- deterministically, not merely
by convention. See the `Restart safety` and `Same-process rollback safety`
sections of `completion.rs`'s `AdaptiveCompletionPolicy` docs.

### Completion-policy panics are contained

`begin_chunk`, `is_complete`, and `end_chunk` are synchronous calls into a
public, user-implementable trait, and were previously invoked directly in
`chunk_runtime.rs`'s read loop with no panic boundary -- unlike every other
user-supplied component call in the same module. A panic in any of the three
(including one raised by a `CompositeCompletionPolicy` child, since the
composite's own dispatch is inside the same call) is now caught with the
same `catch_unwind(AssertUnwindSafe(...))` discipline the reader/processor/
writer/listener calls already use, fails the attempt through the existing
typed path (`ChunkFailure::CompletionPolicyPanic`), and never suppresses an
already-committed chunk's counts.

### Completion-policy configuration is restart-relevant

`CompletionPolicy` gained `fingerprint(&self) -> String` (default: the
concrete type name, overridden by every policy in this module with its
actual configuration -- `CompositeCompletionPolicy` recurses into its
members' fingerprints, so nested structure participates too).
`ChunkComponentRevisions` gained an optional `completion_policy` revision
slot (`with_completion_policy_revision`), populated automatically --
`ChunkJob::new` and `FlowJob::with_chunk_step` hash whatever policy the
`ChunkStep` actually has installed into a `ComponentRevision` and fold it in,
so an application never has to remember to bump anything, and can never
silently change completion semantics across a restart without the
definition fingerprint changing too. A step with no completion policy
installed folds in nothing, so its fingerprint is byte-for-byte identical to
one built before this existed.

`crates/oxide-batch/tests/postgres_completion_policy_restart.rs` proves, with
a real `PostgreSQL` transaction (`PostgresChunkTransactionManager`, not a
`NoopTransaction`-style substitute):

- a committed chunk's target is durable and distinct from the configured
  minimum a freshly constructed (never-restored) policy reports;
- a **rollback** (not just an uncommitted read) leaves that committed target
  authoritative -- a second, slower attempt's shrunk in-memory candidate is
  never even proposed to `PostgreSQL`, and the previously committed row is
  unaffected;
- a **freshly constructed** `AdaptiveCompletionPolicy` -- exactly what a real
  restart builds -- restores the committed target via `ItemStream::open`,
  distinguishing *restoring* the authoritative persisted decision from
  merely *rebuilding* the policy (which would silently default to the
  configured minimum instead).

This file does not duplicate `postgres_item_stream_crash_recovery.rs`'s
process-kill (`SIGKILL`) harness: that harness proves the
`commit_with_component_state` transaction boundary is atomic across a real
OS process crash, generically, for any conforming `ItemStream` --
`AdaptiveCompletionPolicy` introduces no new persistence path for that
harness to need duplicating against. What this file adds is specific to the
policy's own contract (rollback vs. commit, restore vs. rebuild), which the
generic harness does not exercise.

Unit coverage (`completion.rs`'s `#[cfg(test)]` module): count-policy
boundaries (minimum/normal/exact/above), time-threshold bounds validation,
composite empty/oversized rejection, composite `Any`/`All` semantics,
composite depth boundary and overflow (direct and indirect nesting),
adaptive bounds rejection (`min > max`), the `adjust_target` function's
convergence and clamping at both bounds, `AdaptiveCompletionPolicy`'s
`update`/`end_chunk` promotion-vs-discard state machine (commit, rollback,
and a discarded pending value never surviving `open`), and `fingerprint`
determinism/sensitivity for every policy family including nested composites.

`crates/oxide-batch/tests/chunk_runtime.rs`'s
`adaptive_completion_policy_integration` module drives the real
`ChunkStep::execute` path (not the policy's methods called directly):
cross-chunk growth from one `Arc<AdaptiveCompletionPolicy>` registered via
`with_adaptive_completion_policy` for both roles at once, a real
transaction-manager rollback leaving `confirmed` exactly where the last
commit left it, a second `execute` call on the same step/policy instance
recovering from that unchanged baseline, a panicking `CompletionPolicy`
(and a panicking composite child) failing with
`ChunkFailure::CompletionPolicyPanic` and no payload leak, and
`ChunkComponentRevisions`'s definition fingerprint changing with completion-policy
configuration (including nested composite structure and `AdaptiveBounds`)
while staying identical for identical configuration.
`crates/oxide-batch/tests/postgres_completion_policy_restart.rs` extends its
real-`PostgreSQL` rollback/restart evidence with an explicit `end_chunk`
call sequence and a same-process replay-then-commit cycle after the
rollback, proving the corrected state machine against a real transaction
rather than only the in-memory fixtures above.

## Listener taxonomy audit (`LISTENER-ITEM-001`)

Auditing `crates/oxide-batch/src/item_listener.rs` (`ReadListener`,
`ProcessListener`, `WriteListener`, `RetryListener`, `SkipListener`) and
`crates/oxide-batch/src/chunk_runtime.rs` (`ChunkListener`) against Spring
Batch 6.0.4's listener population found the taxonomy **already complete** as
of #150: every before/after/error/retry/skip boundary Spring exposes has an
OxideBatch equivalent. `ChunkListener::after_chunk`'s
`ChunkAttemptOutcome` parameter (`Committed`/`RolledBack`/`Stopped`/`Unknown`)
is the accepted Rust-native equivalent of Spring's separate
`afterChunkError` method -- one callback with an outcome enum, which is the
documented divergence, not a gap. #151 therefore added no new listener
trait, method, or registration point; the remaining gap was evidence, not
implementation.

`crates/oxide-batch/tests/chunk_fault_runtime.rs` adds two complete
cross-family order fixtures (the existing suite already covered each family
against the chunk lifecycle largely in isolation, or paired with one other
family):

- `chunk_read_process_write_and_retry_listeners_interleave_in_one_committed_attempt`
  -- a `ChunkListener` bracketing read/process/write listeners and a retry
  listener across a process failure, its reserved retry, and the trailing
  empty end-of-input attempt every chunk step's last attempt makes. The
  asserted order documents two non-obvious, previously-unwritten behaviors:
  a chunk-level retry does not re-invoke the reader for an item already in
  the buffer (only the failed phase re-runs), and `ChunkListener::before_chunk`/
  `after_chunk` bracket every *attempt*, not the logical chunk across its
  retries.
- `chunk_and_item_listeners_observe_a_skip_before_its_commit` -- the same
  bracket around a commit-safe process skip, asserting the chunk listener
  observes work before it, the skip callback precedes its accepting commit,
  and the chunk listener observes the commit after it lands.

## Gate F (`docs/project/m6-design-gate-evidence.md#gate-f--item-listener-allocation`)

Gate F's KEEP decision (the ADR-0002 boxed `ItemListenerSet` representation)
is preserved unmodified: no listener trait, registration API, or dispatch
shape changed. `crates/oxide-batch/tests/item_listener_allocation.rs` adds a
regression, alongside (never merged into) `chunk_allocation.rs`'s
listener-free ADR-0008 guarantee: a chunk with one registered `ReadListener`
shows allocator-call growth that scales with item count, proving the boxed
per-listener-per-item cost the KEEP decision accepted is still real and
still reported as a separate measurement, per Gate F's required scenario
naming (`registered_listener_cost_is_reported_separately_from_component_cost`).
The full Gate H real-component performance protocol remains owned by #153,
per the design-gate evidence.

## Retry/skip/rollback divergence audit (`FT-RETRY-001`/`FT-SKIP-001`/`FT-ROLLBACK-001`)

Each row's current "Known divergence" text was reviewed against the M5 exit
evidence's "expand in M6-M11" note and classified:

| Row | Divergence text | Classification |
| --- | --- | --- |
| `FT-RETRY-001` | "a crash can replay the pre-decision initial call or consume an uninvoked reservation" | M3-engine-internal: durable retry-reservation replay-on-crash mechanics, not an item/component-facing gap. Unchanged. |
| `FT-SKIP-001` | "crash during a pre-commit skip callback may replay the callback" | M3-engine-internal: a commit-boundary replay characteristic, not a missing callback. Unchanged. |
| `FT-ROLLBACK-001` | "No-rollback is capability-scoped and still records a skip; retry and terminal known rollbacks are counted durably without double-counting" | Already satisfied: this text describes implemented, evidenced behavior, not an open gap. Unchanged. |

No category-2 (item/component-facing, #151-owned) divergence was found for
these three rows; #151 therefore made no changes to the M3 fault engine's
retry, skip, or rollback semantics. `FT-BACKOFF-001` is `Verified` and was
not touched.

## Scope confirmation

The M3 fault-tolerance engine (`crates/oxide-batch-core/src/fault.rs`,
`crates/oxide-batch/src/fault_state.rs`, and the chunk-runtime retry/skip/
rollback classification in `chunk_runtime.rs`) was not modified by this
issue. No second fault engine, chunk loop, component-state mechanism, or
side-channel persistence path was introduced.

## Executable evidence

- `crates/oxide-batch/src/completion.rs` (`#[cfg(test)]` unit suite)
- `crates/oxide-batch/tests/chunk_fault_runtime.rs` (cross-family order
  fixtures, plus the existing M3/M6 fault-tolerance suite, unmodified in
  substance)
- `crates/oxide-batch/tests/chunk_runtime.rs`'s
  `adaptive_completion_policy_integration` module (real `ChunkStep::execute`
  growth/rollback/recovery/panic-containment/fingerprint evidence)
- `crates/oxide-batch/tests/item_listener_allocation.rs` (Gate F regression)
- `crates/oxide-batch/tests/postgres_completion_policy_restart.rs`
  (`PostgreSQL`-backed adaptive-decision commit/rollback/replay/restart
  evidence)
