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
back and retried without an intervening commit) always recomputes its
candidate from the same unmodified baseline, never a corrupted or
partially-applied one. The recomputed candidate itself is not guaranteed
identical to the discarded one -- it is a function of this replay's freshly
observed duration, which real timing can differ on; only a fully
deterministic clock (as the unit test in `completion.rs` injects, advancing
by the same fixed amount both attempts) makes the two identical. See the
`Restart safety` and `Same-process rollback safety` sections of
`completion.rs`'s `AdaptiveCompletionPolicy` docs.

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

### Corrective pass: livelock, begin/end pairing, and stream ownership

A further independent strict merge-gate review of PR #179 found three more
blockers, all fixed on top of the corrections above.

**Zero-progress livelock.** The chunk read loop consulted
`CompletionPolicy::is_complete` before this attempt had read anything, so a
policy reporting `is_complete(0) == true` made the step repeatedly commit an
all-empty chunk forever -- never reaching end-of-input, since the reader was
never actually invoked. The read loop now consults the policy only once this
attempt has accepted at least one item; see the forward-progress invariant
documented on `CompletionPolicy::is_complete`.

**Begin/end lifecycle pairing.** `end_chunk` could run without a matching
`begin_chunk` (when the transaction itself failed to begin, and when
`begin_chunk` itself panicked), and could be silently skipped after a
successful `begin_chunk` when the transaction's own rollback subsequently
failed. `chunk_runtime.rs` now threads a `CompletionPolicyAttempt` guard
through each attempt: it performs the one `begin_chunk` call, and its
`finish` method is the only path to the matching `end_chunk`, structurally
preventing either half from running without the other (a `Drop` safety net
covers any future call site that forgets to call `finish` explicitly). A
transaction-begin failure never constructs a "began" attempt at all, and a
rollback failure now reports `ChunkAttemptOutcome::Unknown` to `end_chunk`
rather than skipping it, since the transaction's fate is no longer knowable
but the policy still owes a matching `end` for the `begin` it already ran.

This exactly-once contract also had to be pushed one level down into
`CompositeCompletionPolicy` itself: its `begin_chunk`/`end_chunk` previously
just iterated members with no panic containment of their own, so a middle
member's panic could unwind past members that had already begun (or still
needed their `end_chunk`), leaking their individual lifecycle even though
the outer runtime's own containment correctly failed the attempt as a
whole. `CompositeCompletionPolicy::begin_chunk` now contains each member's
panic, and if one panics, calls `end_chunk(Unknown)` on every member
positioned before it (each of which did begin) before re-panicking so the
caller still observes this composite's `begin_chunk` as failed, exactly
like a non-composite policy's panicking `begin_chunk` would. Symmetrically,
`end_chunk` continues past a panicking member to give every remaining
member its own `end_chunk` call before re-panicking. This composes
correctly through nested composites without special-casing, since each
level provides its parent the same per-call guarantee a leaf policy does.

**Stream registration ownership.** `ChunkStep::with_completion_policy`'s
replacement logic inferred which runtime `ItemStream` registrations belonged
to the policy being replaced by comparing `ComponentStreamIdentity` values
against the previous policy's own reported registrations. A manually
registered stream (via `ChunkStep::with_item_stream`) that happened to share
an identity with a policy's registration -- transiently, before the two are
resolved into a single non-duplicate set at `ChunkJob::new`/
`FlowJob::with_chunk_step` bind time -- could therefore be silently removed
by an unrelated policy replacement, since removal matched on identity value
alone with no way to tell which mechanism had registered which entry. Each
runtime registration now carries an explicit `StreamOwner::Manual` or
`StreamOwner::Policy` tag; a policy replacement removes only entries tagged
`Policy`, regardless of identity overlap with a manually registered one.

### Completion-policy configuration is restart-relevant

`CompletionPolicy` gained `fingerprint(&self) -> String` (default: the
concrete type name, overridden by every policy in this module with its
actual configuration -- `CompositeCompletionPolicy` recurses into its
members' fingerprints, so nested structure participates too).
`ChunkComponentRevisions` gained an optional `completion_policy` revision
slot (`with_completion_policy_revision`). Whether that slot is populated
automatically depends on which constructor binds the step, and the two
differ:

- **`ChunkJob::new`** computes it automatically: it builds the compiled plan
  and the runtime `ChunkStep` from the same call, so it can hash whatever
  policy the step actually has installed into a `ComponentRevision` and fold
  it in itself. An application using this path never has to remember to
  bump anything.
- **`FlowJob::with_chunk_step`** cannot do this for the caller: a flow
  node's plan is compiled from bare `ChunkComponentRevisions` *before* the
  concrete `ChunkStep` (or the completion policy it installs) exists, so
  nothing at compile time can fold a not-yet-installed policy's fingerprint
  in. The caller must compute it explicitly with the public
  `completion_policy_revision` function and fold it into the same
  `ChunkComponentRevisions` used for *both* `FlowGraph` compilation and the
  later `with_chunk_step` call, via `with_completion_policy_revision` --
  the same up-front-declaration pattern already used for a stream revision.
  `with_chunk_step` then only *validates* that the live policy's fingerprint
  still matches what was declared (`validate_completion_policy_revision`),
  returning `FlowJobError::ComponentMismatch` when a declared revision is
  missing, stale, or absent for an installed policy.

Either way, a configuration change that alters completion semantics changes
the resulting `ComponentRevision`, so it can never be mistaken for the same
restart-compatible definition -- but on the `FlowJob` path this guarantee
depends on the caller actually having declared the current revision before
compiling the graph; an omitted or stale declaration is a build-time
`ComponentMismatch`, not a silent fingerprint gap. A step with no completion
policy installed folds in nothing on either path, so its fingerprint is
byte-for-byte identical to one built before this existed.

This is a deliberately narrow guarantee in one further respect: the
framework can only hash whatever a policy's own `fingerprint()` returns.
Every policy in this module overrides the type-name default with its actual
configuration, but a custom, application-supplied `CompletionPolicy` that
doesn't override `fingerprint()` keeps the same fingerprint across a
configuration change, and the framework cannot detect that on its own --
see `CompletionPolicy::fingerprint`'s doc comment for the restart-safety
decision this leaves with the policy author.

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
