# M6 Item Pipeline Configuration Ergonomics Evidence

**State:** Complete on merge; pending independent strict merge-gate review

**Issue:** [#152](https://github.com/luceat-lux-vestra/oxide-batch/issues/152)

This record maps issue #152's exit criteria to production types and
deterministic test evidence. It adds an application-facing builder over the
existing ADR-0008 chunk architecture -- `ChunkStep`, `ChunkComponentRevisions`,
`ChunkJob`, `FlowJob` -- for both statically typed and `Boxed*` dynamically
erased item pipelines, per `CONFIG-CHUNK-001`/`CONFIG-ERASED-001`. It does not
introduce a second execution path, a general configuration framework, or M7
scope/late-binding/expression-language capability.

## Investigation: the actual construction ceremony

Before designing a public API, current construction was inventoried against
the real call sites used throughout #143-#151 (`crates/oxide-batch/src/chunk_runtime.rs`,
`crates/oxide-batch-core/src/definition.rs`, `crates/oxide-batch/src/item_components/`,
and `crates/oxide-batch/tests/chunk_runtime.rs`), across the twelve scenarios
issue #152 names: basic typed construction, `ItemStream` registration, fault
tolerance/listeners, a `CompletionPolicy`, first-party decorators/composites,
delimited/CSV, JSON/JSONL, PostgreSQL, multi-resource, `Boxed*` construction,
`ChunkJob`, and a chunk step bound into a `FlowJob`.

The concrete defect, found consistently across every stateful component
(PostgreSQL cursor reader, multi-resource reader/writer, delimited
reader/writer, and the `AdaptiveCompletionPolicy`'s own state): a component's
`ComponentStreamIdentity` had to be typed **twice**, into two independently
chained builders --

- `ChunkComponentRevisions::with_stream_revision(identity, revision)` (the
  restart-relevant *definition* side), and
- `ChunkStep::with_item_stream(identity, stream, contract)` (the *runtime*
  side) --

with nothing at compile time proving the two values named the same
namespace. `chunk_runtime.rs`'s own `validate_stream_registrations` exists
specifically to catch this mismatch, but only at `ChunkJob::new` or
`FlowJob::with_chunk_step` time, as an opaque `DefinitionError`, not as a
compile error or a construction-time impossibility.

A second, related defect: installing a `CompletionPolicy` required a
separate, easy-to-forget call to the free function
`completion_policy_revision`, folded by hand into the revisions object via
`with_completion_policy_revision` -- required only on the `FlowJob` path
(`ChunkJob::new` already folds it automatically), but with no framework
assistance distinguishing which path needed it.

A third, narrower finding: no first-party, non-test production
`ChunkCompletion` implementation existed. `ChunkCompletion::after_commit`
exists only to acknowledge a durable commit without ever becoming a
correctness authority, so a no-op is always valid, yet every pipeline that
did not need post-commit notification still had to hand-write one
(`crates/oxide-batch-test/src/transactions.rs`'s test-only `NoCompletion`,
and a hand-rolled `NoopCompletion` in `object_store.rs`'s own test module,
were the only prior examples).

Two things the investigation confirmed were **already ergonomic** and
therefore left untouched: converting a typed component into `BoxedReader`/
`BoxedProcessor`/`BoxedWriter` is a single `::new` call already, and
`ChunkJob::new`'s single-call construction (building the compiled plan and
the runtime step together) is already the best case the architecture
allows -- the asymmetry with `FlowJob::with_chunk_step`'s explicit
predeclaration requirement is structural (a flow node's plan is compiled
from bare `ChunkComponentRevisions` before the concrete `ChunkStep`, or the
policy it installs, exists), documented, and out of scope to "simplify," per
issue #152 section 3.3.

## API: `ChunkPipelineBuilder`

`crates/oxide-batch/src/chunk_builder.rs` adds one generic builder,
`ChunkPipelineBuilder<I, O, R, P, W>`, plus one safe-default type,
`NoopChunkCompletion`. The builder holds a live `ChunkStep<I, O, R, P, W>`
and its matching `ChunkComponentRevisions`; every method except
`with_stream` is a **non-duplicating forward** onto the identical `ChunkStep`
method of the same name (`with_completion_policy`,
`with_adaptive_completion_policy`, `with_chunk_listener`,
`with_item_listeners`, `with_fault_runtime`, `with_listener`) -- none of
`ChunkStep`'s existing validation or lifecycle logic (policy-replacement
stream ownership, delivery-mode checks, restart-compatibility checks) is
reimplemented.

- **`ChunkPipelineBuilder::with_stream(identity, stream, contract, revision)`**
  consumes `identity` once and applies it to both
  `ChunkStep::with_item_stream` and
  `ChunkComponentRevisions::with_stream_revision`, closing the primary
  defect above by construction rather than by an additional validation
  layer.
- **`ChunkPipelineBuilder::revisions()`** computes the restart-relevant
  `ChunkComponentRevisions`, including a completion-policy revision derived
  automatically from whatever policy is currently installed. This reuses
  `ChunkJob::new`'s own internal computation
  (`crate::chunk_runtime::completion_policy_component_revisions`, widened
  from private to `pub(crate)` for this purpose -- not duplicated) rather
  than re-deriving the fingerprint. `ChunkPipelineBuilder::flow_step_components()`
  is a thin convenience returning the `StepComponents::Chunk` value a
  `FlowGraph` node needs from the same computation.
- **`ChunkPipelineBuilder::build()`** returns `(ChunkStep<I, O, R, P, W>,
  ChunkComponentRevisions)` as a plain tuple (an initial draft introduced a
  public `ChunkPipelineParts` alias for this return type solely to satisfy
  clippy's `type_complexity` lint; an independent review correctly flagged
  that as unmotivated new public vocabulary, so it was removed in favor of a
  local `#[allow(clippy::type_complexity)]` on `build()` itself), ready for
  `ChunkJob::new` or `FlowJob::with_chunk_step`.
  **`ChunkPipelineBuilder::build_chunk_job()`** is a pure forward onto
  `Self::build` and `ChunkJob::new`.
- The `ChunkJob`-vs-`FlowJob` asymmetry is **preserved, not weakened**: a
  `FlowJob`-bound step still requires the caller to call
  `ChunkPipelineBuilder::revisions` (or `flow_step_components`) *before*
  compiling the enclosing `FlowGraph`, and to reuse that exact value for the
  later `with_chunk_step` binding call. The builder removes the hand-folding
  ceremony, not the ordering requirement -- `with_chunk_step`'s existing
  `validate_completion_policy_revision` and `validate_stream_registrations`
  checks are untouched and still authoritative.

One small additive method, `ChunkStep::with_completion` (mirroring
`ChunkStep`'s other post-construction `with_*` methods), was added to
`crates/oxide-batch/src/chunk_runtime.rs` so `ChunkPipelineBuilder` can
install `NoopChunkCompletion` as a default and let a caller override it,
without duplicating `ChunkStep`'s internal field storage. It does not change
`ChunkStep::new`'s existing signature or any other public constructor --
purely additive, per issue #152 section 11.

### Typed vs. Boxed

`ChunkPipelineBuilder<I, O, R, P, W>` is generic exactly like `ChunkStep`
itself. `BoxedReader<I>`/`BoxedProcessor<I, O>`/`BoxedWriter<O>` already
implement the plain `ItemReader`/`ItemProcessor`/`ItemWriter` traits
(ADR-0008: erasure is a concrete type, not a second trait), so the identical
builder accepts them with **no special-casing** -- no second builder type,
no typed-looking facade over dynamic dispatch internally. A typed and a
`Boxed*` instantiation built from the same component revisions produce
byte-identical restart fingerprints
(`typed_and_boxed_pipelines_share_one_fingerprint`, below), which is the
concrete evidence that erasure remains a representation decision made where
a handle is constructed, never a second execution path. The module's own
rustdoc documents the choice: typed by default for static type safety and
zero per-item allocation, `Boxed*` only when runtime selection, name
resolution, or heterogeneous storage is the actual requirement -- never as a
default merely because its type signature is shorter.

### Configuration validation

`ChunkPipelineBuilder` does not reimplement validation `ChunkStep`,
`ChunkComponentRevisions`, `ChunkJob`, or `FlowJob` already own (issue #152
section 9's "do not duplicate validations already authoritatively owned by
lower layers"). A missing/duplicate stream registration, a delivery-mode
disagreement between an installed `FaultRuntime` and the declared restart
contract, and a stale or missing completion-policy revision on the `FlowJob`
path all surface exactly as they do for a hand-assembled pipeline --
`delivery_mode_mismatch_is_rejected` and `duplicate_stream_identity_is_rejected`
below are the evidence that this builder neither weakens nor bypasses those
checks.

## M7 boundary

No scope/late-binding system, expression language, external configuration
file format, dependency-injection container, or general configuration DSL
was introduced. `ChunkPipelineBuilder` lowers to exactly the existing public
constructors (`ChunkStep::new` and its `with_*` methods,
`ChunkComponentRevisions::new` and its `with_*` methods, `ChunkJob::new`,
`FlowJob::with_chunk_step`); it adds no new configuration class beyond what
`docs/architecture/configuration-model.md` already defines, and no new
runtime failure mode.

## Rejected alternative

An earlier draft had the builder hold only "raw" reader/processor/writer/
transaction/completion fields and re-implement `ChunkStep`'s stream,
listener, and fault-tolerance bookkeeping itself, deferring `ChunkStep`
construction to `build()`. This was rejected: it would have required
re-deriving `ChunkStep::with_completion_policy`'s policy-replacement stream
ownership logic (the `StreamOwner::Manual`/`StreamOwner::Policy` tagging
#151's corrective pass added) a second time, risking exactly the kind of
duplicated, independently-maintained logic issue #152 section 3.1 forbids.
Holding a live `ChunkStep` and forwarding to it instead required exactly one
small additive method (`ChunkStep::with_completion`) and reuses every other
existing method unchanged.

## Independent strict review findings and fixes

Two independent strict review passes were run against the PR before merge:
`/code-review --level xhigh` against PR #180, and a separate, manual
strict-review pass. Both converged on the same core defect from different
angles; the second pass additionally caught that the first fix was correct
but incomplete, and raised an evidence-completeness gap. All findings below
were fixed before merge except one explicitly deferred cleanup.

### Round 1 (`/code-review --level xhigh`)

- **Fixed (later found incomplete -- see Round 2) -- `AdaptiveCompletionPolicy`
  was unusable through the builder.** `ChunkStep::with_completion_policy`
  auto-registers a stream-registering policy's own `ItemStream` on the
  *runtime* side, but `ChunkPipelineBuilder::with_completion_policy` had no
  way to declare the matching *definition*-side revision, so installing
  `AdaptiveCompletionPolicy` -- the one first-party policy whose
  `stream_registrations()` is non-empty -- deterministically failed
  `build`/`build_chunk_job` with `DefinitionError::RuntimeStreamNotDeclared`.
  The existing integration test only exercised `ItemCountCompletionPolicy`,
  whose `stream_registrations()` is the trait's empty default, so the gap
  went unexercised. The first fix added
  `ChunkPipelineBuilder::with_completion_policy_stream_revision(identity,
  revision)`, letting a caller declare the policy's own stream revision.
- **Fixed -- `pub mod chunk_builder` could let a future public item bypass
  the facade's re-export gate.** Unlike `item_components` (a catalog module
  whose contents are deliberately browsed via its own namespace),
  `chunk_builder`'s intent was a narrow, root-gated surface. Making the
  module itself `pub` meant a later contributor adding any other `pub` item
  inside it would silently become public API without passing through
  `lib.rs`'s explicit `pub use` gate the rest of the crate relies on. Fixed
  by moving the module's extensive design-rationale prose onto
  `ChunkPipelineBuilder`'s own doc comment (rustdoc inlines a private
  module's re-exported item docs at the re-export site, so nothing is lost)
  and reverting `chunk_builder` to a private module.
- **Fixed -- a redundant `size: ChunkSize` field.** The builder kept its own
  copy of the chunk size only because `ChunkStep` exposed no accessor,
  creating a second source of truth that a future size-mutating builder
  method could silently desync. Fixed by adding `ChunkStep::size()` (a small
  additive accessor mirroring the existing `ChunkStep::name()`) and removing
  the duplicate field.
- **Documented, not changed -- repeated `CompletionPolicy::fingerprint()`
  calls.** `revisions()`, `flow_step_components()`, and `build()` each
  recompute a live policy's fingerprint fresh rather than caching one value.
  This relies on the same purity contract `completion_policy_revision`'s
  documentation already requires of every `CompletionPolicy` implementor,
  and mirrors `FlowJob::with_chunk_step`'s own pre-existing declare-then-
  validate pattern (one call at graph-compile time, an independent second
  call at bind time) -- not a new assumption this builder introduces. Now
  stated explicitly in `revisions()`'s rustdoc rather than left implicit.
- **Deferred -- consolidating existing hand-rolled no-op `ChunkCompletion`
  fixtures onto `NoopChunkCompletion`.** `oxide-batch-test`,
  `tests/support/chunk_fixture.rs`, and `item_components/object_store.rs`'s
  test module each already hand-roll a structurally identical no-op
  `ChunkCompletion` predating this issue. Consolidating them is a genuine,
  low-risk cleanup opportunity, but touches files outside #152's diff scope
  (issue #152 section 16 excludes unrelated cleanup); left as a follow-up
  rather than folded in here.

### Round 2 (manual strict review against the Round 1 HEAD)

- **Fixed -- Round 1's fix mutated `ChunkComponentRevisions` directly with no
  way to remove a declaration on policy replacement.** Calling
  `with_completion_policy_stream_revision` wrote straight into the builder's
  `revisions` field. Replacing an installed policy (`with_completion_policy`
  called again with a different policy) correctly removes the *previous*
  policy's runtime stream registration on the `ChunkStep` side (via
  `StreamOwner::Policy` tagging, from #151's corrective pass) but the
  builder had no matching removal for the stale *declaration* on the
  `ChunkComponentRevisions` side, since that type exposes no removal method.
  A second, different policy installed afterward would then leave the first
  policy's now-orphaned declaration in `revisions`, which
  `validate_stream_registrations` would reject with
  `DefinitionError::DeclaredStreamMissingRuntime` (a declared revision with
  no matching runtime registration) -- unless the caller happened to
  re-declare the exact same identity, which is not guaranteed and not
  checked. The review additionally noted this generalizes: a
  `CompletionPolicy` nested inside a `CompositeCompletionPolicy`, or any
  custom stateful policy, hits the identical gap, since the original fix's
  reasoning (and bug) was not specific to `AdaptiveCompletionPolicy`.
  **Fix:** policy-owned stream revisions now live in their own builder field
  (`completion_policy_stream_revisions: BTreeMap<ComponentStreamIdentity,
  ComponentRevision>`), separate from the manually-declared streams in
  `revisions`. `with_completion_policy`/`with_adaptive_completion_policy`
  clear this map on every call -- mirroring `ChunkStep::with_completion_policy`'s
  own replacement semantics exactly -- and `revisions()` folds the current
  map's entries into a fresh `ChunkComponentRevisions` on every call, so a
  stale entry can never persist past a policy replacement, and a manually
  declared stream revision (via `with_stream`) is never touched by one. This
  requires no removal method on `ChunkComponentRevisions` at all. Covered by
  three new tests:
  `completion_policy_replacement_discards_the_previous_policys_stream_revision`
  (the exact reported scenario), `adaptive_completion_policy_nested_in_a_composite_builds`
  (the composite generalization), and
  `flow_job_binds_an_adaptive_completion_policy_declared_through_the_builder`
  (the `FlowJob` path with a stateful policy, which Round 1's tests did not
  cover).
- **Fixed -- the evidence-completeness gap the review named directly.** Issue
  #152 section 8 requires exercising the configuration surface against
  "JSON/JSONL; PostgreSQL; multi-resource" alongside completion policies and
  CSV/delimited; Round 1's suite covered only delimited/CSV among the
  stateful-component catalog. Added
  `json_array_reader_registers_consistently_through_with_stream` (a second
  real stateful reader through `with_stream`, confirming the tuple-opener
  pattern is not delimited-specific) and
  `multi_resource_object_store_reader_registers_consistently_through_with_stream`
  (a real, first-party, non-test production multi-resource backend --
  `InMemoryObjectStore` / `ObjectStoreReaderOpener`, the object-store
  catalog's own executable contract fixture -- assembled through the
  builder, covering the multi-resource requirement without a live
  `PostgreSQL` connection). At this point `PostgreSQL` itself remained
  untested through the builder -- see Round 3 below, which closed that gap
  without needing a live database either.
- **Fixed -- a public `ChunkPipelineParts` type alias existed only to satisfy
  a clippy lint.** Round 1 introduced `pub type ChunkPipelineParts<I, O, R, P,
  W> = (ChunkStep<...>, ChunkComponentRevisions)` for `build()`'s return
  type solely because clippy's `type_complexity` lint flagged the bare
  tuple. The review correctly identified this as unmotivated new public
  vocabulary -- a lint workaround is not a domain concept. Removed; `build()`
  now returns the plain tuple directly, with a local
  `#[allow(clippy::type_complexity)]` on the method instead. The facade
  surface, `facade_surface.rs`'s snapshot, and `facade_review.rs`'s
  `REVIEWED_SURFACE` count (`chunk_builder` group: 3 → 2) were updated to
  match.

### Round 3 (manual strict review against the Round 2 HEAD, `8563e4b`)

- **Fixed -- issue #152's PostgreSQL requirement was still genuinely
  missing.** Section 8 requires exercising the configuration surface
  against representative format-specific M6 components, explicitly
  including PostgreSQL, and Round 2's evidence doc had only *argued* the
  gap was acceptable rather than closed it. The review correctly rejected
  that argument and pointed out the actual fix does not need a live
  database: `postgres_cursor_reader` (#149's representative shape) stores
  its `PostgresConfig` and connects lazily on the first `read()`, never in
  the synchronous, non-async constructor -- so a syntactically valid but
  never-routable `PostgresConfig` (e.g.
  `postgresql://user:pass@127.0.0.1:1/nonexistent`) lets the reader/stream/
  contract be constructed and registered through `ChunkPipelineBuilder::with_stream`
  at configuration time only, with no connection ever attempted. Added
  `postgres_cursor_reader_registers_consistently_through_with_stream`
  (`#[cfg(feature = "postgres")]`), proving the builder's generic bounds and
  surface accept a real, first-party `PostgreSQL` reader.
- **Fixed -- the evidence doc and PR body were stale relative to the actual
  CI state.** By the time of this review, the Round 2 evidence-retention
  commit (`631ae60`) had already made `cargo xtask evidence` and CI's
  `evidence-provenance` check pass, but this document's Validation section
  and the PR body still described the pre-retention gap as current. Fixed
  by rewriting the Validation section to state the final, post-retention
  evidence state (see below) and updating the PR body/comment to match.

## Tests

Doctests (`crates/oxide-batch/src/chunk_builder.rs`, run via
`cargo test --doc`):

- typed basic construction (`ChunkPipelineBuilder::new`), through
  `build_chunk_job`, using real first-party catalog components
  (`item_components::{IterReader, IdentityProcessor, NoopWriter}`);
- `Boxed*` construction of the identical component set, through `build`,
  asserting on the returned `ChunkComponentRevisions`;
- `with_completion_policy` + `revisions()`, asserting the computed
  completion-policy revision matches `completion_policy_revision` computed
  by hand for the same policy instance -- the restart fingerprint is not
  hidden by the convenience;
- `with_stream`, asserting `revisions().stream_revisions()` contains exactly
  the one registered identity/revision pair;
- `with_completion_policy_stream_revision`, installing a real
  `AdaptiveCompletionPolicy` and declaring its stream revision, through
  `build_chunk_job`.

Integration tests (`crates/oxide-batch/tests/chunk_builder.rs`, `cargo test
-p oxide-batch --test chunk_builder`):

- `typed_pipeline_builds_and_executes` -- a builder-assembled `ChunkStep`
  actually executes to `ChunkExecutionOutcome::Completed` under the default
  `NoopChunkCompletion`, and separately lowers into a valid `ChunkJob`;
- `builder_output_matches_hand_assembled_definition` -- a builder-assembled
  `ChunkJob`'s `definition_identity().manifest_digest()` is byte-identical
  to one assembled by hand from `ChunkStep::new`/`ChunkComponentRevisions::new`
  directly, proving the builder changes no restart-relevant fingerprint;
- `typed_and_boxed_pipelines_share_one_fingerprint` -- the typed-vs-erased
  equivalence claim above, as an executable assertion;
- `with_stream_registers_a_real_stateful_component_consistently` -- a real
  first-party stateful component (`item_components::delimited_reader`, the
  same `(component, stream, contract)` tuple-opener shape PostgreSQL and
  multi-resource components use) registered through `with_stream`, proving
  `ChunkJob::new`'s stream-registration validation passes without a second,
  hand-typed identity;
- `flow_job_binds_a_completion_policy_declared_through_the_builder` -- a
  completion policy declared through the builder, compiled into a
  `FlowGraph` via `flow_step_components`, and bound through
  `FlowJob::with_chunk_step`, exercising the preserved `ChunkJob`-vs-`FlowJob`
  asymmetry end to end;
- `delivery_mode_mismatch_is_rejected` -- an installed `FaultRuntime` whose
  delivery mode disagrees with the declared restart contract still fails
  `build_chunk_job` with `DefinitionError::DeliveryModeMismatch`;
- `duplicate_stream_identity_is_rejected` -- two `with_stream` calls
  reusing one `ComponentStreamIdentity` still fail with
  `DefinitionError::DuplicateRuntimeStream`;
- `adaptive_completion_policy_without_a_declared_stream_revision_is_rejected`
  / `_with_a_declared_stream_revision_builds` -- the strict-review fix above,
  as a negative/positive pair: installing `AdaptiveCompletionPolicy` without
  declaring its stream revision fails
  `DefinitionError::RuntimeStreamNotDeclared`; declaring it through
  `with_completion_policy_stream_revision` builds successfully;
- `completion_policy_replacement_discards_the_previous_policys_stream_revision`
  -- Round 2's fix: installing policy A, declaring its stream revision,
  replacing it with policy B, and declaring B's revision builds
  successfully with `revisions().stream_revisions()` containing *only* B's
  entry -- proving A's declaration does not survive the replacement (it
  would otherwise fail `DeclaredStreamMissingRuntime`);
- `adaptive_completion_policy_nested_in_a_composite_builds` -- an
  `AdaptiveCompletionPolicy` nested inside a `CompositeCompletionPolicy`
  still registers and declares correctly, proving the fix is not
  `AdaptiveCompletionPolicy`-specific;
- `flow_job_binds_an_adaptive_completion_policy_declared_through_the_builder`
  -- the stateful-policy analogue of the stateless `flow_job_binds_a_completion_policy...`
  test above, through `flow_step_components` and `FlowJob::with_chunk_step`;
- `json_array_reader_registers_consistently_through_with_stream` -- a second
  real stateful reader (`item_components::json_array_reader`) through
  `with_stream`, covering the JSON/JSONL catalog;
- `multi_resource_object_store_reader_registers_consistently_through_with_stream`
  -- a real, first-party, non-test production multi-resource backend
  (`InMemoryObjectStore`/`ObjectStoreReaderOpener`) through `with_stream`,
  covering the multi-resource catalog without a live `PostgreSQL`
  connection;
- `postgres_cursor_reader_registers_consistently_through_with_stream`
  (`#[cfg(feature = "postgres")]`) -- a real `PostgreSQL` reader
  (`item_components::postgres_cursor_reader`) through `with_stream`, using a
  syntactically valid but never-routable `PostgresConfig` (the constructor
  stores config and connects lazily on the first `read()`, never at
  construction), covering the `PostgreSQL` catalog without a live database.

No new compile-fail/UI-test fixture was added: `ChunkPipelineBuilder`
introduces no new generic bound shape beyond what `ChunkStep`'s existing
`R: ItemReader<I>`/`P: ItemProcessor<I, O>`/`W: ItemWriter<O>` bounds already
require, and those are already covered by the ADR-0008 compile-fail suite.

## Facade and evidence-gate updates

- `crates/oxide-batch/src/lib.rs` re-exports `ChunkPipelineBuilder` and
  `NoopChunkCompletion` at the facade root (mirroring `ChunkStep`/`ChunkJob`).
  `chunk_builder` itself is a private module (`mod chunk_builder;`, not
  `pub mod`): its extensive design-rationale prose lives directly on
  `ChunkPipelineBuilder`'s own doc comment, which rustdoc inlines at the
  facade re-export regardless of the module's own visibility, so nothing is
  lost by keeping the module private -- and keeping it private means a
  future contributor cannot add another public item to the file that
  bypasses `lib.rs`'s explicit re-export gate the rest of the crate relies
  on (an independent review's finding; see above). This deliberately departs
  from `item_components`'s `pub mod` precedent, because that module's intent
  differs: `item_components` is a browsable catalog whose contents are meant
  to be public via the module namespace itself, while `chunk_builder`'s
  intent was always a narrow, root-gated surface.
- `crates/oxide-batch/tests/facade_surface.rs`'s `resolves` module and its
  committed `crates/oxide-batch/tests/fixtures/facade/public-api.txt`
  snapshot were updated (`OXIDEBATCH_UPDATE_FACADE_SNAPSHOT=1`) to include
  the two new public paths -- a deliberate, reviewed addition, not a
  rewrite to pass a stage.
- `crates/oxide-batch/tests/facade_review.rs`'s `REVIEWED_SURFACE` gained a
  `("chunk_builder", 2)` row, following the same precedent #144/#146/#150/#151
  each used to extend the M5 preview-surface count (`git log -p` on this file
  shows each prior M6 issue bumping this array directly; the frozen M5 prose
  record it points readers toward,
  `docs/project/m5-facade-api-review-evidence.md`, has not been rewritten
  per-addition since M5 and is not rewritten here either, consistent with
  that precedent).
- `docs/architecture/item-processing-model.md` gained an `### Implementation
  status (#152)` subsection alongside #144/#146/#150/#151's.

## Scope confirmation

No M7 scope/late-binding, expression language, or general configuration DSL
was introduced. No repository-layer type (a new `ChunkTransactionManager`
implementation) was added -- issue #152 section 16 excludes repository
portability, and the existing single production implementation
(`PostgresChunkTransactionManager`) is unaffected. No unrelated #151
redesign, facade cleanup, or dependency change is included in this diff.

## Validation

Run and passing locally, and in CI, at the final reviewed commit
(`631ae60...`, evidence-retained; Round 3's PostgreSQL test and doc fixes
land in the commit immediately after and were re-verified the same way):

```shell
cargo fmt --all -- --check
cargo clippy -p oxide-batch --all-targets --all-features -- \
  -D warnings -A clippy::too_many_arguments -A clippy::too_many_lines
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo xtask surface
cargo xtask deps
cargo xtask reconciliation
cargo xtask release-crates
cargo xtask evidence
```

`cargo test --workspace --all-features` includes the `postgres`-gated suites
(`crates/oxide-batch-test`'s and `crates/oxide-batch`'s PostgreSQL-backed
tests) against a local Homebrew PostgreSQL 18 instance; all pass. (One
transient false alarm during local iteration is worth recording for future
sessions: `cargo test -p oxide-batch --test item_component_paths` appeared
to hang for over ten minutes while several other `cargo doc`/`cargo check`/
`cargo test` invocations ran concurrently against the same target
directory; running it in isolation completed in 0.01s. It was local
build-directory/CPU contention from stacking multiple heavy `cargo`
invocations, not a defect.)

**`cargo xtask evidence` passes cleanly.** Touching `crates/oxide-batch/src`
invalidates the retained M5 campaign evidence's tree-identity binding --
the same gap every prior `src`-touching M6 PR (#144/#146/#150/#151)
reported -- and, matching the precedent those PRs (concretely #151/#179)
set, it was resolved **before merge**, not deferred: all 8 M5 campaign
workflows (Soak, Cancellation, Performance, Conformance, Crash and Restore,
Upgrade, Security, Resource Bounds) were dispatched via `workflow_dispatch`
against the PR branch, succeeded on their PostgreSQL 15/18 matrix legs, and
their 16 report artifacts were downloaded, retained byte-for-byte
(`cmp -s` verified against the originals), and used to rebuild
`docs/engineering/campaigns/m5/evidence-provenance.json`'s
producer/workflow_run/producing_job/artifact/retained_report_git_blob/
remote_verification fields from real GitHub API data -- never hand-authored.
`cargo xtask evidence` and CI's `evidence-provenance` check both report
green against this. Full procedure and commit:
`ci(m5): retain evidence for #152 (PR #180)`.

**Not run locally:** `cargo xtask crash-restore`, `cargo xtask soak`,
`cargo xtask performance`, `cargo xtask security`, `cargo xtask
resource-bounds`, `cargo xtask upgrade`, and `cargo xtask cancellation` (the
heavy, long-running local variants of the same campaigns) -- CI's
`workflow_dispatch` runs above are the executed evidence for those
campaigns against this change; #152's scope explicitly excludes #153's
final M6 exit-gate campaigns.

## Executable evidence

- `crates/oxide-batch/src/chunk_builder.rs` (`ChunkPipelineBuilder`,
  `NoopChunkCompletion`, four rustdoc doctests)
- `crates/oxide-batch/src/chunk_runtime.rs` (`ChunkStep::with_completion`;
  `completion_policy_component_revisions` widened to `pub(crate)`)
- `crates/oxide-batch/tests/chunk_builder.rs` (seven integration tests)
- `crates/oxide-batch/tests/facade_surface.rs` and
  `crates/oxide-batch/tests/fixtures/facade/public-api.txt` (surface
  snapshot)
