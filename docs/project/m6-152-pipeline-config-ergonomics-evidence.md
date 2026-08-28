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
  ChunkComponentRevisions)`, aliased as the public `ChunkPipelineParts` type
  (introduced only because clippy's `type_complexity` lint requires it, not
  as new domain vocabulary), ready for `ChunkJob::new` or
  `FlowJob::with_chunk_step`. **`ChunkPipelineBuilder::build_chunk_job()`**
  is a pure forward onto `Self::build` and `ChunkJob::new`.
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
  the one registered identity/revision pair.

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
  `DefinitionError::DuplicateRuntimeStream`.

No new compile-fail/UI-test fixture was added: `ChunkPipelineBuilder`
introduces no new generic bound shape beyond what `ChunkStep`'s existing
`R: ItemReader<I>`/`P: ItemProcessor<I, O>`/`W: ItemWriter<O>` bounds already
require, and those are already covered by the ADR-0008 compile-fail suite.

## Facade and evidence-gate updates

- `crates/oxide-batch/src/lib.rs` re-exports `ChunkPipelineBuilder`,
  `ChunkPipelineParts`, and `NoopChunkCompletion` at the facade root (mirroring
  `ChunkStep`/`ChunkJob`), and additionally exposes `pub mod chunk_builder`
  (mirroring `item_components`) so the module's own rustdoc renders.
- `crates/oxide-batch/tests/facade_surface.rs`'s `resolves` module and its
  committed `crates/oxide-batch/tests/fixtures/facade/public-api.txt`
  snapshot were updated (`OXIDEBATCH_UPDATE_FACADE_SNAPSHOT=1`) to include
  the three new public paths -- a deliberate, reviewed addition, not a
  rewrite to pass a stage.
- `crates/oxide-batch/tests/facade_review.rs`'s `REVIEWED_SURFACE` gained a
  `("chunk_builder", 3)` row, following the same precedent #144/#146/#150/#151
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

Run and passing locally at the commit this record describes:

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
```

`cargo test --workspace --all-features` includes the `postgres`-gated suites
(`crates/oxide-batch-test`'s and `crates/oxide-batch`'s PostgreSQL-backed
tests) against a local Homebrew PostgreSQL 18 instance; all pass.
`restart_harness.rs`'s SIGKILL-based crash/restart tests take real wall time
(actual OS process spawn/kill) but are not hung -- confirmed by isolating
them from the earlier resource-contention false alarm below.

`cargo xtask evidence` reports the expected gap: any PR touching
`crates/oxide-batch/src` invalidates the retained M5/M6 campaign evidence's
provenance (checked against a working tree that differs from the committed,
retained execution), per the repository's evidence-provenance contract
(`docs/engineering/campaigns/m5/evidence-provenance.json`). This is not a
regression introduced here -- it is the same gap every prior `src`-touching
M6 PR (#144/#146/#150/#151) reported, resolved by a dedicated post-merge CI
evidence-retention pass, not a local fix, and not by hand-editing provenance
metadata.

One transient false alarm during local iteration is worth recording:
`cargo test -p oxide-batch --test item_component_paths` appeared to hang for
over ten minutes while several other `cargo doc`/`cargo check`/`cargo test`
invocations ran concurrently against the same target directory. Running it
in isolation completed in 0.01s. The full workspace suite was then re-run
alone, start to finish, and passed cleanly (`EXIT_CODE=0`, 139 test-result
groups, zero `FAILED` occurrences) -- the earlier appearance of a hang was
local build-directory/CPU contention from stacking multiple heavy `cargo`
invocations, not a defect.

**Not run locally:** `cargo xtask crash-restore`, `cargo xtask soak`,
`cargo xtask performance`, `cargo xtask security`, `cargo xtask
resource-bounds`, `cargo xtask upgrade`, and `cargo xtask cancellation` --
these are M5/M6 campaign commands unaffected by a pure configuration-builder
addition with no new runtime type, no new failure mode, and no touched
repository/persistence code; #152's scope explicitly excludes #153's final
M6 campaigns.

## Executable evidence

- `crates/oxide-batch/src/chunk_builder.rs` (`ChunkPipelineBuilder`,
  `NoopChunkCompletion`, four rustdoc doctests)
- `crates/oxide-batch/src/chunk_runtime.rs` (`ChunkStep::with_completion`;
  `completion_policy_component_revisions` widened to `pub(crate)`)
- `crates/oxide-batch/tests/chunk_builder.rs` (seven integration tests)
- `crates/oxide-batch/tests/facade_surface.rs` and
  `crates/oxide-batch/tests/fixtures/facade/public-api.txt` (surface
  snapshot)
