# M5 Staged Crate-Extraction Evidence

**State:** Stage 1, the ADR-0011 core placement, and stage 2 complete; stage 3
not started

**Issue:** [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99)

**Date:** 2026-08-04

This record is the per-stage evidence the
[staged crate-extraction contract](../architecture/crate-extraction.md)
requires. It covers the publication decision the work forced, the evidence
checks the contract required and the repository lacked, stage 1, the boundary
finding that stopped stage 2 and the correction that unblocked it, and stage 2.

Extraction is behavior-preserving repackaging. Nothing in this record changes
observable batch semantics, persisted bytes, transaction boundaries, lifecycle
writes, restart selection, definition fingerprints, or normalized traces.

## Publication decision

The contract required every extracted crate to be `publish = false` while M5
publishes the `oxide-batch` facade as a `0.x` preview. Cargo rewrites a
published archive's path dependencies to registry dependencies when it verifies
the archive, so a publishable crate cannot depend on an unpublished one. The
failure was reproduced on cargo 1.97.1 before any code moved:

```text
$ cargo publish -p b --dry-run     # b is publishable, path-depends on a
error: failed to prepare local package for uploading
Caused by:
  no matching package named `a` found
  location searched: crates.io index
```

Stage 1 would therefore have made the facade unpublishable and failed this
milestone's own `package_dry_run_succeeds_for_every_workspace_crate` scenario.
[RFC-0011](../rfcs/0011-publication-of-extracted-implementation-crates.md)
records the conflict;
[ADR-0010](../architecture/decisions/0010-extracted-crate-publication.md)
resolves it by publishing the three authorized crates as internal crates in
lockstep with the facade. The
[design-gate record](m5-design-gate-evidence.md#crate-publication-correction)
carries the correction.

The resolution is verified rather than argued: `cargo xtask package` runs the
workspace publish dry run with `oxide-batch-core` extracted, and it succeeds.

## Evidence checks

The contract required a CI dependency check, a facade API snapshot comparison,
packaging evidence, and recorded measurements. None existed, so no stage could
have been accepted. They were built before stage 1 and are stage-independent.

| Scenario | Where it runs | Result |
| --- | --- | --- |
| `facade_import_paths_resolve_unchanged_after_each_stage` | `crates/oxide-batch/tests/facade_surface.rs` | Pass |
| `public_api_snapshot_is_unchanged_by_extraction` | `crates/oxide-batch/tests/facade_surface.rs` | Pass |
| `forbidden_dependency_check_fails_the_build_on_violation` | `xtask/src/deps.rs` | Pass |
| `workspace_has_no_dependency_cycle` | `xtask/src/deps.rs` and `cargo xtask deps` in CI | Pass |
| `golden_fingerprints_are_unchanged_by_extraction` | `plan_fingerprint.rs`, `plan_manifest.rs`, `local_scale_plan.rs` against unmodified committed vectors | Pass |
| `normalized_repository_write_traces_are_unchanged_by_extraction` | `plan_equivalence.rs` against unmodified committed traces | Pass |
| `package_dry_run_succeeds_for_every_workspace_crate` | `cargo xtask package`, and the `packaging` CI job | Pass |

The facade surface test holds the supported surface two ways. Its `resolves`
module names all `424` exported paths, so a path that stops resolving after a
move fails to compile; its snapshot test renders the export set from
`src/lib.rs` and compares it with a committed file. Drift was confirmed to fail
the test before the snapshot was trusted.

The two durable-invariance scenarios are discharged by the existing golden
comparisons plus the fact that no fixture changed: the golden fingerprint
vectors, canonical manifest bytes, and normalized wrapper traces under
`crates/oxide-batch/tests/fixtures/` are byte-identical across every commit in
this work.

**Snapshot limitation.** The public API snapshot pins exported paths and their
feature gate, not item signatures. Signature equivalence is held by the rest of
the suite, the `RUSTDOCFLAGS="-D warnings"` documentation build, the
compile-fail tests, and the operator CLI compiling against the facade. A
signature-level snapshot needs rustdoc JSON, which is nightly-only, and is not
adopted for a pinned stable toolchain.

## Stage 1 — `oxide-batch-core`

**Moved.** Domain identities, typed job parameters, statuses and exit statuses,
execution records, lifecycle rules, bounded versioned execution-context and
checkpoint state, chunk sizing and counting values, and restart-relevant
definition identity with its canonical manifest encoding.

**Not moved.** Everything else. The crate depends only on `serde_json` and
`sha2`, and on no other workspace crate.

### Equivalence

- The workspace test set is identical before and after: `451` tests, same
  names. The only differences are doctest identities, which are `file:line`
  based: the `DefinitionManifest` example moved to the facade documentation so
  that it keeps demonstrating the supported `oxide_batch` import path, and two
  `plan.rs` doctests shifted three lines.
- `cargo test --workspace --all-features` passes with `450` executed tests and
  no failures.
- The public API snapshot is byte-identical.
- `cargo fmt`, `cargo clippy --workspace --all-targets --all-features -D
  warnings`, `cargo check -p oxide-batch --no-default-features`, `cargo check
  -p oxide-batch-cli --no-default-features --all-targets`, and
  `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`
  all pass.
- `cargo xtask deps` and `cargo xtask package` pass.
- The one file that was split rather than moved whole, `chunk.rs`, is a pure
  partition: concatenating the two resulting bodies reproduces the original
  body line for line, with only the module header rewritten.

### Boundary refinements

Three consequences of the move are recorded rather than worked around.

**Graph ceilings moved with their reader.** `MAX_NODES` and `MAX_TRANSITIONS`
are enforced by `DefinitionManifest::read`, which is a core type, so they moved
into core. `oxide_batch::MAX_NODES` and `oxide_batch::MAX_TRANSITIONS` resolve
unchanged. The remaining ceilings stayed with the plan module.
[ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md)
already classifies both as framework capability bounds excluded from the
fingerprint, so the move does not touch definition identity.

**Crate-public internals.** Items the facade still needs across the boundary
became public in core and are not re-exported by the facade: the manifest
format constants, `check_manifest_format`, `validate_token`, the
`definition_token` macro, `DefinitionIdentity::{legacy, step_mapping,
from_flow_manifest}`, `ChunkComponentRevisions::manifest_value`,
`Checkpoint::generation_digest`, `JobParameters::flow_input_digest`,
`FailureCategory::{durable_code, from_durable_code}`,
`ExecutionVersion::next`, and `LifecycleTransition::with_terminal_rollback`.
The curated facade surface is unchanged; only an internal crate's surface
widened, which ADR-0010 exempts from any stability promise.

**Non-exhaustive matching across the boundary.** `#[non_exhaustive]` domain
enums cannot be matched exhaustively outside the crate that declares them, so
four sites in the facade lost their compile-time exhaustiveness check. Each
takes the conservative arm, and each is fail-safe rather than silently wrong:

| Site | Enum | Fallback |
| --- | --- | --- |
| `TelemetrySpanStatus::from_batch_status` | `BatchStatus` | Reports the unknown span outcome |
| `split_status_severity` | `BatchStatus` | Takes the highest severity, so an unrecognized status dominates the aggregate instead of passing as completed |
| Chunk attempt stop masking | `InFlightPolicy` | Never masks a shutdown request |
| PostgreSQL instance-key encoding | `ParameterValueKind` | Rejects with a typed error rather than writing a guessed durable tag |

This is a named residual limitation of every extraction boundary, not of stage
1 alone. Adding a variant to a core enum no longer breaks the facade build; it
silently takes a fallback that this table says is safe. The mitigation is
review of this table whenever a variant is added.

### Imports

Facade modules keep `use crate::{..}`, which resolves through the facade's own
re-exports. Only the explicit `crate::definition::..` paths were repointed at
`oxide_batch_core`. Rewriting every import to name the source crate would have
produced a large diff across twenty files for no behavioral gain, and the
dependency direction is enforced by `cargo xtask deps` rather than by import
spelling.

### Measurements

Provisional development observations from a macOS host, which the support
matrix lists as development-only. Reported, not gated. Raw reports are
[`baseline.json`](../engineering/measurements/m5/baseline.json) and
[`stage-1-core.json`](../engineering/measurements/m5/stage-1-core.json).

| Observation | Baseline | Stage 1 |
| --- | --- | --- |
| Clean workspace build, all features | 23.0 s | 26.6 s |
| Clean facade build, all features | 21.9 s | 20.9 s |
| Incremental facade build | 12.2 s | 12.4 s |
| Release `oxide-batch-cli` binary | 7 031 488 B | 7 111 584 B |
| Packaged files, `oxide-batch` | 119 | 111 |
| Packaged files, `oxide-batch-core` | — | 17 |

Build times moved within the noise this host shows between captures of the same
commit. The binary grew by `80 096` bytes, `1.1 %`, which is crate-boundary
overhead rather than new code. No budget is crossed and none is binding.

### Reversal

Stage 1 is the single commit `refactor(core): extract the domain, state, and
definition boundary`. Reverting it restores the previous module layout and
changes no facade path, persisted byte, or metadata value, so it requires no
migration and no operator action. Reversal stays free until the release that
publishes `oxide-batch-core`; after that the published version is permanent, as
ADR-0010 records.

## ADR-0011 core placement

The twenty-three items ADR-0011 places in the domain layer moved before stage
2: `NodeId`, `FlowTarget`, `TerminalKind`, `StartControls`, `StartLimit`,
`MAX_PARTITIONS`, and the seventeen runtime-free fault-policy values.
`BackoffSleeper` and `BackoffOutcome` stayed with the runtime, and the plan
module kept the compiler and the graph types only it constructs.

The suite keeps its test set at `452` executed tests with no failures, the
facade export snapshot is unchanged, no fixture moved, and `cargo fmt`,
`cargo clippy -D warnings`, both no-default-feature checks, the rustdoc build,
`cargo xtask deps`, and `cargo xtask package` all pass.

**The one API change.** `StartLimit::new` returns
`DefinitionError::ZeroStartLimit` instead of `PlanError::ZeroStartLimit`.
Notably, **the facade export snapshot did not detect it**: the snapshot pins
exported paths and names, and both error types were already exported. This is
the documented name-level limitation reaching a real case. The change is caught
by its test, its changelog entry, and this record instead.

Four more `#[non_exhaustive]` matches crossed the boundary and take the arm
that commits nothing: an unrecognized fault decision rolls back and fails, an
unrecognized terminal fails the job, an unrecognized terminal is rejected
rather than encoded durably, and an unsupported plan stays unsupported. All
fallbacks in the workspace now use one idiom — a final `_` arm whose comment
names the variants it absorbs.

## Stage 2 — the first attempt, not landed

The attempt below preceded the preliminary API changes and the landed move. It
is retained because it is what established the boundary, and because the landed
move is judged against what it predicted.

Stage 2 was attempted after the core placement and is **not** in the history.
The move itself completed: `oxide-batch-repository` was created, the ports,
partition values, durable flow records, and service descriptors moved into it,
the four service implementations were split back into the facade, and the crate
compiles clean with `0` warnings. The facade did not reach a compiling state,
and the work was stopped rather than committed, because the remaining fixes had
started to require mechanical edits whose behavior-preservation could no longer
be verified by reading them. The contract's rule that extraction must not
become a rewrite is the reason for stopping.

The attempt is retained as a `git stash` entry, `wip: stage 2 repository
extraction (incomplete, does not compile)`, and is not a reviewed artifact.

**What the attempt established**, and what the next one does not need to
rediscover:

- The boundary itself is sound. `oxide-batch-repository` depends only on
  `oxide-batch-core`, `serde_json`, and `sha2`, and needs no async runtime,
  driver, or telemetry type. The plan and repository crates are independent
  siblings, as ADR-0011 predicted.
- The four service implementations split out cleanly at contiguous block
  boundaries; the descriptors and ports below them do not reference telemetry.
- Three costs appear only when the split is made, and each needs a decision
  rather than a mechanical fix:
  - `FlowDecisionSequence::new` returns the flow engine's `FlowRuntimeError`,
    which cannot follow the record into the repository crate. It becomes a
    second reviewed API change, on the same terms as `StartLimit::new`.
  - The services construct `OperatorRecordDraft`, `RetentionRecordDraft`,
    `RecoveryEvidence`, and `RecoveryProposal` with struct literals over
    private fields. Each needs a public constructor, which is `29` parameters
    across four types and is a real API addition.
  - The services read `36` private fields of `OperatorRequest`,
    `RecoverySnapshot`, and `FlowStepState`. Most have accessors already;
    `RecoverySnapshot` needs four and `OperatorRequest` needs one, and the
    accessors return references where the field reads moved values, so every
    call site needs review rather than a blanket clone.
- Six further `#[non_exhaustive]` matches cross the stage-2 boundary:
  `OperatorAction`, `OperatorOutcomeClass`, `RetentionAction`,
  `FlowTransitionKind`, and `ExplorerQuery` in both adapters. `OperatorAction`
  additionally needs a new `OperatorRejection::UnsupportedAction` variant,
  because there is no existing rejection for an action the build cannot apply.

The next attempt should treat these as its scope, land the API additions as
their own reviewed change first, and only then move the modules.

## Stage 2 preliminary — the API changes, landed alone

The API work the attempt above identified is landed as its own reviewed commit,
ahead of any module move. Nothing moved between crates in it. The suite holds
at `453` executed tests — the `452` from the core placement plus one new test —
with no failures, and `cargo fmt`, `cargo clippy -D warnings`, both
no-default-feature checks, the rustdoc build, `cargo xtask deps`, and
`cargo xtask package` all pass.

**What landed.**

| Change | Kind |
| --- | --- |
| `OperatorRecordDraft::{applied, rejected}` | Addition |
| `RetentionRecordDraft::{instance_action, purge}` | Addition |
| `RecoveryEvidence::new`, `RecoveryProposal::new` | Addition |
| `OperatorRequest::job_instance_key` | Addition |
| `RecoverySnapshot::{status, owner, updated_at, server_time}` | Addition |
| `OperatorRejection::UnsupportedAction`, `IdentifierKind::FlowDecisionSequence` | Addition |
| `FlowDecisionSequence::new` returns `DomainError` | **Breaking (pre-1.0)** |

The constructors are purpose-named rather than one wide constructor per type.
The attempt above counted `29` parameters across four constructors; naming them
for their purpose costs `19` and makes two invariants hold by construction — an
audit row cannot disagree with the request it audits, and a proposal cannot
carry a digest its evidence does not produce. `from_durable` is unchanged and
still belongs to the adapters.

The accessors needed were `5`, exactly as predicted: four on
`RecoverySnapshot` and one on `OperatorRequest`. Every other field read already
had one.

The reference-versus-value trap was real and needed a different fix in each of
its two shapes. Three `Option<T>` reads — `OperatorRequest::reason` twice and
`FlowStepState::context` once — became `.cloned()` where the field read was
`.clone()`, because the accessor returns `Option<&T>`. Two `RequestDigest` reads
needed a `*` deref instead. Neither shape is what a blanket `.clone()` would
have produced, and the `Option` shape would have compiled as a clone of the
reference rather than of the value.

**The verification technique.** Private field access inside one crate compiles,
so converting the call sites proves nothing by itself. It is provable now, with
no crate created: wrap the moving types in an inline `mod` inside their own
file and re-export them. Rust privacy is module-based, so the compiler reports
exactly the crossings the crate split would report. The wrapper is deleted
before committing.

Run against `flow.rs` it found six crossings in `DecisionStepInput::from_state`
that neither the stage-2 attempt nor a reading of the code had found. Run
against `repository.rs` and `service.rs` it found none, which also proves the
adapters and telemetry never reach into a descriptor: they are already in
different modules, so the compiler has been enforcing that boundary all along.
Only same-file code could cross, which bounds the remaining search to the six
files that declare a moving type beside code that stays.

**What the technique found that this commit does not fix.** Widening a private
*method* is not the same problem as adding an accessor, and the probe found
these on the service side of the boundary:

| Item | File |
| --- | --- |
| `OperatorOutcome::new` | `service/operator.rs` |
| `OperatorRequest::{definition, recovery_guard, recovery_request}` | `service/operator.rs` |
| `OperatorRejection::from_repository` | `service/operator.rs` |
| `RetentionReport::new` | `service/retention.rs` |
| `MonotonicInstant::checked_elapsed_since` | `service/recovery.rs` |

Each is private today and must be `pub` after the split, and because each is an
inherent item on an exported type, `pub` here is a facade API addition rather
than the crate-internal widening stage 1 recorded. They are not in this commit
because they need the same review this one had, and because the explorer's
cursor machinery — `CursorKey`, `QueryWindow`, `decode_cursor`, and the cursor
format constants — has no placement yet: it is named by the `ExplorerRepository`
port and used by `JobExplorer`, so stage 2 must decide which side it lands on
before its visibility can be decided. That decision belongs with the module
split, not ahead of it.

The `pub(crate)` items in `flow.rs`, `partition.rs`, `repository.rs`, and
`service/explorer.rs` are a different case and need no separate review: they
become `pub` on an internal crate that the facade does not re-export, which is
the crate-public-internals pattern stage 1 established and ADR-0010 exempts.

**Snapshot limitation, again.** The facade export snapshot did not detect the
`FlowDecisionSequence::new` change, and did not detect any of the seven
additions either: variants and inherent methods are not exported paths. This is
the second recorded case of the limitation named above. The changes are caught
by their tests, their changelog entries, and this record.

## Stage 2 — `oxide-batch-repository`

The module move landed as one commit after the preliminary API changes above.

**Moved.** The metadata repository, unit-of-work, clock, and identifier ports;
the explorer, operator, retention, and recovery ports; the durable partition
plan and result values; the durable flow-decision records; the operator audit
records and guard vocabulary; the durable retention holds, purge plans, and
audit records; the recovery snapshots, evidence, and proposals; the bounded
operator request envelope; and the keyset pagination vocabulary. The crate
depends on `oxide-batch-core`, `sha2`, and nothing else.

**Not moved.** The two metadata adapters, the four services that drive the
ports, the flow engine, the plan compiler, the runtime, telemetry, and the
contract suite. `decision_matches_manifest` stayed with the adapters because it
reads an `ExitPattern`, and the repository crate may not depend on the plan.

### The three placement decisions the move had to make

**The cursor machinery went with the port.** The stage-2 preliminary record left
`CursorKey`, `QueryWindow`, `decode_cursor`, and the cursor format constants
unplaced. They are in `oxide-batch-repository` with `Cursor`, `Page`,
`PageRequest`, `PageSize`, `ExplorerQuery`, and the `ExplorerRow` implementations
for all eight row types, because every cursor key is an immutable ordering
column of a row the port returns and every token is bound to a query the port
defines. `JobExplorer` alone stayed in the facade.

**Service results stayed with their services.** `OperatorOutcome`,
`OperatorError`, `RetentionReport`, `RecoveryProposer`, and the monotonic-clock
values are named by no port, so they stayed with the four service
implementations. `OperatorOutcome::new` and `RetentionReport::new` therefore did
not need widening, which the stage-2 attempt had predicted they would.

**Error types followed the values that return them.** `ExplorerError`,
`RetentionError`, and `RecoveryError` moved because moved constructors return
them. `OperatorError` did not, because nothing moved returns it.

### Equivalence

- The workspace test set is **identical**: `cargo test --workspace
  --all-features -- --list` produces byte-identical output before and after the
  move, `454` listed tests. The `453` executed tests pass with no failures. Only
  the owning binary changed for the moved unit tests, which now run in
  `oxide-batch-repository`. Unlike stage 1 this move shifted no doctest, because
  no moved item carries one.
- The public API snapshot is byte-identical, and every one of the `424` named
  facade paths still resolves.
- No fixture changed. The golden fingerprint vectors, canonical manifest bytes,
  and normalized wrapper traces under `crates/oxide-batch/tests/fixtures/` are
  unmodified, so the two durable-invariance scenarios hold as in stage 1.
- `cargo fmt`, `cargo clippy --workspace --all-targets --all-features -D
  warnings`, `cargo check -p oxide-batch --no-default-features`, `cargo check -p
  oxide-batch-cli --no-default-features --all-targets`, `RUSTDOCFLAGS="-D
  warnings" cargo doc --workspace --all-features --no-deps`, `cargo xtask deps`,
  and `cargo xtask package` all pass.
- PostgreSQL evidence runs in CI, as it does for every change to this
  repository.

### The documented surface did not change

Stage 1 recorded crate-public internals as invisible from the facade, and the
ADR-0011 record corrected that: an inherent `pub` item on a re-exported type is
reachable through the facade path. Stage 2 answers the correction rather than
repeating the claim. Every item the split forced open is `#[doc(hidden)]`, so
the facade's rustdoc discloses none of them, and the crate documents the
convention. They are still technically callable, which is why each is named
here.

`34` items were opened. `9` are free items or a trait that the facade does not
re-export, and are unreachable from `oxide_batch`:

| Item | Why the boundary needs it |
| --- | --- |
| `recovered_execution`, `aggregate_partition_parent`, `map_partition_aggregation` | Both adapters apply them |
| `page`, `start_window`, `resume_window`, `ExplorerRow` | `JobExplorer` bounds and seals every page with them |
| `hex_digest` | The flow engine and the explorer render digests with it |
| `PartitionMutationError` | Names the rejection of a partition mutation an adapter applies |

`25` are inherent items on re-exported types, so they are reachable under an
`oxide_batch` path despite being undocumented:

| Type | Items | Caller |
| --- | --- | --- |
| `ExecutionControl` | `new` | Both adapters |
| `RecoveryDecision`, `RecoveryResult` | `new` | Both adapters |
| `FlowDecision`, `FlowDecisionRequest` | `new` | Adapters allocate decisions; the engine proposes requests |
| `FlowTransitionKind` | `durable_code`, `from_durable_code` | The PostgreSQL adapter encodes the kind |
| `StepPartition` | `from_snapshot`, `starting`, `assign`, `complete` | Both adapters own partition rows |
| `PartitionAggregate` | `selected_worker_step_execution_id` | Both adapters |
| `PartitionResult` | `from_worker` | The flow engine reads worker attempts |
| `ParameterDescriptor`, `StateEnvelopeDescriptor`, `DefinitionDescriptor` | `new` | Both adapters build redacted descriptors |
| `JobInstanceProjection`, `JobExecutionProjection`, `StepExecutionProjection`, `StepPartitionProjection` | `new` | Both adapters build projections |
| `OperatorRequest` | `definition`, `recovery_guard`, `recovery_request` | `JobOperator` applies the guards |
| `OperatorRejection` | `from_repository` | `JobOperator` classifies repository failures |
| `PurgePlan` | `new` | `RetentionService` seals a plan over its survey |
| `MonotonicInstant` | `checked_elapsed_since` | `RecoveryProposer` bounds its observation window |

`FlowTransitionKind::{durable_code, from_durable_code}` additionally lost their
`#[cfg(feature = "postgres")]` gate, because the repository crate has no
`postgres` feature and must not acquire one.

### Non-exhaustive matching across the boundary

The six crossings the stage-2 attempt predicted are exactly the six that
appeared. Each takes a final `_` arm whose comment names the variants it
absorbs, and each is fail-safe rather than silently wrong:

| Site | Enum | Fallback |
| --- | --- | --- |
| `decision_matches_manifest` | `FlowTransitionKind` | Matches no declared node, so the decision is rejected rather than accepted against a guessed node |
| `JobOperator::apply` | `OperatorAction` | Audits `OperatorRejection::UnsupportedAction` and applies nothing |
| `JobOperator::emit_outcome` | `OperatorOutcomeClass` | Reports the non-accepting event kind; the record still carries the exact class |
| `RetentionService::hold_action` | `RetentionAction` | Changes no hold state, merged with the existing `ApplyPurge` arm |
| `InMemoryExplorer::identity_ceiling` | `ExplorerQuery` | Reports `ExplorerError::UnsupportedCapability` |
| `ceiling_source` (PostgreSQL) | `ExplorerQuery` | Reports `ExplorerError::UnsupportedCapability` |

The `OperatorRejection::UnsupportedAction` variant the preliminary commit added
for this purpose is now used. The residual limitation stage 1 named applies
unchanged: adding a variant to one of these enums no longer breaks the facade
build, and the mitigation is review of this table.

### One test changed shape

Three cases in the `RecoveryProposer` unit tests assigned to private
`RecoverySnapshot` fields to vary one observation. Private fields do not cross a
crate boundary, so those cases now build the whole snapshot through
`RecoverySnapshot::new` with a `snapshot_with` helper. The values are the same;
no assertion changed.

### Measurements

Provisional development observations from the same macOS host as stage 1, which
the support matrix lists as development-only. Reported, not gated. The raw
report is
[`stage-2-repository.json`](../engineering/measurements/m5/stage-2-repository.json).
It was captured on the stage-2 working tree before the commit below, so it
records `clean_tree: false` and the parent commit; `baseline.json` was captured
the same way.

| Observation | Baseline | Stage 1 | Stage 2 |
| --- | --- | --- | --- |
| Clean workspace build, all features | 23.0 s | 26.6 s | 24.9 s |
| Clean facade build, all features | 21.9 s | 20.9 s | 22.2 s |
| Incremental facade build | 12.2 s | 12.4 s | 12.1 s |
| Release `oxide-batch-cli` binary | 7 031 488 B | 7 111 584 B | 7 122 752 B |
| Packaged files, `oxide-batch` | 119 | 111 | 110 |
| Packaged files, `oxide-batch-core` | — | 17 | 19 |
| Packaged files, `oxide-batch-repository` | — | — | 16 |

The facade lost one packaged file: two modules left and the adapter module that
replaced them arrived. `oxide-batch-core` gained two files between the stage-1
capture and this one, because the ADR-0011 placement added `flow.rs` and
`fault.rs` to it; stage 2 did not change that crate. The operator binary grew by
`11 168` bytes, `0.16 %`, which is crate-boundary overhead rather than new code.
Build times moved within the noise this host shows between captures of the same
commit. No budget is crossed and none is binding.

The workspace dependency graph after the move is
`oxide-batch -> oxide-batch-core`, `oxide-batch -> oxide-batch-repository`,
`oxide-batch-repository -> oxide-batch-core`, and the three existing edges from
the CLI and the two spikes into the facade. No cycle exists and no crate reaches
a forbidden dependency class, which `cargo xtask deps` verifies.

### Reversal

Stage 2 is the single commit that carries this record. Reverting it restores the
previous module layout and changes no facade path, persisted byte, or metadata
value, so it requires no migration and no operator action. Reversal stays free
until the release that publishes `oxide-batch-repository`.

## Stage 3 — not started

Stage 3 follows stage 2 and is unchanged by this work.

## Consequences for the milestone

- The M5 workstream that this issue owns is partially delivered. Stages 1 and 2
  are complete; stage 3 remains.
- No ledger row moves. Extraction claims no capability and promotes nothing.
- The release path changed: `release.yml` and `release-draft.yml` now package,
  dry-run, checksum, generate SBOMs for, attest, and publish
  `oxide-batch-core` and `oxide-batch-repository` alongside the facade, in
  dependency order. The operator CLI stays outside the release scope, as before.
- **M5 cannot close until stage 3 lands.** The kickoff gate's "each
  completed crate extraction" clause is the quality bar applied to an
  extraction, not permission to stop after one. The same gate states that exit
  work follows all implementation streams, the design gate makes issue
  [#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103) follow all
  implementation and evidence work, and this issue's own exit criteria require
  every authorized stage. Issue #99 stays open.
