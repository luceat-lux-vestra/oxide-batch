# M5 Staged Crate-Extraction Evidence

**State:** Stage 1 and the ADR-0011 core placement complete; stage 2 attempted
and not landed; stage 3 not started

**Issue:** [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99)

**Date:** 2026-08-03

This record is the per-stage evidence the
[staged crate-extraction contract](../architecture/crate-extraction.md)
requires. It covers the publication decision the work forced, the evidence
checks the contract required and the repository lacked, stage 1, and the
boundary finding that stops stages 2 and 3.

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

## Stage 2 — attempted, not landed

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

## Stage 3 — not started

Stage 3 follows stage 2 and is unchanged by this work.

## Consequences for the milestone

- The M5 workstream that this issue owns is partially delivered. Stage 1 is
  complete; stages 2 and 3 need the boundary correction above.
- No ledger row moves. Extraction claims no capability and promotes nothing.
- The release path changed: `release.yml` and `release-draft.yml` now package,
  dry-run, checksum, generate SBOMs for, attest, and publish
  `oxide-batch-core` alongside the facade, in dependency order. The operator
  CLI stays outside the release scope, as before.
- **M5 cannot close until stages 2 and 3 land.** The kickoff gate's "each
  completed crate extraction" clause is the quality bar applied to an
  extraction, not permission to stop after one. The same gate states that exit
  work follows all implementation streams, the design gate makes issue
  [#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103) follow all
  implementation and evidence work, and this issue's own exit criteria require
  every authorized stage. Issue #99 stays open.
