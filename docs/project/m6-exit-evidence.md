# M6 Exit Evidence

**State:** CANDIDATE COMPLETE — READY FOR STRICT REVIEW; DO NOT MERGE.
This record describes the exact candidate branch HEAD and retained evidence;
it does not claim that #140 is closed. The umbrella closes only after an
independent strict review passes on the final PR HEAD and the PR merges.

## Source/base identity

- Starting canonical `main`: `e740790c9fd55b9f1f8095d30ee8897b3254c4c0`.
- Candidate branch: `m6/153-exit-gate`.
- Campaign producer branch HEAD: `ba73051eb74fc02d46dc4f9cfd81ec84d99a9cde`;
  the retained reports execute merge ref
  `51f8e8c64d2dfb43b091268a8ffa893b5b3252b2`. The exact final PR HEAD remains
  the GitHub PR reference and must be rechecked after any retention commit;
  each retained provenance entry records the producer branch head separately.
- PR: #187, `M6: complete conformance, performance evidence, docs, and exit gate (#153)`.
- #152 is `CLOSED (COMPLETED)` and PR #180 is `MERGED` at the starting
  canonical SHA. #153 and #140 remain `OPEN`.
- `origin/main` subsequently advanced through dependency PR #184. The
  retained campaigns below were dispatched on the exact candidate branch ref
  so their semantic object manifests bind to the candidate tree; no rebase or
  merge was performed in this focused campaign.

## M6 scope and delivery

M6 delivered the item-processing component model, `ItemStream` state,
completion policies, listener/fault taxonomy, file/JSON/JSONL/database and
multi-resource components, the `oxide-batch-test` surface, and pipeline-builder
ergonomics across #143–#152. #153 adds no capability, API abstraction, or new
execution path. It supplies the frozen campaign evidence, documentation,
ledger reconciliation, and exit record.

| Issue | Delivered | Primary PR |
|---|---|---|
| #143 | ADR-0008 item contract and chunk runtime migration | #160 |
| #144 | `ItemStream` lifecycle and component-state contract | #161, #173/#175 |
| #145 | `oxide-batch-test` application/component/restart harness | #162 |
| #146 | processors, delegates, classifiers, and composites | #164 |
| #147 | restartable delimited/CSV/fixed-width components | #165 |
| #148 | restartable JSON/JSONL components | #166 |
| #149 | PostgreSQL cursor/paging/SQL batch and enlisted writer | #169 |
| #150 | multi-resource and object-store components | #176, #178 |
| #151 | completion policies and listener-taxonomy evidence | #179 |
| #152 | item pipeline configuration ergonomics | #180 |

## Gate B — typed versus Boxed transaction/restart equivalence

Frozen protocol: `docs/project/m6-design-gate-evidence.md#gate-b--transactionrestart-equivalence-protocol`.
The retained reports are `docs/engineering/campaigns/m6/gate-b-campaign-postgres-15.json`
and `gate-b-campaign-postgres-18.json`, produced by the successful workflow run
[33298170899](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170899)
triggered by the recorded candidate branch HEAD. Both PostgreSQL jobs passed
with nine targets and zero violations.

| Scenario | PostgreSQL 15 | PostgreSQL 18 |
|---|---:|---:|
| B-01 `normal_enlisted_commit_is_representation_identical` | PASS | PASS |
| B-02 `writer_failure_before_commit_rolls_back_identically` | PASS | PASS |
| B-03 `state_checkpoint_counter_share_one_atomic_boundary` | PASS | PASS |
| B-04 `unknown_commit_outcome_forces_recovery_not_inference` | PASS | PASS |
| B-05 `process_kill_before_commit_restart_is_identical` | PASS | PASS |
| B-06 `process_kill_around_commit_acknowledgement_is_identical` | PASS | PASS |
| B-07 `multi_chunk_restart_selects_identically` | PASS | PASS |
| B-08 `representation_does_not_change_definition_or_restart_identity` | PASS | PASS |

The comparison uses structured `GateBObservation` values rather than log
eyeballing. The process-kill cases use real worker processes and fresh
connections, and restartable `ItemStream` state is restored from PostgreSQL
rather than manually injecting a reader position. Durable business rows,
checkpoints, component-state envelopes, optimistic versions, counters,
normalized repository writes, and lifecycle observations are representation
independent in both release-blocking matrix points. B-04 additionally asserts
the runtime `ChunkExecutionOutcome::Unknown` and public launch
`TaskletOutcome::CommitOutcomeUnknown` paths before durable recovery is
explicitly selected.

## Gate H / P-002 — real-component performance

Frozen protocol: `docs/project/m6-design-gate-evidence.md#gate-h--p-002-real-component-performance-protocol`.
The workload is the shipped `DelimitedReader`/`DelimitedWriter` around
`IdentityProcessor`, with the same dataset, component logic, chunk semantics,
and release compiler profile for typed and `BoxedReader`/`BoxedProcessor`/
`BoxedWriter`. Retained report:
`docs/engineering/campaigns/m6/gate-h-campaign.json`, from workflow run
[33298170870](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170870).

Hard gates:

- typed framework-controlled future allocation per item: **0**, structural
  proof in `gate_h_dispatch.rs`;
- typed framework-controlled dynamic dispatch per item: **0**, structural
  proof in `gate_h_dispatch.rs`;
- correctness and restart/durable observations: **PASS** through the Gate B
  campaign before performance interpretation;
- no invented throughput or latency threshold is used.

Raw disclosure values from the retained Linux `x86_64` release campaign:

| Metric | Typed | Boxed |
|---|---:|---:|
| allocator calls/item | 24.00141414141414 | 28.00141414141414 |
| allocator calls/chunk | 475228 | 554428 |
| allocated bytes, delta | 10301044 | 11409844 |
| latency samples, ns | 68441542, 68154769, 67735559, 67554583, 68086973 | 67580652, 67501003, 67968142, 67876842, 68053691 |
| min / mean / max latency, ns | 67554583 / 67994685 / 68441542 | 67501003 / 67796066 / 68053691 |
| derived throughput, items/s | 735351.5940253271 | 737505.9195912636 |
| release reference binary bytes | 1728000 | 1744392 |
| release reference compile seconds | 33.060027751 | 33.098316529 |

Each reference was built from a clean, isolated `CARGO_TARGET_DIR`; the
report records `target_directory_isolation: clean-per-reference`. The prior
sequential same-target-directory compile numbers are invalid and are not used
as evidence.

Copied bytes and internal buffer reuse are recorded as `null` with an explicit
“not measurable/not exposed at this component boundary” note. Future boxing
and dynamic-dispatch counts are architecture invariants, not fabricated
runtime counters. The environment records source commit, Rust 1.97.1,
Linux/x86_64, release profile, matrix `performance-linux-release`, workload,
chunk size, warmup, repetitions, and raw samples.

The listener-enabled companion is separate from the listener-free invariant:
the retained listener target measured 158421 allocator calls over the 19,800
item delta, or 8.001060606060607 calls/item. This is expected boxed listener
representation under the accepted Gate F decision and is not included in the
typed component allocation guarantee.

## Full M6 component conformance and failure matrix

The fixed denominator is 45 targets and 441 tests for each PostgreSQL matrix
point, with zero ignored tests, zero non-success outcomes, and zero campaign
violations. Reports:

- `docs/engineering/campaigns/m6/m6-conformance-campaign-postgres-15.json`;
- `docs/engineering/campaigns/m6/m6-conformance-campaign-postgres-18.json`;
- workflow run [33298170825](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170825).

The catalog and normalized dispositions are in
`docs/engineering/campaigns/m6/component-conformance-matrix.md`. It covers
typed and Boxed item components, `ItemStream`, completion policies including
adaptive/composite policies, processors/delegates/classifiers/composites,
delimited/fixed-width/JSON/JSONL, multi-resource and object-store components,
PostgreSQL cursor/paging/writer components, listener ergonomics,
`ChunkPipelineBuilder`, `ChunkJob`, `FlowJob`, and the public
`oxide-batch-test` surface. Each applicable component disposition covers
success, malformed input, partial/failure, rollback, stop/cancellation, panic,
close/lifecycle, restart/process-kill, state corruption/version rejection,
resource bounds, and diagnostic redaction. Contract-based `N/A` and inherited
evidence are explicitly reasoned; they are not omissions.

One narrow M6 correctness gap was found and closed: `MultiResourceWriter`'s
durable-checkpoint contract lacked a crash/restart fixture. The existing API
was sufficient, so the fix is a regression test in
`crates/oxide-batch-test/tests/postgres_multi_resource_restart.rs`, not a new
capability. A test-only mutex also serializes two shared migration-counter
fixtures exposed by the full all-features run; no production state semantics
changed.

## Documentation and support

The four user-facing guides are published and indexed by the documentation
strategy:

- `docs/guides/component-reference.md`;
- `docs/guides/extension-guide.md`;
- `docs/guides/restart-and-state.md`;
- `docs/guides/test-kit-tutorial.md`.

They document inputs/outputs, formats, state and checkpoint ownership,
ordering, restartability, transaction/delivery capability, bounds,
cancellation, close behavior, thread safety/reentrancy, malformed-input and
failure classification, redaction, and support tier. The test-kit tutorial
uses the actual public API in compile-checked examples for component, job,
restart, deterministic fixture, failure, panic, stop, and supported
crash/restart testing.

Every shipped M6 surface is classified as **First-party** in the existing
integration model. Candidate evidence is complete, but release-backed
`Verified` promotion remains pending a named release. Object-store durability
limitations and other support boundaries are recorded in
`docs/release/support-matrix.md`.

## Compatibility ledger reconciliation

All 23 M6-scoped rows in `docs/compatibility/conformance-matrix.md` were
reviewed, including the four shared M2/M6 rows. Four shared rows received
wording corrections so their released `0.5.0` verification cannot be read as
retroactive M6 evidence. M6-specific rows now point to the candidate evidence
and retain their implementation disposition. No row was promoted to released
`Verified`: the ledger requires a named released version carrying the required
evidence, and campaign PASS alone does not bypass that rule.

## Retained evidence and provenance

M5 and M6 were re-run and re-retained for the release candidate because the
release preparation changed Cargo manifests, `Cargo.lock`, and verifier/release
inputs in the campaigns' semantic closure. The 16 M5 reports and five M6 reports
are byte-for-byte extracted artifacts from fresh successful PR #209 runs, with
run IDs, attempts, producing jobs, artifact IDs/digests/sizes, report git blobs,
execution commit, and remote verification recorded in:

- `docs/engineering/campaigns/m5/evidence-provenance.json`;
- `docs/engineering/campaigns/m6/evidence-provenance.json`.

The M6 artifact set is retained by Gate B run
[33298170899](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170899),
Gate H run
[33298170870](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170870),
and full conformance run
[33298170825](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170825).
The fresh M5 campaign runs are
[33298170868](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170868)
(cancellation),
[33298170865](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170865)
(conformance),
[33298170873](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170873)
(crash/restore),
[33298170821](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170821)
(performance),
[33298170845](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170845)
(resource bounds),
[33298170850](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170850)
(security),
[33298170886](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170886)
(soak), and
[33298170794](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33298170794)
(upgrade). All completed successfully with PostgreSQL 15/18 where applicable.

Every fresh report records execution tree
`1ac9cdc5f0337e361187eb0f6dcffacc1fdd49d1`, triggered from PR #209 branch
head `a233e12787cab06ce998d34e05a0ba703ac04474`. The final
evidence-retention commit changes no declared campaign semantic-closure path.
The provenance verifier checks both milestone directories.

The retained M6 artifact identities are: Gate B PostgreSQL 15 artifact
9728089273 with digest
`sha256:d416ff38066e96b96fc4e46fdcfb3e0a577a2aea73c2211f73a516999061ee6e`,
Gate B PostgreSQL 18 artifact 9728088920 with digest
`sha256:c60aa41505746725aa258da235b5e10c3af13cac69dd1710c357772f737ca9cb`,
Gate H artifact 9728101917 with digest
`sha256:626cfb4b6a01e9f6823792cb1c766a3bbd08f47d71113647c9e577ea2f76180a`,
and full-conformance PostgreSQL 15/18 artifacts 9728114048 and 9728112543
with digests
`sha256:5c1be7ab68cef3151848dac04121107519fe6c185cc7397b3eeaaa13c8db29fe`
and `sha256:77f3e9008bbce3d2a86fd94265f7b3ec0ab6f88f59e4bc0690f5afdd27cb678a`.

## P0/P1 review and limitations

Live GitHub issue review found no open `priority:p0`. Open `priority:p1`
includes #188, #189, #153, and #140. #188 is a blocking issue by its
canonical body (`Blocks #153`), and this PR declares `Closes #188`. Its
completion-policy CI criterion is satisfied by the explicitly named
`postgres_completion_policy_restart` target in the M6 conformance campaign;
it ran once and passed on both PostgreSQL 15 and 18 with zero ignored tests.
The issue remains open only because this PR has not merged yet; merging this
PR closes it. #189 is a separate M7–M14 ledger audit and is outside the M6
exit scope. No other correctness P0/P1 remains unaddressed in the candidate
evidence.

Known non-blocking follow-ups are the release-backed promotion of reviewed
ledger rows and the future M7–M14 ledger reconciliation in #189. #188 is not
a follow-up: it is the blocking issue linked for closure by this PR. No new
API, architecture, or capability follow-up was invented by this campaign.

## Final M6 exit disposition

The candidate evidence is complete from producer branch HEAD
`ba73051eb74fc02d46dc4f9cfd81ec84d99a9cde`: Gate B 8/8 on PostgreSQL 15/18,
Gate H hard invariants and disclosure metrics, full 45-target component
conformance on PostgreSQL 15/18, M5 re-retention, documentation, and ledger
reconciliation are complete. The latest retained reports use merge ref
`51f8e8c64d2dfb43b091268a8ffa893b5b3252b2`. The PR Evidence workflow remains the
authoritative check of the merge-ref state; any commit added after the
producer head requires strict review against the new exact PR HEAD.

**Final disposition: READY FOR STRICT REVIEW — DO NOT MERGE.** #188 remains
open until this PR merges; #140 also remains open and is not closed by this
PR. Both closure actions remain outside this agent's boundary.

## Release-preparation handoff (2026-08-30)

The closure statements above are retained historical observations from the M6
exit record and are not the current issue state. The merged M6 exit tree is
now `main` at `47eedc5b3a8e8f82f0a8c7386f9f49698ca82d2e`; #153, #188, and #140
are closed, and #190 is open with `status:ready`. This release-preparation
candidate selects package version `0.6.0` and prospective tag `v0.6.0`, but
neither exists yet. The retained M6 campaign PASS remains candidate evidence:
no M6 ledger row is promoted to released `Verified` until the named release,
packaged artifacts, provenance, and post-publication consumers are verified.
