# M6 Exit Evidence

**State:** CANDIDATE COMPLETE — READY FOR STRICT REVIEW; DO NOT MERGE.
This record describes the exact candidate branch HEAD and retained evidence;
it does not claim that #140 is closed. The umbrella closes only after an
independent strict review passes on the final PR HEAD and the PR merges.

## Source/base identity

- Starting canonical `main`: `e740790c9fd55b9f1f8095d30ee8897b3254c4c0`.
- Candidate branch: `m6/153-exit-gate`.
- Campaign producer branch HEAD: `cf65947eff30f634763136d15fdcea0df2bd5767`;
  the retained reports execute merge ref
  `98ccb88693262bbb24ba87a33349588e83b14292`. The exact final PR HEAD remains
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
[33263122857](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33263122857)
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
connections. Durable business rows, checkpoints, component state, counters,
repository writes, and normalized lifecycle observations are representation
independent in both release-blocking matrix points.

## Gate H / P-002 — real-component performance

Frozen protocol: `docs/project/m6-design-gate-evidence.md#gate-h--p-002-real-component-performance-protocol`.
The workload is the shipped `DelimitedReader`/`DelimitedWriter` around
`IdentityProcessor`, with the same dataset, component logic, chunk semantics,
and release compiler profile for typed and `BoxedReader`/`BoxedProcessor`/
`BoxedWriter`. Retained report:
`docs/engineering/campaigns/m6/gate-h-campaign.json`, from workflow run
[33263122819](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33263122819).

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
| latency samples, ns | 66131859, 201461846, 65671345, 66619066, 120843021 | 68098785, 69597966, 68833337, 67686219, 123467769 |
| min / mean / max latency, ns | 65671345 / 104145427 / 201461846 | 67686219 / 79536815 / 123467769 |
| derived throughput, items/s | 480097.89234432735 | 628639.706027957 |
| release reference binary bytes | 1728000 | 1744392 |
| release reference compile seconds | 23.177247687 | 3.102704736 |

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
- workflow run [33263122809](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33263122809).

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

M5 was re-run and re-retained because the M6 campaign wiring changed shared
semantic-closure paths. The 16 M5 reports and five M6 reports are byte-for-byte
extracted workflow artifacts, with run IDs, attempts, producing jobs, artifact
IDs/digests/sizes, report git blobs, execution commit, and remote verification
recorded in:

- `docs/engineering/campaigns/m5/evidence-provenance.json`;
- `docs/engineering/campaigns/m6/evidence-provenance.json`.

The M6 artifact set is retained by Gate B run 33263122857, Gate H run
33263122819, and full conformance run 33263122809. The M5 candidate-triggered
campaigns are runs 33263122825 (cancellation), 33263122798 (conformance),
33263122817 (crash/restore), 33263122812 (performance), 33263122836
(resource bounds), 33263122816 (security), 33263122794 (soak), and
33263122814 (upgrade); the exact reports and provenance are retained from
these successful runs. Branch-ref campaign runs were also executed to check
checkout behavior, but are not substituted for these retained merge-ref
artifacts. All completed successfully with PostgreSQL 15/18 where applicable.
The provenance verifier checks both milestone directories.

The retained M6 artifact identities are: Gate B PostgreSQL 15 artifact
9717851433 with digest
`sha256:ff83f32f5d6d9f6330bf604dd27e679adae3711ea3a91780bcfec20fac917a2e`,
Gate B PostgreSQL 18 artifact 9717850271 with digest
`sha256:86a0de40460a2a58226588e74a1f08891bb8fb281c5374041457b08c00693979`,
Gate H artifact 9717858251 with digest
`sha256:35dd71ff563fba074194a79f7c78f489fcbbfbb8cc0fdac572c2b6e136021cf7`,
and full-conformance PostgreSQL 15/18 artifacts 9717872761 and 9717872755
with digests
`sha256:4325d0a0cbcf7c8f9fd751eb9b3f55cc9a581322ad88a63b4e0a97fb20f2a478`
and `sha256:e2cf4d74093bea5e6dda1c685a82e57c77f24780bb9a7dbb8d3f333e7515d523`.

The PR `pull_request` workflows passed at candidate HEAD. Their merge-ref
reports are the retained artifacts because the repository-local evidence
verifier must evaluate the same merge-ref tree in the PR Evidence job after
the base branch advanced.
The final documentation-only retention commits changed no declared campaign
semantic-closure path.

## P0/P1 review and limitations

Live GitHub issue review found no open `priority:p0`. Open `priority:p1`
includes #188, #189, #153, and #140. The #188 completion-policy CI criterion
is satisfied by the explicitly named
`postgres_completion_policy_restart` target in the M6 conformance campaign;
it ran once and passed on both PostgreSQL 15 and 18 with zero ignored tests.
The issue remains open for independent review and merge/closure, so it is a
tracked closure dependency rather than an unaddressed evidence gap. #189 is a
separate M7–M14 ledger audit and is outside the M6 exit scope. No correctness
P0/P1 remains unaddressed in the candidate evidence.

Known non-blocking follow-ups are the release-backed promotion of reviewed
ledger rows, the closure of #188 after merge, and the future M7–M14 ledger
reconciliation in #189. No new API, architecture, or capability follow-up
was invented by this campaign.

## Final M6 exit disposition

The candidate evidence is complete from producer branch HEAD
`cf65947eff30f634763136d15fdcea0df2bd5767`: Gate B 8/8 on PostgreSQL 15/18,
Gate H hard invariants and disclosure metrics, full 45-target component
conformance on PostgreSQL 15/18, M5 re-retention, documentation, and ledger
reconciliation are complete. The latest retained reports use merge ref
`98ccb88693262bbb24ba87a33349588e83b14292`, and `cargo xtask evidence` passes
locally across all 21 retained reports. The PR Evidence workflow remains the
authoritative check of the merge-ref state; any commit added after the
producer head requires strict review against the new exact PR HEAD.

**Final disposition: READY FOR STRICT REVIEW — DO NOT MERGE.** #140 closes
only after this PR passes independent strict review and merges.
