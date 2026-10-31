# M7 Advanced Flow, Scope, Repeat, and Composition Exit Evidence

**State:** Accepted M7 exit evidence for issue #200.

**Acceptance:** PR [#624](https://github.com/luceat-lux-vestra/oxide-batch/pull/624)
reviewed exact candidate `5e2a1ff6bcd4666d0fb4db6131b3d9d8d4886347`.
Ready-state PR CI run #811 / `41166958319` completed successfully, and the
candidate was squash-merged as
`cad7abd196406b6290ca24b78ddb0fe5b1cbea5c`.

**Milestone:** M7 / GitHub milestone 8

**Umbrella:** [#192](https://github.com/luceat-lux-vestra/oxide-batch/issues/192)

**Exit owner:** [#200](https://github.com/luceat-lux-vestra/oxide-batch/issues/200)

**Evidence baseline:** `main@e04848b7ccebb9a12bde3db7cc3a94b812245ca1`

## Decision boundary

This record closes the M7 implementation/evidence milestone only. It does not
create a new released support claim, does not promote unreleased M7 ledger rows
to `Verified`, and does not imply M8-M14 completion or project-wide 1.0/GA
readiness.

M7 semantics are repository- and database-neutral at the public/core contract
boundary. PostgreSQL 15/18 provide the currently implemented durable-adapter
evidence. Additional database adapters and cross-database certification remain
later portability scope.

The released support boundary remains the published `0.6.0` record in
[`docs/release/support-matrix.md`](../release/support-matrix.md). The M6
operator and developer guides remain release-bound documents and are not
rewritten here as M7 release claims.

## Semantic conformance

The bounded M7 implementation owners are complete:

- [#195](https://github.com/luceat-lux-vestra/oxide-batch/issues/195) owns
  advanced compiled flow, nested/split/nested-job composition, and custom-step
  extension under the existing execution authority.
- [#196](https://github.com/luceat-lux-vestra/oxide-batch/issues/196) owns job
  and step scopes, component factories, late binding, cleanup, sensitivity, and
  restart provenance.
- [#197](https://github.com/luceat-lux-vestra/oxide-batch/issues/197) owns
  repeat context, interceptor composition, durable repeat identity/state, and
  flow-level fault composition.
- [#198](https://github.com/luceat-lux-vestra/oxide-batch/issues/198) owns the
  DefinitionRegistry, compatible-upgrade rules, explicit fork lineage, and
  fail-closed drift rejection. Its accepted matrix is
  [`m7-gate-d-acceptance-evidence.md`](m7-gate-d-acceptance-evidence.md).
- [#199](https://github.com/luceat-lux-vestra/oxide-batch/issues/199) owns the
  parameter incrementer, launcher identity, non-restartable/start-control
  completion, strict/compatible operator restart selection, explicit fork
  operation, and bounded explorer projections.

Supporting accepted M7 records include:

- [M7 design-gate evidence](m7-design-gate-evidence.md);
- [repeat definition-identity evidence](m7-299-repeat-definition-identity-evidence.md);
- [fork-lineage contract](m7-552-fork-lineage-contract.md);
- [start-controls evidence](m7-start-controls-evidence.md);
- [operator restart-selection evidence](m7-operator-restart-selection-evidence.md);
- [operator fork evidence](m7-operator-fork-evidence.md).

No second execution engine, repository authority, or operator-side lifecycle
authority is introduced by these slices.

## Repository-neutral contract

[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md)
remains the feature-status authority.

Gate-F reconciliation in PR
[#621](https://github.com/luceat-lux-vestra/oxide-batch/pull/621) reviewed all
25 M7-scoped ledger rows. Nine rows retain `Verified` only for their named
released base; sixteen unreleased M7 rows are `Implemented` candidates.
No M7 row remains `Planned` or `Partial` solely because an M7 implementation
tail is unfinished.

Those dispositions are defined by the public/core and repository contracts,
not by a PostgreSQL-specific semantic model. Durable-adapter evidence is a
separate proof layer.

## PostgreSQL durable-adapter evidence

PostgreSQL 15 and 18 are the currently implemented durable-adapter proof axes
for M7.

PR [#623](https://github.com/luceat-lux-vestra/oxide-batch/pull/623) reconciled
the committed upgrade/security campaign denominator to schema 12 before final
Gate-F execution. In particular it preserves the historical schema 1/2/3
direct-upgrade denominator, records the later populated migration boundaries
through schema 12, advances current-schema rejection/rollback authority, and
adds the schema-11 immutable fork-lineage privilege evidence.

The final no-skip conformance proof is the manually dispatched
[M5 Conformance run #1914](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/41049339895),
run ID `41049339895`, against exact
`main@e04848b7ccebb9a12bde3db7cc3a94b812245ca1`.

The following jobs all completed successfully:

- PostgreSQL 15 deep conformance shard 0;
- PostgreSQL 15 deep conformance shard 1;
- PostgreSQL 18 deep conformance shard 0;
- PostgreSQL 18 deep conformance shard 1;
- PostgreSQL 15 deep canonical conformance campaign;
- PostgreSQL 18 deep canonical conformance campaign;
- final `postgres-15-conformance-campaign` required context;
- final `postgres-18-conformance-campaign` required context.

The final required contexts explicitly reported that the deep proof completed
successfully for PostgreSQL 15 and PostgreSQL 18. This is visible
`workflow_dispatch` execution, not a green result obtained by skipping the
deep route.

The run retained six evidence artifacts for 30 days:

- `conformance-shard-postgres-15-0`;
- `conformance-shard-postgres-15-1`;
- `conformance-shard-postgres-18-0`;
- `conformance-shard-postgres-18-1`;
- `conformance-campaign-postgres-15`;
- `conformance-campaign-postgres-18`.

Gate-D and the later accepted M7 slices retain the dedicated process-kill,
crash/restart, migration, rollback, corruption/newer-schema rejection, and
least-privilege evidence for the newly durable M7 boundaries. The manual deep
run is the final visible conformance execution over the reconciled schema-12
authority; it does not replace those focused records.

## Bounded resources, cancellation, telemetry, and diagnostics

M7 composes with the existing bounded execution, cancellation, telemetry, and
diagnostic authorities rather than introducing parallel mechanisms. The
accepted M7 implementation records preserve bounded graph/split behavior,
attempt-local scope cleanup, deterministic repeat/fault composition, bounded
explorer projections, and fail-closed definition/restart selection.

The schema-12 campaign reconciliation also keeps the security campaign's
redaction sweep and restricted-role evidence aligned with the newly durable M7
tables. M7 therefore does not widen public diagnostic exposure or create an
unbounded query/control surface as an exit side effect.

## Documentation and release-boundary reconciliation

PR [#622](https://github.com/luceat-lux-vestra/oxide-batch/pull/622) reconciled
README, roadmap, AGENTS, documentation index, changelog, and M7 historical or
accepted evidence states after #195-#199 completed.

The reconciliation deliberately leaves the published `0.6.0` support matrix,
operator guide, and developer guide release-bound. M7 implementation evidence
is not a release publication event and therefore does not automatically
promote an unreleased row to release-backed `Verified`.

## Severity gate

At the accepted evidence baseline there is no open P0 issue. The only open P1 found in
the repository is
[#521](https://github.com/luceat-lux-vestra/oxide-batch/issues/521), a CI
compile/link performance research issue. It is not a correctness, security, or
data-integrity blocker for M7 exit.

## Criterion-by-criterion disposition

| #200 criterion | Disposition | Evidence |
| --- | --- | --- |
| Every M7-scoped ledger row has a reviewed disposition and complete evidence links | Satisfied | PR #621; compatibility ledger; #195-#199 accepted ownership |
| No correctness/security/data-integrity P0/P1 remains | Satisfied | No open P0; open P1 #521 is CI performance research |
| Required PostgreSQL 15/18 evidence is visibly executed, not green-by-skip | Satisfied | M5 Conformance run #1914 / `41049339895`, exact `e04848b7ccebb9a12bde3db7cc3a94b812245ca1`, all deep shards/campaigns/final contexts successful |
| M7 docs distinguish implemented, released/verified, deferred, and later-milestone capability | Satisfied | PR #622 and unchanged release-bound `0.6.0` support/guides |
| This exit record exists with criterion-by-criterion evidence | Satisfied | PR #624 reviewed the exact candidate, ready-state PR CI #811 succeeded, and merge commit `cad7abd196406b6290ca24b78ddb0fe5b1cbea5c` placed this record on `main` |
| #192 can close without implying M8-M14 or project-wide GA readiness | Ready for final bookkeeping after #200 | This record explicitly excludes M8-M14 and 1.0/GA claims; #192 must be reconciled and closed separately |

## Portability and later milestones

M7 does not claim MySQL, SQLite, Oracle, or another durable adapter. It also
does not reserve adapter-specific behavior in the public/core semantic
contract.

Repository portability and additional database adapters belong to M8 and later
certification work. Messaging, high-performance local execution, distributed
execution, Spring migration closure, extension-ecosystem work, and project-wide
GA remain M9-M14 scope.

Closing M7 therefore means only that the M7 capability and evidence gate is
complete on its accepted boundary.

## Exit disposition

The technical Gate-F evidence is complete at the baseline recorded above, and
this document is accepted through PR #624.

With the exit record now accepted:

1. #200 may mark the exit-record criterion satisfied and close after its final
   authoritative readback;
2. #192 must then be reconciled against this accepted M7 record and the current
   milestone issue state;
3. #192 may close separately without changing any M8-M14 or project-wide GA
   status.

This document is accepted M7 Gate-F evidence. #200 and #192 remain open until
their separate issue-bookkeeping checkpoints complete.
