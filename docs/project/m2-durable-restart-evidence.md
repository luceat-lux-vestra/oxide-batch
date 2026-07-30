# M2 Durable Restart and Explicit Recovery Evidence

**State:** Complete on merge

**Issue:** [#44](https://github.com/luceat-lux-vestra/oxide-batch/issues/44)

**Date:** 2026-07-30

This record maps the sixth M2 workstream's exit criteria to definition-guarded
restart selection and audited PostgreSQL recovery. It does not claim that the
M2 crash matrix, release conformance slice, recovery runbook, or milestone exit
gate is complete; issue #45 owns those results.

| Exit criterion | Evidence |
| --- | --- |
| Canonical instance and distinct attempts | Launch selection still locks the database-authoritative instance and allocates a new job execution attempt. A restart creates a new step execution ID and retains the prior execution through `restart_of_execution_id`. |
| Exact definition identity | `DefinitionIdentity` builds bounded framework-owned canonical manifests from validated job, step, chunk, application component revisions, checkpoint/context schemas, and declared delivery mode. SHA-256 digests are persisted with the application revision. Reusing a revision with a different digest returns `DefinitionDrift` before execution creation or user work. |
| Directed compatibility | A different digest is rejected as `IncompatibleDefinition` unless one exact, registered `DefinitionUpgrade` exists from the checkpoint-producing definition to the proposed definition. Edges reject self, empty, duplicate-source, and duplicate-target mappings and are never inferred, reversed, or made transitive. |
| Compatible committed state | PostgreSQL step creation resolves an exact or mapped source step and copies only its committed checkpoint, execution context, and six counters into a new `STARTING` step attempt. Missing mapped state is typed and prevents a partial attempt. The M2 upgrade is deliberately byte-preserving and therefore supports only unchanged state schemas; it does not claim schema transformation. |
| Terminal and unresolved selection | `COMPLETED` and `ABANDONED` instances remain non-restartable. `STARTING`, `STARTED`, `STOPPING`, and `UNKNOWN` attempts return `ExecutionAlreadyActive`; no age, process liveness, or transport signal automatically steals them. |
| Explicit audited recovery | `RecoveryRequest` requires the observed execution version, a `FAILED` or `ABANDONED` disposition, bounded reason and operator correlation, a 32-byte external evidence digest, and a typed failure category/correlation. PostgreSQL rereads the execution under `FOR UPDATE`, appends the audit row, and compare-and-swap updates status in one savepoint-protected repository transaction. |
| Durable inspection before replay | Recovery decisions operate only on the freshly locked durable snapshot. `UNKNOWN` does not become restart-eligible until the audited transition to `FAILED` commits; a stale request fails without publishing an audit or state mutation. The append-only decision is queryable by execution without exposing evidence contents or an unbounded list API. |
| Deterministic reference behavior | `explicit_recovery_is_audited_before_restart`, definition identity tests, and the in-memory definition registry cover bounded validation, redaction, version conflicts, direct edges, and restart blocking without a database. |
| PostgreSQL conformance slice | `durable_restart_requires_compatible_definition_and_inherits_checkpoint` covers drift, absent-edge rejection, directed renamed-step compatibility, distinct attempts, and inherited checkpoint/context/counters. `unknown_execution_requires_audited_postgres_recovery` covers durable `UNKNOWN`, audit inspection, and post-decision restart. CI runs the repository suite on PostgreSQL 15 and 18. |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
./tests/fixtures/postgres/run-design-gate.sh 15
./tests/fixtures/postgres/run-design-gate.sh 18
```

The PostgreSQL commands require a running Docker-compatible daemon. The local
environment used while authoring this record had no daemon, so PostgreSQL
execution remains a CI-required result rather than a local claim.

## Deliberate limits and handoff

- Definition manifests contain only framework-selected non-secret names,
  settings, and application revision tokens. OxideBatch does not inspect or
  hash Rust executable code.
- M2 direct upgrades preserve durable state bytes. Schema-changing checkpoint
  or context upgrades remain rejected until an explicit bounded transformation
  contract is implemented.
- Recovery authentication and authorization are supplied by the deployment.
  The facade retains only an opaque operator correlation and evidence digest;
  no credential or free-form evidence enters metadata.
- M2 exposes the correctness-bearing repository operation and inspection
  history, not the M4/M7 `JobOperator`, CLI, abandonment workflow for already
  finished work, pagination, or general recovery service.
- Issue #45 must still execute crash injection around every commit phase, run
  the PostgreSQL 15/18 release gate, publish setup/recovery operations, and
  record the M2 exit decision.
