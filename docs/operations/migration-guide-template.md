# OxideBatch Metadata Migration: Version N to N+1

**State:** Template

**OxideBatch versions:** `<old>` to `<new>`

**PostgreSQL majors tested:** `<explicit majors>`

## Compatibility and impact

- source schema version:
- target schema version:
- oldest/newest application versions allowed before and after migration:
- rolling deployment supported: yes/no, with reason:
- expected advisory-lock wait, migration duration, and launcher downtime:
- transactional and non-transactional phases:
- additional disk/WAL/connection requirements:

## Before migration

1. Stop or quiesce launchers and record remaining execution states.
2. Resolve or explicitly retain every orphan/`UNKNOWN` execution.
3. Record OxideBatch, application, PostgreSQL, and schema versions.
4. Validate runtime and migrator roles plus certificate expiry/hostname.
5. Take the named backup and prove it can be restored.
6. Run the documented capacity and invariant queries.

Include exact, redacted commands and expected safe output here.

## Apply

List each immutable migration filename and checksum. Show how deployment tooling
supplies the migrator identity without putting credentials in shell history,
logs, configuration examples, or the migration file.

For each non-transactional phase, document its completion marker, idempotent
resume command, and manual-repair escalation.

## Verify

- singleton schema version and released migration checksums;
- table/constraint/index invariants changed by this release;
- representative job-instance, execution, step, checkpoint, and recovery reads;
- runtime DML succeeds while runtime DDL fails;
- operator reader remains read-only;
- validated TLS and safe diagnostics;
- canary launch/restart and acceptance IDs.

Include expected row counts or invariant results, never parameter, context,
checkpoint, record, credential, or certificate values.

## Resume service

Describe canary selection, observation window, abort thresholds, staged
launcher resumption, and the owner who approves full resumption.

## Rollback and restore

State whether a tested reverse migration exists. The default is:

1. stop all new writers;
2. preserve failed-migration diagnostics and audit evidence;
3. recreate a clean compatible database/schema;
4. restore the verified pre-migration backup;
5. verify the old schema and application;
6. resume through a canary.

Name data created after the backup that would be lost, the recovery decision
owner, restore-time objective, and escalation path.

## Evidence

Link CI upgrade fixtures for every supported source version, backup/restore
output, query-plan changes, review approval, release notes, and any accepted
residual risk.
