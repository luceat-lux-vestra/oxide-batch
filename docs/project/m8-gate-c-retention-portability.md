# M8 Gate-C Retention, Archive, Export, and Restore Portability

**State:** Accepted design  
**Owner:** M8 tracker [#201](https://github.com/luceat-lux-vestra/oxide-batch/issues/201),
Gate-C [#645](https://github.com/luceat-lux-vestra/oxide-batch/issues/645)

**Governing decisions:**
[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md),
[RFC-0010](../rfcs/0010-metadata-and-spring-migration.md),
the [repository and transaction model](../architecture/repository-and-transaction-model.md),
the [M4 operator/explorer/retention contract](../architecture/operator-and-explorer-services.md),
and [persistence and migrations](../operations/persistence-and-migrations.md).

This record freezes the M8 portability boundary for metadata retention,
archive/export, and restore evidence. It preserves the accepted PostgreSQL M4
hold plus bounded two-phase purge semantics without turning PostgreSQL SQL,
table layout, role names, or physical backup formats into portable APIs.

It is a design contract, not implementation or release evidence. In
particular, it does not promote `REPO-RETENTION-001` or
`META-RETENTION-001`, add retention capabilities to an adapter, or claim a
released support tier.

## Ownership boundary

Gate-C separates four concerns that must not collapse into one generic
repository API.

| Concern | Portable ownership |
| --- | --- |
| Holds | Optional repository capability `InstanceHolds` |
| Bounded purge | Optional repository capability `RetentionPurge` driven by the portable `RetentionService` |
| Retention archive/export | Optional framework operational service/profile above repository ports |
| Backup/restore | Backend-private operational tooling plus adapter support-certification evidence |
| Neutral migration package encoding/import | M12 / RFC-0010 |
| Retention scheduling | External orchestration/control plane |

Archive/export is therefore not a mandatory `JobRepository` method. Backup
is not archive, and an archive package is not a database backup. Scheduling may
invoke bounded retention operations but never weakens their guards.

An adapter that does not claim an optional retention profile fails closed.
Runnable-core or database support does not imply retention support.

## Portable purge floor

The accepted M4 purge model is the portable semantic floor.

An execution may be selected only when its status is `COMPLETED`, `FAILED`,
`STOPPED`, or `ABANDONED`, it satisfies the requested minimum age and
terminal-status set, and its logical instance is not held. If any execution of
the same instance is `STARTING`, `STARTED`, `STOPPING`, or `UNKNOWN`,
the whole instance is excluded.

Definitions, definition lineage needed to interpret retained history, schema
markers, and retained retention evidence are never implicit purge candidates.

Gate-B durable-effect authority has an independent survival floor. A generic
history purge must not cascade away an unresolved effect journal record, pending
or exposed-unknown Outbox obligation, live InboxDedup marker, or the minimum
identity/non-reuse/reconciliation tombstone required by the
[Gate-B record](m8-gate-b-durable-effect-delivery.md). Effect/dedup cleanup is a
separately authorized bounded operation whose own replay/collision window must
have ended.

### Plan and apply

Purge remains two phase.

1. Planning is bounded and read-only. The canonical version-1 plan digest covers
   the job name, terminal statuses in stable order, minimum age, batch bound,
   candidate instance/execution identities, and observed execution versions.
   Projected row counts are informational and are not deletion authority.
2. Apply is authoritative. Inside one bounded atomic unit it revalidates the
   exact plan digest, candidate eligibility and versions, absence of
   active/stopping/`UNKNOWN` siblings, and absence of a hold.

A stale plan fails with no deletion. The portable ordering guarantee is
referential/dependency safety rather than one fixed SQL statement order:
referencing/child metadata is removed or detached before the execution or
instance it references, no durable dangling reference remains, and the
successful retention audit record commits with the deletion effects.

Interrupted work exposes either no batch or one complete committed batch.
Replanning observes only remaining eligible history. Replaying a completed
operation identifier returns the recorded outcome rather than deleting twice.

A profile that claims `RetentionPurge` necessarily provides
`InstanceHolds`; hold exclusion is part of purge correctness, not an optional
enhancement. Certification must reject a profile that advertises purge without
holds.

## Archive-before-purge profile

Gate-C does not invent a second migration-package encoding. Instead it freezes
the semantic profile a future neutral RFC-0010 package must satisfy when that
package is used as retention archive evidence.

A retention archive identifies at least:

- framework version or exact source revision;
- adapter kind, exact database/server version where applicable, and metadata
  schema version;
- export/archive tool version and retention-archive profile version;
- represented definition/manifest and context/checkpoint codec/schema versions;
- bounded source selection and its canonical digest;
- source snapshot, quiescence identity, or equivalent consistency token;
- package format version, immutable package/root digest, and segment/object
  digests when segmented;
- per-record-family counts, total logical record count, and encoded size;
- lineage/mapping versions, redaction classification, and every omitted or
  transformed-field disposition;
- restorability class and verification verdict.

Unknown package/profile versions, definition semantics, status semantics, or
required context/checkpoint codecs fail closed. Older forms are accepted only
through an explicit bounded directed reader/migration path. Numeric version
ordering never implies compatibility, and a source schema is not silently
reinterpreted under a different schema.

Cross-database import/restore is not implied by an adapter-neutral package.

### Restorability classes

`history_only` preserves interpretable historical evidence but makes no
restart claim.

`restart_restorable` additionally preserves and certifies the definition or
manifest identity, parameters, checkpoint/context state, durable decisions, and
other restart authority needed by the supported restart contract.

Archiving `FAILED` or `STOPPED` history never silently promises restart.
Before an archive-required purge, policy either requires
`restart_restorable` evidence or records an explicit irreversible
history-only decision.

### Archive success

A successful exporter process is insufficient. Archive success requires all
applicable checks below:

- quiesced or explicitly snapshot-consistent source observation;
- successful digest verification for every package/segment;
- manifest counts matching decoded or streamed record counts;
- identity and referential reconciliation;
- every omission/transformation declared and permitted by the selected profile;
- complete readability by the certified reader/profile;
- representative normalized records reconciled against source observations;
- durable retained archive evidence.

A partial artifact never becomes a completed archive and never authorizes
purge. Immutable completed segments may be reused only after digest
verification; the success marker is published after all verification.

When policy requires archive before purge, archive evidence is bound to the
exact source selection and, when derived from a purge plan, that exact plan
digest. If lifecycle, version, or hold state changes and the plan becomes
stale, old archive evidence does not silently authorize the new deletion set.

Ordinary guarded purge remains valid for policies that do not require archive.

## Evidence surviving source deletion

Retention/archive evidence outlives the instance and execution rows whose
history it authorizes deleting. It is not cascade-owned by those rows.

The durable bounded evidence may retain:

- operation identifier, opaque actor reference, and closed reason/policy code;
- action/profile version;
- source framework/adapter/database/schema identity;
- source-selection or purge-plan digest;
- package root digest or immutable artifact reference;
- bounded counts, restorability class, verification verdict, and timestamps;
- a bounded omitted/transformed-field summary or digest.

It must not duplicate parameter values, context/checkpoint payloads,
credentials, connection details, user error text, or raw archived records.
Detailed record identities and payloads live in the governed immutable package,
not unbounded audit rows.

Deleting or expiring the archive artifact/evidence is a separately governed
retention action. Source-history purge must not silently delete the only proof
that an archive-required policy succeeded.

## Least privilege

Portable privilege classes describe authority, not backend usernames.

- the ordinary runtime cannot perform destructive retention or archive
  administration;
- a retention reader/planner may read only the bounded metadata needed to plan
  and cannot delete;
- an archive/export identity may read required metadata and write the
  deployment-owned archive artifact, but cannot delete source metadata or
  change schema;
- a purge identity may perform the bounded guarded deletion and append its
  retention evidence, but cannot administer schema or mutate a completed
  archive artifact;
- migration/import and backup/restore identities remain separate operational
  authorities.

Adapters certify equivalent restrictions with backend-specific grants or
permissions. PostgreSQL's current security campaign is the reference evidence,
not the portable role naming scheme.

## Resource, replay, and interruption bounds

The portable contract does not impose one arbitrary maximum archive size.
Every operation nevertheless has explicit finite limits for source page/query
size, records per segment, total logical records, total encoded bytes,
manifest/object count, database read deadlines, artifact read/write deadlines,
verification deadlines, and retained evidence size.

Export, verification, and import are streaming/bounded operations; certification
must not require loading full history or a full package into memory.

Exact replay reconciles operation and artifact/source identity before repeating
work. Conflicting reuse fails closed. Cancellation before finalization leaves
no success claim or purge authority; cancellation after finalization does not
invalidate already verified immutable evidence.

The existing purge batch ceiling remains `MAX_PURGE_BATCH = 1000`; archive
segmentation need not equal the purge batch size.

## Backup/restore support certification

Backup/restore remains an adapter-private operational surface. Named support
requires evidence that a consistent backup can restore into a clean isolated
target and recover the repository's supported semantic state, not merely a
successful backup-tool exit.

The default rollback path is restore of the verified pre-change backup.
Reverse migration is supported only when separately designed and tested.

For a first-support adapter with no released predecessor schema, same-schema
backup/restore is sufficient; Gate-C never invents an unsupported migration
source. MongoDB therefore certifies first support using schema-3 to clean
schema-3 restore. The first future release-backed MongoDB schema bump must add
pre-upgrade backup plus restore-only rollback evidence.

## Current implementation disposition

PostgreSQL already declares `InstanceHolds` and `RetentionPurge`, runs the
shared full service contract, and has retained PostgreSQL 15/18
least-privilege security evidence. That makes it the reference implementation
for the existing purge floor; it does not yet implement the archive-evidence
service described above.

MySQL/MariaDB, SQLite, SQL Server, and MongoDB currently declare no optional
retention capabilities. This is an accepted fail-closed profile, not an
implicit retention claim.

M8 follow-up implementation remains bounded to:

1. explicit retention capability negotiation/certification, including the rule
   that purge implies holds;
2. a portable bounded archive-evidence service/profile;
3. PostgreSQL persistence and certification for that archive evidence.

General neutral package encoding, Spring extraction/mapping, and general
metadata import/export remain M12 work under RFC-0010.

## Ledger boundary

This design record does not change ledger status.

- `REPO-RETENTION-001` remains `Partial` until its M8 implementation and
  certification tail is completed.
- `META-RETENTION-001` remains `Planned`; M8 owns archive/export portability
  and M14 owns final compatibility/readiness carry-forward.
- `MIG-METADATA-001` remains `Planned` and M12-owned.

Documentation or a green design review alone cannot promote any of these rows.
