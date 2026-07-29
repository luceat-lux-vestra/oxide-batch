# PostgreSQL Physical Metadata Model

**State:** Implemented for M2

**Schema version:** released `1`

This model defines the first durable repository. The immutable released DDL is
`crates/oxide-batch/migrations/0001_initial_metadata.sql`. The executable draft
under `tests/fixtures/postgres/design-gate/` remains pre-release design evidence
and is not a supported migration source.

## Namespace, encodings, and common rules

M2 uses the fixed `oxide_batch` schema. Configurable identifiers are deferred
until a safe migration/query strategy exists.

- primary and foreign keys are positive signed 64-bit identity values;
- facade IDs remain nonzero `u64` values but the PostgreSQL adapter rejects
  values above `i64::MAX`;
- domain names, definition revisions, schema identifiers, upgrade keys, exit
  codes, and reason codes use bounded UTF-8 `varchar` columns with
  `COLLATE "C"` and explicit `octet_length` checks;
- job and step names, parameter names, definition revisions, and schema/upgrade
  identifiers are at most 128 bytes; exit and reason codes are at most 64
  bytes;
- names are compared byte for byte and are never normalized by the database;
- instance keys and definition digests are 32-byte SHA-256 values stored as
  `bytea`, so locale and textual digest spelling cannot affect equality;
- timestamps are `timestamptz` UTC instants; database `now()` is only a
  bootstrap default, while repository writes bind the facade clock value;
- counters and optimistic versions are non-negative `bigint`;
- JSON envelopes are `jsonb`, must be objects, carry explicit format/schema
  fields, and are checked by `pg_column_size` against the limits below;
- status and category columns use checked text rather than PostgreSQL enums so
  forward migrations can add values transactionally.

The canonical job-instance key hashes a version byte followed by the job name
and identifying parameters in parameter-name byte order. Every field uses an
unsigned big-endian length prefix. Values include a one-byte type tag:
`string`, `i64`, `u64`, or `bool`; integers are fixed-width big-endian and
booleans are `0` or `1`. Only identifying parameters participate. The encoded
input is limited to 1 MiB before hashing. This algorithm is version `1` and has
golden-vector tests in the adapter workstream.

Persisted parameter JSON uses an object keyed by parameter name. Each value is
an envelope with `type`, `identifying`, and `value` members. It preserves
signed/unsigned type identity and is limited to 1 MiB. Parameter and context
payloads are never returned in diagnostics.

## Tables and invariants

### `ob_schema_version`

One row (`singleton = true`) records the OxideBatch schema version. Its primary
key and check constraint prevent a second row or non-positive version.
Repository startup reads it before all other metadata and rejects a value above
the runtime's supported version.

### `ob_job_definition`

One row identifies a restart-relevant definition manifest.

- primary key: `id`;
- unique: `(job_name, definition_revision)`, preventing revision drift;
- unique: `(job_name, manifest_digest)`, allowing exact-definition reuse;
- checks: 32-byte digest, manifest format greater than zero, object-shaped
  manifest no larger than 64 KiB, and bounded byte lengths;
- ownership: registered and read by the runtime repository; immutable after
  insert.

The repository compares the canonical manifest after a uniqueness collision;
the revision match plus digest mismatch is a typed drift error.

### `ob_definition_upgrade`

One directed compatibility edge approved by the application.

- primary key: `(from_definition_id, to_definition_id)`;
- foreign keys: both definitions, restricted on delete;
- unique: `(to_definition_id, upgrade_key)`;
- check: source and target differ, mapping is an object no larger than 64 KiB;
- invariant: both definitions have the same job name, checked in the
  registration transaction;
- ownership: registered by the runtime before it can create an upgraded
  execution; immutable once referenced.

`step_mapping` contains source-to-target step names and context/checkpoint
upgrade identifiers, not user context.

### `ob_job_instance`

One logical occurrence selected by job name plus identifying parameters.

- primary key: `id`;
- authoritative unique key: `(job_name, instance_key)`;
- checks: 32-byte instance key and object-shaped identifying-parameter envelope
  no larger than 1 MiB;
- ownership: created by launch selection and never reassigned to another job
  name or instance key.

The insert uses `ON CONFLICT DO NOTHING`; a follow-up read selects the one
database-authoritative row.

### `ob_job_execution`

One launch or restart attempt.

- primary key: `id`;
- foreign keys: `job_instance_id`, `definition_id`,
  `restart_of_execution_id`, and optional `definition_upgrade` pair;
- unique: `(job_instance_id, attempt)`;
- checks: positive attempt, legal status/category values, timestamp order,
  active/terminal end-time consistency, non-negative version, object-shaped
  parameters and job context within 1 MiB;
- optimistic invariant: every mutation increments `version` exactly once and
  qualifies the update with the expected prior version;
- ownership: runtime lifecycle writes; operator recovery uses the audited
  recovery path.

The partial unique index `ob_job_execution_one_unresolved` permits at most one
`STARTING`, `STARTED`, `STOPPING`, or `UNKNOWN` attempt per job instance.
`UNKNOWN` deliberately blocks automatic restart.

The definition-upgrade columns are both null for exact-definition launches and
both non-null for upgraded restarts. Their composite foreign key proves that
the recorded edge was registered.

### `ob_step_execution`

One named step attempt inside a job execution.

- primary key: `id`;
- foreign key: `job_execution_id`;
- unique: `(job_execution_id, step_name)`;
- checks: lifecycle/timestamps, all six counters and version non-negative,
  checkpoint and step-context envelopes are objects within 1 MiB;
- checkpoint columns: `checkpoint_format`, `checkpoint_schema`,
  `checkpoint_schema_version`, and `checkpoint_payload`;
- context columns: `context_format`, `context_schema`,
  `context_schema_version`, and `context_payload`;
- optimistic invariant: the chunk transaction updates business work,
  checkpoint, context, counters, and `version = version + 1` with
  `WHERE version = expected_version`; any affected-row count other than one is
  a conflict and rolls back the whole transaction.

Before the first committed chunk, payloads are empty versioned envelopes rather
than SQL null. A restart reads the latest compatible step execution through its
job instance and approved definition mapping.

### `ob_recovery_decision`

An append-only audit record for resolving an orphan or `UNKNOWN` execution.

- primary key: `id`;
- foreign key: `job_execution_id`;
- unique: `(job_execution_id, execution_version)`, allowing one decision for
  the observed version;
- fields: prior/result status, bounded reason code, opaque operator reference,
  evidence digest, facade-clock timestamp, and observed execution version;
- check: only documented recovery transitions and a 32-byte evidence digest;
- invariant: inserting the decision and compare-and-swap updating the execution
  happen in one transaction.

Free-form evidence and credentials do not belong in this table. External audit
systems use the opaque operator reference and evidence digest.

## Indexes and named query ownership

Every non-constraint index serves a named repository or operator query.
Implementations keep SQL in the PostgreSQL adapter and test plans with realistic
fixtures.

| Query ID | Purpose and predicate | Supporting key/index |
| --- | --- | --- |
| `SCHEMA-READ-001` | Read singleton schema version | `ob_schema_version` PK |
| `DEF-REGISTER-001` | Insert definition or detect revision drift | unique `(job_name, definition_revision)` |
| `DEF-EXACT-001` | Find exact manifest for a job | unique `(job_name, manifest_digest)` |
| `DEF-UPGRADE-001` | Resolve one direct compatibility edge | `ob_definition_upgrade` PK |
| `INSTANCE-CREATE-001` | Serialize create by job name and canonical key | unique `(job_name, instance_key)` |
| `INSTANCE-READ-001` | Read the row after a create conflict | same unique key |
| `EXEC-CREATE-001` | Allocate next attempt and reject unresolved work | unique `(job_instance_id, attempt)` plus `ob_job_execution_one_unresolved` |
| `EXEC-LATEST-001` | Select latest attempt for an instance | `ob_job_execution_instance_latest` on `(job_instance_id, attempt DESC)` |
| `EXEC-CAS-001` | Update one execution by ID and expected version | execution PK; version remains a filter |
| `EXEC-ACTIVE-001` | Inspect old active/unknown work by status and update time | `ob_job_execution_status_updated` on `(status, updated_at, id)` |
| `STEP-CREATE-001` | Create or read a named step in an execution | unique `(job_execution_id, step_name)` |
| `STEP-CAS-001` | Commit checkpoint/counters by ID and expected version | step PK; version remains a filter |
| `STEP-RESTART-001` | Find latest durable step state for an instance/name | execution latest index plus `ob_step_execution_job_name` |
| `RECOVERY-APPEND-001` | Append one decision for observed version | unique `(job_execution_id, execution_version)` |
| `RECOVERY-HISTORY-001` | Inspect execution decisions chronologically | `ob_recovery_execution_time` on `(job_execution_id, decided_at, id)` |

No general-purpose index is added for JSON payloads. Repository queries never
filter by parameter values, context contents, or checkpoint contents.

## Delete and retention ownership

M2 exposes no automatic purge. Foreign keys use `RESTRICT`, except child step
executions may use `CASCADE` only when a future audited purge deletes their job
execution. Definition and upgrade rows remain while referenced. The later
operator API must delete in instance-owned order and cannot target unresolved
executions.

## Transaction ownership

Launch selection, definition registration/comparison, instance creation, and
execution creation use one repository transaction. A chunk transaction is
owned by the PostgreSQL adapter and contains enlisted business writes plus the
step compare-and-swap update. Recovery decision insertion and lifecycle update
also share one transaction.

All SQL uses bound values. Schema, table, and column identifiers are fixed
adapter constants, never URL or application input.
