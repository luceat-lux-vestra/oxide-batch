# M8 Gate-A — Repository portability and adapter certification

**State:** Accepted by Gate-A #643  
**Owner:** M8 tracker #201  
**Scope:** repository semantics, adapter boundaries, support dispositions, and certification ownership

This record freezes the repository-portability decisions needed before M8 creates
database-adapter implementation work. It does not claim support for any adapter
that has not passed the named certification profile.

## Existing invariants carried forward

The accepted repository and transaction model remains authoritative:

- OxideBatch owns job-instance identity, execution lifecycle, checkpoints,
  execution context, optimistic versions, recovery evidence, and metadata
  schema semantics.
- Driver, pool, connection, row, transaction, SQL error, and migration-library
  types do not cross the portable repository boundary.
- PostgreSQL remains the reference semantic profile and fast path.
- Missing optional capabilities fail explicitly; they never downgrade restart,
  durability, or delivery guarantees.
- There is no generic cross-resource exactly-once claim.
- Business credentials, SQL text, bound values, context payloads, and raw
  driver diagnostics remain redacted from public errors and telemetry.
- The current workloads campaign remains evidence for its pinned framework
  baseline. M8 candidate branches do not rewrite that evidence.

## Adapter dispositions

| Adapter | M8 disposition | Implementation direction | Support claim |
| --- | --- | --- | --- |
| PostgreSQL | Reference first-party adapter | Keep the existing SQLx path and its optimized semantics | Existing named-release evidence remains bounded to those releases |
| MySQL | Planned first-party relational adapter | SQLx-backed implementation family | Only after named MySQL versions pass certification |
| MariaDB | Planned first-party relational adapter | Shares the MySQL protocol implementation family where semantics permit | Certified separately from MySQL |
| SQLite | Planned first-party constrained profile | SQLx-backed adapter with explicit locking/concurrency limitations | Never implies server-database concurrency semantics |
| SQL Server | Planned first-party relational adapter | Driver-private TDS implementation; not SQLx `Any` | Driver selection and named SQL Server versions require separate evidence |
| MongoDB | Planned non-relational metadata adapter | Distinct implementation behind the same semantic ports | Experimental until the full Mongo profile passes certification |
| Oracle | Later-tier explicit disposition | M13 extension/certification by default; first-party only with an accepted driver and demand case | No current support claim |
| DB2 | Later-tier explicit disposition | M13 extension/certification by default; first-party only with an accepted driver and demand case | No current support claim |
| SAP HANA | Later-tier explicit disposition | M13 extension/certification by default; first-party only with an accepted driver and demand case | No current support claim |

MySQL and MariaDB may share code, but a passing MySQL matrix is not evidence for
MariaDB and vice versa.

SQLx 0.9 is an implementation option for PostgreSQL, MySQL/MariaDB, and SQLite.
It is not the repository abstraction. SQL Server support was removed from SQLx
before the current 0.9 line, so M8 cannot make SQLx or `sqlx::Any` the
portability boundary.

The pinned Spring Batch 6.0.4 baseline contains a MongoDB-backed job repository,
so `DB-MONGO-001` is not NotApplicable. OxideBatch treats MongoDB as a
separate non-relational metadata-adapter track rather than pretending it is a
relational dialect.

## External baseline evidence

The support dispositions above are grounded in the pinned/current dependency
sources rather than inferred from product names:

- Spring Batch v6.0.4
  [`EnableMongoJobRepository`](https://github.com/spring-projects/spring-batch/blob/v6.0.4/spring-batch-core/src/main/java/org/springframework/batch/core/configuration/annotation/EnableMongoJobRepository.java)
  explicitly enables a MongoDB-backed job repository.
- Spring Batch v6.0.4
  [`MongoJobRepositoryFactoryBean`](https://github.com/spring-projects/spring-batch/blob/v6.0.4/spring-batch-core/src/main/java/org/springframework/batch/core/repository/support/MongoJobRepositoryFactoryBean.java)
  constructs the repository from Mongo-specific metadata DAOs and sequence
  incrementers.
- SQLx 0.9
  [documents PostgreSQL, MySQL/MariaDB, and SQLite support](https://docs.rs/crate/sqlx/0.9.0)
  and records that MSSQL support was removed pending a driver rewrite.

These references justify the Gate-A disposition. They are not adapter
certification evidence for OxideBatch.

## Portable contract versus adapter mechanics

The following belong to the portable semantic contract:

- unique logical instance selection under concurrent launch;
- durable execution and step identity;
- compare-and-swap lifecycle transitions;
- restart/checkpoint/context authority;
- definition/restart state required by a selected feature;
- known-not-committed versus commit-outcome-unknown classification;
- bounded service operations and typed unsupported-capability rejection;
- capability-specific transaction/delivery guarantees;
- schema compatibility and migration-version rejection;
- redacted stable failure categories.

The following remain private to an adapter:

- SQL or query language;
- driver and pool types;
- connection and transaction handles;
- bind/row types;
- sequence, identity, RETURNING, generated-key, or collection-ID mechanics;
- locking syntax and isolation configuration;
- server-specific error objects and messages;
- migration execution library;
- query-plan and index mechanics;
- concrete backup/restore commands.

A future adapter may therefore use different physical primitives while
preserving the same observable capability it declares.

## Capability negotiation boundary

M8 keeps one versioned `RepositoryDescriptor` as the deployment's durable
capability declaration. It does **not** create a second universal database
interface.

Capability checks occur at the narrowest point where a guarantee becomes
required:

1. **Plan/launcher requirements** are rejected before the first durable
   transaction. Examples include durable partitions, custom-leaf state, nested
   jobs, scoped resolution, repeat state, and execution ownership.
2. **Feature/service requirements** are checked when that feature is selected
   or an operator/maintenance action is invoked. Definition evolution,
   operator audit/idempotency, stop controls, holds, and purge do not become
   requirements for jobs that never use them.
3. **Explorer reads** are not added to `RepositoryDescriptor`. The explorer
   port is additive and each unsupported query fails closed with
   `ExplorerError::UnsupportedCapability`.
4. **Always-required repository behavior** is not advertised as an optional
   capability. An implementation calling itself a durable `JobRepository`
   must satisfy the core identity/lifecycle/checkpoint contract.

The current boolean capability set is retained as the compatibility base.
M8 may version the descriptor to add typed profiles and limits, but those
profiles express framework guarantees rather than driver features.

### Descriptor-v2 information classes

The M8 implementation may add typed descriptor subprofiles for:

- schema version and accepted upgrade-source window;
- transaction/delivery modes, including same-resource enlistment;
- commit-outcome classification support;
- server-time / lease / fencing support where claimed;
- bounded identifier, parameter, context, checkpoint, page, and batch limits;
- migration, backup/restore, and rolling-upgrade support;
- capability-specific resource constraints.

Throughput knobs such as pool size and statement timeout remain deployment
configuration rather than definition identity unless a future guarantee
explicitly requires otherwise.

## Repository SPI rule

`oxide-batch-repository` remains the driver-neutral port crate. Concrete
adapters may remain behind facade feature flags or move into implementation
crates without changing this semantic boundary. Packaging is not allowed to
expose a driver type through the facade.

A concrete adapter implements the existing command/unit-of-work ports directly.
M8 does not introduce a lowest-common-denominator `Database`, generic row,
generic transaction, or generic query-builder abstraction.

## Database item-component boundary

`IO-DB-001` is adjacent to, but not the same thing as, metadata repository
portability.

- Cursor, paging, and batch components preserve driver-neutral component
  semantics while concrete SQL/bind behavior stays adapter-specific.
- #218 is assigned to this workstream: the stable `BusinessValue` model must
  grow beyond `Text/Bytes/I64/Bool/Null` without exposing SQLx values or
  creating a PostgreSQL-only escape hatch.
- Upsert is an optional database-component capability with a documented
  conflict/key contract; it is not a mandatory `JobRepository` method.
- Stored-procedure/call support is an optional database-component capability;
  it is not a universal transaction primitive.
- ORM/repository-style application integration remains an `ItemWriter` /
  custom-component composition surface rather than a Java ORM compatibility
  layer.

## Error and diagnostics boundary

#220 is a cross-adapter M8 diagnostics child.

Portable failures may carry stable, bounded, value-redacted classification and
backend-neutral provenance sufficient for policy and operations. They must not
carry raw SQL, bound values, credentials, endpoints, or driver error objects.
A vendor code or SQLSTATE-like identifier may be retained only if its format is
bounded and reviewed as non-secret; raw driver messages remain internal.

The public distinction between known-not-committed and unknown outcome is
correctness-relevant and must survive every adapter mapping.

## Durable-effect boundary

Gate-B #644 owns outbox, inbox/dedup, effect journal, idempotent external
effects, at-least-once/best-effort semantics, and unknown external-effect
reconciliation.

Gate-A owns only the repository capability mechanism needed to persist or
declare those primitives. A database adapter cannot invent a stronger delivery
guarantee than Gate-B defines.

## Retention boundary

Gate-C #645 owns archive, export/import, purge portability, backup/restore
evidence, and destructive-maintenance semantics.

Gate-A owns the capability/certification mechanism. A database adapter may
declare only the retention capabilities it implements and has certified.

## Certification profile

A first-party or certified durable adapter must record exact adapter, client,
database, and configuration versions and pass the applicable evidence below.

### Core repository certification

1. schema initialization and current-version readback;
2. older supported schema upgrade and newer-schema rejection;
3. unique logical instance under concurrent duplicate launch;
4. durable ID allocation without collision/reuse;
5. lifecycle compare-and-swap and stale-version rejection;
6. restart after failed/stopped execution with checkpoint/context preservation;
7. crash/disconnect at transaction begin, mutation, pre-commit, commit, and
   post-commit response boundaries;
8. explicit `CommitOutcomeUnknown` handling where durable outcome cannot be
   proven;
9. bounded Explorer baseline queries, ordering, keyset/cursor integrity, and
   redaction for the queries the support profile requires;
10. bounded resource behavior and timeout/cancellation evidence;
11. privilege/authentication/TLS guidance appropriate to the backend;
12. backup/restore evidence sufficient for the supported migration policy.

### Capability-specific certification

A capability is not supported merely because a method exists. Declaring it adds
its own contract suite, for example:

- definition registry / upgrade / fork lineage;
- execution ownership, server time, lease, and fencing;
- stop requests and operator audit/idempotency;
- local partition plans and aggregation;
- custom-leaf, scope, nested-job, and repeat durable state;
- same-resource enlisted business writes;
- retention hold/purge/archive/export;
- Gate-B durable-effect primitives.

### Support promotion

- **Experimental**: partial implementation is usable only for evaluation; every
  unsupported capability is explicit.
- **First-party**: maintained in the OxideBatch organization and covered by the
  release support matrix for named database versions.
- **Certified third-party**: maintained externally but passes the versioned
  certification kit for named versions.

No row is promoted to `Verified` until a named OxideBatch release carries the
support claim and its evidence.

## Enterprise-row split

The aggregate `DB-ENTERPRISE-001` row is retired as an implementation row and
split before verification into:

- `DB-ORACLE-001`;
- `DB-DB2-001`;
- `DB-HANA-001`.

The aggregate identifier may remain only as historical reconciliation text.
Each database has independent ownership, limitation, and evidence.

## Child-work ordering after Gate-A

After this gate is accepted, #201 may create implementation children in this
order:

1. descriptor/profile and reusable certification-kit changes;
2. #218 database value/component contract and #220 redacted diagnostics;
3. MySQL/MariaDB adapter;
4. SQLite constrained adapter/profile;
5. SQL Server driver selection and adapter;
6. MongoDB metadata adapter #646;
7. per-database enterprise/later-tier evaluation handed to M13 as appropriate.

Gate-B and Gate-C can progress in parallel because their semantic contracts are
separate from concrete relational dialect work.

## Gate-A exit checklist

- [x] relational target set and support terminology are explicit;
- [x] MongoDB has a bounded Planned disposition;
- [x] Oracle, DB2, and HANA have independent later-tier dispositions;
- [x] driver types are excluded from the repository SPI;
- [x] capability negotiation scope is separated from additive explorer reads;
- [x] `IO-DB-001` tails are separated from metadata-repository methods;
- [x] durable-effect ownership is assigned to #644;
- [x] retention/archive/export ownership is assigned to #645;
- [x] #218 and #220 are classified;
- [x] the reusable adapter certification profile is frozen.

Gate-A does not close until this record and the corresponding ledger updates
land on `main`.
