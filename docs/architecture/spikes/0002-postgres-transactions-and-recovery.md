# Spike 0002: PostgreSQL Transactions, Locks, and Recovery

- **State:** Complete
- **Owner:** maintainers
- **Issue:** [#6](https://github.com/luceat-lux-vestra/oxide-batch/issues/6)
- **Date:** 2026-07-29
- **Decision/ADR:** [ADR-0003](../decisions/0003-postgres-metadata.md)

## Decision to unblock

Determine whether SQLx and PostgreSQL can implement the required transaction
enlistment, duplicate-launch serialization, optimistic locking, migration, and
crash-recovery semantics without leaking SQLx into the core contract.

## Hypotheses

1. A PostgreSQL-backed implementation of a core-owned business transaction port
   can place business rows and checkpoint metadata in one transaction.
2. A unique constraint serializes colliding job-instance inserts, and
   `ON CONFLICT` prevents duplicate instances.
3. Version-qualified updates produce one winner and one observable optimistic
   conflict.
4. Process exit before commit rolls back business and metadata together; exit
   after commit leaves both durable.
5. Connection loss during a deliberately delayed commit returns an error,
   rolls back this transaction, and requires the suspect connection to be
   discarded before pool reuse.

## Constraints

- PostgreSQL 18.4, default isolation level (`READ COMMITTED`);
- SQLx 0.9.0 with PostgreSQL, migrations, JSON, Tokio, and Rustls native roots;
- Tokio 1.53.1;
- Rust 1.97.1 development toolchain and Rust 1.95 MSRV;
- bound SQL parameters only;
- one metadata schema version in M0; multi-version production upgrades remain
  an M2 suite.

## Experiment

Source, migration, and tests:

- `spikes/m0-architecture/src/postgres.rs`;
- `spikes/m0-architecture/src/bin/crash-worker.rs`;
- `spikes/m0-architecture/migrations/0001_spike_metadata.sql`;
- `spikes/m0-architecture/tests/postgres.rs`.

Container reproduction:

```console
./spikes/m0-architecture/run-postgres-spike.sh
```

CI runs the same test target against a PostgreSQL 18 service. To use an existing
database:

```console
OXIDEBATCH_SPIKE_DATABASE_URL=postgres://user:password@host/database \
  cargo test -p oxide-batch-m0-spikes --test postgres \
  -- --nocapture --test-threads=1
```

The database must be disposable and the test role must be allowed to terminate
its own PostgreSQL backend.

## Acceptance and rejection criteria

Acceptance requires:

- business rows, checkpoint, context, count, and version all commit or all roll
  back;
- a colliding unique insert demonstrably waits on the uncommitted index entry;
- concurrent launches create one instance;
- an optimistic race has exactly one successful update;
- every pre-commit process-exit phase leaves no partial row;
- post-commit process exit leaves both resources durable;
- forced connection loss during commit returns an error and no partial commit;
- a cancelled long query leaves no committed effect and does not poison a
  healthy pool;
- migrations are repeatable and a newer explicit schema version is rejected.

SQLx is rejected if transaction lifetimes must appear in the core port or a
failed/cancelled connection cannot be kept out of subsequent work.

## Results

Observed output on PostgreSQL 18.4:

```text
running 8 tests
test backend_termination_makes_commit_fail_and_rolls_back_both_resources ... ok
test cancelling_a_slow_query_leaves_no_committed_effect_and_pool_recovers ... ok
test concurrent_duplicate_launches_create_exactly_one_instance ... ok
test enlisted_business_and_checkpoint_writes_commit_or_roll_back_together ... ok
test migrations_are_idempotent_and_newer_schema_versions_are_rejected ... ok
test optimistic_update_race_has_one_winner_and_one_conflict ... ok
test process_exit_crash_matrix_matches_the_commit_boundary ... ok
test unique_index_lock_serializes_duplicate_launches ... ok

test result: ok. 8 passed; 0 failed; finished in 1.30s
```

Transaction/crash observations:

| Injection point | Business rows | Checkpoint rows | Recovery interpretation |
| --- | ---: | ---: | --- |
| Before transaction | 0 | 0 | replay |
| After business write, before checkpoint | 0 | 0 | replay |
| After checkpoint, before commit | 0 | 0 | replay |
| After commit, before acknowledgement | 1 | 1 | read durable checkpoint; do not replay |
| Backend terminated during delayed commit | 0 | 0 | commit returned error; inspect durable state |

An uncommitted unique-index collision reached PostgreSQL error `55P03` after a
150 ms `lock_timeout`, proving that the contender waited on database lock state.
After the first transaction committed, retrying with `ON CONFLICT DO NOTHING`
affected zero rows. Twelve synchronized launch attempts produced one insert.

Two readers observed version 0, then raced the same `UPDATE ... WHERE version =
0`; affected-row counts were `[0, 1]`.

The SQLx adapter owned `Transaction<'static, Postgres>` internally and exposed
only `&mut dyn BusinessTransaction` to the writer. SQLx types did not enter the
core port.

An initial harness revision kept a pool in a process-global cell across
independent `#[tokio::test]` runtimes and produced acquire timeouts. Creating a
pool inside its owning runtime removed the failure. This was a harness defect,
but it confirms that runtime and pool ownership must have the same lifecycle.

## Correctness and risk review

- PostgreSQL is the sole serialization authority for instance uniqueness.
- Optimistic conflict is determined by affected-row count, never by a
  read-then-write application check.
- Commit success is the only positive acknowledgement. A commit error is
  treated as outcome-unknown in the general contract even though this injected
  termination rolled back.
- A connection involved in protocol cancellation or commit failure is removed
  instead of returned to the pool. Recovery uses a healthy connection to read
  durable state.
- Business and checkpoint atomicity applies only when the writer uses the
  enlisted PostgreSQL resource. External resources remain at-least-once.
- The spike used a superuser-like disposable test role only for
  `pg_terminate_backend`; production runtime roles do not receive that power.
- The migration is forward-only and idempotent through SQLx's migration table
  plus an OxideBatch schema-version row. Upgrade fixtures from multiple released
  source versions begin when a second schema version exists.

## Conclusion

Accept PostgreSQL plus SQLx for the 1.0 metadata adapter. Keep SQLx and concrete
transactions inside the adapter; expose a borrowed, application-specific
transaction port for enlisted writers.

Use database uniqueness for job-instance identity and compare-and-swap version
updates for optimistic concurrency. After cancellation, connection loss, or a
commit error, discard the affected connection, classify the commit outcome as
unknown until durable state is read, and never infer external effects.

Confidence is high for the M0 selection. TLS deployment variants, supported
PostgreSQL major matrix, pool sizing, and migration from future schema versions
remain M2 gates.

## Follow-up

- promote the logical spike tables into an M2 physical-model issue rather than
  treating them as production schema;
- add PostgreSQL supported-major CI when the repository adapter is introduced;
- test TLS modes and runtime-role privileges in M2;
- retain the crash-worker pattern for the first vertical slice;
- revisit SQLx if its types escape the adapter or its failure semantics cannot
  satisfy the connection-discard rule.
