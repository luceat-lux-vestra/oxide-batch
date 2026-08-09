//! A schema-2 runtime meeting a database that has been upgraded to schema 3.
//!
//! This is the situation the M5 upgrade contract is really about. An operator
//! upgrades the metadata schema while instances of the previous release are
//! still running, or rolls a deployment back after the migrator has already
//! moved forward. What must not happen then is an old runtime working on a
//! database it does not understand: guessing that the extra structure is
//! compatible, ignoring it, or migrating it downwards to a shape it recognizes.
//! It must refuse, and refuse without writing.
//!
//! The runtime that does the refusing here is a real one. Its supported schema
//! version is `2`, which no build of this working tree can report, so the
//! report checks out the last revision before schema 3 was added, builds it,
//! and runs the campaign's committed probe program against a database this
//! crate's own migrator has just upgraded to schema 3. Both of that runtime's
//! entry points are asked — the repository, which is what a running instance
//! opens, and the migrator, which is what a rolled-back deployment step runs.
//!
//! The workspace already has a lower-level regression test for the same
//! invariant: `postgres_repository::newer_schema_is_rejected_without_guessing_compatibility`
//! moves the recorded version one past whatever this runtime supports and
//! requires the typed rejection. That test is kept and is not this report. It
//! proves the comparison is wired up; it does not prove that the runtime which
//! shipped against schema 2 refuses the schema 3 this crate now installs,
//! because it runs the current runtime against a version number rather than a
//! previous runtime against a real database.

#![cfg(feature = "postgres")]

mod upgrade;

use std::error::Error;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use oxide_batch::{PostgresJobRepository, PostgresMigrator};
use serde_json::{Value, json};

use upgrade::{
    DurableDigest, Failure, FixedClock, ProbeRun, SourceColumns, admin_url, apply_seed,
    assert_historical_shape, config, durable_tables, fixtures, install_historical_schema,
    major_version, migrator_url, read_through_port, recreate_database, retain_observation,
    run_schema2_runtime, schema_version, server_version, with_database,
};

/// The schema the rejected runtime supports.
const RUNTIME_SCHEMA_VERSION: u64 = 2;

/// The schema the database is upgraded to before the runtime is pointed at it.
const DATABASE_SCHEMA_VERSION: u32 = 3;

/// The database the report builds, upgrades, and offers to the old runtime.
const DATABASE: &str = "oxide_batch_m5_upgrade_rejection";

#[test]
fn schema2_runtime_rejects_schema3() -> Result<(), Box<dyn Error>> {
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let Some(admin) = admin_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_BACKUP_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_report(&migrator, &admin))
}

/// Upgrades a schema-2 database to schema 3 and offers it to a schema-2 runtime.
#[allow(
    clippy::too_many_lines,
    reason = "building a schema-2 database, upgrading it, building the runtime that shipped \
              against schema 2, and requiring it to refuse without writing form one report that \
              is only meaningful in order"
)]
async fn run_report(migrator: &str, admin: &str) -> Result<(), Box<dyn Error>> {
    let url = with_database(migrator, DATABASE)?;
    recreate_database(admin, DATABASE).await?;

    // The database reaches schema 3 the way an operator's does: it starts at
    // schema 2 with durable state on it and is upgraded. A database created at
    // schema 3 would be a weaker fixture, because the rejection this report is
    // about happens to databases that were something else first.
    install_historical_schema(&url, RUNTIME_SCHEMA_VERSION.try_into()?).await?;
    assert_historical_shape(&url, RUNTIME_SCHEMA_VERSION.try_into()?).await?;
    apply_seed(&url, &fixtures().join("schema-2").join("seed.sql")).await?;

    PostgresMigrator::migrate(&config(url.clone())?).await?;
    let installed = schema_version(&url).await?;
    assert_eq!(
        installed,
        Some(DATABASE_SCHEMA_VERSION),
        "the report offers a schema-{DATABASE_SCHEMA_VERSION} database and this one is not",
    );
    assert_historical_shape(&url, DATABASE_SCHEMA_VERSION).await?;

    let tables = durable_tables(DATABASE_SCHEMA_VERSION);
    let columns = SourceColumns::capture(&url, &tables).await?;
    let before = DurableDigest::read(&url, &columns, &tables).await?;
    let applied_before = applied_migrations(&url).await?;

    // The reading that only a real schema-2 runtime can produce.
    let probe = run_schema2_runtime(&url)?;
    let observed = probe_report(&probe)?;

    assert!(
        probe.exit_success,
        "the schema-2 runtime did not fail closed on a schema-{DATABASE_SCHEMA_VERSION} \
         database: {}",
        probe.report,
    );
    assert_eq!(
        observed.supported, RUNTIME_SCHEMA_VERSION,
        "the report is about a runtime that supports schema {RUNTIME_SCHEMA_VERSION}, and the \
         one that ran reports {}",
        observed.supported,
    );
    for (entry, attempt) in [
        (&observed.repository_open, "repository startup"),
        (&observed.migrator_run, "the migrator"),
    ] {
        assert!(
            !entry.accepted,
            "{attempt} on the schema-2 runtime accepted a schema-{DATABASE_SCHEMA_VERSION} \
             database",
        );
        assert_eq!(
            entry.error.as_deref(),
            Some("NewerSchema"),
            "{attempt} on the schema-2 runtime must refuse with the typed newer-schema failure \
             rather than any other error, which would refuse for a reason that is not the \
             contract",
        );
        assert_eq!(
            entry.observed,
            Some(u64::from(DATABASE_SCHEMA_VERSION)),
            "{attempt} must report the schema version it actually found",
        );
        assert_eq!(
            entry.supported,
            Some(RUNTIME_SCHEMA_VERSION),
            "{attempt} must report the schema version that runtime supports",
        );
    }

    // Refusing is only half the contract. A runtime that rejected the database
    // and wrote to it on the way — a partial migration, a bookkeeping row, a
    // downgraded version — would have done the damage the refusal exists to
    // prevent.
    let after = DurableDigest::read(&url, &columns, &tables).await?;
    assert_eq!(
        before.differences(&after),
        Vec::<String>::new(),
        "the schema-2 runtime changed durable state while refusing the database",
    );
    assert_eq!(
        schema_version(&url).await?,
        Some(DATABASE_SCHEMA_VERSION),
        "the schema-2 runtime changed the recorded schema version while refusing the database",
    );
    assert_eq!(
        applied_migrations(&url).await?,
        applied_before,
        "the schema-2 runtime applied or removed a migration while refusing the database",
    );

    // The database is still the one the current runtime works with, which is
    // what makes the refusal non-destructive rather than merely unsuccessful.
    let repository =
        PostgresJobRepository::connect(config(url.clone())?, Arc::new(FixedClock(UNIX_EPOCH)))
            .await?;
    let reading = read_through_port(&repository).await?;
    assert!(
        reading.instance.is_some(),
        "the current runtime must still open and project the database the schema-2 runtime \
         refused",
    );
    repository.close().await?;

    let server = server_version(&url).await?;
    retain_observation(
        "schema-rejection",
        &json!({
            "report": "a schema-2 runtime refusing a schema-3 database",
            "scenario": "schema2_runtime_rejects_schema3",
            "fixture": "postgres-upgrade",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "paths": [{
                "source_schema_version": RUNTIME_SCHEMA_VERSION,
                "target_schema_version": DATABASE_SCHEMA_VERSION,
                "migration_result": "ok",
                "repository_open_result": "refused: NewerSchema",
                "durable_state_verified": true,
                "backup_restore_result": Value::Null,
                "observed_schema_version": installed,
                "database": DATABASE,
                "reached_schema_3_by":
                    "upgrading a seeded schema-2 database with this crate's migrator",
                "runtime": {
                    "revision": probe.revision,
                    "supported_schema_version": observed.supported,
                    "built_from": "a worktree of the last revision before schema 3 was added",
                    "probe": "tests/fixtures/upgrade/schema-2-runtime/probe.rs",
                },
                "runtime_repository_open": observed.repository_open.rendered.clone(),
                "runtime_migrator_run": observed.migrator_run.rendered.clone(),
                "failed_closed": true,
                "durable_state_unchanged": true,
                "rows_compared": before.counts(),
                "applied_migrations_unchanged": true,
                "current_runtime_still_opens_it": true,
                "violations": Vec::<String>::new(),
                "passed": true,
            }],
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Reads the applied-migration bookkeeping, so a silent rewrite is visible.
async fn applied_migrations(url: &str) -> Result<Vec<(i64, String)>, Box<dyn Error>> {
    use sqlx::Row;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let rows = sqlx::query(
        "SELECT version, description FROM oxide_batch._sqlx_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await?;
    let mut applied = Vec::new();
    for row in &rows {
        applied.push((row.try_get::<i64, _>(0)?, row.try_get::<String, _>(1)?));
    }
    pool.close().await;
    Ok(applied)
}

/// What the schema-2 runtime reported, in the shape the report requires.
struct Observed {
    /// The schema version that runtime supports.
    supported: u64,
    /// What its repository startup did.
    repository_open: Attempt,
    /// What its migrator did.
    migrator_run: Attempt,
}

/// One entry point's answer to a database it does not understand.
struct Attempt {
    /// Whether the database was accepted.
    accepted: bool,
    /// The typed failure it reported, when it reported one.
    error: Option<String>,
    /// The schema version it found in the database.
    observed: Option<u64>,
    /// The schema version it says it supports.
    supported: Option<u64>,
    /// The entry as the probe rendered it, for the retained evidence.
    rendered: Value,
}

/// Reads the probe's one line of structured output.
fn probe_report(probe: &ProbeRun) -> Result<Observed, Box<dyn Error>> {
    let supported = probe
        .report
        .get("supported_schema_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure("the probe reported no supported schema version".to_owned()))?;
    Ok(Observed {
        supported,
        repository_open: attempt(&probe.report, "repository_open")?,
        migrator_run: attempt(&probe.report, "migrator_run")?,
    })
}

/// Reads one attempt out of the probe's reading.
fn attempt(report: &Value, name: &str) -> Result<Attempt, Box<dyn Error>> {
    let entry = report
        .get(name)
        .ok_or_else(|| Failure(format!("the probe reported no {name}")))?;
    Ok(Attempt {
        accepted: entry
            .get("accepted")
            .and_then(Value::as_bool)
            .ok_or_else(|| Failure(format!("the probe's {name} says nothing about acceptance")))?,
        error: entry
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_owned),
        observed: entry.get("observed_schema_version").and_then(Value::as_u64),
        supported: entry
            .get("supported_schema_version")
            .and_then(Value::as_u64),
        rendered: entry.clone(),
    })
}
