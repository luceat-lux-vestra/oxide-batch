//! Historical runtimes meeting a database newer than they support.
//!
//! This is the situation the M5 upgrade contract is really about. An operator
//! upgrades the metadata schema while instances of a previous release are
//! still running, or rolls a deployment back after the migrator has already
//! moved forward. What must not happen then is an old runtime working on a
//! database it does not understand: guessing that the extra structure is
//! compatible, ignoring it, or migrating it downward to a shape it
//! recognizes. It must refuse, and refuse without writing.
//!
//! The M5 preview's original claim was about one runtime: the one that
//! shipped against schema 2, meeting the schema 3 that was current when M5
//! wrote it. M6 `#144` then added schema 4, so this report now builds *two*
//! real historical runtimes rather than one — the last revision before schema
//! 3 was added, and the last revision before schema 4 was added — and points
//! both at the same database, upgraded by the current migrator to the current
//! schema. That single database is newer than either historical runtime
//! supports, so it exercises both edges the M6 addition created at once: a
//! schema-2 runtime meeting something newer than 2 (the M5 preview's original
//! claim, still true), and a schema-3 runtime meeting something newer than 3
//! (the edge M6 added). Neither build is possible from this working tree: the
//! supported schema version is a constant of the crate, so the report checks
//! out each historical revision, builds it, and runs the campaign's committed
//! probe program for that revision against the upgraded database.
//!
//! Both of each runtime's entry points are asked — the repository, which is
//! what a running instance opens, and the migrator, which is what a
//! rolled-back deployment step runs.
//!
//! The workspace already has a lower-level regression test for the same
//! invariant: `postgres_repository::newer_schema_is_rejected_without_guessing_compatibility`
//! moves the recorded version one past whatever this runtime supports and
//! requires the typed rejection. That test is kept and is not this report. It
//! proves the comparison is wired up; it does not prove that a runtime which
//! shipped against an older schema refuses the current one this crate
//! installs, because it runs the current runtime against a version number
//! rather than a previous runtime against a real database.

#![cfg(feature = "postgres")]

mod upgrade;

use std::error::Error;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use oxide_batch::{PostgresJobRepository, PostgresMigrator};
use serde_json::{Value, json};

use upgrade::{
    DurableDigest, Failure, FixedClock, ProbeRun, SourceColumns, admin_url, apply_seed,
    assert_historical_shape, config, durable_tables, execution_manifest, fixtures,
    install_historical_schema, major_version, migrator_url, read_through_port, recreate_database,
    retain_observation, run_schema2_runtime, run_schema3_runtime, schema_version, server_version,
    with_database,
};

/// The schema the database is seeded at before it is upgraded.
const SEED_SCHEMA_VERSION: u32 = 2;

/// The database the report builds, upgrades, and offers to both historical
/// runtimes.
const DATABASE: &str = "oxide_batch_m5_upgrade_rejection";

/// One historical runtime this report builds and offers the upgraded database.
struct HistoricalRuntime {
    /// The schema version that runtime supports.
    supported_schema_version: u64,
    /// Runs the pinned worktree's probe program against `target`.
    run: fn(&str) -> Result<ProbeRun, Box<dyn Error>>,
    /// The committed probe fixture, named for the retained observation.
    probe_path: &'static str,
}

/// The historical runtimes this report builds, oldest first.
const HISTORICAL_RUNTIMES: &[HistoricalRuntime] = &[
    HistoricalRuntime {
        supported_schema_version: 2,
        run: run_schema2_runtime,
        probe_path: "tests/fixtures/upgrade/schema-2-runtime/probe.rs",
    },
    HistoricalRuntime {
        supported_schema_version: 3,
        run: run_schema3_runtime,
        probe_path: "tests/fixtures/upgrade/schema-3-runtime/probe.rs",
    },
];

#[test]
fn historical_runtimes_reject_the_current_schema() -> Result<(), Box<dyn Error>> {
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

/// Upgrades a schema-2 database to the current schema and offers it to every
/// historical runtime this report builds.
#[allow(
    clippy::too_many_lines,
    reason = "building a schema-2 database, upgrading it, building each historical runtime, and \
              requiring every one of them to refuse without writing form one report that is only \
              meaningful in order"
)]
async fn run_report(migrator: &str, admin: &str) -> Result<(), Box<dyn Error>> {
    let url = with_database(migrator, DATABASE)?;
    recreate_database(admin, DATABASE).await?;

    // The database reaches the current schema the way an operator's does: it
    // starts at schema 2 with durable state on it and is upgraded. A database
    // created at the current schema directly would be a weaker fixture,
    // because the rejection this report is about happens to databases that
    // were something else first.
    install_historical_schema(&url, SEED_SCHEMA_VERSION).await?;
    assert_historical_shape(&url, SEED_SCHEMA_VERSION).await?;
    apply_seed(
        &url,
        &fixtures()
            .join(format!("schema-{SEED_SCHEMA_VERSION}"))
            .join("seed.sql"),
    )
    .await?;

    PostgresMigrator::migrate(&config(url.clone())?).await?;
    let current = PostgresMigrator::supported_schema_version();
    let installed = schema_version(&url).await?;
    assert_eq!(
        installed,
        Some(current),
        "the report offers the current schema and this database is not at it",
    );
    assert_historical_shape(&url, current).await?;

    let tables = durable_tables(current);
    let columns = SourceColumns::capture(&url, &tables).await?;
    let before = DurableDigest::read(&url, &columns, &tables).await?;
    let applied_before = applied_migrations(&url).await?;

    let mut paths = Vec::new();
    for runtime in HISTORICAL_RUNTIMES {
        paths.push(offer_to_historical_runtime(runtime, &url, current)?);
    }

    // Refusing is only half the contract. A runtime that rejected the database
    // and wrote to it on the way — a partial migration, a bookkeeping row, a
    // downgraded version — would have done the damage the refusal exists to
    // prevent.
    let after = DurableDigest::read(&url, &columns, &tables).await?;
    assert_eq!(
        before.differences(&after),
        Vec::<String>::new(),
        "a historical runtime changed durable state while refusing the database",
    );
    assert_eq!(
        schema_version(&url).await?,
        Some(current),
        "a historical runtime changed the recorded schema version while refusing the database",
    );
    assert_eq!(
        applied_migrations(&url).await?,
        applied_before,
        "a historical runtime applied or removed a migration while refusing the database",
    );

    // The database is still the one the current runtime works with, which is
    // what makes every refusal non-destructive rather than merely
    // unsuccessful.
    let repository =
        PostgresJobRepository::connect(config(url.clone())?, Arc::new(FixedClock(UNIX_EPOCH)))
            .await?;
    let reading = read_through_port(&repository).await?;
    assert!(
        reading.instance.is_some(),
        "the current runtime must still open and project the database every historical runtime \
         refused",
    );
    repository.close().await?;

    let server = server_version(&url).await?;
    retain_observation(
        "schema-rejection",
        &json!({
            "report": "historical runtimes refusing the current schema",
            "scenario": "historical_runtimes_reject_the_current_schema",
            "fixture": "postgres-upgrade",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "seed_schema_version": SEED_SCHEMA_VERSION,
            "current_schema_version": current,
            "reached_current_schema_by":
                format!("upgrading a seeded schema-{SEED_SCHEMA_VERSION} database with this \
                         crate's migrator"),
            "paths": paths,
            "durable_state_unchanged": true,
            "rows_compared": before.counts(),
            "applied_migrations_unchanged": true,
            "current_runtime_still_opens_it": true,
            "execution_manifest": execution_manifest()?,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Builds one historical runtime and requires it to refuse `url` closed.
fn offer_to_historical_runtime(
    runtime: &HistoricalRuntime,
    url: &str,
    current_schema_version: u32,
) -> Result<Value, Box<dyn Error>> {
    let probe = (runtime.run)(url)?;
    let observed = probe_report(&probe)?;

    assert!(
        probe.exit_success,
        "the schema-{}-supporting runtime did not fail closed on the current schema: {}",
        runtime.supported_schema_version, probe.report,
    );
    assert_eq!(
        observed.supported, runtime.supported_schema_version,
        "the report is about a runtime that supports schema {}, and the one that ran reports {}",
        runtime.supported_schema_version, observed.supported,
    );
    for (entry, attempt) in [
        (&observed.repository_open, "repository startup"),
        (&observed.migrator_run, "the migrator"),
    ] {
        assert!(
            !entry.accepted,
            "{attempt} on the schema-{}-supporting runtime accepted the current schema",
            runtime.supported_schema_version,
        );
        assert_eq!(
            entry.error.as_deref(),
            Some("NewerSchema"),
            "{attempt} on the schema-{}-supporting runtime must refuse with the typed \
             newer-schema failure rather than any other error, which would refuse for a reason \
             that is not the contract",
            runtime.supported_schema_version,
        );
        assert_eq!(
            entry.observed,
            Some(u64::from(current_schema_version)),
            "{attempt} must report the schema version it actually found",
        );
        assert_eq!(
            entry.supported,
            Some(runtime.supported_schema_version),
            "{attempt} must report the schema version that runtime supports",
        );
    }

    Ok(json!({
        "source_schema_version": runtime.supported_schema_version,
        "target_schema_version": current_schema_version,
        "migration_result": "ok",
        "repository_open_result": "refused: NewerSchema",
        "durable_state_verified": true,
        "backup_restore_result": Value::Null,
        "observed_schema_version": current_schema_version,
        "database": DATABASE,
        "runtime": {
            "revision": probe.revision,
            "supported_schema_version": observed.supported,
            "built_from": format!(
                "a worktree of the last revision before schema {} was added",
                runtime.supported_schema_version + 1,
            ),
            "probe": runtime.probe_path,
        },
        "runtime_repository_open": observed.repository_open.rendered.clone(),
        "runtime_migrator_run": observed.migrator_run.rendered.clone(),
        "failed_closed": true,
        "violations": Vec::<String>::new(),
        "passed": true,
    }))
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

/// What one historical runtime reported, in the shape the report requires.
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
