//! Direct upgrade of a schema-1 and a schema-2 database to schema 3.
//!
//! The M5 preview says a `PostgreSQL` database at schema 1 or schema 2 upgrades
//! directly to schema 3. This target is that report, and it performs the
//! upgrade on real databases at those schemas rather than on a reconstruction
//! of them: each source database is built by running the immutable migration
//! set up to the version under test and stopping there, so its tables,
//! columns, constraints, indexes, and applied-migration bookkeeping are the
//! ones that version produced when it was the whole schema.
//!
//! Each source is then seeded with the durable state an operator's database
//! would have held — registered definitions and the upgrade edge between them,
//! a job instance, a resolved attempt and a live one, their step executions
//! with durable checkpoints, contexts, and counters, and a recovery decision —
//! and the schema-2 source additionally with the logical step identity, retry
//! and skip counters, and flow decision that schema introduced.
//!
//! What the report requires of the upgrade is four things. The recorded version
//! becomes `3` and the schema-3 structures appear. Every value of every column
//! the source schema declared is byte-identical afterwards, compared through
//! the source's own column list so a column a later schema added cannot mask a
//! loss. The upgraded database opens through the current repository and
//! projects its job through the explorer, so the result is one the runtime can
//! work with rather than merely one that migrated. And running the migrator
//! again changes nothing.
//!
//! The single transformation the chain does make to an existing column is
//! asserted rather than tolerated: schema 2 gave every schema-1 step execution
//! a logical identity equal to its step name, and the report requires exactly
//! that.

#![cfg(feature = "postgres")]

mod upgrade;

use std::error::Error;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use oxide_batch::{PostgresJobRepository, PostgresMigrator};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

use upgrade::{
    DurableDigest, FixedClock, SourceColumns, admin_url, apply_seed, assert_historical_shape,
    config, durable_tables, execution_manifest, fixtures, install_historical_schema, major_version,
    migrator_url, read_through_port, recreate_database, retain_observation, schema_version,
    server_version, with_database,
};

/// The schema versions the M5 preview promises a direct upgrade from.
const SOURCE_VERSIONS: [u32; 2] = [1, 2];

/// The schema version the upgrade must reach.
const TARGET_VERSION: u32 = 3;

#[test]
fn schema1_and_schema2_upgrade_directly_to_schema3() -> Result<(), Box<dyn Error>> {
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

/// Upgrades one database per source version and reports on every one.
async fn run_report(migrator: &str, admin: &str) -> Result<(), Box<dyn Error>> {
    let server = server_version(migrator).await?;
    let mut paths = Vec::new();
    for source in SOURCE_VERSIONS {
        paths.push(upgrade_from(migrator, admin, source).await?);
    }

    retain_observation(
        "schema-upgrade",
        &json!({
            "report": "direct upgrade to schema 3",
            "scenario": "schema1_and_schema2_upgrade_directly_to_schema3",
            "fixture": "postgres-upgrade",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "paths": paths,
            "execution_manifest": execution_manifest()?,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Builds one prior-schema database, upgrades it, and requires the contract.
#[allow(
    clippy::too_many_lines,
    reason = "building a prior schema, seeding it, upgrading it, comparing every durable \
              value, reading it through the port, and repeating the migration form one report \
              that is only meaningful in order"
)]
async fn upgrade_from(migrator: &str, admin: &str, source: u32) -> Result<Value, Box<dyn Error>> {
    let database = format!("oxide_batch_m5_upgrade_from_{source}");
    let url = with_database(migrator, &database)?;
    recreate_database(admin, &database).await?;

    install_historical_schema(&url, source).await?;
    assert_historical_shape(&url, source).await?;

    let seed = fixtures().join(format!("schema-{source}")).join("seed.sql");
    apply_seed(&url, &seed).await?;

    let tables = durable_tables(source);
    let columns = SourceColumns::capture(&url, &tables).await?;
    let before = DurableDigest::read(&url, &columns, &tables).await?;
    assert!(
        !before.is_empty(),
        "the schema-{source} fixture seeded no durable state, so the upgrade would carry nothing",
    );
    let logical_before = step_names(&url).await?;

    // The upgrade itself: one invocation of the migrator this crate ships,
    // against a database at the prior schema. Nothing intermediate is applied
    // by hand, which is what makes the path direct.
    PostgresMigrator::migrate(&config(url.clone())?).await?;

    let installed = schema_version(&url).await?;
    assert_eq!(
        installed,
        Some(TARGET_VERSION),
        "a schema-{source} database must record schema {TARGET_VERSION} after the upgrade",
    );
    assert_historical_shape(&url, TARGET_VERSION).await?;

    let after = DurableDigest::read(&url, &columns, &tables).await?;
    assert_eq!(
        before.differences(&after),
        Vec::<String>::new(),
        "the upgrade from schema {source} changed a value of a column schema {source} declared",
    );

    // Schema 2 is the only migration in the chain that writes to a column an
    // earlier schema already had, and what it writes is the step's own name as
    // its logical identity. An upgrade that invented one instead would leave a
    // step unaddressable across the definition change the identity exists for.
    let logical_after = step_logical_ids(&url).await?;
    assert_eq!(
        logical_after, logical_before,
        "every step execution carried forward from schema {source} must keep its name as its \
         logical identity",
    );

    let repository =
        PostgresJobRepository::connect(config(url.clone())?, Arc::new(FixedClock(UNIX_EPOCH)))
            .await?;
    let reading = read_through_port(&repository).await?;
    assert!(
        reading.instance.is_some(),
        "the upgraded database must report the seeded instance under the identity the domain \
         computes for it",
    );
    assert_eq!(
        reading.executions.len(),
        2,
        "the upgraded database must report both seeded attempts",
    );
    assert!(
        reading.projections.iter().all(Option::is_some),
        "the upgraded database must project every attempt through the explorer",
    );
    assert!(
        reading.recovery_decisions.iter().any(Option::is_some),
        "the upgraded database must still report the recovery decision that resolved the first \
         attempt",
    );
    if source >= 2 {
        assert!(
            reading
                .flow_decisions
                .iter()
                .any(|decisions| !decisions.is_empty()),
            "a schema-2 database's recorded flow decision must survive the upgrade",
        );
    }

    // Running the migrator again is the ordinary operational case: a second
    // process, a retried deployment step, or an operator who is not sure the
    // first run finished. It must be a no-op rather than a second upgrade.
    PostgresMigrator::migrate(&config(url.clone())?).await?;
    assert_eq!(
        schema_version(&url).await?,
        Some(TARGET_VERSION),
        "migrating an already-upgraded database must leave it at schema {TARGET_VERSION}",
    );
    let repeated = DurableDigest::read(&url, &columns, &tables).await?;
    assert_eq!(
        after.differences(&repeated),
        Vec::<String>::new(),
        "migrating an already-upgraded database changed durable state",
    );
    let reading_again = read_through_port(&repository).await?;
    assert_eq!(
        reading, reading_again,
        "migrating an already-upgraded database changed what the durable contracts report",
    );

    repository.close().await?;

    let observation = json!({
        "source_schema_version": source,
        "target_schema_version": TARGET_VERSION,
        "migration_result": "ok",
        "repository_open_result": "ok",
        "durable_state_verified": true,
        "backup_restore_result": Value::Null,
        "observed_schema_version": installed,
        "database": database,
        "fixture": {
            "seed": format!("tests/fixtures/upgrade/schema-{source}/seed.sql"),
            "installed_by": format!(
                "the immutable migration set run to {source} and stopped there"
            ),
            "tables_compared": columns.tables(),
            "rows_compared": before.counts(),
        },
        "durable_state_preserved": true,
        "step_logical_identity": logical_after,
        "port_reading": reading.summary(),
        "idempotent_remigration": {
            "result": "ok",
            "durable_state_preserved": true,
            "port_reading_unchanged": true,
        },
        "violations": Vec::<String>::new(),
        "passed": true,
    });

    // The upgraded database is left in place. A campaign that dropped its
    // evidence could not be inspected after a failure, and the next run
    // recreates it before doing anything.
    Ok(observation)
}

/// Reads each step execution's name, by identifier.
///
/// This and the logical identity beside it are read directly rather than
/// through the port because the comparison spans a schema change: before the
/// upgrade the logical identity does not exist, and no contract of the current
/// runtime can report a schema-1 database at all.
async fn step_names(url: &str) -> Result<Vec<(i64, String)>, Box<dyn Error>> {
    read_identities(
        url,
        "SELECT id, step_name FROM oxide_batch.ob_step_execution ORDER BY id",
    )
    .await
}

/// Reads each step execution's logical identity, by identifier.
async fn step_logical_ids(url: &str) -> Result<Vec<(i64, String)>, Box<dyn Error>> {
    read_identities(
        url,
        "SELECT id, step_logical_id FROM oxide_batch.ob_step_execution ORDER BY id",
    )
    .await
}

/// Reads one identifier and text column pair, in identifier order.
async fn read_identities(
    url: &str,
    statement: &'static str,
) -> Result<Vec<(i64, String)>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let rows = sqlx::query(statement).fetch_all(&pool).await?;
    let mut identities = Vec::new();
    for row in &rows {
        identities.push((row.try_get::<i64, _>(0)?, row.try_get::<String, _>(1)?));
    }
    pool.close().await;
    Ok(identities)
}
