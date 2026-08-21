//! Direct upgrade of a schema-1, schema-2, and schema-3 database to the
//! current installed schema.
//!
//! The M5 preview's original claim was narrower than what this report now
//! proves: it said a `PostgreSQL` database at schema 1 or schema 2 upgrades
//! directly to schema 3, which was the current schema when M5 wrote it. M6
//! `#144` then added `0005_item_stream_component_state.sql`, an additive
//! migration that carries the installed schema to 4 without changing anything
//! schema 3 declared. The historical M5 claim (1/2 -> 3, direct) is preserved
//! below as an intermediate structural checkpoint every path still passes
//! through; the report's actual target is now the current schema, whatever
//! that is, and a schema-3 source is added so the 3 -> 4 edge M6 introduced
//! gets the same direct-upgrade evidence the 1 -> 3 and 2 -> 3 edges already
//! had. This target performs every upgrade on a real database at its source
//! schema rather than on a reconstruction of one: each source database is
//! built by running the immutable migration set up to the version under test
//! and stopping there, so its tables, columns, constraints, indexes, and
//! applied-migration bookkeeping are the ones that version produced when it
//! was the whole schema.
//!
//! Each source is then seeded with the durable state an operator's database
//! would have held — registered definitions and the upgrade edge between them,
//! a job instance, a resolved attempt and a live one, their step executions
//! with durable checkpoints, contexts, and counters, and a recovery decision —
//! the schema-2 and schema-3 sources additionally with the logical step
//! identity, retry and skip counters, and flow decision schema 2 introduced,
//! and the schema-3 source additionally with the stop request, operator
//! request, retention action, and step partition schema 3 introduced.
//!
//! What the report requires of the upgrade is five things. The recorded
//! version becomes the current schema version and every structural checkpoint
//! from the source's own schema up through the current one appears in order
//! (so a schema-1 source is still shown passing through schema 3's shape on
//! its way to schema 4). Every value of every column the source schema
//! declared is byte-identical afterwards, compared through the source's own
//! column list so a column a later schema added cannot mask a loss. The new
//! `ItemStream` component-state table schema 4 adds carries no row for any of
//! these upgrades — the migration is additive, not a backfill, and a row
//! appearing there would mean the migration invented state for an execution
//! that never ran a stream. The upgraded database opens through the current
//! repository and projects its job through the explorer, so the result is one
//! the runtime can work with rather than merely one that migrated. And running
//! the migrator again changes nothing.
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

/// The schema versions this report upgrades from directly: the M5 preview's
/// original 1/2 -> 3 claim, plus the schema-3 source the 3 -> 4 M6 edge needs.
const SOURCE_VERSIONS: [u32; 3] = [1, 2, 3];

/// The schema version the upgrade must reach: the current installed schema
/// (4, since M6 `#144`'s additive `ItemStream` component-state migration),
/// not the schema-3 target the M5 preview named when it was current.
const TARGET_VERSION: u32 = 4;

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

    // Schema 4's migration is additive: it adds the `ItemStream`
    // component-state table and backfills nothing into it. A row appearing
    // here would mean the migration invented restart state for an execution
    // that never registered a stream, which is exactly the silent
    // reinterpretation an additive migration must not perform.
    let component_state_rows = component_state_row_count(&url).await?;
    assert_eq!(
        component_state_rows, 0,
        "upgrading a schema-{source} database must not invent component-state rows",
    );

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
        "component_state_rows_invented": component_state_rows,
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

/// Counts the `ItemStream` component-state rows a database holds.
///
/// Schema 4's migration adds this table and backfills nothing into it, so any
/// row here after upgrading a database that never registered a stream would
/// be state the migration invented rather than state it carried forward.
async fn component_state_row_count(url: &str) -> Result<i64, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM oxide_batch.ob_component_state")
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    Ok(count)
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
