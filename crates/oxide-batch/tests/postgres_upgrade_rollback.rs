//! Rolling an upgrade back by restoring the backup taken before it.
//!
//! The M5 preview's rollback story is restore-based, and this target is the
//! report for it. It is deliberately not a downgrade migration: no SQL in this
//! repository turns a schema-3 database back into a schema-2 one, and none is
//! written here. What an operator has instead is the logical backup taken
//! before the upgrade, and what this report proves is that restoring it returns
//! a database at the prior schema carrying the state it had at that moment.
//!
//! Both directions of the M5 upgrade contract are covered, so the rollback is
//! reported for a schema-1 database and a schema-2 one rather than for whichever
//! is more convenient.
//!
//! Each run does the whole operational sequence. A prior-schema database is
//! built and seeded, `pg_dump` writes a custom-format archive of the metadata
//! schema, the migrator upgrades the database to schema 3, and the upgraded
//! database is then used — a hold is placed through the retention service,
//! which writes to a column and an audit table that exist only in schema 3, so
//! the upgraded state genuinely diverges from the backed-up state rather than
//! merely being labelled differently. `pg_restore` then loads the archive into
//! a separate, freshly created database.
//!
//! What the report requires of the restored database is that it be the prior
//! one: the recorded version is the source version, the structures schema 3
//! introduced are absent, every durable value equals the reading taken
//! immediately before the archive was written, and the current schema-3 runtime
//! refuses to open it — with `MigrationRequired`, naming the version it found.
//! That last requirement is the one that keeps the report honest. A rollback
//! that produced something the schema-3 runtime accepted would not be a
//! rollback, and nothing here claims the schema-3 state was converted: the
//! upgraded database is checked afterwards and still has everything the restore
//! did not bring back.

#![cfg(feature = "postgres")]

mod upgrade;

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use oxide_batch::{
    ActorRef, OperationId, PostgresJobRepository, PostgresMigrator, ReasonCode, RepositoryError,
    RetentionService,
};
use serde_json::{Value, json};

use upgrade::{
    DurableDigest, Failure, FixedClock, SourceColumns, admin_url, apply_seed,
    assert_historical_shape, config, drop_database, durable_tables, fixtures,
    install_historical_schema, major_version, migrator_url, read_through_port, recreate_database,
    retain_observation, run_tool, schema_version, server_version, with_database,
};

/// The schema versions an upgrade is rolled back to.
const SOURCE_VERSIONS: [u32; 2] = [1, 2];

/// The schema version the upgrade reaches before the rollback.
const UPGRADED_VERSION: u32 = 3;

/// The metadata schema the logical backup covers.
const DUMPED_SCHEMA: &str = "oxide_batch";

/// The actor the post-upgrade hold is placed by.
const HOLD_ACTOR: &str = "operator:m5-upgrade-campaign";

/// The reason the post-upgrade hold is placed for.
const HOLD_REASON: &str = "M5_UPGRADE_ROLLBACK";

#[test]
fn schema3_backup_restores_the_prior_schema() -> Result<(), Box<dyn Error>> {
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

/// Rolls one upgrade back per source version and reports on every one.
async fn run_report(migrator: &str, admin: &str) -> Result<(), Box<dyn Error>> {
    let server = server_version(migrator).await?;
    let mut paths = Vec::new();
    for source in SOURCE_VERSIONS {
        paths.push(roll_back_from(migrator, admin, source).await?);
    }

    retain_observation(
        "upgrade-rollback",
        &json!({
            "report": "restore-based rollback of an upgrade to schema 3",
            "scenario": "schema3_backup_restores_the_prior_schema",
            "fixture": "postgres-upgrade",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "method": "a logical pg_dump archive taken before the upgrade, restored with \
                       pg_restore into a separate freshly created database",
            "no_downgrade_migration": true,
            "paths": paths,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Backs up one prior schema, upgrades it, and restores the backup elsewhere.
#[allow(
    clippy::too_many_lines,
    reason = "backing up a prior schema, upgrading it, using the upgraded database, restoring \
              the backup into a separate one, and comparing both afterwards form one report \
              that is only meaningful in order"
)]
async fn roll_back_from(migrator: &str, admin: &str, source: u32) -> Result<Value, Box<dyn Error>> {
    let upgraded_database = format!("oxide_batch_m5_rollback_from_{source}");
    let restored_database = format!("oxide_batch_m5_restored_to_{source}");
    let upgraded_url = with_database(migrator, &upgraded_database)?;
    let restored_url = with_database(migrator, &restored_database)?;

    recreate_database(admin, &upgraded_database).await?;
    install_historical_schema(&upgraded_url, source).await?;
    assert_historical_shape(&upgraded_url, source).await?;
    apply_seed(
        &upgraded_url,
        &fixtures().join(format!("schema-{source}")).join("seed.sql"),
    )
    .await?;

    let tables = durable_tables(source);
    let columns = SourceColumns::capture(&upgraded_url, &tables).await?;
    let at_backup = DurableDigest::read(&upgraded_url, &columns, &tables).await?;
    assert!(
        !at_backup.is_empty(),
        "the schema-{source} fixture seeded no durable state, so the backup would carry nothing",
    );

    // The backup an operator would take before running the migrator. It is a
    // real logical archive written by the real tool, and the report records
    // which tool wrote it.
    let archive = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("m5-upgrade-rollback-{source}.dump"));
    let dump = run_tool(
        "pg_dump",
        &[
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            &format!("--file={}", archive.display()),
            &format!("--schema={DUMPED_SCHEMA}"),
            &upgraded_url,
        ],
    )?;
    let archive_bytes = std::fs::metadata(&archive)?.len();
    assert!(
        archive_bytes > 0,
        "a logical backup that wrote nothing is not a backup",
    );

    PostgresMigrator::migrate(&config(upgraded_url.clone())?).await?;
    assert_eq!(
        schema_version(&upgraded_url).await?,
        Some(UPGRADED_VERSION),
        "the database must be at schema {UPGRADED_VERSION} before the rollback is meaningful",
    );

    // Using the upgraded database, through a path that exists only in schema 3.
    // Without this the restored copy and the upgraded one would differ by a
    // version number alone, and the report would not be able to say the restore
    // brought back the earlier state rather than the later one.
    let repository = PostgresJobRepository::connect(
        config(upgraded_url.clone())?,
        Arc::new(FixedClock(UNIX_EPOCH)),
    )
    .await?;
    let upgraded_reading = read_through_port(&repository).await?;
    let instance = upgraded_reading
        .instance
        .as_ref()
        .ok_or_else(|| Failure("the upgraded database reports no instance".to_owned()))?
        .id();
    let retention = RetentionService::new(repository.clone(), Arc::new(FixedClock(UNIX_EPOCH)));
    retention
        .place_hold(
            OperationId::new(format!("m5-upgrade-rollback-{source}"))?,
            ActorRef::new(HOLD_ACTOR)?,
            ReasonCode::new(HOLD_REASON)?,
            instance,
        )
        .await?;
    assert!(
        retention.hold(instance).await?.is_some(),
        "the upgraded database must record the hold that only schema 3 can hold",
    );
    repository.close().await?;

    // The rollback. A separate database, created empty, loaded from the archive
    // taken before the upgrade. Nothing is downgraded in place.
    recreate_database(admin, &restored_database).await?;
    let restore = run_tool(
        "pg_restore",
        &[
            "--exit-on-error",
            "--no-owner",
            "--no-privileges",
            &format!("--dbname={restored_url}"),
            &archive.display().to_string(),
        ],
    )?;

    assert_eq!(
        schema_version(&restored_url).await?,
        Some(source),
        "the restored database must be at the schema the backup was taken from",
    );
    assert_historical_shape(&restored_url, source).await?;

    let restored = DurableDigest::read(&restored_url, &columns, &tables).await?;
    assert_eq!(
        at_backup.differences(&restored),
        Vec::<String>::new(),
        "the restored database must report the durable state the backup was taken from",
    );

    // A restored prior schema is a prior schema, and this runtime says so. An
    // upgrade rolled back is a runtime that has to be rolled back with it.
    let opened = PostgresJobRepository::connect(
        config(restored_url.clone())?,
        Arc::new(FixedClock(UNIX_EPOCH)),
    )
    .await;
    let Err(refusal) = opened else {
        return Err(Box::new(Failure(format!(
            "the schema-{UPGRADED_VERSION} runtime opened a database restored to schema {source}"
        ))));
    };
    assert_eq!(
        refusal,
        RepositoryError::MigrationRequired {
            current: source,
            supported: PostgresMigrator::supported_schema_version(),
        },
        "the current runtime must refuse a database restored to schema {source} by naming the \
         version it found, rather than treating it as compatible",
    );

    // The rollback restored the earlier state; it did not convert the later
    // one. The upgraded database is untouched by all of this, hold included.
    assert_eq!(
        schema_version(&upgraded_url).await?,
        Some(UPGRADED_VERSION),
        "restoring the backup elsewhere must not change the upgraded database",
    );
    let after = PostgresJobRepository::connect(
        config(upgraded_url.clone())?,
        Arc::new(FixedClock(UNIX_EPOCH)),
    )
    .await?;
    let retention_after = RetentionService::new(after.clone(), Arc::new(FixedClock(UNIX_EPOCH)));
    assert!(
        retention_after.hold(instance).await?.is_some(),
        "the upgraded database must still hold the schema-3 state the restore did not bring back",
    );
    assert_eq!(
        read_through_port(&after).await?,
        upgraded_reading,
        "restoring the backup elsewhere must not change what the upgraded database reports",
    );
    after.close().await?;

    let observation = json!({
        "source_schema_version": source,
        "target_schema_version": UPGRADED_VERSION,
        "migration_result": "ok",
        "repository_open_result": "refused: MigrationRequired",
        "durable_state_verified": true,
        "backup_restore_result": "ok",
        "observed_schema_version": source,
        "databases": {
            "upgraded": upgraded_database,
            "restored_into": restored_database,
        },
        "backup": {
            "taken_at_schema_version": source,
            "tool": dump,
            "format": "custom",
            "schema": DUMPED_SCHEMA,
            "archive_bytes": archive_bytes,
            "rows_covered": at_backup.counts(),
        },
        "restore": {
            "tool": restore,
            "into": "a separate database created empty for the restore",
            "downgrade_migration_applied": false,
        },
        "restored_schema_version": source,
        "restored_state_matches_backup": true,
        "schema3_structures_absent_after_restore": true,
        "current_runtime_refuses_restored_database": format!("{refusal:?}"),
        "upgraded_database_unchanged_by_rollback": true,
        "schema3_only_state_on_upgraded_database": {
            "retention_hold": {"actor": HOLD_ACTOR, "reason": HOLD_REASON},
            "restored_copy_cannot_carry_it": true,
        },
        "violations": Vec::<String>::new(),
        "passed": true,
    });

    drop_database(admin, &restored_database).await?;
    Ok(observation)
}
