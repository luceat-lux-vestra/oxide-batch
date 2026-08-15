//! Mechanics the M5 `PostgreSQL` upgrade campaign's reports share.
//!
//! The campaign's three reports all need the same four things, and each of them
//! is stated once here.
//!
//! **A database that really is at a prior schema.** The historical schemas are
//! not reconstructed: `crates/oxide-batch/migrations` is the immutable
//! migration set this crate has always shipped, `0001` installs schema 1 and
//! `0002` installs schema 2, and [`install_historical_schema`] runs that set
//! through sqlx's own migrator up to the requested version and stops. What the
//! campaign gets is the schema those migrations produced when they were the
//! whole set, with sqlx's applied-migration bookkeeping to match, so the
//! upgrade afterwards is the real remaining chain rather than a replay. The
//! alternative — installing schema 3 and lowering the recorded number — would
//! prove nothing about an upgrade path, and [`assert_historical_shape`] fails
//! the campaign if a fixture ever drifts into looking like one.
//!
//! **A reading of durable state that survives a schema change.** A prior schema
//! cannot be read through the repository port, because the current runtime
//! refuses to open one. [`DurableDigest`] therefore reads rows directly, but
//! only through the column list the *source* schema declared, captured before
//! the upgrade from `information_schema`. Columns a later schema adds are
//! outside the projection and cannot mask a lost or rewritten value, and the
//! comparison is exact: the upgrade chain rewrites no value of a column that
//! already existed, so anything but equality is a defect.
//!
//! **Databases to work in.** Every report needs a database at a known starting
//! state, and two of them need a second one. They are created and dropped here
//! rather than shared, because a campaign that ran against leftover state would
//! be reporting on something other than what it built.
//!
//! **A machine-readable observation.** Each report retains one, for the reason
//! `cargo xtask upgrade` exists: a `PostgreSQL` report returns success without a
//! database because it skips, so the runner requires evidence that the work
//! actually happened rather than trusting an exit code.

#![allow(
    dead_code,
    reason = "each report uses part of this module; the whole of it is used across the three"
)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use oxide_batch::{PostgresConfig, TlsMode};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, Connection, PgConnection, Row};

/// The variable that tells a report where to retain its observation.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_UPGRADE_OBSERVATIONS";

/// The metadata schema every report inspects.
pub const METADATA_SCHEMA: &str = "oxide_batch";

/// The revision whose runtime supports schema 2 and knows no schema 3.
///
/// The rejection report needs a runtime whose supported schema version really
/// is 2, and no such runtime can be built from this working tree: the version
/// is a constant of the crate. This is the commit immediately before schema 3
/// was added, so the runtime built from it is the one that shipped against
/// schema 2 rather than a reconstruction of it. It is pinned by hash because a
/// branch name would let the reference move and the report would then be about
/// a different runtime than the one it names.
pub const SCHEMA2_RUNTIME_REVISION: &str = "397a38bcada93d961dbb2ca3d9960311a3fb4395";

/// The tables schema 1 declared, in dependency order.
pub const SCHEMA1_TABLES: &[&str] = &[
    "ob_schema_version",
    "ob_job_definition",
    "ob_definition_upgrade",
    "ob_job_instance",
    "ob_job_execution",
    "ob_step_execution",
    "ob_recovery_decision",
];

/// The tables schema 2 added to schema 1.
pub const SCHEMA2_TABLES: &[&str] = &["ob_flow_decision"];

/// The tables schema 3 added to schema 2.
pub const SCHEMA3_TABLES: &[&str] = &[
    "ob_operator_request",
    "ob_retention_action",
    "ob_step_partition",
];

/// Columns schema 2 added to tables schema 1 already declared.
pub const SCHEMA2_COLUMNS: &[(&str, &str)] = &[
    ("ob_step_execution", "step_logical_id"),
    ("ob_step_execution", "read_retry_count"),
    ("ob_step_execution", "fault_state_payload"),
];

/// Columns schema 3 added to tables schema 2 already declared.
pub const SCHEMA3_COLUMNS: &[(&str, &str)] = &[
    ("ob_job_execution", "owner_token"),
    ("ob_job_execution", "stop_requested_at"),
    ("ob_job_instance", "hold_actor"),
];

/// The immutable migration set this crate ships.
///
/// This is the same directory the adapter embeds. The campaign installs a prior
/// schema by running a prefix of it rather than by writing its own DDL, so a
/// fixture cannot describe a schema that never existed.
static MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

/// Returns the migrating connection string, when the fixture supplies one.
///
/// This is the campaign's template: every database it creates is named on the
/// same server, by the same role, with the same connection parameters.
#[must_use]
pub fn migrator_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL")
}

/// Returns the administrative connection string, when the fixture supplies one.
///
/// Every report builds its prior-schema database from nothing, and two of them
/// restore into a second one, which cannot be created or dropped from inside
/// the database being worked on. A separate variable also keeps the campaign
/// out of runs that supply only a runtime database.
#[must_use]
pub fn admin_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_BACKUP_TEST_URL")
}

/// Reads one environment variable, treating an empty value as absent.
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Builds the campaign's plaintext repository configuration.
///
/// # Errors
///
/// Returns the configuration failure when the URL or a timeout is rejected.
pub fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?
        .with_tls_mode(TlsMode::Plaintext)
        .with_statement_timeout(Duration::from_mins(2))?
        .with_lock_timeout(Duration::from_mins(2))?)
}

/// Installs the schema the migration set produced at `version` and stops there.
///
/// The connection is prepared exactly as the adapter prepares its own before
/// migrating — the metadata schema is created and the search path is set — so
/// the applied-migration bookkeeping lands where the adapter will look for it
/// and the upgrade under test continues the chain instead of restarting it.
///
/// # Errors
///
/// Returns the database or migration failure that prevented installation.
pub async fn install_historical_schema(url: &str, version: u32) -> Result<(), Box<dyn Error>> {
    let mut connection = PgConnection::connect(url).await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch")
        .execute(&mut connection)
        .await?;
    sqlx::query("SET search_path TO oxide_batch, pg_catalog")
        .execute(&mut connection)
        .await?;
    MIGRATIONS
        .run_to(i64::from(version), &mut connection)
        .await?;
    connection.close().await?;

    let installed = schema_version(url).await?;
    if installed != Some(version) {
        return Err(Box::new(Failure(format!(
            "installing schema {version} recorded {installed:?}"
        ))));
    }
    Ok(())
}

/// Reads the recorded schema version, or `None` when none is installed.
///
/// # Errors
///
/// Returns the database failure when the reading cannot be taken.
pub async fn schema_version(url: &str) -> Result<Option<u32>, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let version: Option<i32> = sqlx::query_scalar(
        "SELECT version FROM oxide_batch.ob_schema_version WHERE singleton = TRUE",
    )
    .fetch_optional(&pool)
    .await
    .unwrap_or(None);
    pool.close().await;
    Ok(version.map(|value| u32::try_from(value).unwrap_or(u32::MAX)))
}

/// Reports whether the metadata schema declares a table.
///
/// # Errors
///
/// Returns the database failure when the catalogue cannot be read.
pub async fn table_exists(url: &str, table: &str) -> Result<bool, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_schema = $1 AND table_name = $2)",
    )
    .bind(METADATA_SCHEMA)
    .bind(table)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(present)
}

/// Reports whether the metadata schema declares a column.
///
/// # Errors
///
/// Returns the database failure when the catalogue cannot be read.
pub async fn column_exists(url: &str, table: &str, column: &str) -> Result<bool, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
    )
    .bind(METADATA_SCHEMA)
    .bind(table)
    .bind(column)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(present)
}

/// Requires a database to carry the schema the named version declared, and no
/// structure a later one introduced.
///
/// This is what separates a historical fixture from a current schema wearing an
/// older number. It runs before every upgrade the campaign performs, so a
/// fixture that silently became schema 3 fails the report that depends on it
/// rather than passing it.
///
/// # Errors
///
/// Returns the reading failure, or the first structure that is present and
/// should not be, or absent and should not be.
pub async fn assert_historical_shape(url: &str, version: u32) -> Result<(), Box<dyn Error>> {
    let installed = schema_version(url).await?;
    if installed != Some(version) {
        return Err(Box::new(Failure(format!(
            "the fixture must record schema {version} and records {installed:?}"
        ))));
    }

    for table in SCHEMA1_TABLES {
        if !table_exists(url, table).await? {
            return Err(Box::new(Failure(format!(
                "schema {version} declares {table} and the fixture does not have it"
            ))));
        }
    }

    for (present, tables, columns) in [
        (version >= 2, SCHEMA2_TABLES, SCHEMA2_COLUMNS),
        (version >= 3, SCHEMA3_TABLES, SCHEMA3_COLUMNS),
    ] {
        for table in tables {
            if table_exists(url, table).await? != present {
                return Err(Box::new(Failure(format!(
                    "a schema {version} fixture must {} {table}",
                    if present { "declare" } else { "not declare" }
                ))));
            }
        }
        for (table, column) in columns {
            if column_exists(url, table, column).await? != present {
                return Err(Box::new(Failure(format!(
                    "a schema {version} fixture must {} {table}.{column}",
                    if present { "declare" } else { "not declare" }
                ))));
            }
        }
    }

    Ok(())
}

/// The durable rows of one schema, read through that schema's own columns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableDigest {
    /// Every table's rows, rendered in a stable order.
    rows: BTreeMap<String, Vec<String>>,
}

impl DurableDigest {
    /// Reads every table `tables` names through the columns `projection` fixes.
    ///
    /// # Errors
    ///
    /// Returns the database failure when a table cannot be read.
    pub async fn read(
        url: &str,
        projection: &SourceColumns,
        tables: &[&str],
    ) -> Result<Self, Box<dyn Error>> {
        let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        let mut rows = BTreeMap::new();
        for table in tables {
            let Some(columns) = projection.rows.get(*table) else {
                continue;
            };
            // The projection is the server's own `quote_ident` rendering of the
            // columns the source schema declared, and the table is one of this
            // campaign's compile-time constants, so the statement carries no
            // caller-supplied text.
            let statement = format!(
                "SELECT row_to_json(projected)::text FROM \
                 (SELECT {columns} FROM {METADATA_SCHEMA}.{table}) AS projected"
            );
            let mut rendered = sqlx::query(AssertSqlSafe(statement))
                .fetch_all(&pool)
                .await?
                .iter()
                .map(|row| row.try_get::<String, _>(0))
                .collect::<Result<Vec<_>, _>>()?;
            rendered.sort();
            rows.insert((*table).to_owned(), rendered);
        }
        pool.close().await;
        Ok(Self { rows })
    }

    /// Returns how many rows the digest covers, by table.
    #[must_use]
    pub fn counts(&self) -> BTreeMap<String, usize> {
        self.rows
            .iter()
            .map(|(table, rows)| (table.clone(), rows.len()))
            .collect()
    }

    /// Returns how many rows the digest covers in total.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.values().map(Vec::len).sum()
    }

    /// Reports whether the digest covers no row at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Names the tables whose rows differ from another reading.
    #[must_use]
    pub fn differences(&self, other: &Self) -> Vec<String> {
        let mut differences = Vec::new();
        for table in self.rows.keys().chain(other.rows.keys()) {
            if self.rows.get(table) != other.rows.get(table) && !differences.contains(table) {
                differences.push(table.clone());
            }
        }
        differences
    }
}

/// The columns one schema declared, as the projection a later reading must use.
#[derive(Clone, Debug)]
pub struct SourceColumns {
    /// Each table's quoted column list, in declaration order.
    rows: BTreeMap<String, String>,
}

impl SourceColumns {
    /// Captures the column list of every table `tables` names.
    ///
    /// The rendering is the server's, through `quote_ident`, so the projection
    /// it produces is a valid identifier list rather than an assembled string.
    ///
    /// # Errors
    ///
    /// Returns the database failure when the catalogue cannot be read.
    pub async fn capture(url: &str, tables: &[&str]) -> Result<Self, Box<dyn Error>> {
        let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        let mut rows = BTreeMap::new();
        for table in tables {
            let columns: Option<String> = sqlx::query_scalar(
                "SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2",
            )
            .bind(METADATA_SCHEMA)
            .bind(table)
            .fetch_one(&pool)
            .await?;
            if let Some(columns) = columns {
                rows.insert((*table).to_owned(), columns);
            }
        }
        pool.close().await;
        Ok(Self { rows })
    }

    /// Returns the tables the capture covers.
    #[must_use]
    pub fn tables(&self) -> Vec<String> {
        self.rows.keys().cloned().collect()
    }
}

/// Runs one committed seed script against a prior-schema database.
///
/// Nothing is returned to be recorded. How much state a seed produced is read
/// back out of the database as the rows the comparison covers, and a second
/// count derived from the script itself would be a different measurement
/// wearing the same name.
///
/// The script is applied as one statement batch so a partially seeded fixture
/// cannot be reported on. The prior schema's own constraints decide whether the
/// rows belong to it, which is what makes a committed script evidence rather
/// than an assertion: a column, a status, or a shape that schema did not have
/// is rejected by the database rather than by this campaign.
///
/// # Errors
///
/// Returns the database failure, which for a seed that does not belong to the
/// schema is the constraint that rejected it.
pub async fn apply_seed(url: &str, script: &Path) -> Result<(), Box<dyn Error>> {
    let source = fs::read_to_string(script)
        .map_err(|error| Failure(format!("could not read {}: {error}", script.display())))?;
    let mut connection = PgConnection::connect(url).await?;
    // The script is a committed fixture of this campaign, read from the
    // repository rather than supplied by a caller.
    let outcome = sqlx::raw_sql(AssertSqlSafe(source))
        .execute(&mut connection)
        .await;
    connection.close().await?;
    outcome?;
    Ok(())
}

/// Returns the campaign's committed fixture directory.
#[must_use]
pub fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests")
        .join("fixtures")
        .join("upgrade")
}

/// Returns the workspace root that contains this package.
#[must_use]
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Replaces the database in a connection URL, keeping everything else.
///
/// # Errors
///
/// Returns the failure when the URL names no database to replace.
pub fn with_database(url: &str, name: &str) -> Result<String, Box<dyn Error>> {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(base, query)| (base, Some(query.to_owned())));
    let prefix = base
        .rsplit_once('/')
        .map(|(prefix, _)| prefix)
        .ok_or_else(|| Failure(format!("{url} names no database")))?;
    Ok(match query {
        Some(query) => format!("{prefix}/{name}?{query}"),
        None => format!("{prefix}/{name}"),
    })
}

/// Drops and recreates one database, so a report starts from nothing.
///
/// # Errors
///
/// Returns the database failure when the database cannot be replaced.
pub async fn recreate_database(admin_url: &str, name: &str) -> Result<(), Box<dyn Error>> {
    drop_database(admin_url, name).await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await?;
    // The database name is a compile-time constant of this campaign, so the
    // statement carries no caller-supplied text.
    sqlx::query(AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Drops one database, disconnecting anything still attached to it.
///
/// # Errors
///
/// Returns the database failure when the database cannot be dropped.
pub async fn drop_database(admin_url: &str, name: &str) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await?;
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(name)
    .execute(&pool)
    .await?;
    sqlx::query(AssertSqlSafe(format!("DROP DATABASE IF EXISTS \"{name}\"")))
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

/// Returns the major version of a `PostgreSQL` server version string.
///
/// The campaign runs on a supported-version matrix, and two reports from two
/// matrix points are otherwise indistinguishable in the retained evidence.
#[must_use]
pub fn major_version(server: &str) -> String {
    server.split(['.', ' ']).next().unwrap_or(server).to_owned()
}

/// Reads the server version a report ran against.
///
/// # Errors
///
/// Returns the database failure when the reading cannot be taken.
pub async fn server_version(url: &str) -> Result<String, Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let version: String = sqlx::query("SHOW server_version")
        .fetch_one(&pool)
        .await?
        .try_get(0)?;
    pool.close().await;
    Ok(version)
}

/// Runs one `PostgreSQL` client tool and returns the version that ran.
///
/// The version is recorded rather than assumed: an archive is only evidence if
/// the report says which tool wrote it, and a client older than the server
/// refuses to dump at all, which must fail the campaign rather than be skipped.
///
/// # Errors
///
/// Returns the failure when the tool is absent or does not succeed.
pub fn run_tool(program: &str, arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    let version = Command::new(program)
        .arg("--version")
        .output()
        .map_err(|error| {
            Failure(format!(
                "the campaign needs {program} on PATH and could not run it: {error}"
            ))
        })?;
    if !version.status.success() {
        return Err(Box::new(Failure(format!("{program} --version failed"))));
    }

    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(Box::new(Failure(format!(
            "{program} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))));
    }
    Ok(String::from_utf8_lossy(&version.stdout).trim().to_owned())
}

/// Reads the declared semantic closure of the `PostgreSQL` upgrade campaign.
///
/// Read from `tests/fixtures/upgrade/campaign-semantics.json` rather than
/// listed here, because the xtask verifier reads the same document: a closure
/// kept in two places is one that will disagree.
///
/// # Errors
///
/// Returns the failure when the closure document cannot be read or parsed, or
/// declares no paths.
pub fn semantics_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let path = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("upgrade")
        .join("campaign-semantics.json");
    let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
    let categories = document
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure("the semantics document declares no categories".to_owned()))?;
    let mut paths = categories
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        return Err(Box::new(Failure(
            "the semantics document declares no paths".to_owned(),
        )));
    }
    Ok(paths)
}

/// Records the object identity of the campaign's closure, as executed.
///
/// This is the provenance root, and it is taken here rather than reconstructed
/// later for one reason: this process is the campaign, so the tree it can see
/// is by definition the tree that ran. In CI that is the pull-request merge
/// commit the workflow checked out — an ephemeral object no later clone can
/// resolve — so a verifier that tried to re-derive these identities from a
/// commit name would be depending on something GitHub throws away. Recording
/// them in the report makes the binding permanent and offline.
///
/// # Errors
///
/// Returns the failure when the closure cannot be read, or when git cannot
/// describe the tree the campaign is running against.
pub fn execution_manifest() -> Result<Value, Box<dyn Error>> {
    let root = workspace_root();
    let commit = git(&root, &["rev-parse", "HEAD"])
        .ok_or_else(|| Failure("the campaign is not running inside a git tree".to_owned()))?;
    let mut objects = serde_json::Map::new();
    for path in semantics_paths()? {
        let object = git(&root, &["rev-parse", &format!("HEAD:{path}")]).ok_or_else(|| {
            Failure(format!(
                "{path} is declared as campaign semantics and is not present"
            ))
        })?;
        objects.insert(path, Value::String(object));
    }
    Ok(serde_json::json!({
        "execution_commit": commit,
        "execution_commit_note": "The tree this run actually executed against, read from the \
                                  checkout the campaign is running in. In CI this is the \
                                  pull-request merge commit rather than the branch head, and it \
                                  is the authority: the objects below are its objects.",
        "tree_clean": git(&root, &["status", "--porcelain"]).map(|status| status.is_empty()),
        "objects": Value::Object(objects),
    }))
}

/// Runs one git command against the workspace, tolerating failure.
fn git(root: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Retains one report's observation where `cargo xtask upgrade` will read it.
///
/// Returns `None` when the campaign is not driving the run, which is what an
/// ordinary `cargo test` does. The campaign requires the file to exist, so a
/// report that skipped or never reached its end cannot be counted as evidence.
///
/// # Errors
///
/// Returns the failure when the observation cannot be rendered or written.
pub fn retain_observation(name: &str, document: &Value) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let Some(directory) = variable(OBSERVATIONS_ENV) else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{name}.json"));
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(document)?),
    )?;
    Ok(Some(path))
}

/// The job name every fixture's durable state belongs to.
pub const FIXTURE_JOB: &str = "m5_upgrade";

/// A clock pinned to one instant so nothing a report reads depends on time.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub std::time::SystemTime);

impl oxide_batch::Clock for FixedClock {
    fn now(&self) -> std::time::SystemTime {
        self.0
    }
}

/// Returns the tables whose rows an upgrade from `version` must preserve.
///
/// `ob_schema_version` is not one of them. It is the row the upgrade exists to
/// change, and it is checked by version rather than by comparison.
#[must_use]
pub fn durable_tables(version: u32) -> Vec<&'static str> {
    let mut tables = SCHEMA1_TABLES
        .iter()
        .copied()
        .filter(|table| *table != "ob_schema_version")
        .collect::<Vec<_>>();
    if version >= 2 {
        tables.extend_from_slice(SCHEMA2_TABLES);
    }
    if version >= 3 {
        tables.extend_from_slice(SCHEMA3_TABLES);
    }
    tables
}

/// Everything the durable contracts report about the fixture's job.
///
/// A row comparison says the upgrade moved no byte it should not have. This
/// says the result is a database the current runtime can actually work with:
/// the instance is found by the identity the domain computes, every attempt and
/// step decodes into its typed form, and the explorer projects each attempt
/// with its definition descriptor. A migration that left rows intact but
/// unreadable would pass the first check and fail this one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortReading {
    /// The logical instance, when the job has one.
    pub instance: Option<oxide_batch::JobInstance>,
    /// Every attempt, oldest first.
    pub executions: Vec<oxide_batch::JobExecution>,
    /// The explorer projection of every attempt.
    pub projections: Vec<Option<oxide_batch::JobExecutionProjection>>,
    /// Every step attempt, by enclosing attempt.
    pub steps: Vec<Vec<oxide_batch::StepExecution>>,
    /// The append-only recovery decision of every attempt.
    pub recovery_decisions: Vec<Option<oxide_batch::RecoveryDecision>>,
    /// The append-only flow decisions of every attempt.
    pub flow_decisions: Vec<Vec<oxide_batch::FlowDecision>>,
}

impl PortReading {
    /// Describes the reading for a retained observation.
    ///
    /// This is a description rather than the comparison: the reports compare
    /// the whole reading, and this exists so a reader of the evidence can see
    /// what was compared.
    #[must_use]
    pub fn summary(&self) -> Value {
        serde_json::json!({
            "instance_found": self.instance.is_some(),
            "attempts": self.executions.len(),
            "attempt_statuses": self
                .executions
                .iter()
                .map(|execution| execution.metadata().status().as_str())
                .collect::<Vec<_>>(),
            "attempt_versions": self
                .executions
                .iter()
                .map(|execution| execution.version().get())
                .collect::<Vec<_>>(),
            "definition_digests": self
                .projections
                .iter()
                .map(|projection| projection
                    .as_ref()
                    .and_then(oxide_batch::JobExecutionProjection::definition)
                    .map(oxide_batch::DefinitionDescriptor::manifest_digest_hex))
                .collect::<Vec<_>>(),
            "steps": self.steps.iter().map(Vec::len).collect::<Vec<_>>(),
            "step_statuses": self
                .steps
                .iter()
                .map(|attempt| attempt
                    .iter()
                    .map(|step| step.metadata().status().as_str())
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            "recovery_decisions": self
                .recovery_decisions
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>(),
            "flow_decisions": self.flow_decisions.iter().map(Vec::len).collect::<Vec<_>>(),
        })
    }
}

/// Reads the fixture's job through the repository and explorer contracts.
///
/// # Errors
///
/// Returns the repository or explorer failure that prevented a complete
/// reading. A partial reading is never returned, because a comparison over one
/// is not evidence.
pub async fn read_through_port(
    repository: &oxide_batch::PostgresJobRepository,
) -> Result<PortReading, Box<dyn Error>> {
    use oxide_batch::{ExplorerRepository, JobRepository};

    let key = oxide_batch::JobInstanceKey::new(
        oxide_batch::JobName::new(FIXTURE_JOB)?,
        &oxide_batch::JobParameters::new(),
    );
    let explorer = oxide_batch::PostgresExplorer::new(repository.clone());
    let mut unit = repository.begin().await?;

    let Some(instance) = unit.find_job_instance(&key).await? else {
        unit.rollback().await?;
        return Ok(PortReading {
            instance: None,
            executions: Vec::new(),
            projections: Vec::new(),
            steps: Vec::new(),
            recovery_decisions: Vec::new(),
            flow_decisions: Vec::new(),
        });
    };

    let executions = unit.job_executions(instance.id()).await?;
    let mut steps = Vec::new();
    let mut recovery_decisions = Vec::new();
    let mut flow_decisions = Vec::new();
    for execution in &executions {
        steps.push(unit.step_executions(execution.id()).await?);
        recovery_decisions.push(unit.recovery_decision(execution.id()).await?);
        flow_decisions.push(
            unit.flow_decisions(execution.id())
                .await
                .unwrap_or_default(),
        );
    }
    unit.rollback().await?;

    let mut projections = Vec::new();
    for execution in &executions {
        projections.push(explorer.execution(execution.id()).await?);
    }

    Ok(PortReading {
        instance: Some(instance),
        executions,
        projections,
        steps,
        recovery_decisions,
        flow_decisions,
    })
}

/// What a historical runtime reported when it was pointed at a database.
#[derive(Clone, Debug)]
pub struct ProbeRun {
    /// The revision the runtime was built from.
    pub revision: String,
    /// Whether the runtime's own exit status said it failed closed.
    pub exit_success: bool,
    /// The one line of structured output the probe printed.
    pub report: Value,
}

/// Builds the runtime at `SCHEMA2_RUNTIME_REVISION` and points it at `target`.
///
/// The runtime is checked out into a worktree and built there, because that is
/// the only way to obtain one whose supported schema version is 2: the version
/// is a constant of the crate and this working tree's is 3. The probe program
/// is this campaign's committed fixture rather than something written into the
/// worktree by hand, so what runs against the database is reviewable here.
///
/// The build directory is outside the worktree and is not cleaned between
/// runs, so a repeated campaign compiles the historical runtime once.
///
/// # Errors
///
/// Returns the failure that prevented the historical runtime from being built
/// or run, including the revision being absent from the repository — which is
/// what a shallow clone produces, and which must fail the campaign rather than
/// silently skip the only reading that proves the contract.
pub fn run_schema2_runtime(target: &str) -> Result<ProbeRun, Box<dyn Error>> {
    let root = workspace_root();
    let staging = root.join("target").join("m5-upgrade");
    let worktree = staging.join("schema2-runtime");
    let build = staging.join("schema2-build");

    let present = Command::new("git")
        .current_dir(&root)
        .args([
            "cat-file",
            "-e",
            &format!("{SCHEMA2_RUNTIME_REVISION}^{{commit}}"),
        ])
        .status()?;
    if !present.success() {
        return Err(Box::new(Failure(format!(
            "the campaign builds the schema-2 runtime from {SCHEMA2_RUNTIME_REVISION} and this \
             repository does not have that commit; fetch the full history"
        ))));
    }

    // The worktree is replaced rather than reused so the runtime under test is
    // the revision named here even if an earlier run left something else.
    if worktree.exists() {
        remove_worktree(&root, &worktree);
    }
    fs::create_dir_all(&staging)?;
    let added = Command::new("git")
        .current_dir(&root)
        .args(["worktree", "add", "--detach", "--force"])
        .arg(&worktree)
        .arg(SCHEMA2_RUNTIME_REVISION)
        .output()?;
    if !added.status.success() {
        return Err(Box::new(Failure(format!(
            "could not check out {SCHEMA2_RUNTIME_REVISION}: {}",
            String::from_utf8_lossy(&added.stderr).trim()
        ))));
    }

    let examples = worktree.join("crates").join("oxide-batch").join("examples");
    fs::create_dir_all(&examples)?;
    fs::copy(
        fixtures().join("schema-2-runtime").join("probe.rs"),
        examples.join("m5_schema2_probe.rs"),
    )?;

    let output = Command::new("cargo")
        .current_dir(&worktree)
        .args([
            "run",
            "--quiet",
            "--package",
            "oxide-batch",
            "--features",
            "postgres",
            "--example",
            "m5_schema2_probe",
        ])
        .env("CARGO_TARGET_DIR", &build)
        .env("OXIDEBATCH_PROBE_URL", target)
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let report = stdout
        .lines()
        .find_map(|line| line.strip_prefix("M5_SCHEMA2_PROBE "))
        .ok_or_else(|| {
            Failure(format!(
                "the schema-2 runtime printed no reading: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        })?;
    let report: Value = serde_json::from_str(report)?;

    remove_worktree(&root, &worktree);

    Ok(ProbeRun {
        revision: SCHEMA2_RUNTIME_REVISION.to_owned(),
        exit_success: output.status.success(),
        report,
    })
}

/// Detaches one campaign worktree and removes what it left behind.
///
/// Failure is not reported. The worktree is a build artifact of the campaign,
/// and a report that ran and observed what it needed should not fail on the
/// tidying afterwards; the next run replaces whatever is left either way.
fn remove_worktree(root: &Path, worktree: &Path) {
    let _ = Command::new("git")
        .current_dir(root)
        .args(["worktree", "remove", "--force"])
        .arg(worktree)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    let _ = fs::remove_dir_all(worktree);
}

/// A campaign input or expectation that did not hold.
#[derive(Debug)]
pub struct Failure(pub String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
