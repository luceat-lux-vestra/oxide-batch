//! Mechanics the M5 `PostgreSQL` security campaign's reports share.
//!
//! The campaign's two database reports — the `verify-full` TLS report and the
//! least-privilege role matrix — need the same four things: the fixture's
//! connection material, a database of their own built from nothing, a way to
//! describe the server they ran against, and a place to retain the observation
//! the runner reconciles. Those are stated once here.
//!
//! The fixture is deliberately more than a connection string. A TLS report
//! needs a server that really speaks TLS, a certificate authority that really
//! signed its certificate, a second authority that signed nothing, a name the
//! certificate does not carry, and an endpoint that offers no TLS at all.
//! None of that can be derived from a URL, so each is its own variable and the
//! campaign refuses to run when one is missing rather than reporting on the
//! subset it happened to receive.

#![allow(
    dead_code,
    reason = "each report uses a subset of the shared mechanics"
)]

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use oxide_batch::{CaCertificate, PostgresConfig, TlsMode};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, Row};

/// The variable that tells a report where to retain its observation.
pub const OBSERVATIONS_ENV: &str = "OXIDEBATCH_SECURITY_OBSERVATIONS";

/// The schema the metadata lives in.
pub const METADATA_SCHEMA: &str = "oxide_batch";

/// Returns the administrative connection string the fixture supplies.
///
/// Every report creates the database it reports on, and the role matrix
/// additionally creates and drops the roles it exercises, neither of which can
/// be done from inside the database being replaced.
#[must_use]
pub fn admin_url() -> Option<String> {
    variable("OXIDEBATCH_POSTGRES_ADMIN_TEST_URL")
}

/// Returns the host name the server's certificate is issued for.
#[must_use]
pub fn tls_host() -> Option<String> {
    variable("OXIDEBATCH_SECURITY_TLS_HOST")
}

/// Returns a host that reaches the same server under a name it does not carry.
///
/// This is what makes a hostname failure a hostname failure: the address is
/// reachable, the certificate chain is trusted, and the only thing wrong is the
/// name. A host that simply did not resolve would fail for a different reason
/// and would prove nothing about hostname verification.
#[must_use]
pub fn tls_mismatch_host() -> Option<String> {
    variable("OXIDEBATCH_SECURITY_TLS_MISMATCH_HOST")
}

/// Returns the authority that signed the server's certificate.
#[must_use]
pub fn tls_ca() -> Option<PathBuf> {
    variable("OXIDEBATCH_SECURITY_TLS_CA").map(PathBuf::from)
}

/// Returns an authority that signed nothing this server presents.
#[must_use]
pub fn tls_untrusted_ca() -> Option<PathBuf> {
    variable("OXIDEBATCH_SECURITY_TLS_UNTRUSTED_CA").map(PathBuf::from)
}

/// Returns an endpoint that offers no TLS at all, when the fixture has one.
///
/// A supported-mode connection to it must fail. Without this endpoint the
/// campaign can only show that a bad certificate is refused, which a client
/// that silently downgraded to plaintext when TLS was unavailable would also
/// show.
#[must_use]
pub fn plaintext_url() -> Option<String> {
    variable("OXIDEBATCH_SECURITY_PLAINTEXT_TEST_URL")
}

/// Reads one environment variable, treating an empty value as absent.
#[must_use]
pub fn variable(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

/// Builds the supported production configuration for one connection.
///
/// This is the campaign's whole point of contact with the support contract:
/// `PostgresConfig` with the default TLS mode, which is `verify-full`. Nothing
/// here selects a mode, and no report may. A campaign that had to name a
/// "production mode" to obtain one would be reporting on an option rather than
/// on what an operator gets.
///
/// # Errors
///
/// Returns the configuration failure when the URL or a timeout is rejected.
pub fn supported_config(
    url: String,
    ca_certificate: Option<CaCertificate>,
) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?
        .with_tls_mode(TlsMode::VerifyFull { ca_certificate })
        .with_connect_timeout(Duration::from_secs(20))?
        .with_statement_timeout(Duration::from_mins(2))?
        .with_lock_timeout(Duration::from_mins(2))?)
}

/// Builds the configuration a fixture step uses, with transport left explicit.
///
/// Fixture work — creating a database, provisioning roles, seeding rows — is
/// not what either report is about, and it runs against the same server over
/// whatever transport the fixture URL already implies.
///
/// # Errors
///
/// Returns the configuration failure when the URL or a timeout is rejected.
pub fn fixture_config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?
        .with_tls_mode(TlsMode::Plaintext)
        .with_statement_timeout(Duration::from_mins(2))?
        .with_lock_timeout(Duration::from_mins(2))?)
}

/// Reads a PEM certificate authority bundle off the fixture.
///
/// # Errors
///
/// Returns the failure when the bundle cannot be read or is not accepted.
pub fn read_ca(path: &Path) -> Result<CaCertificate, Box<dyn Error>> {
    let pem = fs::read(path)
        .map_err(|error| Failure(format!("could not read {}: {error}", path.display())))?;
    Ok(CaCertificate::new(pem)?)
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

/// Replaces the host in a connection URL, keeping port, credentials, and path.
///
/// The TLS report reaches one server under two names, and everything except the
/// name must stay identical for the difference between the two attempts to be
/// the name. Bracketed IPv6 literals are not handled, and the fixture does not
/// supply one.
///
/// # Errors
///
/// Returns the failure when the URL carries no scheme and authority.
pub fn with_host(url: &str, host: &str) -> Result<String, Box<dyn Error>> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| Failure(format!("{url} is not a connection URL")))?;
    let (authority, tail) = match rest.find(['/', '?']) {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, ""),
    };
    let (credentials, endpoint) = match authority.rsplit_once('@') {
        Some((credentials, endpoint)) => (Some(credentials), endpoint),
        None => (None, authority),
    };
    let port = endpoint
        .rsplit_once(':')
        .map(|(_, port)| port)
        .filter(|port| !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()));

    let mut replaced = String::from(scheme);
    replaced.push_str("://");
    if let Some(credentials) = credentials {
        replaced.push_str(credentials);
        replaced.push('@');
    }
    replaced.push_str(host);
    if let Some(port) = port {
        replaced.push(':');
        replaced.push_str(port);
    }
    replaced.push_str(tail);
    Ok(replaced)
}

/// Replaces the role and password in a connection URL.
///
/// # Errors
///
/// Returns the failure when the URL carries no scheme and authority.
pub fn with_role(url: &str, role: &str, password: &str) -> Result<String, Box<dyn Error>> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| Failure(format!("{url} is not a connection URL")))?;
    let (authority, tail) = match rest.find(['/', '?']) {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, ""),
    };
    let endpoint = authority
        .rsplit_once('@')
        .map_or(authority, |(_, endpoint)| endpoint);
    Ok(format!("{scheme}://{role}:{password}@{endpoint}{tail}"))
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

/// Runs one statement that carries no caller-supplied text.
///
/// # Errors
///
/// Returns the database failure the statement produced.
pub async fn run_statement(url: &str, statement: String) -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let outcome = sqlx::query(AssertSqlSafe(statement)).execute(&pool).await;
    pool.close().await;
    outcome?;
    Ok(())
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

/// Returns the major version of a `PostgreSQL` server version string.
///
/// The campaign runs on a supported-version matrix, and two reports from two
/// matrix points are otherwise indistinguishable in the retained evidence.
#[must_use]
pub fn major_version(server: &str) -> String {
    server.split(['.', ' ']).next().unwrap_or(server).to_owned()
}

/// Returns the campaign's committed fixture directory.
#[must_use]
pub fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests")
        .join("fixtures")
        .join("security")
}

/// Retains one report's observation where `cargo xtask security` will read it.
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

/// A clock pinned to one instant so nothing a report reads depends on time.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub SystemTime);

impl oxide_batch::Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// A report failure that is not a database failure.
#[derive(Debug)]
pub struct Failure(pub String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
