//! Executable evidence for the M2 `PostgreSQL` operational design gate.

#![allow(clippy::expect_used, clippy::panic)]

use std::path::Path;
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgConnection, PgSslMode};
use sqlx::{Connection, Executor};

fn fixture_options(variable: &str) -> Option<PgConnectOptions> {
    let database_url = std::env::var(variable).ok()?;
    let root_certificate =
        std::env::var("OXIDEBATCH_DESIGN_GATE_TLS_ROOT").expect("TLS root path must be set");

    Some(
        PgConnectOptions::from_str(&database_url)
            .expect("fixture URL must parse")
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert(Path::new(&root_certificate)),
    )
}

fn database_code(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(database) => database.code().map(std::borrow::Cow::into_owned),
        _ => None,
    }
}

#[tokio::test]
async fn rustls_verify_full_runtime_role_has_dml_but_not_ddl() {
    let Some(options) = fixture_options("OXIDEBATCH_DESIGN_GATE_RUNTIME_URL") else {
        eprintln!("skipped: M2 PostgreSQL design-gate fixture is not running");
        return;
    };
    let mut connection = PgConnection::connect_with(&options)
        .await
        .expect("runtime must connect with validated TLS");

    let (role, tls): (String, bool) = sqlx::query_as(
        "SELECT current_user, ssl \
         FROM pg_stat_ssl \
         WHERE pid = pg_backend_pid()",
    )
    .fetch_one(&mut connection)
    .await
    .expect("TLS session state must be inspectable");
    assert_eq!(role, "oxide_batch_runtime");
    assert!(tls);

    let schema_version: i32 = sqlx::query_scalar(
        "SELECT version \
         FROM oxide_batch.ob_schema_version \
         WHERE singleton = true",
    )
    .fetch_one(&mut connection)
    .await
    .expect("runtime must read schema version");
    assert_eq!(schema_version, 1);

    let updated = sqlx::query(
        "UPDATE oxide_batch.ob_step_execution \
         SET updated_at = CURRENT_TIMESTAMP, version = version + 1 \
         WHERE step_name = 'import' AND version = 1",
    )
    .execute(&mut connection)
    .await
    .expect("runtime must update metadata");
    assert_eq!(updated.rows_affected(), 1);

    let ddl = connection
        .execute("CREATE TABLE oxide_batch.runtime_must_not_create (id integer)")
        .await
        .expect_err("runtime DDL must be denied");
    assert_eq!(database_code(&ddl).as_deref(), Some("42501"));
}

#[tokio::test]
async fn operator_reader_is_tls_validated_and_read_only() {
    let Some(options) = fixture_options("OXIDEBATCH_DESIGN_GATE_READER_URL") else {
        eprintln!("skipped: M2 PostgreSQL design-gate fixture is not running");
        return;
    };
    let mut connection = PgConnection::connect_with(&options)
        .await
        .expect("operator reader must connect with validated TLS");

    let (role, tls): (String, bool) = sqlx::query_as(
        "SELECT current_user, ssl \
         FROM pg_stat_ssl \
         WHERE pid = pg_backend_pid()",
    )
    .fetch_one(&mut connection)
    .await
    .expect("TLS session state must be inspectable");
    assert_eq!(role, "oxide_batch_operator_reader");
    assert!(tls);

    let executions: i64 = sqlx::query_scalar("SELECT count(*) FROM oxide_batch.ob_job_execution")
        .fetch_one(&mut connection)
        .await
        .expect("operator reader must inspect metadata");
    assert_eq!(executions, 1);

    let write = connection
        .execute(
            "UPDATE oxide_batch.ob_job_execution \
             SET updated_at = CURRENT_TIMESTAMP",
        )
        .await
        .expect_err("operator reader writes must be denied");
    assert_eq!(database_code(&write).as_deref(), Some("42501"));
}
