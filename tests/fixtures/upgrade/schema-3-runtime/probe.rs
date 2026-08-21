//! Asks a schema-3 OxideBatch runtime to open a schema-4 database.
//!
//! This file is a fixture of the M5 upgrade campaign rather than a member of
//! this workspace. The rejection report checks out the last revision whose
//! runtime supported schema 3 (the commit immediately before M6 `#144`'s
//! `0005_item_stream_component_state.sql` added schema 4), copies this
//! program into it as an example, and runs it against a database the current
//! migrator has upgraded to schema 4. It is written here and compiled there
//! because the runtime under test cannot be built from the working tree — the
//! supported schema version is a constant of the crate, and a runtime that
//! reports `3` is the one that shipped against schema 3, not a reconstruction
//! of it.
//!
//! It therefore uses only the public API that revision exposed: no item added
//! afterwards, and nothing that reaches inside the adapter. What it reports is
//! three facts the report requires and cannot obtain any other way — the schema
//! version that runtime supports, what its repository startup did with a
//! database at schema 4, and what its migrator did with one.
//!
//! The migrator is asked as well as the runtime because an operator rolling a
//! deployment back runs both. A schema-3 migrator that treated a schema-4
//! database as having pending work would rewrite it downwards, and refusing is
//! the only safe answer.
//!
//! The outcome is printed on one line behind a marker so the report can find it
//! among the compiler's own output, and the exit status is success only when
//! both attempts were refused for the stated reason.

use std::process::ExitCode;
use std::sync::Arc;

use oxide_batch::{
    PostgresConfig, PostgresJobRepository, PostgresMigrator, RepositoryError, SystemClock, TlsMode,
};
use serde_json::{Value, json};

/// The marker the report finds this program's one line of output by.
const MARKER: &str = "M5_SCHEMA3_PROBE";

/// The variable naming the schema-4 database to attempt.
const URL: &str = "OXIDEBATCH_PROBE_URL";

fn main() -> ExitCode {
    match probe() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{MARKER} could not run: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Attempts both startup paths and reports what this runtime did.
fn probe() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let url = std::env::var(URL)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let supported = PostgresMigrator::supported_schema_version();
    let (open, migrate) = runtime.block_on(async {
        let open = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            Arc::new(SystemClock),
        )
        .await
        .map(|_| ());
        let migrate = PostgresMigrator::migrate(
            &PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
        )
        .await;
        Ok::<_, Box<dyn std::error::Error>>((open, migrate))
    })?;

    let closed = refused(&open) && refused(&migrate);
    let document = json!({
        "supported_schema_version": supported,
        "repository_open": outcome(&open),
        "migrator_run": outcome(&migrate),
        "failed_closed": closed,
    });
    println!("{MARKER} {document}");

    Ok(if closed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Describes one attempt for the report, naming the schema versions involved.
fn outcome(result: &Result<(), RepositoryError>) -> Value {
    match result {
        Ok(()) => json!({"accepted": true}),
        Err(RepositoryError::NewerSchema { current, supported }) => json!({
            "accepted": false,
            "error": "NewerSchema",
            "observed_schema_version": current,
            "supported_schema_version": supported,
        }),
        Err(other) => json!({"accepted": false, "error": format!("{other:?}")}),
    }
}

/// Reports whether one attempt was refused as a newer schema.
///
/// Any other refusal is still a refusal, and is still reported, but it is not
/// the one the contract promises: a runtime that failed to connect at all would
/// look identical from the outside while proving nothing about schema checking.
fn refused(result: &Result<(), RepositoryError>) -> bool {
    matches!(result, Err(RepositoryError::NewerSchema { .. }))
}
