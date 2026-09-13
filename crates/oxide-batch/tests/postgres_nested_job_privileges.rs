//! Schema-5 least-privilege evidence for durable nested-job linkage.
//!
//! The long-lived M5 role matrix predates `ob_nested_job_link`; this report
//! exercises the schema-5 table itself under the committed roles and grants so
//! the new runtime-owned state is not covered only by policy prose. The runtime
//! must be able to read, create, and update linkage but not delete it. Explorer,
//! operator, and retention may read the linkage through their existing bounded
//! read grants, but direct writes and direct deletes must be refused. Parent
//! retention removes linkage only through the schema-owned parent FK cascade,
//! which is covered by `postgres_nested_job_parent_purge_cascades_link_without_deleting_child`.

#![cfg(feature = "postgres")]

mod security;

use std::collections::BTreeSet;
use std::error::Error;

use oxide_batch::PostgresMigrator;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};

use security::{
    Failure, INSUFFICIENT_PRIVILEGE, StatementOutcome, admin_url, apply_script, attempt_statement,
    drop_database, execution_manifest, fixture_config, fixtures, major_version, recreate_database,
    retain_observation, run_statement, server_version, with_database, with_role,
};

const DATABASE: &str = "oxide_batch_m7_nested_job_privileges";
const MIGRATION_ROLE: &str = "oxide_batch_m5_migration";
const RUNTIME_ROLE: &str = "oxide_batch_m5_runtime";
const EXPLORER_ROLE: &str = "oxide_batch_m5_explorer";
const OPERATOR_ROLE: &str = "oxide_batch_m5_operator";
const RETENTION_ROLE: &str = "oxide_batch_m5_retention";
const ROLES: [&str; 5] = [
    MIGRATION_ROLE,
    RUNTIME_ROLE,
    EXPLORER_ROLE,
    OPERATOR_ROLE,
    RETENTION_ROLE,
];

#[derive(Clone, Copy)]
enum Expected {
    Allowed,
    Forbidden,
}

impl Expected {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Forbidden => "forbidden",
        }
    }
}

struct Probe {
    id: &'static str,
    role: &'static str,
    operation: &'static str,
    statement: &'static str,
    expected: Expected,
}

const PROBES: &[Probe] = &[
    Probe {
        id: "runtime.read-nested-job-link",
        role: RUNTIME_ROLE,
        operation: "read nested-job linkage",
        statement: "SELECT id FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "runtime.create-nested-job-link",
        role: RUNTIME_ROLE,
        operation: "create nested-job linkage",
        statement: "INSERT INTO oxide_batch.ob_nested_job_link \
                    SELECT * FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "runtime.update-nested-job-link",
        role: RUNTIME_ROLE,
        operation: "update nested-job terminal observation",
        statement: "UPDATE oxide_batch.ob_nested_job_link \
                    SET terminal_observed_at = terminal_observed_at WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "runtime.delete-nested-job-link",
        role: RUNTIME_ROLE,
        operation: "delete nested-job linkage directly",
        statement: "DELETE FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "explorer.read-nested-job-link",
        role: EXPLORER_ROLE,
        operation: "read nested-job linkage",
        statement: "SELECT id FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "explorer.create-nested-job-link",
        role: EXPLORER_ROLE,
        operation: "create nested-job linkage",
        statement: "INSERT INTO oxide_batch.ob_nested_job_link \
                    SELECT * FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "explorer.update-nested-job-link",
        role: EXPLORER_ROLE,
        operation: "update nested-job terminal observation",
        statement: "UPDATE oxide_batch.ob_nested_job_link \
                    SET terminal_observed_at = terminal_observed_at WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "explorer.delete-nested-job-link",
        role: EXPLORER_ROLE,
        operation: "delete nested-job linkage directly",
        statement: "DELETE FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "operator.read-nested-job-link",
        role: OPERATOR_ROLE,
        operation: "read nested-job linkage",
        statement: "SELECT id FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "operator.create-nested-job-link",
        role: OPERATOR_ROLE,
        operation: "create nested-job linkage",
        statement: "INSERT INTO oxide_batch.ob_nested_job_link \
                    SELECT * FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "operator.update-nested-job-link",
        role: OPERATOR_ROLE,
        operation: "update nested-job terminal observation",
        statement: "UPDATE oxide_batch.ob_nested_job_link \
                    SET terminal_observed_at = terminal_observed_at WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "operator.delete-nested-job-link",
        role: OPERATOR_ROLE,
        operation: "delete nested-job linkage directly",
        statement: "DELETE FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "retention.read-nested-job-link",
        role: RETENTION_ROLE,
        operation: "read nested-job linkage while planning retention",
        statement: "SELECT id FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "retention.create-nested-job-link",
        role: RETENTION_ROLE,
        operation: "create nested-job linkage",
        statement: "INSERT INTO oxide_batch.ob_nested_job_link \
                    SELECT * FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "retention.update-nested-job-link",
        role: RETENTION_ROLE,
        operation: "update nested-job terminal observation",
        statement: "UPDATE oxide_batch.ob_nested_job_link \
                    SET terminal_observed_at = terminal_observed_at WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "retention.delete-nested-job-link",
        role: RETENTION_ROLE,
        operation: "delete nested-job linkage directly rather than through parent cascade",
        statement: "DELETE FROM oxide_batch.ob_nested_job_link WHERE false",
        expected: Expected::Forbidden,
    },
];

#[test]
fn nested_job_link_privileges_match_schema5_policy() -> Result<(), Box<dyn Error>> {
    let Some(admin) = admin_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_ADMIN_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run_report(&admin))
}

#[test]
fn declared_nested_job_privilege_probes_match_scope_denominator() -> Result<(), Box<dyn Error>> {
    let source = std::fs::read_to_string(fixtures().join("campaign-scope.json"))?;
    let scope: Value = serde_json::from_str(&source)?;
    let denominator = scope
        .get("nested_job_privileges")
        .ok_or("scope declares no nested_job_privileges denominator")?;
    let cells = denominator
        .get("probes")
        .and_then(Value::as_array)
        .ok_or("nested_job_privileges declares no probes")?;

    let identity =
        |value: (&str, &str, &str)| (value.0.to_owned(), value.1.to_owned(), value.2.to_owned());
    let declared = PROBES
        .iter()
        .map(|probe| identity((probe.id, probe.role, probe.expected.as_str())))
        .collect::<BTreeSet<_>>();
    let committed = cells
        .iter()
        .map(|probe| {
            Ok::<_, Box<dyn Error>>(identity((
                probe
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or("probe has no id")?,
                probe
                    .get("role")
                    .and_then(Value::as_str)
                    .ok_or("probe has no role")?,
                probe
                    .get("expected")
                    .and_then(Value::as_str)
                    .ok_or("probe has no expected")?,
            )))
        })
        .collect::<Result<BTreeSet<_>, Box<dyn Error>>>()?;

    assert_eq!(
        declared.len(),
        PROBES.len(),
        "probe identities must be unique"
    );
    assert_eq!(
        committed.len(),
        cells.len(),
        "scope probe identities must be unique"
    );
    assert_eq!(
        declared, committed,
        "source probes and scope denominator differ"
    );
    assert_eq!(
        denominator.get("total_probes").and_then(Value::as_u64),
        Some(PROBES.len() as u64),
    );
    assert_eq!(
        denominator.get("allowed_probes").and_then(Value::as_u64),
        Some(
            PROBES
                .iter()
                .filter(|probe| matches!(probe.expected, Expected::Allowed))
                .count() as u64
        ),
    );
    assert_eq!(
        denominator.get("forbidden_probes").and_then(Value::as_u64),
        Some(
            PROBES
                .iter()
                .filter(|probe| matches!(probe.expected, Expected::Forbidden))
                .count() as u64
        ),
    );

    Ok(())
}

async fn run_report(admin: &str) -> Result<(), Box<dyn Error>> {
    let database = with_database(admin, DATABASE)?;
    recreate_database(admin, DATABASE).await?;

    let result = run_report_in_database(admin, &database).await;
    let cleanup = drop_database(admin, DATABASE).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(Box::new(Failure(format!(
            "nested-job privilege report failed: {error}; cleanup also failed: {cleanup_error}"
        )))),
    }
}

async fn run_report_in_database(admin: &str, database: &str) -> Result<(), Box<dyn Error>> {
    apply_script(database, &fixtures().join("roles.sql")).await?;
    let password = disposable_password(database).await?;
    for role in ROLES {
        run_statement(database, format!("ALTER ROLE {role} PASSWORD '{password}'")).await?;
    }

    let migration = with_role(database, MIGRATION_ROLE, &password)?;
    PostgresMigrator::migrate(&fixture_config(migration.clone())?).await?;
    apply_script(&migration, &fixtures().join("grants.sql")).await?;

    let server = server_version(admin).await?;
    let mut matrix = Vec::with_capacity(PROBES.len());
    for probe in PROBES {
        let role_url = with_role(database, probe.role, &password)?;
        let outcome = attempt_statement(&role_url, probe.statement).await?;
        match probe.expected {
            Expected::Allowed => assert_eq!(
                outcome,
                StatementOutcome::Succeeded,
                "{} must be allowed to {} and PostgreSQL returned {}",
                probe.role,
                probe.operation,
                outcome.as_str(),
            ),
            Expected::Forbidden => assert_eq!(
                outcome.code(),
                Some(INSUFFICIENT_PRIVILEGE),
                "{} must be refused when it tries to {}; PostgreSQL returned {}",
                probe.role,
                probe.operation,
                outcome.as_str(),
            ),
        }
        matrix.push(probe_observation(probe, &outcome));
    }

    retain_observation(
        "nested-job-privileges",
        &json!({
            "report": "schema-5 nested-job least-privilege",
            "scenario": "nested_job_link_privileges_match_schema5_policy",
            "fixture": "postgres-security-roles",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "schema_version": PostgresMigrator::supported_schema_version(),
            "matrix": matrix,
            "violations": Vec::<String>::new(),
            "passed": true,
            "execution_manifest": execution_manifest()?,
        }),
    )?;

    Ok(())
}

fn probe_observation(probe: &Probe, outcome: &StatementOutcome) -> Value {
    json!({
        "id": probe.id,
        "role": probe.role,
        "operation": probe.operation,
        "expected": probe.expected.as_str(),
        "observed": match outcome {
            StatementOutcome::Succeeded => "succeeded",
            StatementOutcome::Refused(_) => "refused",
        },
        "error_class": outcome.code(),
        "passed": true,
    })
}

async fn disposable_password(database: &str) -> Result<String, Box<dyn Error>> {
    let mut connection = PgConnection::connect(database).await?;
    let password = sqlx::query_scalar("SELECT gen_random_uuid()::text")
        .fetch_one(&mut connection)
        .await?;
    connection.close().await?;
    Ok(password)
}
