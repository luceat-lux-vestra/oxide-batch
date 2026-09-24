//! Schema-7 least-privilege evidence for durable repeat state.
//!
//! The long-lived M5 role matrix predates `ob_repeat_execution`.
//! Runtime may read, insert, and update bounded repeat state, but may not
//! directly delete it. Explorer, operator, and retention may read it through
//! their bounded read grants and must be refused every direct write.

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

const DATABASE: &str = "oxide_batch_m7_repeat_state_privileges";
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
        id: "runtime.read-repeat-state",
        role: RUNTIME_ROLE,
        operation: "read repeat state",
        statement: "SELECT step_execution_id FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "runtime.create-repeat-state",
        role: RUNTIME_ROLE,
        operation: "create repeat state",
        statement: "INSERT INTO oxide_batch.ob_repeat_execution \
                    SELECT * FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "runtime.update-repeat-state",
        role: RUNTIME_ROLE,
        operation: "update repeat state",
        statement: "UPDATE oxide_batch.ob_repeat_execution \
                    SET decision = decision WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "runtime.delete-repeat-state",
        role: RUNTIME_ROLE,
        operation: "delete repeat state directly",
        statement: "DELETE FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "explorer.read-repeat-state",
        role: EXPLORER_ROLE,
        operation: "read repeat state",
        statement: "SELECT step_execution_id FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "explorer.create-repeat-state",
        role: EXPLORER_ROLE,
        operation: "create repeat state",
        statement: "INSERT INTO oxide_batch.ob_repeat_execution \
                    SELECT * FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "explorer.update-repeat-state",
        role: EXPLORER_ROLE,
        operation: "update repeat state",
        statement: "UPDATE oxide_batch.ob_repeat_execution \
                    SET decision = decision WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "explorer.delete-repeat-state",
        role: EXPLORER_ROLE,
        operation: "delete repeat state directly",
        statement: "DELETE FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "operator.read-repeat-state",
        role: OPERATOR_ROLE,
        operation: "read repeat state",
        statement: "SELECT step_execution_id FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "operator.create-repeat-state",
        role: OPERATOR_ROLE,
        operation: "create repeat state",
        statement: "INSERT INTO oxide_batch.ob_repeat_execution \
                    SELECT * FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "operator.update-repeat-state",
        role: OPERATOR_ROLE,
        operation: "update repeat state",
        statement: "UPDATE oxide_batch.ob_repeat_execution \
                    SET decision = decision WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "operator.delete-repeat-state",
        role: OPERATOR_ROLE,
        operation: "delete repeat state directly",
        statement: "DELETE FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "retention.read-repeat-state",
        role: RETENTION_ROLE,
        operation: "read repeat state while planning retention",
        statement: "SELECT step_execution_id FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Allowed,
    },
    Probe {
        id: "retention.create-repeat-state",
        role: RETENTION_ROLE,
        operation: "create repeat state",
        statement: "INSERT INTO oxide_batch.ob_repeat_execution \
                    SELECT * FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "retention.update-repeat-state",
        role: RETENTION_ROLE,
        operation: "update repeat state",
        statement: "UPDATE oxide_batch.ob_repeat_execution \
                    SET decision = decision WHERE false",
        expected: Expected::Forbidden,
    },
    Probe {
        id: "retention.delete-repeat-state",
        role: RETENTION_ROLE,
        operation: "delete repeat state directly rather than through parent cascade",
        statement: "DELETE FROM oxide_batch.ob_repeat_execution WHERE false",
        expected: Expected::Forbidden,
    },
];

#[test]
fn repeat_state_privileges_match_schema7_policy() -> Result<(), Box<dyn Error>> {
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
fn declared_repeat_state_privilege_probes_match_scope_denominator() -> Result<(), Box<dyn Error>> {
    let source = std::fs::read_to_string(fixtures().join("campaign-scope.json"))?;
    let scope: Value = serde_json::from_str(&source)?;
    let denominator = scope
        .get("repeat_state_privileges")
        .ok_or("scope declares no repeat_state_privileges denominator")?;
    let cells = denominator
        .get("probes")
        .and_then(Value::as_array)
        .ok_or("repeat_state_privileges declares no probes")?;

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
            "repeat-state privilege report failed: {error}; cleanup also failed: {cleanup_error}"
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
        "repeat-state-privileges",
        &json!({
            "report": "schema-7 repeat-state least-privilege",
            "scenario": "repeat_state_privileges_match_schema7_policy",
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
