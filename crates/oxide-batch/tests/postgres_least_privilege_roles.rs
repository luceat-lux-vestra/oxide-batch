//! Least-privilege separation across the five M5 privilege classes.
//!
//! The M5 preview separates migration, runtime, explorer, operator, and
//! retention. Separation is a claim about what each identity *cannot* do, and
//! the only thing that can settle it is a real database refusing a real
//! statement, so that is what this report collects.
//!
//! It builds a database from nothing, provisions the five classes from the
//! committed policy in `tests/fixtures/security/roles.sql` and
//! `tests/fixtures/security/grants.sql`, migrates it to schema 3 as the
//! migration class, and then fills in a matrix. Every cell is one class
//! attempting one operation. An allowed cell must succeed; a forbidden cell
//! must be refused by the server under `42501`, the code it uses for want of
//! privilege and no other.
//!
//! Requiring that exact code matters more than it looks. An `INSERT` a class
//! may not perform and an `INSERT` that violates a constraint both fail, and a
//! matrix that only asked whether the statement failed would pass just as
//! happily when the privilege was wide open. For the same reason no forbidden
//! statement can change anything if it unexpectedly succeeds: the destructive
//! ones carry `WHERE false`, and the inserting ones select no rows. A cell that
//! passes and a cell that fails leave the database identical.
//!
//! The allowed side is not made of `has_table_privilege` lookups. Each class
//! does its real work through the path an operator would use — the migrator
//! migrates, the runtime creates an execution graph through the repository, the
//! explorer answers through `JobExplorer`, the operator stops an execution
//! through `JobOperator`, and retention places and releases a hold and plans a
//! purge through `RetentionService` — so a grant that is present but unusable
//! fails the report.
//!
//! Three things hold across all five classes and are checked once, because a
//! matrix of grants proves nothing if the classes can escape it. No class holds
//! a cluster-level privilege, read from `pg_roles` rather than assumed from the
//! script that created them. Nothing reaches any class through `PUBLIC`. And
//! the migration bookkeeping table is readable by none of them, because a class
//! that could rewrite it could tell a runtime it was looking at a different
//! schema.
//!
//! The passwords the classes log in with are generated per run and appear
//! nowhere in the retained evidence, which records the class, the operation,
//! what was expected, what the server did, and the code it did it under.

#![cfg(feature = "postgres")]

mod security;

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    ActorRef, BatchStatus, JobExplorer, JobInstanceKey, JobName, JobOperator, JobParameter,
    JobParameters, JobRepository, LifecycleTransition, OperationId, OperatorRequest, ParameterName,
    ParameterRole, ParameterValue, PostgresExplorer, PostgresJobRepository, PostgresMigrator,
    PurgeBatchBound, PurgePlanRequest, ReasonCode, RetentionService, StepName, TerminalStatusSet,
};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

use security::{
    Failure, FixedClock, INSUFFICIENT_PRIVILEGE, StatementOutcome, admin_url, apply_script,
    attempt_statement, execution_manifest, fixture_config, fixtures, major_version,
    recreate_database, retain_observation, run_statement, server_version, with_database, with_role,
};

/// The database this report builds and reports on.
const DATABASE: &str = "oxide_batch_m5_security_roles";

/// The job every seeded row belongs to.
const FIXTURE_JOB: &str = "m5_security_roles";

/// The instant the report pins its clock to.
///
/// It is far enough past the epoch for a purge plan to look backwards from it.
/// A clock at the epoch cannot subtract the minimum age a survey is bounded by,
/// which is a property of the fixture rather than of the service.
fn at() -> SystemTime {
    UNIX_EPOCH + Duration::from_hours(20_000 * 24)
}

/// The smallest age a planned purge may survey under.
///
/// Planning is a read, and the report plans rather than applies, so the bound
/// only has to be one the service accepts.
const MINIMUM_PURGE_AGE: Duration = Duration::from_hours(1);

/// One of the five privilege classes the M5 preview separates.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum Class {
    /// Installs and upgrades the metadata schema.
    Migration,
    /// Drives the job, step, partition, and flow lifecycle.
    Runtime,
    /// Answers bounded read questions.
    Explorer,
    /// Records guarded, audited operator actions.
    Operator,
    /// Plans and applies purges, and places and releases holds.
    Retention,
}

/// Every class, in the order the design gate names them.
const CLASSES: [Class; 5] = [
    Class::Migration,
    Class::Runtime,
    Class::Explorer,
    Class::Operator,
    Class::Retention,
];

impl Class {
    /// Returns the stable name the retained evidence uses.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Migration => "migration",
            Self::Runtime => "runtime",
            Self::Explorer => "explorer",
            Self::Operator => "operator",
            Self::Retention => "retention",
        }
    }

    /// Returns the database role that carries the class.
    const fn role(self) -> &'static str {
        match self {
            Self::Migration => "oxide_batch_m5_migration",
            Self::Runtime => "oxide_batch_m5_runtime",
            Self::Explorer => "oxide_batch_m5_explorer",
            Self::Operator => "oxide_batch_m5_operator",
            Self::Retention => "oxide_batch_m5_retention",
        }
    }
}

/// What the policy requires of one operation.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Expected {
    /// The class's own work, which must succeed.
    Allowed,
    /// Beyond the class's boundary, which the server must refuse.
    Forbidden,
}

impl Expected {
    /// Returns the stable name the retained evidence uses.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Forbidden => "forbidden",
        }
    }
}

/// How one cell reached the database.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Surface {
    /// Through the service or adapter an operator would use.
    ServicePath,
    /// As one statement, run as the class.
    Statement,
}

impl Surface {
    /// Returns the stable name the retained evidence uses.
    const fn as_str(self) -> &'static str {
        match self {
            Self::ServicePath => "service-path",
            Self::Statement => "statement",
        }
    }
}

/// One statement a class must not be able to run.
struct Boundary {
    /// The stable identity the committed role-matrix denominator uses. Prose
    /// may be revised without moving this; the denominator moves only when
    /// this does.
    id: &'static str,
    /// The class attempting it.
    class: Class,
    /// What the attempt would amount to if it succeeded.
    operation: &'static str,
    /// The class whose work this is instead, or the reason it belongs to none.
    belongs_to: &'static str,
    /// The statement, which changes nothing even if the server allows it.
    statement: &'static str,
}

/// Every boundary the five classes must not cross.
///
/// Each statement is written so that it cannot alter the database: the
/// destructive ones match no row, and the inserting ones select none. What the
/// server is being asked is whether the class may perform the operation at all,
/// and that is decided before any row is touched.
const BOUNDARIES: &[Boundary] = &[
    // The migration class owns the schema, so its boundary is not inside it.
    // What must hold is that it cannot reach past the database it migrates.
    Boundary {
        id: "migration.create-login-role",
        class: Class::Migration,
        operation: "create a login role",
        belongs_to: "no class: role administration is outside the preview",
        statement: "CREATE ROLE oxide_batch_m5_escalated LOGIN",
    },
    Boundary {
        id: "migration.create-database",
        class: Class::Migration,
        operation: "create a database",
        belongs_to: "no class: database administration is outside the preview",
        statement: "CREATE DATABASE oxide_batch_m5_escalated",
    },
    Boundary {
        id: "migration.read-cluster-credentials",
        class: Class::Migration,
        operation: "read the cluster's stored credentials",
        belongs_to: "no class: only a superuser may read pg_authid",
        statement: "SELECT rolname FROM pg_authid WHERE false",
    },
    Boundary {
        id: "migration.execute-program-on-server",
        class: Class::Migration,
        operation: "execute a program on the server",
        belongs_to: "no class: server-side execution is outside the preview",
        statement: "COPY (SELECT 1) TO PROGRAM 'true'",
    },
    // The runtime drives the lifecycle and may do nothing else to the schema.
    Boundary {
        id: "runtime.add-table-to-metadata-schema",
        class: Class::Runtime,
        operation: "add a table to the metadata schema",
        belongs_to: "migration",
        statement: "CREATE TABLE oxide_batch.ob_m5_runtime_probe (id integer)",
    },
    Boundary {
        id: "runtime.drop-metadata-table",
        class: Class::Runtime,
        operation: "drop a metadata table",
        belongs_to: "migration",
        statement: "DROP TABLE oxide_batch.ob_flow_decision",
    },
    Boundary {
        id: "runtime.rewrite-schema-version",
        class: Class::Runtime,
        operation: "rewrite the recorded schema version",
        belongs_to: "migration",
        statement: "UPDATE oxide_batch.ob_schema_version SET version = 99 WHERE false",
    },
    Boundary {
        id: "runtime.read-migration-bookkeeping",
        class: Class::Runtime,
        operation: "read the migration bookkeeping",
        belongs_to: "migration",
        statement: "SELECT version FROM oxide_batch._sqlx_migrations WHERE false",
    },
    Boundary {
        id: "runtime.record-recovery-decision",
        class: Class::Runtime,
        operation: "record a recovery decision",
        belongs_to: "operator",
        statement: "INSERT INTO oxide_batch.ob_recovery_decision \
                    SELECT * FROM oxide_batch.ob_recovery_decision WHERE false",
    },
    Boundary {
        id: "runtime.record-retention-action",
        class: Class::Runtime,
        operation: "record a retention action",
        belongs_to: "retention",
        statement: "INSERT INTO oxide_batch.ob_retention_action \
                    SELECT * FROM oxide_batch.ob_retention_action WHERE false",
    },
    Boundary {
        id: "runtime.delete-job-execution",
        class: Class::Runtime,
        operation: "delete a job execution",
        belongs_to: "retention",
        statement: "DELETE FROM oxide_batch.ob_job_execution WHERE false",
    },
    // The runtime may lock an instance row and write the identity columns it
    // created. The hold columns on the same table are retention's, and the
    // column-level split is what keeps that true.
    Boundary {
        id: "runtime.place-retention-hold",
        class: Class::Runtime,
        operation: "place a retention hold",
        belongs_to: "retention",
        statement: "UPDATE oxide_batch.ob_job_instance SET hold_actor = 'probe' WHERE false",
    },
    // The explorer reads and does nothing else at all.
    Boundary {
        id: "explorer.create-job-instance",
        class: Class::Explorer,
        operation: "create a job instance",
        belongs_to: "runtime",
        statement: "INSERT INTO oxide_batch.ob_job_instance \
                    SELECT * FROM oxide_batch.ob_job_instance WHERE false",
    },
    Boundary {
        id: "explorer.move-execution-status",
        class: Class::Explorer,
        operation: "move an execution's status",
        belongs_to: "runtime and operator",
        statement: "UPDATE oxide_batch.ob_job_execution SET status = 'STOPPED' WHERE false",
    },
    Boundary {
        id: "explorer.place-retention-hold",
        class: Class::Explorer,
        operation: "place a retention hold",
        belongs_to: "retention",
        statement: "UPDATE oxide_batch.ob_job_instance SET hold_actor = 'probe' WHERE false",
    },
    Boundary {
        id: "explorer.delete-flow-decision",
        class: Class::Explorer,
        operation: "delete a flow decision",
        belongs_to: "retention",
        statement: "DELETE FROM oxide_batch.ob_flow_decision WHERE false",
    },
    Boundary {
        id: "explorer.add-table-to-metadata-schema",
        class: Class::Explorer,
        operation: "add a table to the metadata schema",
        belongs_to: "migration",
        statement: "CREATE TABLE oxide_batch.ob_m5_explorer_probe (id integer)",
    },
    Boundary {
        id: "explorer.read-migration-bookkeeping",
        class: Class::Explorer,
        operation: "read the migration bookkeeping",
        belongs_to: "migration",
        statement: "SELECT version FROM oxide_batch._sqlx_migrations WHERE false",
    },
    // The operator records guarded decisions. It resolves executions; it does
    // not run them, own them, or remove them.
    Boundary {
        id: "operator.claim-ownership-of-execution",
        class: Class::Operator,
        operation: "claim ownership of a live execution",
        belongs_to: "runtime",
        statement: "UPDATE oxide_batch.ob_job_execution SET owner_token = NULL WHERE false",
    },
    Boundary {
        id: "operator.create-step-execution",
        class: Class::Operator,
        operation: "create a step execution",
        belongs_to: "runtime",
        statement: "INSERT INTO oxide_batch.ob_step_execution \
                    SELECT * FROM oxide_batch.ob_step_execution WHERE false",
    },
    Boundary {
        id: "operator.delete-job-execution",
        class: Class::Operator,
        operation: "delete a job execution",
        belongs_to: "retention",
        statement: "DELETE FROM oxide_batch.ob_job_execution WHERE false",
    },
    Boundary {
        id: "operator.record-retention-action",
        class: Class::Operator,
        operation: "record a retention action",
        belongs_to: "retention",
        statement: "INSERT INTO oxide_batch.ob_retention_action \
                    SELECT * FROM oxide_batch.ob_retention_action WHERE false",
    },
    Boundary {
        id: "operator.add-table-to-metadata-schema",
        class: Class::Operator,
        operation: "add a table to the metadata schema",
        belongs_to: "migration",
        statement: "CREATE TABLE oxide_batch.ob_m5_operator_probe (id integer)",
    },
    Boundary {
        id: "operator.read-migration-bookkeeping",
        class: Class::Operator,
        operation: "read the migration bookkeeping",
        belongs_to: "migration",
        statement: "SELECT version FROM oxide_batch._sqlx_migrations WHERE false",
    },
    // Retention removes history and holds instances. It does not run jobs and
    // does not decide operator questions.
    Boundary {
        id: "retention.create-step-execution",
        class: Class::Retention,
        operation: "create a step execution",
        belongs_to: "runtime",
        statement: "INSERT INTO oxide_batch.ob_step_execution \
                    SELECT * FROM oxide_batch.ob_step_execution WHERE false",
    },
    Boundary {
        id: "retention.move-execution-status",
        class: Class::Retention,
        operation: "move an execution's status",
        belongs_to: "runtime and operator",
        statement: "UPDATE oxide_batch.ob_job_execution SET status = 'STOPPED' WHERE false",
    },
    Boundary {
        id: "retention.record-operator-request",
        class: Class::Retention,
        operation: "record an operator request",
        belongs_to: "operator",
        statement: "INSERT INTO oxide_batch.ob_operator_request \
                    SELECT * FROM oxide_batch.ob_operator_request WHERE false",
    },
    Boundary {
        id: "retention.rewrite-instance-identity",
        class: Class::Retention,
        operation: "rewrite an instance's identity",
        belongs_to: "runtime",
        statement: "UPDATE oxide_batch.ob_job_instance SET job_name = 'renamed' WHERE false",
    },
    Boundary {
        id: "retention.add-table-to-metadata-schema",
        class: Class::Retention,
        operation: "add a table to the metadata schema",
        belongs_to: "migration",
        statement: "CREATE TABLE oxide_batch.ob_m5_retention_probe (id integer)",
    },
    Boundary {
        id: "retention.read-migration-bookkeeping",
        class: Class::Retention,
        operation: "read the migration bookkeeping",
        belongs_to: "migration",
        statement: "SELECT version FROM oxide_batch._sqlx_migrations WHERE false",
    },
];

/// One statement a class must be able to run, beside its service path.
struct Permitted {
    /// The stable identity the committed role-matrix denominator uses.
    id: &'static str,
    /// The class attempting it.
    class: Class,
    /// What the attempt amounts to.
    operation: &'static str,
    /// The statement, which changes nothing.
    statement: &'static str,
}

/// The statement-level privileges each class must hold.
///
/// These sit beside the service paths rather than replacing them. Two of them
/// exist to make a boundary legible as a boundary: the runtime may set an
/// execution's owner token and the operator may not, and retention may delete a
/// flow decision where every other class may not, so the same statement appears
/// on both sides of the matrix under different classes.
const PERMITTED: &[Permitted] = &[
    Permitted {
        id: "runtime.claim-ownership-of-execution",
        class: Class::Runtime,
        operation: "claim ownership of an execution",
        statement: "UPDATE oxide_batch.ob_job_execution SET owner_token = NULL WHERE false",
    },
    Permitted {
        id: "retention.delete-flow-decision",
        class: Class::Retention,
        operation: "delete a flow decision",
        statement: "DELETE FROM oxide_batch.ob_flow_decision WHERE false",
    },
    Permitted {
        id: "retention.place-retention-hold",
        class: Class::Retention,
        operation: "place a retention hold",
        statement: "UPDATE oxide_batch.ob_job_instance SET hold_actor = 'probe' WHERE false",
    },
    Permitted {
        id: "operator.ask-execution-to-stop",
        class: Class::Operator,
        operation: "ask an execution to stop",
        statement: "UPDATE oxide_batch.ob_job_execution SET stop_requested_by = 'probe' \
                    WHERE false",
    },
];

/// One cell proved through the path an operator would use rather than by one
/// statement.
struct ServicePathCell {
    /// The stable identity the committed role-matrix denominator uses.
    id: &'static str,
    /// The class exercising its service.
    class: Class,
    /// What the service call amounts to.
    operation: &'static str,
}

/// The one service-path cell each class contributes.
///
/// Looked up by [`service_cell`] rather than written inline at each call
/// site, so the identity, class, and prose a function reports are the same
/// values the role-matrix denominator is reconciled against — a call site
/// cannot silently drift from what is declared here.
const SERVICE_PATH_CELLS: &[ServicePathCell] = &[
    ServicePathCell {
        id: "runtime.service-path",
        class: Class::Runtime,
        operation: "create a job instance, execution, and step execution and move the step to \
                    STARTED through JobRepository",
    },
    ServicePathCell {
        id: "explorer.service-path",
        class: Class::Explorer,
        operation: "project one execution and page its step executions through JobExplorer",
    },
    ServicePathCell {
        id: "operator.service-path",
        class: Class::Operator,
        operation: "apply a guarded, audited stop through JobOperator, recording the request and \
                    the execution's stop columns",
    },
    ServicePathCell {
        id: "retention.service-path",
        class: Class::Retention,
        operation: "place and release an audited hold and plan a purge through RetentionService",
    },
    ServicePathCell {
        id: "migration.service-path",
        class: Class::Migration,
        operation: "apply the shipped migrator and report the installed schema version",
    },
];

/// One allowed statement-level cell beside [`PERMITTED`] and the service
/// paths.
struct ExtraAllowedCell {
    /// The stable identity the committed role-matrix denominator uses.
    id: &'static str,
    /// The class attempting it.
    class: Class,
    /// What the attempt amounts to.
    operation: &'static str,
}

/// The migration class's one statement-level allowed cell.
///
/// It owns the schema its service path installs, so it must also be able to
/// add and remove an object inside it — a claim the migrator's own
/// installation step does not exercise. Looked up by
/// [`extra_allowed_cell`] for the same reason [`SERVICE_PATH_CELLS`] is
/// looked up rather than written inline.
const EXTRA_ALLOWED_CELLS: &[ExtraAllowedCell] = &[ExtraAllowedCell {
    id: "migration.add-remove-table-in-owned-schema",
    class: Class::Migration,
    operation: "add and remove a table in the metadata schema it owns",
}];

/// Every cell identity [`BOUNDARIES`], [`PERMITTED`], [`SERVICE_PATH_CELLS`],
/// and [`EXTRA_ALLOWED_CELLS`] declare, as the committed role-matrix
/// denominator records them.
///
/// This is the producer half of the producer/denominator reconciliation: it
/// is derived from the same tables the report actually attempts against the
/// database, so it cannot drift from what the report does without also
/// changing what this function returns.
fn declared_cell_identities() -> Vec<Value> {
    let mut identities = Vec::new();
    for boundary in BOUNDARIES {
        identities.push(json!({
            "id": boundary.id,
            "class": boundary.class.as_str(),
            "surface": Surface::Statement.as_str(),
            "expected": Expected::Forbidden.as_str(),
        }));
    }
    for permitted in PERMITTED {
        identities.push(json!({
            "id": permitted.id,
            "class": permitted.class.as_str(),
            "surface": Surface::Statement.as_str(),
            "expected": Expected::Allowed.as_str(),
        }));
    }
    for service in SERVICE_PATH_CELLS {
        identities.push(json!({
            "id": service.id,
            "class": service.class.as_str(),
            "surface": Surface::ServicePath.as_str(),
            "expected": Expected::Allowed.as_str(),
        }));
    }
    for extra in EXTRA_ALLOWED_CELLS {
        identities.push(json!({
            "id": extra.id,
            "class": extra.class.as_str(),
            "surface": Surface::Statement.as_str(),
            "expected": Expected::Allowed.as_str(),
        }));
    }
    identities
}

#[test]
fn least_privilege_role_cannot_exceed_its_class() -> Result<(), Box<dyn Error>> {
    let Some(admin) = admin_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_ADMIN_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_report(&admin))
}

/// Provisions the classes, exercises each one, and reports the whole matrix.
#[allow(
    clippy::too_many_lines,
    reason = "provisioning the classes, migrating as one of them, exercising each through its \
              own service, and filling in both halves of the matrix form one report that is \
              only meaningful in order"
)]
async fn run_report(admin: &str) -> Result<(), Box<dyn Error>> {
    let server = server_version(admin).await?;
    let database = with_database(admin, DATABASE)?;
    recreate_database(admin, DATABASE).await?;

    // The policy is applied as an administrative identity, once, from the
    // committed script. Nothing below adds a grant.
    apply_script(&database, &fixtures().join("roles.sql")).await?;
    let password = disposable_password();
    for class in CLASSES {
        // The password is generated for this run and is never retained. The
        // statement is built here rather than committed for that reason.
        run_statement(
            &database,
            format!("ALTER ROLE {} PASSWORD '{password}'", class.role()),
        )
        .await?;
    }
    let url = |class: Class| with_role(&database, class.role(), &password);

    // Schema 3, installed by the migration class through the shipped migrator.
    let migration = url(Class::Migration)?;
    PostgresMigrator::migrate(&fixture_config(migration.clone())?).await?;
    apply_script(&migration, &fixtures().join("grants.sql")).await?;
    let attributes = class_attributes(&database).await?;
    let public_grants = public_grants(&database).await?;
    assert!(
        public_grants.is_empty(),
        "PUBLIC still reaches {public_grants:?} in the metadata schema, so no grant below is a \
         boundary",
    );
    let mut cells = Vec::new();
    let seeded = seed_through_runtime(&url(Class::Runtime)?, &mut cells).await?;
    explore_through_service(&url(Class::Explorer)?, &seeded, &mut cells).await?;
    operate_through_service(&url(Class::Operator)?, &seeded, &mut cells).await?;
    retain_through_service(&url(Class::Retention)?, &seeded, &mut cells).await?;
    migrate_through_service(&migration, &mut cells).await?;

    for permitted in PERMITTED {
        let outcome = attempt_statement(&url(permitted.class)?, permitted.statement).await?;
        assert_eq!(
            outcome,
            StatementOutcome::Succeeded,
            "the {} class must be able to {} and the server refused it under {}",
            permitted.class.as_str(),
            permitted.operation,
            outcome.as_str(),
        );
        cells.push(cell(
            permitted.id,
            permitted.class,
            permitted.operation,
            Surface::Statement,
            Expected::Allowed,
            &outcome,
            None,
        ));
    }

    for boundary in BOUNDARIES {
        let outcome = attempt_statement(&url(boundary.class)?, boundary.statement).await?;
        assert_eq!(
            outcome.code(),
            Some(INSUFFICIENT_PRIVILEGE),
            "the {} class must not be able to {}, which belongs to {}, and the server answered \
             {} instead of refusing it for want of privilege",
            boundary.class.as_str(),
            boundary.operation,
            boundary.belongs_to,
            outcome.as_str(),
        );
        cells.push(cell(
            boundary.id,
            boundary.class,
            boundary.operation,
            Surface::Statement,
            Expected::Forbidden,
            &outcome,
            Some(boundary.belongs_to),
        ));
    }

    // Every class must appear on both sides of the matrix. A class that only
    // ever succeeded would say nothing about separation, and one that only ever
    // failed would say nothing about being usable.
    for class in CLASSES {
        for expected in [Expected::Allowed, Expected::Forbidden] {
            assert!(
                cells.iter().any(|entry| {
                    entry.get("class").and_then(Value::as_str) == Some(class.as_str())
                        && entry.get("expected").and_then(Value::as_str) == Some(expected.as_str())
                }),
                "the matrix records no {} operation for the {} class",
                expected.as_str(),
                class.as_str(),
            );
        }
        assert!(
            cells.iter().any(|entry| {
                entry.get("class").and_then(Value::as_str) == Some(class.as_str())
                    && entry.get("surface").and_then(Value::as_str)
                        == Some(Surface::ServicePath.as_str())
            }),
            "the {} class proved nothing through the path an operator would use",
            class.as_str(),
        );
    }

    retain_observation(
        "least-privilege-roles",
        &json!({
            "report": "least-privilege separation across the five M5 classes",
            "scenario": "least_privilege_role_cannot_exceed_its_class",
            "fixture": "postgres-security-roles",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "schema_version": PostgresMigrator::supported_schema_version(),
            "policy": [
                "tests/fixtures/security/roles.sql",
                "tests/fixtures/security/grants.sql",
            ],
            "classes": CLASSES.map(|class| json!({
                "class": class.as_str(),
                "role": class.role(),
            })),
            "class_attributes": attributes,
            "public_grants": public_grants,
            "matrix": cells,
            "violations": Vec::<String>::new(),
            "passed": true,
            "execution_manifest": execution_manifest()?,
        }),
    )?;

    Ok(())
}

/// Renders one matrix cell.
fn cell(
    id: &str,
    class: Class,
    operation: &str,
    surface: Surface,
    expected: Expected,
    observed: &StatementOutcome,
    belongs_to: Option<&str>,
) -> Value {
    json!({
        "id": id,
        "class": class.as_str(),
        "role": class.role(),
        "operation": operation,
        "surface": surface.as_str(),
        "expected": expected.as_str(),
        "observed": match observed {
            StatementOutcome::Succeeded => "succeeded",
            StatementOutcome::Refused(_) => "refused",
        },
        "error_class": observed.code(),
        "belongs_to": belongs_to,
        "passed": true,
    })
}

/// Renders the one declared service-path cell for `id`.
///
/// Looked up from [`SERVICE_PATH_CELLS`] rather than taking a class and
/// prose directly, so a call site cannot report a class or an operation the
/// committed role-matrix denominator does not also declare for that
/// identity.
///
/// # Panics
///
/// Panics when `id` names no declared service-path cell, which is a defect in
/// this file rather than a database result.
///
/// # Errors
///
/// Returns the failure when `id` names no declared service-path cell, which
/// is a defect in this file rather than a database result.
fn service_cell(id: &str) -> Result<Value, Box<dyn Error>> {
    let declared = SERVICE_PATH_CELLS
        .iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| Failure(format!("{id} is not a declared service-path cell")))?;
    Ok(cell(
        declared.id,
        declared.class,
        declared.operation,
        Surface::ServicePath,
        Expected::Allowed,
        &StatementOutcome::Succeeded,
        None,
    ))
}

/// Renders the one declared extra allowed cell for `id`.
///
/// See [`service_cell`]: looked up from [`EXTRA_ALLOWED_CELLS`] for the same
/// reason.
///
/// # Errors
///
/// Returns the failure when `id` names no declared extra allowed cell.
fn extra_allowed_cell(id: &str) -> Result<Value, Box<dyn Error>> {
    let declared = EXTRA_ALLOWED_CELLS
        .iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| Failure(format!("{id} is not a declared extra allowed cell")))?;
    Ok(cell(
        declared.id,
        declared.class,
        declared.operation,
        Surface::Statement,
        Expected::Allowed,
        &StatementOutcome::Succeeded,
        None,
    ))
}

/// What the runtime class created, for the classes that act on it afterwards.
struct Seeded {
    /// The instance the retention class holds.
    instance: oxide_batch::JobInstanceId,
    /// The execution the operator class stops.
    execution: oxide_batch::JobExecutionId,
    /// That execution's version, for the operator's compare-and-swap.
    version: oxide_batch::ExecutionVersion,
}

/// Creates an execution graph as the runtime class, through the repository.
async fn seed_through_runtime(url: &str, cells: &mut Vec<Value>) -> Result<Seeded, Box<dyn Error>> {
    let repository =
        PostgresJobRepository::connect(fixture_config(url.to_owned())?, Arc::new(FixedClock(at())))
            .await?;

    let key = JobInstanceKey::new(
        JobName::new(FIXTURE_JOB)?,
        &JobParameters::try_from_iter([(
            ParameterName::new("business_date")?,
            JobParameter::new(
                ParameterValue::string("2026-08-09")?,
                ParameterRole::Identifying,
            ),
        )])?,
    );

    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let execution = unit.create_job_execution(instance.id()).await?;
    let step = unit
        .create_step_execution(execution.id(), &StepName::new("only")?)
        .await?;
    unit.transition_step_execution(
        step.id(),
        step.version(),
        LifecycleTransition::new(BatchStatus::Started, at() + Duration::from_secs(1)),
    )
    .await?;
    unit.commit().await?;

    cells.push(service_cell("runtime.service-path")?);

    repository.close().await?;
    Ok(Seeded {
        instance: instance.id(),
        execution: execution.id(),
        version: execution.version(),
    })
}

/// Answers bounded read questions as the explorer class.
async fn explore_through_service(
    url: &str,
    seeded: &Seeded,
    cells: &mut Vec<Value>,
) -> Result<(), Box<dyn Error>> {
    let repository =
        PostgresJobRepository::connect(fixture_config(url.to_owned())?, Arc::new(FixedClock(at())))
            .await?;
    let explorer = JobExplorer::new(PostgresExplorer::new(repository.clone()));

    let projection = explorer.get_execution(seeded.execution).await?;
    assert!(
        projection.is_some(),
        "the explorer class must be able to read the execution the runtime created",
    );
    let steps = explorer
        .list_step_executions(seeded.execution, &page())
        .await?;
    assert!(
        !steps.rows().is_empty(),
        "the explorer class must be able to read the step the runtime created",
    );

    cells.push(service_cell("explorer.service-path")?);

    repository.close().await?;
    Ok(())
}

/// Applies one guarded operator action as the operator class.
async fn operate_through_service(
    url: &str,
    seeded: &Seeded,
    cells: &mut Vec<Value>,
) -> Result<(), Box<dyn Error>> {
    let repository =
        PostgresJobRepository::connect(fixture_config(url.to_owned())?, Arc::new(FixedClock(at())))
            .await?;
    let operator = JobOperator::new(repository.clone(), Arc::new(FixedClock(at())));

    let outcome = operator
        .execute(&OperatorRequest::stop(
            OperationId::new("m5-security-stop-1")?,
            ActorRef::new("campaign:m5-security")?,
            seeded.execution,
            seeded.version,
        ))
        .await?;
    assert!(
        matches!(outcome.class(), oxide_batch::OperatorOutcomeClass::Applied),
        "the operator class must be able to apply a guarded stop and the service reported {:?}",
        outcome.class(),
    );

    cells.push(service_cell("operator.service-path")?);

    repository.close().await?;
    Ok(())
}

/// Holds, releases, and plans a purge as the retention class.
async fn retain_through_service(
    url: &str,
    seeded: &Seeded,
    cells: &mut Vec<Value>,
) -> Result<(), Box<dyn Error>> {
    let repository =
        PostgresJobRepository::connect(fixture_config(url.to_owned())?, Arc::new(FixedClock(at())))
            .await?;
    let retention = RetentionService::new(repository.clone(), Arc::new(FixedClock(at())));

    retention
        .place_hold(
            OperationId::new("m5-security-hold-1")?,
            ActorRef::new("campaign:m5-security")?,
            ReasonCode::new("M5_SECURITY_CAMPAIGN")?,
            seeded.instance,
        )
        .await?;
    let held = retention.hold(seeded.instance).await?;
    assert!(
        held.is_some(),
        "the retention class placed a hold and cannot read it back",
    );
    retention
        .release_hold(
            OperationId::new("m5-security-release-1")?,
            ActorRef::new("campaign:m5-security")?,
            ReasonCode::new("M5_SECURITY_CAMPAIGN")?,
            seeded.instance,
        )
        .await?;
    // Planning surveys the history a purge would remove. It is the read half of
    // the delete privilege the statement matrix proves the other half of.
    retention
        .plan_purge(&PurgePlanRequest::new(
            JobName::new(FIXTURE_JOB)?,
            TerminalStatusSet::all(),
            MINIMUM_PURGE_AGE,
            PurgeBatchBound::new(10)?,
        )?)
        .await?;
    cells.push(service_cell("retention.service-path")?);

    repository.close().await?;
    Ok(())
}

/// Reports the schema as the migration class, through the shipped migrator.
async fn migrate_through_service(url: &str, cells: &mut Vec<Value>) -> Result<(), Box<dyn Error>> {
    let config = fixture_config(url.to_owned())?;

    // Re-running the migrator is the ordinary operational case and is a no-op
    // here. It is the migration class's own work, done through the path that
    // ships.
    PostgresMigrator::migrate(&config).await?;
    let installed = PostgresMigrator::installed_schema_version(&config).await?;
    assert_eq!(
        installed,
        Some(PostgresMigrator::supported_schema_version()),
        "the migration class must report the schema it installed",
    );

    // Schema lifecycle inside the schema it owns.
    let outcome = attempt_statement(
        url,
        "CREATE TABLE oxide_batch.ob_m5_migration_probe (id integer)",
    )
    .await?;
    assert_eq!(
        outcome,
        StatementOutcome::Succeeded,
        "the migration class must be able to add a table to the schema it owns",
    );
    let dropped = attempt_statement(url, "DROP TABLE oxide_batch.ob_m5_migration_probe").await?;
    assert_eq!(
        dropped,
        StatementOutcome::Succeeded,
        "the migration class must be able to remove a table it added",
    );

    cells.push(service_cell("migration.service-path")?);
    cells.push(extra_allowed_cell(
        "migration.add-remove-table-in-owned-schema",
    )?);
    Ok(())
}

/// Reads what the cluster lets each class do, rather than what the script said.
///
/// A policy that granted a cluster-level privilege by accident would leave every
/// grant below it decorative, so the attributes are read back from `pg_roles`.
async fn class_attributes(database_url: &str) -> Result<Value, Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let mut attributes = Vec::new();
    for class in CLASSES {
        let row = sqlx::query(
            "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls \
             FROM pg_roles WHERE rolname = $1",
        )
        .bind(class.role())
        .fetch_one(&pool)
        .await?;

        for (attribute, held) in [
            ("superuser", row.try_get::<bool, _>("rolsuper")?),
            ("createdb", row.try_get::<bool, _>("rolcreatedb")?),
            ("createrole", row.try_get::<bool, _>("rolcreaterole")?),
            ("replication", row.try_get::<bool, _>("rolreplication")?),
            ("bypassrls", row.try_get::<bool, _>("rolbypassrls")?),
        ] {
            assert!(
                !held,
                "the {} class holds the cluster-level {attribute} privilege, which puts it \
                 outside every grant this report checks",
                class.as_str(),
            );
        }
        attributes.push(json!({
            "class": class.as_str(),
            "role": class.role(),
            "superuser": false,
            "createdb": false,
            "createrole": false,
            "replication": false,
            "bypassrls": false,
        }));
    }
    pool.close().await;
    Ok(Value::Array(attributes))
}

/// Reads every privilege `PUBLIC` still holds in the metadata schema.
///
/// A grant to `PUBLIC` reaches every class at once, including ones a future
/// deployment adds, so the separation this report describes only means anything
/// while this list is empty.
async fn public_grants(database_url: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let rows = sqlx::query(
        "SELECT format('%s on table %s', privilege_type, table_name) AS grant_text \
         FROM information_schema.role_table_grants \
         WHERE grantee = 'PUBLIC' AND table_schema = 'oxide_batch' \
         UNION ALL \
         SELECT 'usage on schema oxide_batch' \
         WHERE has_schema_privilege('public', 'oxide_batch', 'USAGE') \
         UNION ALL \
         SELECT 'create on schema oxide_batch' \
         WHERE has_schema_privilege('public', 'oxide_batch', 'CREATE') \
         ORDER BY 1",
    )
    .fetch_all(&pool)
    .await?;
    pool.close().await;

    let mut grants = Vec::new();
    for row in &rows {
        grants.push(row.try_get::<String, _>(0)?);
    }
    Ok(grants)
}

/// Returns the page the explorer reads its steps with.
fn page() -> oxide_batch::PageRequest {
    oxide_batch::PageRequest::first(
        oxide_batch::PageSize::new(50).unwrap_or_else(|_| oxide_batch::PageSize::default()),
    )
}

/// Builds a disposable login password for this run.
///
/// The classes need a credential to log in with, and a committed one would be a
/// credential in the repository. This one lives for the length of the report,
/// is never written to the retained evidence, and is replaced the next time the
/// report runs.
fn disposable_password() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    format!("m5s{:x}{:x}", std::process::id(), nanos)
}

/// Reconciles this file's declared cells against the committed exact
/// denominator.
///
/// This runs without a database and without the `postgres` fixture, so it
/// runs in ordinary review rather than only in the campaign. It is the
/// producer half of the bidirectional binding: `xtask/src/security.rs`
/// reconciles the same committed file against the raw observation this
/// report retains, so a mismatch on either side — the source declaring a
/// cell the denominator does not, or the denominator declaring one the
/// source does not — fails closed somewhere before evidence is ever
/// promoted.
#[test]
fn declared_cells_match_the_committed_role_matrix_denominator() -> Result<(), Box<dyn Error>> {
    let path = fixtures().join("role-matrix.json");
    let source = std::fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)?;

    // Each cell's identity is normalized to a plain tuple rather than
    // compared as `serde_json::Value` — which has no total order — so what
    // is checked is the (id, class, surface, expected) member set of each
    // cell, not a serialized byte order that a harmless field reordering
    // could disturb.
    let identity = |cell: &Value| -> (String, String, String, String) {
        (
            cell.get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            cell.get("class")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            cell.get("surface")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            cell.get("expected")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
        )
    };

    let committed = document
        .get("cells")
        .and_then(Value::as_array)
        .ok_or("the role-matrix denominator declares no cells")?
        .iter()
        .map(identity)
        .collect::<std::collections::BTreeSet<_>>();

    let declared_cells = declared_cell_identities();
    let declared = declared_cells
        .iter()
        .map(identity)
        .collect::<std::collections::BTreeSet<_>>();

    let missing = committed.difference(&declared).collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the role-matrix denominator declares {missing:?} and no BOUNDARIES, PERMITTED, \
         SERVICE_PATH_CELLS, or EXTRA_ALLOWED_CELLS entry in this file produces it",
    );
    let undeclared = declared.difference(&committed).collect::<Vec<_>>();
    assert!(
        undeclared.is_empty(),
        "this file declares {undeclared:?} and tests/fixtures/security/role-matrix.json does not, \
         so the campaign's committed denominator no longer matches what the report actually \
         attempts",
    );

    assert_eq!(
        document.get("total_cells").and_then(Value::as_u64),
        Some(declared.len() as u64),
        "the denominator's total_cells must equal the number of cells this file declares",
    );

    let ids = declared_cells
        .iter()
        .filter_map(|cell| cell.get("id").and_then(Value::as_str).map(str::to_owned))
        .collect::<Vec<_>>();
    let unique_ids = ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ids.len(),
        unique_ids.len(),
        "every declared cell identity must be unique within this file",
    );

    Ok(())
}
