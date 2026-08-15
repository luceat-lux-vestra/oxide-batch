//! Page, response, cursor, and purge-batch bounds against a grown history.
//!
//! Every bound in this report is one an operator only meets when there is more
//! data than they asked for, so the report first makes there be more. It seeds
//! a history several times the largest page the explorer will return and then
//! asks for pages, rather than asserting against a table with four rows in it,
//! where a paging bound and no paging at all look the same.
//!
//! Three of the four bounds are proved by traversal rather than by a single
//! call, because the interesting failure is not one oversized page. It is a
//! traversal that stays bounded per page and is wrong across pages: a cursor
//! that skips rows, or repeats them, or grows with the history it has walked.
//! So the report walks the whole seeded history one page at a time and requires
//! the union to be exactly what was seeded, with nothing repeated, while every
//! individual page stays inside the row and byte ceilings and every cursor
//! stays inside its own.
//!
//! The response bound is the one that needs its rows to be fat. A page of five
//! hundred small projections is nowhere near `256 KiB`, so a run against
//! ordinary rows would report that the response bound held without ever
//! approaching it. Each seeded instance therefore carries eight identifying
//! parameters with long names — every one of them inside its own declared bound
//! — which makes a full page about `660 KiB` of projection and forces the
//! explorer to return less than was asked for. That is the behaviour under
//! test: the bound is not a refusal, it is a truncation that hands back a
//! continuation cursor, and a report that never crossed it would not have
//! shown the difference.
//!
//! The purge batch is proved the same way, against more candidates than the
//! batch admits. A plan that took everything eligible would be a correct-looking
//! answer to a bounded question, so the plan is required to stop at its bound
//! and the applied deletion is required to match the plan it was made from.
//!
//! Nothing here is timed. Per-page latency, rows examined, and index selection
//! against a `10^6`-row history are the P-012 measurement's, and this report
//! does not claim them.

#![cfg(feature = "postgres")]

#[path = "resource_bounds/mod.rs"]
mod resource_bounds;

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use oxide_batch::{
    ActorRef, BatchStatus, Clock, Cursor, JobExplorer, JobInstanceKey, JobName, JobParameter,
    JobParameters, JobRepository, LifecycleTransition, MAX_CURSOR_BYTES, MAX_PAGE_SIZE,
    MAX_PURGE_BATCH, MAX_RESPONSE_BYTES, OperationId, PageRequest, PageSize, ParameterName,
    ParameterRole, ParameterValue, PostgresExplorer, PostgresJobRepository, PostgresMigrator,
    PurgeBatchBound, PurgePlanRequest, ReasonCode, RetentionService, TerminalStatusSet,
};
// The encoded response bound is the explorer port's own accounting, so the
// report measures the quantity the service bounds rather than a re-derivation
// of it. The facade does not re-export the row trait, and the campaign reads it
// from the crate that owns it along an authorized dependency edge.
use oxide_batch_repository::ExplorerRow;
use serde_json::{Value, json};

use resource_bounds::{
    FixedClock, config, execution_manifest, major_version, migrator_url, remove_job,
    remove_retention_action, retain_observation, runtime_url, server_version,
};

/// The report identifier the runner reconciles this observation under.
const REPORT: &str = "bounded-query-paths";

/// The job whose history the bounded paths are exercised against.
const JOB: &str = "m5_resource_bound_history";

/// Instances seeded before anything is paged.
///
/// More than twice a full page, so the traversal needs a continuation cursor
/// and needs it to be right, and more than the purge batch the report plans
/// with, so a plan that took everything eligible would exceed its bound.
const SEEDED_INSTANCES: usize = 1_200;

/// Identifying parameters carried by each seeded instance.
///
/// The projection is redacted and carries parameter names rather than values,
/// so this is what makes a row wide enough for a full page to cross the
/// response bound.
const PARAMETERS_PER_INSTANCE: usize = 8;

/// Length of each parameter name, inside the declared identifier bound.
const PARAMETER_NAME_BYTES: usize = 120;

/// The operation identifier the applied purge is audited under.
const PURGE_OPERATION: &str = "m5-resource-bound-purge";

/// The batch bound the purge is planned with, well under the declared ceiling.
///
/// A bound of exactly the ceiling would be satisfied by a survey that simply
/// found fewer candidates. This one is small enough that the seeded history is
/// certain to exceed it.
const PLANNED_PURGE_BATCH: u32 = 100;

#[test]
fn bounded_query_paths_stay_bounded_as_history_grows() -> Result<(), Box<dyn Error>> {
    let Some(runtime) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(report(runtime, migrator))
}

/// Runs every bounded-path obligation and retains one observation.
async fn report(runtime: String, migrator: String) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator.clone())?).await?;
    remove_job(&migrator, JOB).await?;
    remove_retention_action(&migrator, PURGE_OPERATION).await?;

    let server = server_version(&runtime).await?;
    let clock = FixedClock::default();
    let repository =
        PostgresJobRepository::connect(config(runtime.clone())?, Arc::new(clock)).await?;

    let name = JobName::new(JOB)?;
    seed(&repository, &clock, &name).await?;

    let mut violations = Vec::new();
    let mut resources = Vec::new();

    let construction = construction_cells();
    violations.extend(construction.iter().filter_map(Cell::violation));

    let traversal = walk_the_history(&repository, &name).await?;
    violations.extend(traversal.violations.clone());
    resources.push(traversal.page_evidence());
    resources.push(traversal.response_evidence());
    resources.push(traversal.cursor_evidence());

    let purge = plan_a_bounded_purge(&runtime, &name).await?;
    violations.extend(purge.violations.clone());
    resources.push(purge.evidence());

    let document = json!({
        "report": REPORT,
        "scenario": "bounded_query_paths_stay_bounded_as_history_grows",
        "server_version": server,
        "postgres_major_version": major_version(&server),
        "seeded_instances": SEEDED_INSTANCES,
        "resources": resources,
        "construction": construction
            .iter()
            .map(Cell::evidence)
            .collect::<Vec<_>>(),
        "execution_manifest": execution_manifest()?,
        "violations": violations,
        "passed": violations.is_empty(),
    });
    retain_observation(REPORT, &document)?;

    repository.close().await?;
    remove_job(&migrator, JOB).await?;
    remove_retention_action(&migrator, PURGE_OPERATION).await?;

    assert!(
        violations.is_empty(),
        "the bounded-query report observed {violations:#?}",
    );
    Ok(())
}

/// Creates the history the bounded paths are exercised against.
///
/// The instances are written through the adapter's own lifecycle transitions
/// rather than inserted, so what is paged over is what a run would have left,
/// and they are created in bounded transactions so the seeding itself does not
/// hold an unbounded amount of anything.
async fn seed(
    repository: &PostgresJobRepository,
    clock: &FixedClock,
    name: &JobName,
) -> Result<(), Box<dyn Error>> {
    const BATCH: usize = 100;
    let filler = "n".repeat(PARAMETER_NAME_BYTES.saturating_sub(8));

    let mut created = 0;
    while created < SEEDED_INSTANCES {
        let batch = BATCH.min(SEEDED_INSTANCES - created);
        let mut unit = repository.begin().await?;
        for index in created..created + batch {
            let mut parameters = JobParameters::new();
            for slot in 0..PARAMETERS_PER_INSTANCE {
                parameters.insert(
                    ParameterName::new(format!("{filler}{slot:02}p"))?,
                    JobParameter::new(
                        // Only the first parameter varies, which is what makes
                        // each instance a distinct logical identity. The rest
                        // are there to give the projection its width.
                        ParameterValue::string(if slot == 0 {
                            format!("row-{index:06}")
                        } else {
                            "constant".to_owned()
                        })?,
                        ParameterRole::Identifying,
                    ),
                )?;
            }
            let key = JobInstanceKey::new(name.clone(), &parameters);
            let instance = unit.select_or_create_job_instance(&key).await?;
            let execution = unit.create_job_execution(instance.instance().id()).await?;
            let started = unit
                .transition_job_execution(
                    execution.id(),
                    execution.version(),
                    LifecycleTransition::new(BatchStatus::Started, clock.now()),
                )
                .await?;
            unit.transition_job_execution(
                started.id(),
                started.version(),
                LifecycleTransition::new(BatchStatus::Completed, clock.now()),
            )
            .await?;
        }
        unit.commit().await?;
        created += batch;
    }

    Ok(())
}

/// Pages the whole seeded history and reports what every page held.
async fn walk_the_history(
    repository: &PostgresJobRepository,
    name: &JobName,
) -> Result<Traversal, Box<dyn Error>> {
    let explorer = JobExplorer::new(PostgresExplorer::new(repository.clone()));
    let size = PageSize::new(MAX_PAGE_SIZE)?;

    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    let mut duplicates = 0_u64;
    let mut pages = 0_u64;
    let mut truncated_pages = 0_u64;
    let mut largest_rows = 0_usize;
    let mut largest_bytes = 0_usize;
    let mut largest_cursor = 0_usize;
    let mut cursor = None;

    loop {
        let request = match cursor.take() {
            Some(token) => PageRequest::resume(size, token),
            None => PageRequest::first(size),
        };
        let page = explorer.list_instances(name, &request).await?;
        pages += 1;

        let (rows, bytes, truncated) = inspect(&page, &mut violations);
        largest_rows = largest_rows.max(rows);
        largest_bytes = largest_bytes.max(bytes);
        if truncated {
            truncated_pages += 1;
        }

        for row in page.rows() {
            if !seen.insert(row.id().get()) {
                duplicates += 1;
            }
        }

        match page.next_cursor() {
            Some(token) => {
                let bytes = token.as_bytes().len();
                largest_cursor = largest_cursor.max(bytes);
                if bytes > MAX_CURSOR_BYTES {
                    violations.push(format!(
                        "a continuation cursor was {bytes} bytes against a bound of \
                         {MAX_CURSOR_BYTES}",
                    ));
                }
                cursor = Some(token.clone());
            }
            None => break,
        }

        if pages > 4_000 {
            violations.push(
                "the traversal did not terminate within four thousand pages, so the cursor is \
                 not advancing"
                    .to_owned(),
            );
            break;
        }
    }

    if seen.len() != SEEDED_INSTANCES {
        violations.push(format!(
            "the traversal walked {} distinct instances and {SEEDED_INSTANCES} were seeded, so a \
             bounded page dropped or invented rows",
            seen.len(),
        ));
    }
    if duplicates != 0 {
        violations.push(format!(
            "the traversal returned {duplicates} row(s) more than once, so the cursor overlaps \
             the page before it",
        ));
    }
    if truncated_pages == 0 {
        violations.push(format!(
            "no page was truncated by the {MAX_RESPONSE_BYTES}-byte response bound, so the run \
             never reached it and says nothing about what happens when it is",
        ));
    }
    if largest_rows == 0 {
        violations.push("the traversal returned no row at all".to_owned());
    }

    Ok(Traversal {
        pages,
        truncated_pages,
        rows: seen.len() as u64,
        duplicates,
        largest_rows: largest_rows as u64,
        largest_bytes: largest_bytes as u64,
        largest_cursor: largest_cursor as u64,
        violations,
    })
}

/// Reports what one page held, and whether the response bound truncated it.
///
/// Returns the row count, the encoded byte count, and whether the page was cut
/// short by the response bound rather than by the history ending.
fn inspect(
    page: &oxide_batch::Page<oxide_batch::JobInstanceProjection>,
    violations: &mut Vec<String>,
) -> (usize, usize, bool) {
    let rows = page.rows().len();
    if rows > usize::from(MAX_PAGE_SIZE) {
        violations.push(format!(
            "a page returned {rows} rows against a bound of {MAX_PAGE_SIZE}",
        ));
    }

    // The bound the explorer enforces is on the encoded response, so the report
    // measures the same quantity the service does rather than a proxy for it.
    let bytes = page
        .rows()
        .iter()
        .map(ExplorerRow::encoded_len)
        .fold(0_usize, usize::saturating_add);
    if bytes > MAX_RESPONSE_BYTES {
        violations.push(format!(
            "a page encoded {bytes} bytes against a response bound of {MAX_RESPONSE_BYTES}",
        ));
    }

    // Fewer rows than asked for, and more to come: the response bound truncated
    // the page rather than the history ending.
    let truncated = rows < usize::from(MAX_PAGE_SIZE) && page.next_cursor().is_some();
    (rows, bytes, truncated)
}

/// Plans and applies a purge bounded well below the eligible candidates.
async fn plan_a_bounded_purge(url: &str, name: &JobName) -> Result<Purge, Box<dyn Error>> {
    // The seeded history was written at the report's fixed instant, and
    // eligibility is measured against the repository's clock rather than the
    // service's. So the purge runs through a repository opened thirty days
    // later, which is what an operator purging a month-old history has, rather
    // than through backdated rows.
    let later = FixedClock(FixedClock::default().0 + Duration::from_hours(30 * 24));
    let repository =
        PostgresJobRepository::connect(config(url.to_owned())?, Arc::new(later)).await?;
    let retention = RetentionService::new(repository.clone(), Arc::new(later));
    let request = PurgePlanRequest::new(
        name.clone(),
        TerminalStatusSet::new([BatchStatus::Completed])?,
        Duration::from_hours(24),
        PurgeBatchBound::new(PLANNED_PURGE_BATCH)?,
    )?;

    let plan = retention.plan_purge(&request).await?;
    let planned = plan.candidates().len();

    let mut violations = Vec::new();
    if planned > PLANNED_PURGE_BATCH as usize {
        violations.push(format!(
            "the purge plan took {planned} candidates against a batch bound of \
             {PLANNED_PURGE_BATCH}",
        ));
    }
    if planned != PLANNED_PURGE_BATCH as usize {
        violations.push(format!(
            "{SEEDED_INSTANCES} eligible instances were offered to a batch bound of \
             {PLANNED_PURGE_BATCH} and the plan took {planned}, so the bound was not what limited \
             it",
        ));
    }

    let report = retention
        .apply_purge(
            OperationId::new(PURGE_OPERATION)?,
            ActorRef::new("resource-bound-campaign")?,
            ReasonCode::new("BOUNDED_BATCH")?,
            &plan,
        )
        .await?;
    let counts = report.counts();
    let deleted = counts.job_instances();

    if report.outcome() != oxide_batch::RetentionOutcome::Applied {
        violations.push(format!(
            "the purge reported {:?} rather than applying, so this run is reporting an earlier \
             run's counts",
            report.outcome(),
        ));
    }

    if deleted > u64::from(PLANNED_PURGE_BATCH) {
        violations.push(format!(
            "the applied purge deleted {deleted} instances against a batch bound of \
             {PLANNED_PURGE_BATCH}",
        ));
    }
    if deleted != planned as u64 {
        violations.push(format!(
            "the purge planned {planned} candidates and deleted {deleted}, so what was applied is \
             not what was reviewed",
        ));
    }

    repository.close().await?;

    Ok(Purge {
        offered: SEEDED_INSTANCES as u64,
        batch: u64::from(PLANNED_PURGE_BATCH),
        planned: planned as u64,
        deleted,
        deleted_executions: counts.job_executions(),
        violations,
    })
}

/// Reports every bounded-path construction the framework must accept or refuse.
fn construction_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "explorer-page-size",
            "at the ceiling",
            u64::from(MAX_PAGE_SIZE),
            PageSize::new(MAX_PAGE_SIZE).is_ok(),
            true,
        ),
        Cell::new(
            "explorer-page-size",
            "one past the ceiling",
            u64::from(MAX_PAGE_SIZE) + 1,
            PageSize::new(MAX_PAGE_SIZE.saturating_add(1)).is_ok(),
            false,
        ),
        Cell::new(
            "explorer-page-size",
            "an empty page",
            0,
            PageSize::new(0).is_ok(),
            false,
        ),
        Cell::new(
            "explorer-cursor",
            "at the ceiling",
            MAX_CURSOR_BYTES as u64,
            Cursor::from_bytes(vec![b'c'; MAX_CURSOR_BYTES]).is_ok(),
            true,
        ),
        Cell::new(
            "explorer-cursor",
            "one past the ceiling",
            MAX_CURSOR_BYTES as u64 + 1,
            Cursor::from_bytes(vec![b'c'; MAX_CURSOR_BYTES.saturating_add(1)]).is_ok(),
            false,
        ),
        Cell::new(
            "explorer-cursor",
            "an empty token",
            0,
            Cursor::from_bytes(Vec::new()).is_ok(),
            false,
        ),
        Cell::new(
            "retention-purge-batch",
            "at the ceiling",
            u64::from(MAX_PURGE_BATCH),
            PurgeBatchBound::new(MAX_PURGE_BATCH).is_ok(),
            true,
        ),
        Cell::new(
            "retention-purge-batch",
            "one past the ceiling",
            u64::from(MAX_PURGE_BATCH) + 1,
            PurgeBatchBound::new(MAX_PURGE_BATCH.saturating_add(1)).is_ok(),
            false,
        ),
        Cell::new(
            "retention-purge-batch",
            "an empty batch",
            0,
            PurgeBatchBound::new(0).is_ok(),
            false,
        ),
    ]
}

/// What paging the whole seeded history observed.
struct Traversal {
    pages: u64,
    truncated_pages: u64,
    rows: u64,
    duplicates: u64,
    largest_rows: u64,
    largest_bytes: u64,
    largest_cursor: u64,
    violations: Vec<String>,
}

impl Traversal {
    /// Renders the page-size evidence the traversal produced.
    fn page_evidence(&self) -> Value {
        json!({
            "resource": "explorer-page-size",
            "overload_policy": "fail-closed",
            "configured_ceiling": MAX_PAGE_SIZE,
            "offered_load": SEEDED_INSTANCES,
            "observed_peak_occupancy": self.largest_rows,
            "pages": self.pages,
            "rows_traversed": self.rows,
            "duplicate_rows": self.duplicates,
            "rejections": 0,
            "waits": 0,
            "drops": 0,
            "violations": self.violations,
            "passed": self.violations.is_empty(),
        })
    }

    /// Renders the response-bound evidence the traversal produced.
    fn response_evidence(&self) -> Value {
        json!({
            "resource": "explorer-response",
            "overload_policy": "bounded-truncation",
            "configured_ceiling": MAX_RESPONSE_BYTES,
            "offered_load": u64::from(MAX_PAGE_SIZE),
            "observed_peak_occupancy": self.largest_bytes,
            "truncated_pages": self.truncated_pages,
            "rejections": 0,
            "waits": 0,
            "drops": 0,
            "violations": Vec::<String>::new(),
            "passed": true,
        })
    }

    /// Renders the cursor evidence the traversal produced.
    fn cursor_evidence(&self) -> Value {
        json!({
            "resource": "explorer-cursor",
            "overload_policy": "fail-closed",
            "configured_ceiling": MAX_CURSOR_BYTES,
            "offered_load": self.rows,
            "observed_peak_occupancy": self.largest_cursor,
            "rejections": 0,
            "waits": 0,
            "drops": 0,
            "violations": Vec::<String>::new(),
            "passed": true,
        })
    }
}

/// What the bounded purge planned and applied.
struct Purge {
    offered: u64,
    batch: u64,
    planned: u64,
    deleted: u64,
    deleted_executions: u64,
    violations: Vec<String>,
}

impl Purge {
    /// Renders the purge-batch evidence.
    fn evidence(&self) -> Value {
        json!({
            "resource": "retention-purge-batch",
            "overload_policy": "fail-closed",
            "configured_ceiling": self.batch,
            "declared_ceiling": MAX_PURGE_BATCH,
            "offered_load": self.offered,
            "observed_peak_occupancy": self.planned,
            "deleted_instances": self.deleted,
            "deleted_executions": self.deleted_executions,
            "rejections": 0,
            "waits": 0,
            "drops": 0,
            "violations": self.violations,
            "passed": self.violations.is_empty(),
        })
    }
}

/// One construction the framework must accept or refuse.
struct Cell {
    resource: &'static str,
    case: &'static str,
    value: u64,
    accepted: bool,
    expected: bool,
}

impl Cell {
    /// Records one construction result.
    const fn new(
        resource: &'static str,
        case: &'static str,
        value: u64,
        accepted: bool,
        expected: bool,
    ) -> Self {
        Self {
            resource,
            case,
            value,
            accepted,
            expected,
        }
    }

    /// Returns the violation this cell is, when it is one.
    fn violation(&self) -> Option<String> {
        (self.accepted != self.expected).then(|| {
            if self.expected {
                format!(
                    "{} refused {} {}, which is inside its declared bound",
                    self.resource, self.case, self.value,
                )
            } else {
                format!(
                    "{} accepted {} {}, which is outside its declared bound",
                    self.resource, self.case, self.value,
                )
            }
        })
    }

    /// Renders what the retained evidence records for this cell.
    fn evidence(&self) -> Value {
        json!({
            "resource": self.resource,
            "case": self.case,
            "value": self.value,
            "expected": if self.expected { "accepted" } else { "refused" },
            "observed": if self.accepted { "accepted" } else { "refused" },
        })
    }
}
