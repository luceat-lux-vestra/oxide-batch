//! The committed cancellation denominator, as the reports read it.
//!
//! The reports take their workload shape, their deadline set, and their phase
//! mapping from `tests/fixtures/cancellation/campaign-scope.json` rather than
//! declaring their own. The deadline set is the reason this module exists:
//! P-014 owes the count of unjoined tasks *at each deadline*, so what "each"
//! means has to be fixed somewhere a reviewer can see it and the runner can
//! check against it. Constants in the report would be a third place for the set
//! to be written and a second place for it to drift, and a report that quietly
//! dropped a deadline would still be green.
//!
//! The held-task counts are read from the same place and for a sharper reason.
//! The campaign asserts that the reported unjoined count equals the number of
//! tasks actually held, so the two numbers have to come from one source; if the
//! report chose its own held count and the runner read the document's, a report
//! that held a different number than it declared would reconcile against
//! itself.

use std::error::Error;
use std::fs;
use std::time::Duration;

use serde_json::Value;

use super::{Failure, workspace_root};

/// The committed campaign scope document.
#[derive(Debug)]
pub struct Scope {
    /// The workload every report launches.
    pub workload: Workload,
    /// The deadlines the drain report must observe an unjoined count at.
    pub deadlines: Vec<Deadline>,
    /// The escalation observation, which ends waiting the other way.
    pub escalation: Escalation,
    /// The correctness obligations the campaign fails on.
    pub correctness: Vec<String>,
    /// The `ShutdownTaskPhase` names this campaign claims to observe.
    pub observed_phases: Vec<String>,
}

/// The workload a report launches.
#[derive(Debug)]
pub struct Workload {
    /// The job name every report launches under.
    pub job_name: String,
    /// Partitions the job offers.
    pub partitions: u16,
    /// Concurrent partition workers the step admits.
    pub worker_budget: u8,
    /// Connections the repository pool is opened with.
    pub pool_size: u32,
    /// The interval the owning runtime re-reads the durable stop request at.
    pub stop_poll_interval: Duration,
    /// The bounded await each worker performs as its work.
    pub worker_work: Duration,
}

/// One declared deadline and the number of tasks held past it.
#[derive(Debug)]
pub struct Deadline {
    /// The identifier the report and the runner reconcile this point under.
    pub id: String,
    /// The configured shutdown and task-join budget.
    pub duration: Duration,
    /// The accepted constant this value corresponds to, where it names one.
    pub accepted_constant: Option<String>,
    /// Tasks the report holds past the deadline.
    pub held_tasks: usize,
}

/// The escalation observation.
#[derive(Debug)]
pub struct Escalation {
    /// The identifier the report and the runner reconcile it under.
    pub id: String,
    /// Tasks the report holds when the second request ends waiting.
    pub held_tasks: usize,
}

impl Scope {
    /// Reads the committed scope document.
    ///
    /// # Errors
    ///
    /// Returns the failure when the document cannot be read, cannot be parsed,
    /// or does not declare something the reports need.
    pub fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("cancellation")
            .join("campaign-scope.json");
        let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;

        let workload = document
            .get("workload")
            .ok_or_else(|| Failure::boxed("the scope declares no workload"))?;
        let deadlines = document
            .get("deadlines")
            .ok_or_else(|| Failure::boxed("the scope declares no deadlines"))?;

        let points = deadlines
            .get("points")
            .and_then(Value::as_array)
            .ok_or_else(|| Failure::boxed("the scope declares no deadline points"))?
            .iter()
            .map(Deadline::read)
            .collect::<Result<Vec<_>, _>>()?;
        if points.is_empty() {
            return Err(Failure::boxed(
                "the scope declares an empty deadline set, so 'each deadline' is no deadline",
            ));
        }

        let escalation = deadlines
            .get("escalation")
            .ok_or_else(|| Failure::boxed("the scope declares no escalation observation"))?;

        Ok(Self {
            workload: Workload {
                job_name: string(workload, "job_name")?,
                partitions: u16::try_from(number(workload, "partitions")?)?,
                worker_budget: u8::try_from(number(workload, "worker_budget")?)?,
                pool_size: u32::try_from(number(workload, "pool_size")?)?,
                stop_poll_interval: Duration::from_millis(number(
                    workload,
                    "stop_poll_interval_millis",
                )?),
                worker_work: Duration::from_millis(number(workload, "worker_work_millis")?),
            },
            deadlines: points,
            escalation: Escalation {
                id: string(escalation, "id")?,
                held_tasks: usize::try_from(number(escalation, "held_tasks")?)?,
            },
            correctness: document
                .get("correctness")
                .and_then(|correctness| correctness.get("requires"))
                .and_then(Value::as_array)
                .ok_or_else(|| Failure::boxed("the scope declares no correctness requirements"))?
                .iter()
                .map(|entry| string(entry, "id"))
                .collect::<Result<Vec<_>, _>>()?,
            observed_phases: document
                .get("phases")
                .and_then(|phases| phases.get("task_phase"))
                .and_then(|phase| phase.get("observed_by_this_campaign"))
                .and_then(Value::as_array)
                .ok_or_else(|| Failure::boxed("the scope declares no observed task phases"))?
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        })
    }
}

impl Deadline {
    /// Reads one declared deadline point.
    fn read(entry: &Value) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            id: string(entry, "id")?,
            duration: Duration::from_millis(number(entry, "millis")?),
            accepted_constant: entry
                .get("accepted_constant")
                .and_then(Value::as_str)
                .map(str::to_owned),
            held_tasks: usize::try_from(number(entry, "held_tasks")?)?,
        })
    }
}

/// Reads one required string field.
fn string(document: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Failure::boxed(format!("the scope entry has no {name}")))
}

/// Reads one required unsigned field.
fn number(document: &Value, name: &str) -> Result<u64, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure::boxed(format!("the scope entry has no numeric {name}")))
}
