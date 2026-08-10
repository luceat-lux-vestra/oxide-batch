//! The committed soak denominator, as the report reads it.
//!
//! The report takes its cycle counts, its workload shape, its correctness
//! obligations, and its growth rules from
//! `tests/fixtures/soak/campaign-scope.json` rather than declaring its own. A
//! soak's whole claim is "over *this* period, doing *this* work, nothing
//! accumulated", so the period and the work have to be fixed somewhere a
//! reviewer can see them and a runner can check against them. Constants in the
//! test would be a third place for them to be written and a second place for
//! them to drift.
//!
//! The growth rules are read as data for the same reason and one further one:
//! the runner has to require the report to have decided every rule the campaign
//! declares. Rules named here, decided in the report, and required by the
//! runner cannot be quietly narrowed by any one of the three.

use std::error::Error;
use std::fs;

use serde_json::Value;

use super::{Failure, workspace_root};

/// The committed campaign scope document.
#[derive(Debug)]
pub struct Scope {
    /// The workload every cycle runs.
    pub workload: Workload,
    /// The warmup and measured window the campaign declares.
    pub window: Window,
    /// The per-cycle durable comparisons the campaign requires.
    pub correctness: Vec<String>,
    /// The growth rules the report must decide and the runner requires.
    pub rules: Vec<Rule>,
}

/// The workload one cycle performs.
#[derive(Debug)]
pub struct Workload {
    /// The job name every cycle launches under.
    pub job_name: String,
    /// Partitions offered in each cycle.
    pub partitions_per_cycle: u16,
    /// Concurrent partition workers each cycle admits.
    pub worker_budget: u8,
    /// Connections the repository pool is opened with.
    pub pool_size: u32,
    /// Launches each cycle performs, the failed attempt and the restart.
    pub launches_per_cycle: u64,
    /// Tasks each cycle's coordinator owns and must join.
    pub owned_tasks_per_drain: usize,
    /// The bounded await each worker performs as its work.
    pub worker_work_millis: u64,
}

/// The declared warmup and measurement window.
#[derive(Debug)]
pub struct Window {
    /// Cycles run before any sample is eligible for a growth rule.
    pub warmup_cycles: usize,
    /// Cycles run inside the measured window.
    pub measured_cycles: usize,
    /// How long a boundary is allowed to settle before it is sampled.
    pub settle_millis: u64,
    /// The fewest measured samples a report may pass with.
    pub minimum_measured_samples: usize,
}

/// One growth rule, as declared.
#[derive(Clone, Debug)]
pub struct Rule {
    /// The identity the report's verdict and the runner's requirement share.
    pub id: String,
    /// The per-sample metric the rule is decided from.
    pub metric: String,
    /// How the metric's measured series decides the rule.
    pub decides: String,
    /// The allowance the rule is decided against, where it has one.
    pub budget: Option<i64>,
}

impl Scope {
    /// Reads and parses the committed scope document.
    ///
    /// # Errors
    ///
    /// Returns the failure naming the field the document is missing.
    pub fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("soak")
            .join("campaign-scope.json");
        let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;

        let workload = document
            .get("workload")
            .ok_or_else(|| Failure::boxed("the scope document declares no workload"))?;
        let window = document
            .get("window")
            .ok_or_else(|| Failure::boxed("the scope document declares no window"))?;
        let growth = document
            .get("growth_rules")
            .ok_or_else(|| Failure::boxed("the scope document declares no growth rules"))?;

        let mut rules = Vec::new();
        for rule in array(growth, "rules")? {
            rules.push(Rule {
                id: string(rule, "id")?,
                metric: string(rule, "metric")?,
                decides: string(rule, "rule")?,
                budget: rule.get("budget").and_then(Value::as_i64),
            });
        }

        let correctness = document
            .get("correctness")
            .ok_or_else(|| Failure::boxed("the scope document declares no correctness checks"))?;
        let correctness = array(correctness, "checks")?
            .iter()
            .map(|check| string(check, "id"))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            workload: Workload {
                job_name: string(workload, "job_name")?,
                partitions_per_cycle: count(workload, "partitions_per_cycle")?,
                worker_budget: count(workload, "worker_budget")?,
                pool_size: count(workload, "pool_size")?,
                launches_per_cycle: count(workload, "launches_per_cycle")?,
                owned_tasks_per_drain: count(workload, "owned_tasks_per_drain")?,
                worker_work_millis: count(workload, "worker_work_millis")?,
            },
            window: Window {
                warmup_cycles: count(window, "warmup_cycles")?,
                measured_cycles: count(window, "measured_cycles")?,
                settle_millis: count(window, "settle_millis")?,
                minimum_measured_samples: count(
                    window
                        .get("sampling")
                        .ok_or_else(|| Failure::boxed("the window declares no sampling"))?,
                    "minimum_measured_samples",
                )?,
            },
            correctness,
            rules,
        })
    }
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| Failure::boxed(format!("the scope document has no {name}")))
}

/// Reads one required string field.
fn string(value: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Failure::boxed(format!("a scope entry has no {name}")))
}

/// Reads one required count, rejecting a value the workload cannot use.
fn count<T: TryFrom<u64>>(value: &Value, name: &str) -> Result<T, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| Failure::boxed(format!("a scope entry has no {name}")))?
        .try_into()
        .map_err(|_| Failure::boxed(format!("the declared {name} does not fit the workload")))
}
