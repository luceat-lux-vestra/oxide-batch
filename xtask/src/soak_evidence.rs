//! Independent evaluation of a soak report's raw observations.
//!
//! The soak report carries two kinds of thing, and only one of them is
//! evidence. Its samples and its per-cycle journal are observations: they are
//! what the run saw. Its correctness checks, its growth verdicts and its
//! campaign totals are *conclusions the report drew about itself*, and a
//! conclusion a program draws about its own run is not evidence for that run —
//! it is the claim under examination.
//!
//! Everything in this module recomputes a conclusion from the observations and
//! requires the report's own to match. That is a stronger relation than
//! agreement: where they differ, the recomputation is authoritative and the
//! report is wrong, because the recomputation is the one derived from what was
//! observed.
//!
//! The order the checks run in is load-bearing and is the order of the trust
//! graph itself:
//!
//! 1. **chronology** — the samples and cycles are one contiguous, ordered,
//!    correctly phased run of the declared length. Nothing below means anything
//!    until this holds, because every later step indexes into these arrays and
//!    a reordered or duplicated array quietly changes what "the first measured
//!    cycle" and "the last third of the window" refer to.
//! 2. **lifecycle** — folded from the cycles, not read from the summary.
//! 3. **correctness** — every obligation recomputed per cycle from the journal.
//! 4. **growth** — recomputed from the sample series, in verified cycle order.
//!
//! The report's summary fields survive as conveniences. They are compared, and
//! never consulted.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

/// One cycle's raw observations, as the journal recorded them.
///
/// Parsed once so every evaluator below reads the same values, and so a journal
/// that cannot be read fails as a parse rather than as a silently absent field.
pub struct Cycle {
    /// The cycle's own index.
    pub index: u64,
    /// Which window it belongs to.
    pub phase: String,
    /// The durable status the failed attempt reached.
    pub failed_status: String,
    /// How the failed attempt was classified.
    pub failed_outcome: String,
    /// The partition the fault was injected into.
    pub injected: String,
    /// Partitions the failed attempt committed.
    pub committed: BTreeSet<String>,
    /// Whether the injected worker's wait for its siblings expired.
    pub fault_wait_expired: bool,
    /// Whether the restart was a new execution on the same instance.
    pub recovered: bool,
    /// Partitions the restart invoked again.
    pub re_run: BTreeSet<String>,
    /// The terminal record, by field.
    pub terminal: Terminal,
    /// Invocation count per partition key across both attempts.
    pub invocations: BTreeMap<String, u64>,
    /// Repository transactions the cycle's launches began.
    pub transactions: u64,
    /// Greatest and residual worker occupancy.
    pub worker_peak: u64,
    pub worker_residue: u64,
    /// The cycle's drain.
    pub drain: String,
    pub unjoined: u64,
    pub panicked: u64,
    /// How much the durable history grew across the cycle.
    pub history: BTreeMap<String, i64>,
}

/// The terminal durable record of one cycle.
pub struct Terminal {
    pub outcome: String,
    pub job_status: String,
    pub job_exit_status: String,
    pub parent_status: String,
    pub parent_exit_status: String,
    pub parent_counts: String,
    pub step_executions: u64,
    /// Every partition by key, with its status, exit status and counters.
    pub partitions: BTreeMap<String, String>,
}

impl Cycle {
    /// Reads one cycle out of the journal.
    fn read(value: &Value) -> Result<Self, String> {
        let index = number(value, "/cycle")?;
        let terminal = Terminal {
            outcome: string(value, "/terminal/outcome")?,
            job_status: string(value, "/terminal/job_status")?,
            job_exit_status: string(value, "/terminal/job_exit_status")?,
            parent_status: string(value, "/terminal/parent_status")?,
            parent_exit_status: string(value, "/terminal/parent_exit_status")?,
            parent_counts: render(value.pointer("/terminal/parent_counts"))?,
            step_executions: number(value, "/terminal/step_executions")?,
            partitions: value
                .pointer("/terminal/partitions")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("cycle {index} records no terminal partitions"))?
                .iter()
                .map(|(key, partition)| Ok((key.clone(), render(Some(partition))?)))
                .collect::<Result<_, String>>()?,
        };
        Ok(Self {
            index,
            phase: string(value, "/phase")?,
            failed_status: string(value, "/failed_attempt/durable_status")?,
            failed_outcome: string(value, "/failed_attempt/outcome")?,
            injected: string(value, "/failed_attempt/injected_partition")?,
            committed: keys(value, "/failed_attempt/partitions_committed", index)?,
            fault_wait_expired: value
                .pointer("/failed_attempt/fault_wait_expired")
                .and_then(Value::as_bool)
                .ok_or_else(|| format!("cycle {index} records no fault wait result"))?,
            recovered: value
                .pointer("/restart/new_execution_on_same_instance")
                .and_then(Value::as_bool)
                .ok_or_else(|| format!("cycle {index} records no recovery result"))?,
            re_run: keys(value, "/restart/partitions_re_run", index)?,
            terminal,
            invocations: value
                .pointer("/invocations")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("cycle {index} records no invocations"))?
                .iter()
                .map(|(key, count)| {
                    count
                        .as_u64()
                        .map(|count| (key.clone(), count))
                        .ok_or_else(|| format!("cycle {index} records a non-numeric invocation"))
                })
                .collect::<Result<_, String>>()?,
            transactions: number(value, "/repository_transactions")?,
            worker_peak: number(value, "/worker_peak_occupancy")?,
            worker_residue: number(value, "/worker_residue")?,
            drain: string(value, "/drain/result")?,
            unjoined: number(value, "/drain/unjoined_tasks")?,
            panicked: number(value, "/drain/panicked_tasks")?,
            history: value
                .pointer("/durable_history_growth")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("cycle {index} records no durable history growth"))?
                .iter()
                .map(|(key, count)| {
                    count
                        .as_i64()
                        .map(|count| (key.clone(), count))
                        .ok_or_else(|| format!("cycle {index} records a non-numeric growth"))
                })
                .collect::<Result<_, String>>()?,
        })
    }
}

/// Reads every cycle out of the journal.
///
/// # Errors
///
/// Returns the first cycle that cannot be read. A journal that cannot be parsed
/// is a failure rather than an empty set of observations: the alternative is
/// that a malformed report has nothing to disagree with.
pub fn read_cycles(observation: &Value) -> Result<Vec<Cycle>, String> {
    observation
        .get("cycles")
        .and_then(Value::as_array)
        .ok_or_else(|| "the report retains no per-cycle journal".to_owned())?
        .iter()
        .map(Cycle::read)
        .collect()
}

/// The declared shape of the run, as the scope states it.
pub struct Window {
    pub warmup: u64,
    pub measured: u64,
}

/// Requires the samples and cycles to be one contiguous, ordered, phased run.
///
/// This runs before anything else because everything else indexes into these
/// arrays. "The first measured cycle" is the correctness baseline and "the last
/// third of the window" is the memory verdict; both are positions, and a
/// duplicated, missing or reordered entry silently moves them. Serialization
/// order is not authority — the recorded cycle index is, and it is required to
/// agree with the position.
pub fn reconcile_sequence(window: &Window, observation: &Value, cycles: &[Cycle]) -> Vec<String> {
    let mut violations = Vec::new();
    let total = window.warmup + window.measured;

    let samples = observation
        .get("samples")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if samples.len() as u64 != total {
        violations.push(format!(
            "the declared window is {total} cycles and {} samples were retained",
            samples.len(),
        ));
    }
    if cycles.len() as u64 != total {
        violations.push(format!(
            "the declared window is {total} cycles and {} were journalled",
            cycles.len(),
        ));
    }

    let expected = |index: u64| {
        if index < window.warmup {
            "warmup"
        } else {
            "measured"
        }
    };

    let mut seen = BTreeSet::new();
    for (position, cycle) in cycles.iter().enumerate() {
        let position = position as u64;
        if cycle.index != position {
            violations.push(format!(
                "the journal's entry at position {position} records cycle {}, so the cycles are \
                 not in the order they ran",
                cycle.index,
            ));
        }
        if !seen.insert(cycle.index) {
            violations.push(format!(
                "cycle {} is journalled more than once",
                cycle.index
            ));
        }
        if cycle.phase != expected(cycle.index) {
            violations.push(format!(
                "cycle {} is journalled as {} and the declared window makes it {}",
                cycle.index,
                cycle.phase,
                expected(cycle.index),
            ));
        }
    }
    for index in 0..cycles.len() as u64 {
        if !seen.contains(&index) {
            violations.push(format!("cycle {index} is missing from the journal"));
        }
    }

    for (position, sample) in samples.iter().enumerate() {
        let at = position;
        let position = position as u64;
        let index = sample.get("cycle").and_then(Value::as_u64);
        if index != Some(position) {
            violations.push(format!(
                "the sample at position {position} records cycle {index:?}, so the samples are \
                 not in the order they were taken"
            ));
        }
        let phase = sample.get("phase").and_then(Value::as_str);
        match phase {
            Some(phase) if phase == expected(position) => {}
            Some(phase) if phase == "warmup" || phase == "measured" => violations.push(format!(
                "the sample for cycle {position} is phased {phase} and the declared window makes \
                 it {}",
                expected(position),
            )),
            Some(phase) => violations.push(format!(
                "the sample for cycle {position} is phased {phase}, which is not a phase this \
                 campaign has"
            )),
            None => violations.push(format!("the sample for cycle {position} records no phase")),
        }
        if let Some(cycle) = cycles.get(at)
            && phase.is_some_and(|phase| phase != cycle.phase)
        {
            violations.push(format!(
                "cycle {position} is journalled as {} and sampled as {}",
                cycle.phase,
                phase.unwrap_or("nothing"),
            ));
        }
    }

    violations
}

/// The lifecycle totals a run performed, folded from its cycles.
pub struct Lifecycle {
    pub cycles: u64,
    pub faults: u64,
    pub restarts: u64,
    pub recoveries: u64,
    pub drains: u64,
    pub partition_executions: u64,
}

/// Folds the lifecycle totals out of the raw cycles.
///
/// The report carries the same numbers in its summary. They are compared
/// against these and never used in their place: a summary is a claim about the
/// journal, and the journal is the observation.
pub fn fold_lifecycle(cycles: &[Cycle]) -> Lifecycle {
    Lifecycle {
        cycles: cycles.len() as u64,
        faults: cycles
            .iter()
            .filter(|cycle| cycle.failed_status == "Failed" && !cycle.fault_wait_expired)
            .count() as u64,
        restarts: cycles
            .iter()
            .filter(|cycle| !cycle.re_run.is_empty() || cycle.recovered)
            .count() as u64,
        recoveries: cycles.iter().filter(|cycle| cycle.recovered).count() as u64,
        drains: cycles
            .iter()
            .filter(|cycle| cycle.drain == "complete" && cycle.unjoined == 0 && cycle.panicked == 0)
            .count() as u64,
        partition_executions: cycles
            .iter()
            .map(|cycle| cycle.invocations.values().sum::<u64>())
            .sum(),
    }
}

/// Requires every cycle to have performed the whole P-015 lifecycle.
///
/// Totals are not the property. A run of six hundred launches and thirty-two
/// restarts folds to the same partition count as one where every cycle
/// restarted, so each cycle is required to have injected a fault, restarted,
/// recovered and drained — individually.
pub fn reconcile_lifecycle(
    lifecycle: &Lifecycle,
    observation: &Value,
    cycles: &[Cycle],
) -> Vec<String> {
    let mut violations = Vec::new();

    for cycle in cycles {
        let index = cycle.index;
        if cycle.failed_status != "Failed" || cycle.fault_wait_expired {
            violations.push(format!(
                "cycle {index} did not reach an injected failure, so it is not the P-015 lifecycle"
            ));
        }
        if !cycle.recovered {
            violations.push(format!(
                "cycle {index} did not restart onto the same instance"
            ));
        }
        if cycle.re_run.is_empty() {
            violations.push(format!("cycle {index} restarted and re-ran nothing"));
        }
        if cycle.drain != "complete" || cycle.unjoined != 0 || cycle.panicked != 0 {
            violations.push(format!(
                "cycle {index} drained as {} with {} unjoined and {} panicked",
                cycle.drain, cycle.unjoined, cycle.panicked,
            ));
        }
    }

    for (field, folded) in [
        ("completed_cycles", lifecycle.cycles),
        ("faults_injected", lifecycle.faults),
        ("restarts", lifecycle.restarts),
        ("recoveries", lifecycle.recoveries),
        ("drains_completed", lifecycle.drains),
        ("partitions_executed", lifecycle.partition_executions),
    ] {
        let claimed = observation
            .pointer(&format!("/campaign/{field}"))
            .and_then(Value::as_u64);
        if claimed != Some(folded) {
            violations.push(format!(
                "the report summarises {field} as {claimed:?} and folding the journal gives \
                 {folded}"
            ));
        }
    }

    violations
}

/// Reads a required string out of a value.
fn string(value: &Value, pointer: &str) -> Result<String, String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("a journalled cycle records no {pointer}"))
}

/// Reads a required count out of a value.
fn number(value: &Value, pointer: &str) -> Result<u64, String> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("a journalled cycle records no {pointer}"))
}

/// Reads a required set of partition keys out of a value.
fn keys(value: &Value, pointer: &str, index: u64) -> Result<BTreeSet<String>, String> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("cycle {index} records no {pointer}"))?
        .iter()
        .map(|key| {
            key.as_str().map(str::to_owned).ok_or_else(|| {
                format!("cycle {index} records a partition key that is not a string")
            })
        })
        .collect()
}

/// Renders a nested value canonically, for comparison against a baseline.
fn render(value: Option<&Value>) -> Result<String, String> {
    let value = value.ok_or_else(|| "a journalled cycle is missing a field".to_owned())?;
    serde_json::to_string(value).map_err(|error| format!("could not render a field: {error}"))
}

/// The workload the obligations are decided against.
pub struct Workload {
    pub partitions_per_cycle: u64,
    pub worker_budget: u64,
}

/// Recomputes every correctness obligation from the raw journal.
///
/// The report decides these too, and its answers are compared against these
/// rather than trusted. The comparison is on the *failing cycle set* and not
/// only on the boolean: a report that held an obligation to be true while the
/// journal shows it violated in cycle 417 is caught, and so is one that claims
/// it failed somewhere it did not.
///
/// Every obligation is decided against the first measured cycle, which is the
/// baseline the campaign declares — and which is only well defined once the
/// chronology check has established that the first measured cycle is where it
/// should be.
#[allow(
    clippy::too_many_lines,
    reason = "each obligation is one named decision, and reading them as one list is what lets \
              the declared set be reconciled against the decided set"
)]
pub fn recompute_correctness(workload: &Workload, cycles: &[Cycle]) -> BTreeMap<String, Vec<u64>> {
    let mut decided: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let measured = cycles
        .iter()
        .filter(|cycle| cycle.phase == "measured")
        .collect::<Vec<_>>();
    let Some(baseline) = measured.first() else {
        return decided;
    };
    let partitions = workload.partitions_per_cycle;

    for cycle in &measured {
        let index = cycle.index;
        let terminal = &cycle.terminal;
        let base = &baseline.terminal;
        let mut check = |id: &str, holds: bool| {
            let offenders = decided.entry(id.to_owned()).or_default();
            if !holds {
                offenders.push(index);
            }
        };

        check(
            "final-job-status",
            terminal.job_status == base.job_status
                && terminal.job_exit_status == base.job_exit_status
                && terminal.outcome == base.outcome
                && terminal.job_status == "Completed",
        );
        check(
            "final-step-status",
            terminal.parent_status == base.parent_status
                && terminal.parent_exit_status == base.parent_exit_status
                && terminal.parent_status == "Completed",
        );
        check(
            "execution-counts",
            terminal.parent_counts == base.parent_counts
                && terminal.step_executions == base.step_executions,
        );
        check(
            "partition-count",
            terminal.partitions.len() as u64 == partitions,
        );
        check(
            "partition-key-set",
            terminal.partitions.keys().eq(base.partitions.keys()),
        );
        check(
            "partition-terminal-state",
            terminal.partitions.iter().all(|(key, partition)| {
                partition.contains("Completed") && base.partitions.get(key) == Some(partition)
            }),
        );
        check(
            "restart-position",
            cycle.committed.len() as u64 == partitions - 1
                && !cycle.committed.contains(&cycle.injected)
                && cycle.re_run.len() == 1
                && cycle.re_run.contains(&cycle.injected),
        );
        check(
            "committed-work-reused",
            !cycle.committed.is_empty()
                && cycle
                    .committed
                    .iter()
                    .all(|key| !cycle.re_run.contains(key)),
        );
        check(
            "no-duplicate-durable-work",
            terminal.partitions.len() == cycle.invocations.len()
                && cycle
                    .invocations
                    .iter()
                    .all(|(key, count)| *count == u64::from(*key == cycle.injected) + 1),
        );
        check(
            "no-missing-durable-work",
            cycle.history == baseline.history
                && cycle.history.get("instances") == Some(&1)
                && cycle.history.get("executions") == Some(&2)
                && cycle.invocations.len() as u64 == partitions,
        );
        check(
            "failure-not-forged",
            cycle.failed_status == "Failed"
                && cycle.failed_outcome.starts_with("Failed")
                && !cycle.fault_wait_expired,
        );
        check("recovery-semantics", cycle.recovered);
        check(
            "no-worker-outlives-its-parent",
            cycle.worker_residue == 0 && cycle.worker_peak <= workload.worker_budget,
        );
        check(
            "drain-complete",
            cycle.drain == "complete" && cycle.unjoined == 0 && cycle.panicked == 0,
        );
        check(
            "constant-repository-work",
            cycle.transactions == baseline.transactions,
        );
    }

    decided
}

/// Requires the report's correctness result to be the one the journal implies.
///
/// The declared, decided and recomputed sets are reconciled as unique sets in
/// all three directions. Duplicates are rejected outright rather than
/// deduplicated: a checks array carrying the same identity twice has no single
/// answer, and which one a reader believes depends on which one they reach
/// first — the last-wins reading is exactly the forgery this rejects.
pub fn reconcile_correctness(
    declared: &[String],
    recomputed: &BTreeMap<String, Vec<u64>>,
    observation: &Value,
) -> Vec<String> {
    let mut violations = Vec::new();
    let checks = observation
        .pointer("/correctness/checks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut reported: BTreeMap<String, &Value> = BTreeMap::new();
    for check in &checks {
        let Some(id) = check.get("id").and_then(Value::as_str) else {
            violations
                .push("the report decided a correctness obligation with no identity".to_owned());
            continue;
        };
        if reported.insert(id.to_owned(), check).is_some() {
            violations.push(format!(
                "the report decided {id} more than once, so its answer for that obligation \
                 depends on which entry is read"
            ));
        }
    }

    let declared_set = declared.iter().cloned().collect::<BTreeSet<_>>();
    let reported_set = reported.keys().cloned().collect::<BTreeSet<_>>();
    let recomputed_set = recomputed.keys().cloned().collect::<BTreeSet<_>>();

    for id in declared_set.difference(&reported_set) {
        violations.push(format!(
            "the campaign declares the {id} obligation and the report decided nothing for it"
        ));
    }
    for id in reported_set.difference(&declared_set) {
        violations.push(format!(
            "the report decided {id}, which the campaign scope does not declare"
        ));
    }
    for id in declared_set.difference(&recomputed_set) {
        violations.push(format!(
            "the campaign declares the {id} obligation and this runner cannot recompute it from \
             the journal, so the report's answer for it would stand unchecked"
        ));
    }

    for (id, offenders) in recomputed {
        let Some(check) = reported.get(id) else {
            continue;
        };
        let holds = check.get("holds").and_then(Value::as_bool);
        if holds != Some(offenders.is_empty()) {
            violations.push(format!(
                "the report holds {id} to be {holds:?} and the journal makes it {} — it is \
                 violated in cycle(s) {offenders:?}",
                offenders.is_empty(),
            ));
        }
        let claimed = check
            .get("failing_cycles")
            .and_then(Value::as_array)
            .map(|values| values.iter().filter_map(Value::as_u64).collect::<Vec<_>>());
        if claimed.as_deref() != Some(offenders.as_slice()) {
            violations.push(format!(
                "the report records {id} as failing in {claimed:?} and the journal makes it \
                 {offenders:?}"
            ));
        }
        if !offenders.is_empty() {
            violations.push(format!(
                "{id} does not hold in cycle(s) {offenders:?}; a soak whose durable record \
                 changes is a failure whatever its resource trajectory was"
            ));
        }
    }

    let passed = recomputed.values().all(Vec::is_empty);
    if observation
        .pointer("/correctness/passed")
        .and_then(Value::as_bool)
        != Some(passed)
    {
        violations.push(format!(
            "the report's overall correctness result disagrees with the journal, which makes it \
             {passed}"
        ));
    }

    violations
}
