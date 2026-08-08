//! Mechanics the M5 campaign runners share.
//!
//! Two runners drive the workspace's own test targets and retain a report about
//! what ran: `cargo xtask conformance` and `cargo xtask crash-restore`. They
//! ask different questions, but they run targets, read libtest output, resolve
//! a report directory, and describe their environment the same way, and those
//! four things are stated once here.
//!
//! Reading libtest output is the part that most needs one owner. libtest writes
//! `test <name> ... ` before a test runs and its outcome after, on one line, so
//! ordinarily the two arrive together. They do not when the test spawns a child
//! that writes to the same descriptor, which is how every process-kill scenario
//! works: the child's libtest header lands between the prefix and the outcome.
//! Reading only whole lines loses those results as "did not run", and both
//! campaigns are mostly made of them.

use std::collections::BTreeMap;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::{Map, Value, json};

/// Directory used when no explicit campaign directory is configured.
///
/// Retained evidence is written only when `OXIDEBATCH_CAMPAIGN_DIR` names a
/// destination, so an ordinary run never rewrites a committed record.
pub const DEFAULT_DIRECTORY: &str = "target/m5-campaigns";

/// The supported-matrix point a run covers.
///
/// A runner cannot see the database version through a fixture, which is a
/// connection string it never opens. Without this, two reports from two matrix
/// points are byte-identical and neither says which one it is.
pub const MATRIX: &str = "OXIDEBATCH_CAMPAIGN_MATRIX";

/// One target invocation and what libtest reported for it.
#[derive(Default)]
pub struct TargetRun {
    /// The outcome libtest reported for each test path that ran.
    pub results: BTreeMap<String, String>,
    /// Whether the target process exited successfully.
    pub succeeded: bool,
}

/// One cargo test invocation a campaign makes.
pub struct TargetCommand<'a> {
    /// The workspace package that owns the target.
    pub package: &'a str,
    /// The cargo arguments that select the target.
    pub selector: &'a [String],
    /// The libtest arguments that select tests inside it.
    pub filters: &'a [&'a str],
    /// Environment the target needs, beyond what this process already has.
    pub environment: &'a [(&'a str, String)],
    /// Whether the target's own output is left uncaptured.
    pub nocapture: bool,
}

/// Runs one test target and attributes every result libtest reports.
///
/// Targets run through cargo rather than as compiled executables, and one at a
/// time with a single test thread. Cargo, because a test can depend on the
/// environment cargo supplies. One at a time, because that is what attributes a
/// result: several scenario names exist in more than one target.
///
/// # Errors
///
/// Returns the failure that prevented the target from running or being read.
pub fn run_target(root: &Path, target: &TargetCommand<'_>) -> Result<TargetRun, String> {
    let mut command = Command::new("cargo");
    command
        .current_dir(root)
        .args(["test", "--package", target.package, "--all-features"])
        .args(target.selector)
        .arg("--")
        .args(target.filters)
        .args(["--test-threads", "1"]);
    if target.nocapture {
        command.arg("--nocapture");
    }
    for (name, value) in target.environment {
        command.env(name, value);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::inherit());

    let mut child = command
        .spawn()
        .map_err(|error| format!("could not run {}: {error}", target.package))?;
    let reader = child
        .stdout
        .take()
        .ok_or_else(|| format!("{} produced no output", target.package))?;

    let mut run = TargetRun::default();
    let mut running: Option<String> = None;
    for line in BufReader::new(reader).lines() {
        let line = line.map_err(|error| format!("could not read {}: {error}", target.package))?;
        eprintln!("{line}");

        let (name, outcome) = match observe(&line) {
            Some(Observation::Started(name)) => {
                running = Some(name);
                continue;
            }
            Some(Observation::Finished(name, outcome)) => {
                running = None;
                (name, outcome)
            }
            Some(Observation::Outcome(outcome)) => match running.take() {
                Some(name) => (name, outcome),
                None => continue,
            },
            None => continue,
        };
        run.results.insert(name, outcome);
    }

    let status = child
        .wait()
        .map_err(|error| format!("could not wait for {}: {error}", target.package))?;
    run.succeeded = status.success();
    Ok(run)
}

/// The outcomes libtest reports for one test.
const OUTCOMES: &[&str] = &["ok", "FAILED", "ignored"];

/// What one line of a test target's output says about a test.
pub enum Observation {
    /// A test started and its outcome has not been printed yet.
    Started(String),
    /// A test started and finished on the same line.
    Finished(String, String),
    /// The outcome of the test that is still running.
    Outcome(String),
}

/// Reads one line of a test target's output.
///
/// The prefix and the outcome are read separately because they do not always
/// arrive together; see this module's own documentation for why.
#[must_use]
pub fn observe(line: &str) -> Option<Observation> {
    if let Some(rest) = line.strip_prefix("test ") {
        if let Some((name, outcome)) = rest.rsplit_once(" ... ") {
            if name.contains(' ') {
                return None;
            }
            let name = name.to_owned();
            return match outcome.split_whitespace().next() {
                None => Some(Observation::Started(name)),
                Some(outcome) if OUTCOMES.contains(&outcome) => {
                    Some(Observation::Finished(name, outcome.to_lowercase()))
                }
                Some(_) => None,
            };
        }
        return None;
    }

    let trimmed = line.trim();
    OUTCOMES
        .contains(&trimmed)
        .then(|| Observation::Outcome(trimmed.to_lowercase()))
}

/// Resolves the report directory against the workspace root.
#[must_use]
pub fn directory(root: &Path) -> PathBuf {
    let configured =
        env::var("OXIDEBATCH_CAMPAIGN_DIR").unwrap_or_else(|_| DEFAULT_DIRECTORY.to_owned());
    let configured = PathBuf::from(configured);
    if configured.is_absolute() {
        return configured;
    }
    root.join(configured)
}

/// Records the environment a campaign result depends on.
#[must_use]
pub fn environment() -> Value {
    let mut map = Map::new();
    map.insert(
        "source_commit".into(),
        json!(command("git", &["rev-parse", "HEAD"])),
    );
    map.insert(
        "source_tree_clean".into(),
        json!(command("git", &["status", "--porcelain"]).map(|status| status.is_empty())),
    );
    map.insert("rustc".into(), json!(command("rustc", &["--version"])));
    map.insert("os".into(), json!(env::consts::OS));
    map.insert("arch".into(), json!(env::consts::ARCH));
    map.insert("profile".into(), json!("debug"));
    map.insert("matrix".into(), json!(env::var(MATRIX).ok()));
    Value::Object(map)
}

/// Runs one environment-describing command, tolerating an absent tool.
#[must_use]
pub fn command(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Returns the workspace root.
///
/// # Errors
///
/// Returns the failure when cargo cannot describe the workspace.
pub fn workspace_root() -> Result<PathBuf, String> {
    let metadata = metadata()?;
    metadata
        .get("workspace_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| "cargo metadata returned no workspace root".to_owned())
}

/// Reads the workspace metadata.
///
/// # Errors
///
/// Returns the failure when cargo cannot describe the workspace.
pub fn metadata() -> Result<Value, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .map_err(|error| format!("could not run cargo metadata: {error}"))?;

    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("could not parse cargo metadata: {error}"))
}

/// Reads one required string field of a campaign scope document.
///
/// # Errors
///
/// Returns the failure naming the missing field.
pub fn string(value: &Value, name: &str) -> Result<String, String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("a scope entry has no {name}"))
}

#[cfg(test)]
mod tests {
    use super::{Observation, observe};

    /// Replays one target's output and returns what a runner recorded.
    fn record(lines: &[&str]) -> Vec<(String, String)> {
        let mut results = Vec::new();
        let mut running: Option<String> = None;

        for line in lines {
            match observe(line) {
                Some(Observation::Started(name)) => running = Some(name),
                Some(Observation::Finished(name, outcome)) => {
                    running = None;
                    results.push((name, outcome));
                }
                Some(Observation::Outcome(outcome)) => {
                    if let Some(name) = running.take() {
                        results.push((name, outcome));
                    }
                }
                None => {}
            }
        }

        results
    }

    #[test]
    fn an_ordinary_result_is_read_from_one_line() {
        assert_eq!(
            record(&[
                "running 2 tests",
                "test cases::restart_creates_new_execution ... ok",
                "test a_skipped_case ... ignored",
                "test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out",
            ]),
            vec![
                (
                    "cases::restart_creates_new_execution".to_owned(),
                    "ok".to_owned()
                ),
                ("a_skipped_case".to_owned(), "ignored".to_owned()),
            ],
        );
    }

    #[test]
    fn a_result_split_by_a_child_process_is_still_attributed() {
        assert_eq!(
            record(&[
                "running 3 tests",
                "test crash_after_commit_does_not_replay_chunk ... ",
                "",
                "running 1 test",
                "ok",
                "test crash_worker_process ... ok",
            ]),
            vec![
                (
                    "crash_after_commit_does_not_replay_chunk".to_owned(),
                    "ok".to_owned()
                ),
                ("crash_worker_process".to_owned(), "ok".to_owned()),
            ],
        );
    }

    #[test]
    fn a_failure_split_by_a_child_process_is_not_read_as_a_pass() {
        assert_eq!(
            record(&[
                "test crash_before_commit_replays_chunk ... ",
                "running 1 test",
                "FAILED",
            ]),
            vec![(
                "crash_before_commit_replays_chunk".to_owned(),
                "failed".to_owned()
            )],
        );
    }

    #[test]
    fn program_output_is_never_read_as_a_result() {
        assert!(
            record(&[
                "test the operator command with two words ... ok",
                "test something ... maybe",
                "ok",
                "testing the connection",
            ])
            .is_empty()
        );
    }
}
