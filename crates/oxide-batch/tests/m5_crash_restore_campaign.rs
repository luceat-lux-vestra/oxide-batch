//! Scope reconciliation for the M5 crash and restore campaign.
//!
//! The campaign has the same two halves the conformance campaign has, split for
//! the same reason:
//!
//! - **what the campaign owes, and which scenario proves each part of it.**
//!   That is a reconciliation between the accepted
//!   [performance plan](../../../docs/engineering/performance-plan.md), the
//!   [design gate](../../../docs/project/m5-design-gate-evidence.md), the
//!   committed scope document, and the targets this workspace declares. It runs
//!   here, in an ordinary `cargo test`, so a shrinking denominator is caught in
//!   review rather than in the campaign.
//! - **whether the campaign passes.** Every scenario it runs needs a real
//!   database and returns green without one, because it skips. That half is
//!   `cargo xtask crash-restore`, which requires the fixtures, runs the
//!   targets, requires the per-phase observations, and writes the retained
//!   report.
//!
//! The scope document is `tests/fixtures/crash-restore/campaign-scope.json` at
//! the workspace root. Both halves read it, so the phase set, the reports, and
//! the reused M2-M4 evidence are stated once.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

/// The commit-protocol phases this campaign must kill a process in.
///
/// The list is here rather than only in the scope document on purpose. The
/// document says what the runner requires; this says what review accepted, and
/// a phase can only leave the campaign by changing both.
const REQUIRED_PHASES: &[&str] = &[
    "business-written",
    "state-provided",
    "progress-blocked",
    "commit-in-flight",
    "commit-acknowledged",
];

/// The reports the performance plan's crash and restore row requires.
const REQUIRED_REPORTS: &[&str] = &[
    "commit-phase-process-kill",
    "p013-restart-after-many-chunks",
    "logical-backup-restore",
];

/// The scenario the M5 design gate names for this campaign.
const NAMED_SCENARIO: &str = "process_kill_at_each_commit_phase_recovers_without_a_forged_status";

#[test]
fn campaign_scope_matches_the_accepted_crash_and_restore_obligations() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::read()?;

    assert_eq!(
        scope
            .phases
            .iter()
            .map(|phase| phase.id.as_str())
            .collect::<Vec<_>>(),
        REQUIRED_PHASES,
        "the campaign kills a process in every phase of the commit protocol, in commit order",
    );
    assert_eq!(
        scope
            .reports
            .iter()
            .map(|report| report.id.as_str())
            .collect::<BTreeSet<_>>(),
        REQUIRED_REPORTS.iter().copied().collect::<BTreeSet<_>>(),
        "the campaign delivers exactly the reports the performance plan requires",
    );
    for phase in &scope.phases {
        assert!(
            scope.reports.iter().any(|report| report.id == phase.report),
            "{} names {}, which is not a report this campaign delivers",
            phase.id,
            phase.report,
        );
        assert_eq!(
            phase.termination, "SIGKILL",
            "{}: the campaign kills a live process rather than asking one to exit",
            phase.id,
        );
    }

    let gate = read_document("docs/project/m5-design-gate-evidence.md")?;
    assert!(
        gate.contains(NAMED_SCENARIO),
        "the design gate must still name {NAMED_SCENARIO} for the evidence campaigns",
    );
    let plan = read_document("docs/engineering/performance-plan.md")?;
    let row = plan
        .lines()
        .find(|line| line.starts_with("| Crash and restore |"))
        .ok_or_else(|| Failure("the performance plan has no crash and restore row".to_owned()))?;
    for owed in [
        "P-013",
        "process-kill at each commit phase",
        "logical backup",
    ] {
        assert!(
            row.contains(owed),
            "the performance plan's crash and restore row no longer requires {owed}, so the \
             campaign's denominator and the accepted plan disagree",
        );
    }
    for report in &scope.reports {
        assert!(
            row.contains(&report.owes) || gate.contains(&report.owes),
            "{} claims to discharge {}, which neither the plan nor the gate requires",
            report.id,
            report.owes,
        );
    }

    Ok(())
}

#[test]
fn every_declared_scenario_resolves_to_a_test_this_workspace_declares() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::read()?;

    for scenario in scope
        .reports
        .iter()
        .map(|report| &report.scenario)
        .chain(&scope.reused)
    {
        assert!(
            scope.fixtures.contains(&scenario.fixture),
            "{} needs an undeclared fixture: {}",
            scenario.name,
            scenario.fixture,
        );
        let source = workspace_root()
            .join("crates")
            .join(&scenario.package)
            .join("tests")
            .join(format!("{}.rs", scenario.target));
        let declared = fs::read_to_string(&source)
            .map_err(|error| Failure(format!("could not read {}: {error}", source.display())))?;
        assert!(
            declared.contains(&format!("fn {}(", scenario.name)),
            "{} declares no test named {}",
            source.display(),
            scenario.name,
        );
    }

    let phases = fs::read_to_string(
        workspace_root()
            .join("crates")
            .join("oxide-batch")
            .join("tests")
            .join("postgres_commit_phase_process_kill.rs"),
    )?;
    for phase in REQUIRED_PHASES {
        assert!(
            phases.contains(&format!("\"{phase}\"")),
            "the process-kill target no longer implements the {phase} phase",
        );
    }

    Ok(())
}

#[test]
fn reused_evidence_covers_every_durable_write_protocol_m2_to_m4_delivered()
-> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let protocols = scope
        .reused
        .iter()
        .map(|scenario| scenario.protocol.as_str())
        .collect::<BTreeSet<_>>();

    for protocol in [
        "chunk commit",
        "retry reservation",
        "skip commit",
        "flow decision",
        "partition aggregation",
        "split join",
        "unknown commit outcome",
    ] {
        assert!(
            protocols.contains(protocol),
            "the campaign reuses no M2-M4 evidence for the {protocol} protocol, so its \
             denominator is narrower than the durable writes the milestones delivered",
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------
// Semantic closure. The producer records the object identity of every path
// listed in tests/fixtures/crash-restore/campaign-semantics.json from inside
// its own checkout, and the offline evidence verifier requires those
// identities to still hold. Neither restates the list here; this proves the
// closure actually covers what the campaign runs and excludes what it must
// not — mirroring the coverage the conformance campaign's own closure test
// proves, over this campaign's own denominator.
// ---------------------------------------------------------------------

/// Every other M5 campaign's own reconciliation/contract test, plus this
/// campaign's own. None of them is a scenario this campaign runs, and their
/// inclusion in the closure would either create a retention-time
/// self-reference (`m5_campaign_record` reads
/// `docs/project/m5-campaign-evidence.md`, rewritten with a report's own
/// provenance after the report is produced) or bind this campaign's evidence
/// to another campaign's fixtures.
const GOVERNANCE_TARGETS: &[&str] = &[
    "m5_campaign_record",
    "m5_cancellation_campaign",
    "m5_conformance_campaign",
    "m5_crash_restore_campaign",
    "m5_performance_campaign",
    "m5_resource_bounds_campaign",
    "m5_security_campaign",
    "m5_soak_campaign",
    "m5_upgrade_campaign",
];

#[test]
fn the_semantic_closure_covers_what_the_campaign_runs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let paths = closure_paths()?;

    let mut required_targets = BTreeSet::new();
    for scenario in scope
        .reports
        .iter()
        .map(|report| &report.scenario)
        .chain(&scope.reused)
    {
        required_targets.insert(("oxide-batch".to_owned(), scenario.target.clone()));
    }
    assert!(
        !required_targets.is_empty(),
        "the campaign scope named no required target, so this test checks nothing",
    );

    for (package, target) in &required_targets {
        let relative = format!("crates/{package}/tests/{target}.rs");
        assert!(
            covered(&paths, &relative),
            "{relative} backs a report or reused scenario in package {package}, and is not \
             covered by any path in the campaign's semantic closure",
        );
    }

    for governance in GOVERNANCE_TARGETS {
        assert!(
            !required_targets.contains(&("oxide-batch".to_owned(), (*governance).to_owned())),
            "{governance} is a governance test, not a crash-restore scenario, and must not be \
             part of the campaign's required-target set",
        );
    }

    for excluded in [
        "docs/project/m5-campaign-evidence.md",
        "tests/fixtures/soak/campaign-scope.json",
        "tests/fixtures/soak/campaign-semantics.json",
        "tests/fixtures/conformance/accepted-scope.json",
        "tests/fixtures/conformance/campaign-semantics.json",
    ] {
        assert!(
            !paths.iter().any(|path| path == excluded),
            "{excluded} must not be in the crash-restore closure: including it would either \
             create a retention-time self-reference or bind crash-restore evidence to another \
             campaign's fixtures",
        );
    }

    for required in [
        "crates/oxide-batch/src",
        "tests/fixtures/crash-restore/campaign-scope.json",
        "xtask/src/crash_restore.rs",
        "xtask/src/evidence.rs",
        "Cargo.lock",
        "rust-toolchain.toml",
        ".github/workflows/m5-crash-restore.yml",
        "tests/fixtures/crash-restore/execution-contract.json",
        "tests/fixtures/crash-restore/run-ci-campaign.sh",
        "tests/fixtures/crash-restore/verify-ci-contract.sh",
    ] {
        assert!(
            paths.iter().any(|path| path == required),
            "{required} is not in the campaign's semantic closure, so a change to it would leave \
             retained evidence looking valid when it is evidence of something else",
        );
    }

    assert!(
        !paths.iter().any(|path| path == ".github/workflows/ci.yml"),
        "ci.yml is unrelated to the dedicated crash-restore campaign and must not invalidate its \
         evidence",
    );

    for path in &paths {
        assert!(
            workspace_root().join(path).exists(),
            "{path} is declared as campaign semantics and does not exist, so the producer cannot \
             record its object identity",
        );
    }
    Ok(())
}

/// Returns every path the campaign's semantic closure declares.
fn closure_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let closure: Value = serde_json::from_str(&read_document(
        "tests/fixtures/crash-restore/campaign-semantics.json",
    )?)?;
    Ok(closure
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure("the closure declares no categories".to_owned()))?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>())
}

/// Reports whether a repository-relative path is covered by the closure:
/// named exactly, or nested under a closure path that names a directory.
fn covered(paths: &[String], candidate: &str) -> bool {
    paths
        .iter()
        .any(|path| path == candidate || candidate.starts_with(&format!("{path}/")))
}

// ---------------------------------------------------------------------
// Contract-check exactness. `verify-ci-contract.sh` binds the dedicated
// workflow and `run-ci-campaign.sh` by exact git blob identity, not by the
// literal presence checks alone. These tests drive the real script against
// an isolated sandbox copy of those files, so a mutation proves the
// checker's actual behaviour rather than one helper's return value, and
// never touches the repository working tree.
// ---------------------------------------------------------------------

#[test]
fn contract_check_passes_on_the_canonical_workflow_and_script() -> Result<(), Box<dyn Error>> {
    assert!(run_crash_restore_contract_check(|_sandbox| Ok(()))?);
    Ok(())
}

#[test]
fn contract_check_fails_on_an_added_trigger() -> Result<(), Box<dyn Error>> {
    let passed = run_crash_restore_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-crash-restore.yml"),
            "  workflow_dispatch:\n",
            "  schedule:\n    - cron: '0 0 * * *'\n",
        )
    })?;
    assert!(
        !passed,
        "an added trigger must fail the contract check even though every expected trigger is \
         still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_widened_matrix() -> Result<(), Box<dyn Error>> {
    let passed = run_crash_restore_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-crash-restore.yml"),
            "postgres: [\"15\", \"18\"]\n",
            "        include:\n          - postgres: \"16\"\n",
        )
    })?;
    assert!(
        !passed,
        "an additional matrix execution point must fail even though the literal \
         postgres: [\"15\", \"18\"] declaration is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_timeout() -> Result<(), Box<dyn Error>> {
    let passed = run_crash_restore_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-crash-restore.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace("timeout-minutes: 45", "timeout-minutes: 5");
        assert_ne!(
            source, mutated,
            "the timeout literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed timeout must fail the contract check even though every other literal is \
         unchanged",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_report_or_artifact_path() -> Result<(), Box<dyn Error>> {
    let passed = run_crash_restore_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-crash-restore.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace(
            "path: target/m5-campaigns/crash-restore-campaign.json",
            "path: target/m5-campaigns/crash-restore-campaign-renamed.json",
        );
        assert_ne!(
            source, mutated,
            "the report path literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed retained-report path must fail even though the producer command is unchanged",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_an_appended_script_command() -> Result<(), Box<dyn Error>> {
    let passed = run_crash_restore_contract_check(|sandbox| {
        append_line(
            &sandbox.join("tests/fixtures/crash-restore/run-ci-campaign.sh"),
            "echo \"extra command\"",
        )
    })?;
    assert!(
        !passed,
        "an appended command must fail even though the expected cargo command is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_harmless_comment_byte() -> Result<(), Box<dyn Error>> {
    let passed = run_crash_restore_contract_check(|sandbox| {
        append_line(
            &sandbox.join(".github/workflows/m5-crash-restore.yml"),
            "# harmless comment",
        )
    })?;
    assert!(
        !passed,
        "exact git blob identity, not heuristic literal parsing, is the retained-evidence \
         boundary: even a harmless trailing comment must fail",
    );
    Ok(())
}

/// Copies the real workflow, script, and contract into an isolated sandbox,
/// applies `mutate` to that sandbox, then runs the real `verify-ci-contract.sh`
/// against the (possibly mutated) copy and reports whether it exited zero.
fn run_crash_restore_contract_check(
    mutate: impl FnOnce(&Path) -> Result<(), Box<dyn Error>>,
) -> Result<bool, Box<dyn Error>> {
    let root = workspace_root();
    let sandbox = Sandbox::new("crash-restore-contract-check")?;

    let workflow_dir = sandbox.path().join(".github/workflows");
    fs::create_dir_all(&workflow_dir)?;
    fs::copy(
        root.join(".github/workflows/m5-crash-restore.yml"),
        workflow_dir.join("m5-crash-restore.yml"),
    )?;

    let fixture_dir = sandbox.path().join("tests/fixtures/crash-restore");
    fs::create_dir_all(&fixture_dir)?;
    for name in [
        "execution-contract.json",
        "run-ci-campaign.sh",
        "verify-ci-contract.sh",
    ] {
        fs::copy(
            root.join("tests/fixtures/crash-restore").join(name),
            fixture_dir.join(name),
        )?;
    }
    let checker = fixture_dir.join("verify-ci-contract.sh");
    let mut permissions = fs::metadata(&checker)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&checker, permissions)?;

    mutate(sandbox.path())?;

    exec_contract_checker(
        &checker,
        ".github/workflows/m5-crash-restore.yml",
        sandbox.path(),
    )
}

/// Runs a freshly copied and `chmod`-ed contract-check script, retrying on a
/// transient `ExecutableFileBusy`.
///
/// Immediately exec'ing a file this test process just wrote and `chmod`ed
/// can race the kernel's release of the write mapping under heavy parallel
/// `cargo test` fork/exec load, surfacing as `ETXTBSY` even though the file
/// is complete and correctly permissioned. Each sandbox path is unique per
/// test, so this is never a real conflict — retry briefly before treating it
/// as a genuine failure.
fn exec_contract_checker(checker: &Path, arg: &str, cwd: &Path) -> Result<bool, Box<dyn Error>> {
    let mut attempt = 0u32;
    loop {
        match Command::new(checker).arg(arg).current_dir(cwd).status() {
            Ok(status) => return Ok(status.success()),
            Err(err) if err.kind() == io::ErrorKind::ExecutableFileBusy && attempt < 5 => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(20 * u64::from(attempt)));
            }
            Err(err) => return Err(Box::new(err)),
        }
    }
}

/// Inserts `insertion` immediately after the first occurrence of `anchor`.
fn insert_after(path: &Path, anchor: &str, insertion: &str) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    let position = contents.find(anchor).ok_or_else(|| {
        Box::new(Failure(format!(
            "no {anchor:?} anchor found in {}",
            path.display()
        ))) as Box<dyn Error>
    })?;
    let insert_at = position + anchor.len();
    let mut mutated = String::with_capacity(contents.len() + insertion.len());
    mutated.push_str(&contents[..insert_at]);
    mutated.push_str(insertion);
    mutated.push_str(&contents[insert_at..]);
    fs::write(path, mutated)?;
    Ok(())
}

/// Appends one line to a file.
fn append_line(path: &Path, line: &str) -> Result<(), Box<dyn Error>> {
    let mut contents = fs::read_to_string(path)?;
    contents.push('\n');
    contents.push_str(line);
    contents.push('\n');
    fs::write(path, contents)?;
    Ok(())
}

/// A uniquely named temporary directory, removed when it goes out of scope
/// regardless of how the test exits.
struct Sandbox(PathBuf);

impl Sandbox {
    fn new(label: &str) -> Result<Self, Box<dyn Error>> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "oxide-batch-{label}-{}-{unique}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The committed campaign scope document.
struct Scope {
    /// The fixture names the document declares.
    fixtures: BTreeSet<String>,
    /// The reports the campaign delivers.
    reports: Vec<Report>,
    /// The commit-protocol phases the campaign kills a process in.
    phases: Vec<Phase>,
    /// The M2-M4 scenarios the campaign reuses rather than rewrites.
    reused: Vec<Scenario>,
}

/// One report the campaign delivers.
struct Report {
    /// The identifier the runner and the retained observation share.
    id: String,
    /// The obligation the report discharges, as the plan or gate words it.
    owes: String,
    /// The scenario that produces it.
    scenario: Scenario,
}

/// One phase of a durable write protocol a process is killed in.
struct Phase {
    /// The identifier the report and the implementation share.
    id: String,
    /// The report that covers the phase.
    report: String,
    /// How the process is required to die.
    termination: String,
}

/// One executable scenario the campaign runs or reuses.
struct Scenario {
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test name libtest reports.
    name: String,
    /// The durable write protocol it covers.
    protocol: String,
    /// The fixture it needs in order to observe anything.
    fixture: String,
}

impl Scope {
    /// Reads and parses the committed scope document.
    fn read() -> Result<Self, Box<dyn Error>> {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("crash-restore")
            .join("campaign-scope.json");
        let document: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;

        let fixtures = document
            .get("fixtures")
            .and_then(Value::as_object)
            .map(|object| object.keys().cloned().collect())
            .ok_or_else(|| Failure("the scope document declares no fixtures".to_owned()))?;

        let mut reports = Vec::new();
        for report in array(&document, "reports")? {
            reports.push(Report {
                id: field(report, "id")?,
                owes: field(report, "owes")?,
                scenario: Scenario {
                    package: field(report, "package")?,
                    target: field(report, "target")?,
                    name: field(report, "name")?,
                    protocol: "campaign report".to_owned(),
                    fixture: field(report, "fixture")?,
                },
            });
        }

        let mut phases = Vec::new();
        for phase in array(&document, "phases")? {
            phases.push(Phase {
                id: field(phase, "id")?,
                report: field(phase, "report")?,
                termination: field(phase, "termination")?,
            });
        }

        let mut reused = Vec::new();
        for scenario in array(&document, "reused")? {
            reused.push(Scenario {
                package: field(scenario, "package")?,
                target: field(scenario, "target")?,
                name: field(scenario, "name")?,
                protocol: field(scenario, "protocol")?,
                fixture: field(scenario, "fixture")?,
            });
        }

        Ok(Self {
            fixtures,
            reports,
            phases,
            reused,
        })
    }
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Reads one canonical document from the workspace.
fn read_document(relative: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(workspace_root().join(relative))?)
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    document.get(name).and_then(Value::as_array).ok_or_else(|| {
        Box::new(Failure(format!("the scope document has no {name}"))) as Box<dyn Error>
    })
}

/// Reads one required string field.
fn field(value: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Box::new(Failure(format!("a scope entry has no {name}"))) as Box<dyn Error>)
}

/// A reconciliation input the campaign could not read.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
