//! The retained-evidence provenance verifier.
//!
//! A campaign runner decides whether a run proved what it owed. This decides
//! something narrower and separate: whether the reports committed to
//! `docs/engineering/campaigns/m5/` are still the untouched output of a
//! recorded CI run over a tree whose campaign still means what it meant.
//!
//! It exists because retained evidence has a failure mode that has nothing to
//! do with the campaign that produced it. A report is a file in a repository.
//! It can be edited after the fact, it can be kept beside a campaign that has
//! since been changed, and the commit it names can be quietly reinterpreted —
//! all of which leave a green campaign record describing a run that no longer
//! corresponds to anything.
//!
//! ## Why the producer commit is not required to resolve
//!
//! The obvious check — resolve the commit the report names and diff it against
//! today — cannot be the binding one, for a reason that only shows up once:
//! the identifier a report carries is the pull-request *merge ref*, an
//! ephemeral commit GitHub creates by merging the branch head into the base and
//! replaces on the next push. It is absent from every later clone. Requiring it
//! to resolve would make the verifier fail permanently the moment the branch
//! moved, and treating an unresolvable one as acceptable would make the check
//! decide nothing.
//!
//! So the merge-ref SHA is recorded and compared against what the artifact
//! itself says, and never resolved; the branch head is recorded separately and
//! never conflated with it; and the binding is content instead. Two content
//! checks, both of which work from the retained files alone:
//!
//! - each report's git blob identity, which detects any edit after retention;
//! - the git object identity of every path that defines what the campaign
//!   executes, taken at the producer commit. If one differs today, the report
//!   describes a campaign this tree no longer runs, and it may not be promoted.
//!
//! The second is the one that carries weight, and it is what stops the
//! genuinely tempting mistake: keeping last week's green report while quietly
//! changing the rule that made it green.
//!
//! The contract is `docs/engineering/campaigns/m5/evidence-provenance.json`,
//! and this reads it rather than restating it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

use crate::suite;

/// Where the retained evidence and its provenance live.
const DIRECTORY: &str = "docs/engineering/campaigns/m5";

/// The provenance contract this verifier reads.
const PROVENANCE: &str = "evidence-provenance.json";

/// One verification pass and everything it found.
pub struct Verification {
    /// Every failure, as a human-readable line.
    pub violations: Vec<String>,
    /// How many retained reports were checked.
    pub reports: usize,
}

/// Verifies every retained report against its recorded provenance.
///
/// # Errors
///
/// Returns the failure that prevents verification from producing a result at
/// all, such as an unreadable or malformed provenance document.
pub fn run() -> Result<Verification, String> {
    let root = suite::workspace_root()?;
    let directory = root.join(DIRECTORY);
    let path = directory.join(PROVENANCE);
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

    let mut violations = Vec::new();
    let entries = array(&document, "evidence")?;

    let mut reports = Vec::new();
    for entry in entries {
        violations.extend(verify_report(&root, &directory, entry));
        if let Some(name) = entry.get("report").and_then(Value::as_str)
            && let Ok(source) = fs::read_to_string(directory.join(name))
            && let Ok(report) = serde_json::from_str::<Value>(&source)
        {
            reports.push((name.to_owned(), report));
        }
    }
    violations.extend(verify_matrix(&document, entries));
    violations.extend(verify_semantics(&root, &document, &reports));

    Ok(Verification {
        violations,
        reports: entries.len(),
    })
}

/// Verifies one retained report against its recorded provenance.
fn verify_report(root: &Path, directory: &Path, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(name) = entry.get("report").and_then(Value::as_str) else {
        return vec!["a provenance entry names no report".to_owned()];
    };
    let file = directory.join(name);

    let Ok(source) = fs::read_to_string(&file) else {
        return vec![format!(
            "{name} is recorded as retained evidence and is not in {DIRECTORY}"
        )];
    };
    let Ok(report): Result<Value, _> = serde_json::from_str(&source) else {
        return vec![format!("{name} is not readable as a report")];
    };

    // The artifact must be byte-identical to what was retained. A recorded
    // identity that no longer matches means the file was edited afterwards,
    // which is the one thing a retained artifact may never be.
    match (
        git(root, &["hash-object", &file.display().to_string()]),
        entry
            .get("retained_report_git_blob")
            .and_then(Value::as_str),
    ) {
        (Some(observed), Some(recorded)) if observed == recorded => {}
        (Some(observed), Some(recorded)) => violations.push(format!(
            "{name} has git blob {observed} and its provenance records {recorded}, so the \
             retained artifact was modified after it was recorded"
        )),
        (_, None) => violations.push(format!("{name} records no artifact identity")),
        (None, _) => violations.push(format!("{name} could not be hashed")),
    }

    // The report names the tree it ran on. The provenance has to agree with it
    // rather than substitute the branch head, which is a real commit and a
    // different tree — the substitution that would otherwise pass unnoticed.
    let observed = report
        .pointer("/environment/source_commit")
        .and_then(Value::as_str);
    let recorded = entry
        .pointer("/producer/execution_commit")
        .and_then(Value::as_str);
    match (observed, recorded) {
        (Some(observed), Some(recorded)) if observed == recorded => {}
        (Some(observed), Some(recorded)) => violations.push(format!(
            "{name} was produced by {observed} and its provenance records a producer of \
             {recorded}"
        )),
        (None, _) => violations.push(format!("{name} records no source commit")),
        (_, None) => violations.push(format!("{name} has no recorded producer commit")),
    }
    if entry
        .pointer("/producer/branch_head_sha")
        .and_then(Value::as_str)
        .is_none()
    {
        violations.push(format!("{name} records no producer branch head"));
    }
    if entry
        .pointer("/producer/source_tree_clean")
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!(
            "{name} was produced from a tree that was not clean, or does not say"
        ));
    }
    if report
        .pointer("/environment/source_tree_clean")
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!("{name} itself records an unclean producer tree"));
    }

    violations.extend(verify_run_identity(name, entry));
    violations.extend(verify_required_strings(name, entry));

    violations.extend(verify_filing(name, entry, &report));
    violations
}

/// Requires a report to be filed where it belongs, and to be a pass.
///
/// A matrix point is invisible in a connection string, so without the first
/// check a report from one supported major reconciles perfectly inside the
/// other's slot. Retaining a failed report is deliberate and supported;
/// promoting one as official evidence is not.
fn verify_filing(name: &str, entry: &Value, report: &Value) -> Vec<String> {
    let mut violations = Vec::new();

    let filed = entry.get("matrix_point").and_then(Value::as_str);
    let named = report
        .pointer("/environment/matrix")
        .and_then(Value::as_str);
    if filed.is_some() && filed != named {
        violations.push(format!(
            "{name} is filed as {} and records {}",
            filed.unwrap_or("nothing"),
            named.unwrap_or("nothing"),
        ));
    }
    let major = entry.get("postgres_major_version").and_then(Value::as_str);
    let reported = report
        .get("postgresql_major_version")
        .and_then(Value::as_str);
    if major.is_some() && major != reported {
        violations.push(format!(
            "{name} is filed under PostgreSQL {} and ran against {}",
            major.unwrap_or("nothing"),
            reported.unwrap_or("an unrecorded version"),
        ));
    }

    if report.get("passed").and_then(Value::as_bool) != Some(true) {
        violations.push(format!(
            "{name} is retained as official evidence and did not pass"
        ));
    }
    let recorded_violations = report
        .get("violations")
        .and_then(Value::as_array)
        .map_or(usize::MAX, Vec::len);
    if recorded_violations != 0 {
        violations.push(format!(
            "{name} is retained as official evidence and carries {recorded_violations} violation(s)"
        ));
    }

    violations
}

/// Every provenance field that must be present and non-empty.
///
/// Presence is not enough. `as_str` succeeds on `""`, so a field emptied by a
/// generator that lost its input reads as recorded, and the whole point of
/// these fields is that a human can follow them back to one execution.
const REQUIRED_STRINGS: &[&str] = &[
    "/matrix_point",
    "/postgres_major_version",
    "/producer/branch_head_sha",
    "/producer/execution_commit",
    "/workflow_run/workflow",
    "/workflow_run/workflow_file",
    "/workflow_run/event",
    "/workflow_run/conclusion",
    "/producing_job/name",
    "/producing_job/conclusion",
    "/artifact/name",
    "/artifact/digest",
];

/// Requires every named provenance field to be present and non-empty.
fn verify_required_strings(name: &str, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    for pointer in REQUIRED_STRINGS {
        match entry.pointer(pointer).and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => {}
            Some(_) => violations.push(format!("{name} records an empty {pointer}")),
            None => violations.push(format!("{name} records no {pointer}")),
        }
    }
    // The run and the job are different facts. A workflow whose other jobs
    // failed is not one that produced trusted evidence, and the producing job's
    // own success cannot stand in for it.
    for (pointer, what) in [
        ("/workflow_run/conclusion", "producer workflow run"),
        ("/producing_job/conclusion", "producing job"),
    ] {
        if entry
            .pointer(pointer)
            .and_then(Value::as_str)
            .is_some_and(|conclusion| conclusion != "success")
        {
            violations.push(format!(
                "{name} is retained as official evidence and its {what} did not succeed"
            ));
        }
    }
    if entry
        .pointer("/workflow_run/github_artifact_digest")
        .and_then(Value::as_str)
        .is_some_and(|digest| !digest.starts_with("sha256:"))
    {
        violations.push(format!(
            "{name} records an artifact digest that is not a sha256"
        ));
    }
    violations.extend(verify_remote_record(name, entry));
    violations
}

/// Requires the one-time remote provenance check to have been recorded.
///
/// This verifier is repository-local by design: it resolves nothing over the
/// network, so it keeps working long after GitHub has expired the artifacts it
/// describes. That makes it unable to say the retained bytes are the bytes
/// GitHub stored — only that they are the bytes this repository recorded. The
/// difference is real, so the stronger check is performed once, when the
/// evidence is promoted, and its result is recorded here. A retained report
/// whose remote check was never performed, or was performed against a different
/// run, is not carrying the provenance the documentation claims for it.
fn verify_remote_record(name: &str, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(remote) = entry.get("remote_verification") else {
        return vec![format!(
            "{name} records no remote provenance verification, so nothing says the retained bytes \
             are the ones the workflow uploaded"
        )];
    };
    if remote.get("verified").and_then(Value::as_bool) != Some(true) {
        violations.push(format!(
            "{name} records a remote verification that did not pass"
        ));
    }
    for field in ["verified_at"] {
        match remote.get(field).and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => {}
            _ => violations.push(format!("{name} records no remote verification {field}")),
        }
    }
    // Machine-readable results, not a prose method line. Each of these is one
    // thing the retention step confirmed, and the campaign requires all of them
    // rather than a sentence saying they were done.
    for field in [
        "workflow_run_identity",
        "workflow_run_conclusion",
        "producing_job_identity",
        "producing_job_conclusion",
        "artifact_digest",
        "artifact_bytes_match_retained_report",
        "execution_commit_matches_report",
    ] {
        if remote.get(field).and_then(Value::as_bool) != Some(true) {
            violations.push(format!(
                "{name} records no confirmed remote check for {field}"
            ));
        }
    }
    let checked = remote.get("run_id").and_then(Value::as_u64);
    let producing = entry.pointer("/workflow_run/id").and_then(Value::as_u64);
    if checked.is_none() || checked != producing {
        violations.push(format!(
            "{name} records a remote verification of run {checked:?} and was produced by \
             {producing:?}"
        ));
    }
    violations
}

/// Requires the run that produced a report to be identified.
///
/// "Some CI run" is not provenance: without the run, the attempt, and the job,
/// a report cannot be traced back to an execution anyone can look at.
fn verify_run_identity(name: &str, entry: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    for pointer in [
        "/workflow_run/id",
        "/workflow_run/attempt",
        "/producing_job/id",
        "/artifact/id",
        "/artifact/size_bytes",
    ] {
        if entry.pointer(pointer).and_then(Value::as_u64).is_none() {
            violations.push(format!("{name} records no {pointer}"));
        }
    }
    violations
}

/// Requires every declared matrix point to be present exactly once.
///
/// Presence alone is not the property. A set membership check passes a document
/// that lists `PostgreSQL` 15 twice and 18 never as soon as an 18 entry is added
/// beside them, and it passes one that quietly carries a third point nobody
/// declared. Counts are what the campaign means by "the matrix".
fn verify_matrix(document: &Value, entries: &[Value]) -> Vec<String> {
    let mut violations = Vec::new();
    let required = document
        .get("required_matrix")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if required.is_empty() {
        return vec!["the provenance document requires no matrix point".to_owned()];
    }

    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for entry in entries {
        match entry.get("matrix_point").and_then(Value::as_str) {
            Some(point) => *counts.entry(point.to_owned()).or_default() += 1,
            None => violations.push("a provenance entry names no matrix point".to_owned()),
        }
    }

    for point in &required {
        match counts.get(point).copied().unwrap_or_default() {
            1 => {}
            0 => violations.push(format!(
                "{point} is a required matrix point and no retained report covers it, so the \
                 evidence is part of a matrix presented as the whole one"
            )),
            found => violations.push(format!(
                "{point} is covered by {found} retained reports, so which one the campaign result \
                 rests on is unstated"
            )),
        }
    }
    for point in counts.keys() {
        if !required.contains(point) {
            violations.push(format!(
                "{point} is retained as official evidence and is not a declared matrix point"
            ));
        }
    }

    violations.extend(verify_unique(entries, "report", |entry| {
        entry.get("report").and_then(Value::as_str)
    }));
    violations.extend(verify_unique(entries, "artifact_name", |entry| {
        entry
            .pointer("/workflow_run/artifact_name")
            .and_then(Value::as_str)
    }));
    let mut jobs: BTreeMap<u64, usize> = BTreeMap::new();
    for entry in entries {
        if let Some(job) = entry
            .pointer("/workflow_run/job_id")
            .and_then(Value::as_u64)
        {
            *jobs.entry(job).or_default() += 1;
        }
    }
    for (job, count) in jobs {
        if count > 1 {
            violations.push(format!(
                "job {job} is recorded as the producer of {count} retained reports, and one job \
                 produces one report"
            ));
        }
    }

    violations
}

/// Requires one field to be distinct across every entry.
fn verify_unique<'a>(
    entries: &'a [Value],
    field: &str,
    read: impl Fn(&'a Value) -> Option<&'a str>,
) -> Vec<String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for entry in entries {
        if let Some(value) = read(entry) {
            *counts.entry(value).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(value, count)| {
            format!(
                "{count} retained reports share the same {field} {value}, so they cannot both \
                     be what they claim"
            )
        })
        .collect()
}

/// Requires the campaign the reports describe to be the campaign this tree runs.
///
/// The identities compared here are the ones the producer recorded *from inside
/// its own checkout*, in the report, at the moment it ran. That is the root of
/// trust, and it replaces an earlier arrangement that re-derived them from a
/// commit name.
///
/// The reason is not preference. A pull-request run executes against an
/// ephemeral merge commit no later clone can resolve, and the branch head is a
/// different tree — using it as a stand-in means checking the evidence against
/// something that never ran. Re-deriving also made the permanent verifier
/// depend on fetching history that a squash-merge removes. Recording the
/// manifest in the artifact makes the binding exact and offline: the only
/// inputs are the retained report, the declared closure, and the current tree.
fn verify_semantics(root: &Path, document: &Value, reports: &[(String, Value)]) -> Vec<String> {
    let mut violations = Vec::new();
    let declared = match semantics_paths(root) {
        Ok(paths) => paths,
        Err(error) => return vec![error],
    };

    for (name, report) in reports {
        let Some(objects) = report
            .pointer("/observation/execution_manifest/objects")
            .and_then(Value::as_object)
        else {
            violations.push(format!(
                "{name} records no execution manifest, so nothing says which tree it ran against"
            ));
            continue;
        };
        if report
            .pointer("/observation/execution_manifest/execution_commit")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            violations.push(format!("{name} records no execution commit"));
        }
        if report
            .pointer("/observation/execution_manifest/tree_clean")
            .and_then(Value::as_bool)
            != Some(true)
        {
            violations.push(format!("{name} ran against a tree that was not clean"));
        }

        // The manifest must cover the declared closure exactly. One that
        // omitted a path would leave that input unbound; one carrying an extra
        // would bind something the campaign does not declare.
        let recorded = objects.keys().cloned().collect::<BTreeSet<_>>();
        for path in declared.difference(&recorded) {
            violations.push(format!(
                "{name} ran without recording {path}, which the campaign declares as semantics"
            ));
        }
        for path in recorded.difference(&declared) {
            violations.push(format!(
                "{name} records {path}, which the campaign does not declare as semantics"
            ));
        }
        for (path, expected) in objects {
            violations.extend(verify_object(root, name, path, expected));
        }
    }

    violations.extend(verify_one_execution(document, reports));
    violations.extend(verify_worktree(root, &declared));
    violations
}

/// Requires the retained reports to be one campaign run on one tree.
fn verify_one_execution(document: &Value, reports: &[(String, Value)]) -> Vec<String> {
    let mut violations = Vec::new();
    let executed = reports
        .iter()
        .filter_map(|(_, report)| {
            report
                .pointer("/observation/execution_manifest/execution_commit")
                .and_then(Value::as_str)
        })
        .collect::<BTreeSet<_>>();
    if executed.len() > 1 {
        violations.push(format!(
            "the retained reports were executed against {} different trees, so they are not one \
             campaign result",
            executed.len(),
        ));
    }

    // The provenance records the execution commit too, and it must be the one
    // the report itself carries rather than a substitute such as the branch
    // head — which is a real commit, and a different tree.
    for entry in document
        .get("evidence")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(name) = entry.get("report").and_then(Value::as_str) else {
            continue;
        };
        let claimed = entry
            .pointer("/producer/execution_commit")
            .and_then(Value::as_str);
        let actual = reports
            .iter()
            .find(|(report, _)| report == name)
            .and_then(|(_, report)| {
                report
                    .pointer("/observation/execution_manifest/execution_commit")
                    .and_then(Value::as_str)
            });
        if actual.is_some() && claimed != actual {
            violations.push(format!(
                "{name} ran against {actual:?} and its provenance records an execution commit of \
                 {claimed:?}"
            ));
        }
    }
    violations
}

/// Requires `HEAD` to be a truthful stand-in for the working tree.
fn verify_worktree(root: &Path, declared: &BTreeSet<String>) -> Vec<String> {
    let mut arguments = vec![
        "status".to_owned(),
        "--porcelain".to_owned(),
        "--".to_owned(),
    ];
    arguments.extend(declared.iter().cloned());
    let arguments = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    match git(root, &arguments) {
        Some(status) if status.is_empty() => Vec::new(),
        Some(status) => vec![format!(
            "the working tree differs from HEAD in a campaign-semantics path, so the identities \
             above were checked against something that is not what would run: {}",
            status.replace('\n', "; "),
        )],
        None => vec!["could not inspect the working tree".to_owned()],
    }
}

/// Reads the declared semantic closure from its canonical document.
///
/// The producer reads the same document to build its manifest. Neither restates
/// the list: a closure kept in two places is one that will disagree.
fn semantics_paths(root: &Path) -> Result<BTreeSet<String>, String> {
    let path = root
        .join("tests")
        .join("fixtures")
        .join("soak")
        .join("campaign-semantics.json");
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;
    let paths = document
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| "the semantics document declares no categories".to_owned())?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if paths.is_empty() {
        return Err("the semantics document declares no paths".to_owned());
    }
    Ok(paths)
}

/// Requires one executed identity to still be the tree's identity.
fn verify_object(root: &Path, name: &str, path: &str, expected: &Value) -> Vec<String> {
    let Some(expected) = expected.as_str() else {
        return vec![format!(
            "{name} records an identity for {path} that is not a string"
        )];
    };
    match git(root, &["rev-parse", &format!("HEAD:{path}")]) {
        Some(observed) if observed == expected => Vec::new(),
        Some(observed) => vec![format!(
            "{path} was {expected} when {name} was produced and is {observed} now; the report \
             describes a campaign this tree no longer runs, so the campaign has to be run again \
             rather than the evidence re-promoted"
        )],
        None => vec![format!(
            "{path} is declared as campaign semantics and is not in this tree"
        )],
    }
}

/// Runs one git command and returns its trimmed output.
fn git(root: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Reads one required array field.
fn array<'a>(document: &'a Value, name: &str) -> Result<&'a Vec<Value>, String> {
    document
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("the provenance document has no {name}"))
}

/// Returns the retained-evidence directory, for the runner's message.
#[must_use]
pub fn directory() -> PathBuf {
    PathBuf::from(DIRECTORY)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use serde_json::{Value, json};

    // Whether the repository's *current* evidence verifies is deliberately not
    // a unit test. It is a property of the working tree rather than of this
    // logic, and it is false by design in the window between a commit that
    // changes campaign semantics and the retention commit that records the
    // rerun. Asserting it here would take the whole test suite down during that
    // window instead of the one CI job whose question it is. The command
    // answers it; these tests hold the logic that answers it.

    /// A complete, valid provenance entry for the retained `PostgreSQL` 15 report.
    ///
    /// Synthesized against the retained file rather than read from the shipped
    /// document, for the reason the report fixture above is: these tests are
    /// about the schema checks, and an entry taken from disk is stale for the
    /// whole window between a semantics change and the retention that follows
    /// it. Every attack below mutates one field of this.
    fn entry() -> Value {
        let root = crate::suite::workspace_root().expect("workspace root");
        let file = root
            .join(super::DIRECTORY)
            .join("soak-campaign-postgres-15.json");
        let report: Value =
            serde_json::from_str(&std::fs::read_to_string(&file).expect("retained report"))
                .expect("report json");
        json!({
            "report": "soak-campaign-postgres-15.json",
            "matrix_point": "postgres-15",
            "postgres_major_version": "15",
            "producer": {
                "execution_commit": report["environment"]["source_commit"],
                "branch_head_sha": "0".repeat(40),
                "source_tree_clean": true,
            },
            "workflow_run": {
                "workflow": "Rust",
                "workflow_file": ".github/workflows/ci.yml",
                "id": 1_u64, "attempt": 1_u64, "event": "pull_request",
                "conclusion": "success",
            },
            "producing_job": {
                "name": "postgres-15-soak-campaign", "id": 2_u64, "conclusion": "success",
            },
            "artifact": {
                "name": "soak-campaign-postgres-15",
                "id": 3_u64,
                "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "size_bytes": 4_u64,
            },
            "retained_report_git_blob": super::git(
                &root,
                &["hash-object", &file.display().to_string()],
            )
            .expect("a blob for the retained report"),
            "remote_verification": {
                "verified": true,
                "verified_at": "2026-08-10T00:00:00+00:00",
                "run_id": 1_u64,
                "workflow_run_identity": true,
                "workflow_run_conclusion": true,
                "producing_job_identity": true,
                "producing_job_conclusion": true,
                "artifact_digest": true,
                "artifact_bytes_match_retained_report": true,
                "execution_commit_matches_report": true,
            },
        })
    }

    /// Verifies one entry against the real retained directory.
    fn verify(entry: &Value) -> Vec<String> {
        let root = crate::suite::workspace_root().expect("workspace root");
        let directory = root.join(super::DIRECTORY);
        super::verify_report(&root, &directory, entry)
    }

    /// Replaces one field of the entry, addressed by JSON pointer.
    fn with(pointer: &str, value: Value) -> Value {
        let mut entry = entry();
        *entry.pointer_mut(pointer).expect(pointer) = value;
        entry
    }

    #[test]
    fn the_real_entry_verifies() {
        assert_eq!(verify(&entry()), Vec::<String>::new());
    }

    #[test]
    fn an_edited_artifact_is_rejected() {
        let violations = verify(&with("/retained_report_git_blob", json!("0".repeat(40))));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("modified after it was recorded")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_producer_commit_the_report_does_not_name_is_rejected() {
        // The substitution this prevents is naming the retention commit, or
        // the branch head, as the tree the campaign ran on.
        let violations = verify(&with(
            "/producer/execution_commit",
            json!("82627f72a5bb3d6d069827ee8d890a5f7dcd66f6"),
        ));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("its provenance records a producer of")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_missing_branch_head_is_rejected() {
        let mut entry = entry();
        entry["producer"]
            .as_object_mut()
            .expect("producer")
            .remove("branch_head_sha");
        let violations = verify(&entry);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("records no producer branch head")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_unidentified_workflow_run_is_rejected() {
        for (section, field, pointer) in [
            ("workflow_run", "id", "/workflow_run/id"),
            ("workflow_run", "attempt", "/workflow_run/attempt"),
            ("producing_job", "id", "/producing_job/id"),
            ("artifact", "id", "/artifact/id"),
        ] {
            let mut entry = entry();
            entry[section].as_object_mut().expect(section).remove(field);
            let violations = verify(&entry);
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(&format!("records no {pointer}"))),
                "{pointer}: {violations:?}",
            );
        }
    }

    #[test]
    fn rejects_a_workflow_run_that_did_not_succeed() {
        // The producing job succeeding is not the whole run succeeding, and
        // official evidence requires both.
        let violations = verify(&with("/workflow_run/conclusion", json!("failure")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("producer workflow run did not succeed")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_a_remote_check_that_was_not_confirmed() {
        for field in [
            "workflow_run_identity",
            "workflow_run_conclusion",
            "producing_job_identity",
            "producing_job_conclusion",
            "artifact_digest",
            "artifact_bytes_match_retained_report",
            "execution_commit_matches_report",
        ] {
            let violations = verify(&with(
                &format!("/remote_verification/{field}"),
                json!(false),
            ));
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(&format!("remote check for {field}"))),
                "{field}: {violations:?}",
            );
        }
    }

    #[test]
    fn a_report_filed_under_the_wrong_matrix_point_is_rejected() {
        let violations = verify(&with("/matrix_point", json!("postgres-18")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("is filed as postgres-18 and records")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_report_filed_under_the_wrong_major_is_rejected() {
        let violations = verify(&with("/postgres_major_version", json!("18")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("filed under PostgreSQL 18 and ran against")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_unclean_producer_tree_is_rejected() {
        let violations = verify(&with("/producer/source_tree_clean", json!(false)));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("was not clean")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_report_that_is_not_retained_is_rejected() {
        let violations = verify(&with("/report", json!("soak-campaign-postgres-42.json")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("is not in")),
            "{violations:?}",
        );
    }

    /// The shipped provenance document, for tests that mutate it.
    fn provenance() -> Value {
        let root = crate::suite::workspace_root().expect("workspace root");
        let path = root.join(super::DIRECTORY).join(super::PROVENANCE);
        serde_json::from_str(&std::fs::read_to_string(path).expect("provenance document"))
            .expect("provenance json")
    }

    /// Two reports carrying a manifest that matches this tree exactly.
    ///
    /// Synthesized against the current tree rather than read off disk. These
    /// tests are about the verifier's logic, and a fixture taken from the
    /// retained files would be false for the whole window between a commit that
    /// changes campaign semantics and the retention commit that records the
    /// rerun — which is the same repo-state coupling that has broken this
    /// suite twice already.
    fn reports() -> Vec<(String, Value)> {
        let root = crate::suite::workspace_root().expect("workspace root");
        let objects = super::semantics_paths(&root)
            .expect("declared semantics")
            .into_iter()
            .map(|path| {
                let object = super::git(&root, &["rev-parse", &format!("HEAD:{path}")])
                    .unwrap_or_else(|| panic!("{path} is declared and absent"));
                (path, json!(object))
            })
            .collect::<serde_json::Map<_, _>>();
        let manifest = json!({
            "execution_commit": super::git(&root, &["rev-parse", "HEAD"]).expect("HEAD"),
            "tree_clean": true,
            "objects": objects,
        });
        provenance()["evidence"]
            .as_array()
            .expect("evidence")
            .iter()
            .map(|entry| {
                let name = entry["report"].as_str().expect("report").to_owned();
                (
                    name,
                    json!({ "observation": { "execution_manifest": manifest } }),
                )
            })
            .collect()
    }

    /// Runs the semantics check over a mutated provenance document.
    fn semantics(document: &Value) -> Vec<String> {
        semantics_of(document, &reports())
    }

    /// Runs the semantics check over mutated reports.
    fn semantics_of(document: &Value, reports: &[(String, Value)]) -> Vec<String> {
        let root = crate::suite::workspace_root().expect("workspace root");
        super::verify_semantics(&root, document, reports)
    }

    /// Replaces one object identity in every retained report's manifest.
    fn with_manifest(path: &str, object: &Value) -> Vec<(String, Value)> {
        let mut reports = reports();
        for (_, report) in &mut reports {
            report["observation"]["execution_manifest"]["objects"][path] = object.clone();
        }
        reports
    }

    #[test]
    fn rejects_provenance_rewritten_to_current_head() {
        // The substitution the manifest exists to prevent: provenance that
        // names some other real commit as the tree the campaign ran on. The
        // report carries its own execution commit, so the two disagree.
        let mut document = provenance();
        for entry in document["evidence"].as_array_mut().expect("evidence") {
            entry["producer"]["execution_commit"] = json!("0".repeat(40));
        }
        let violations = semantics(&document);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("its provenance records an execution commit")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_reports_from_different_execution_trees() {
        let mut reports = reports();
        reports[0].1["observation"]["execution_manifest"]["execution_commit"] =
            json!("0".repeat(40));
        let violations = semantics_of(&provenance(), &reports);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("executed against 2 different trees")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_an_unclean_execution_tree() {
        let mut reports = reports();
        reports[0].1["observation"]["execution_manifest"]["tree_clean"] = json!(false);
        let violations = semantics_of(&provenance(), &reports);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("ran against a tree that was not clean")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_a_report_with_no_execution_manifest() {
        let mut reports = reports();
        reports[0].1["observation"]
            .as_object_mut()
            .expect("observation")
            .remove("execution_manifest");
        let violations = semantics_of(&provenance(), &reports);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("records no execution manifest")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_a_manifest_missing_a_declared_path() {
        let mut reports = reports();
        for (_, report) in &mut reports {
            report["observation"]["execution_manifest"]["objects"]
                .as_object_mut()
                .expect("objects")
                .remove("Cargo.lock");
        }
        let violations = semantics_of(&provenance(), &reports);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("ran without recording Cargo.lock")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_a_manifest_binding_an_undeclared_path() {
        let reports = with_manifest("README.md", &json!("0".repeat(40)));
        let violations = semantics_of(&provenance(), &reports);
        assert!(
            violations.iter().any(|violation| violation
                .contains("records README.md, which the campaign does not declare")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_changed_cargo_lock() {
        rejects_a_changed_semantics_path("Cargo.lock");
    }

    #[test]
    fn rejects_changed_campaign_dependency_manifest() {
        rejects_a_changed_semantics_path("crates/oxide-batch/Cargo.toml");
    }

    #[test]
    fn rejects_changed_database_migration() {
        rejects_a_changed_semantics_path("crates/oxide-batch/migrations");
    }

    #[test]
    fn rejects_changed_toolchain_pin() {
        rejects_a_changed_semantics_path("rust-toolchain.toml");
    }

    #[test]
    fn rejects_changed_execution_contract() {
        // Changing how CI runs the soak invalidates the evidence, while
        // changing an unrelated CI job does not — which is the whole reason
        // the contract lives beside the campaign rather than in the workflow.
        rejects_a_changed_semantics_path("tests/fixtures/soak/execution-contract.json");
        rejects_a_changed_semantics_path("tests/fixtures/soak/run-ci-campaign.sh");
    }

    #[test]
    fn rejects_changed_verifier() {
        rejects_a_changed_semantics_path("xtask/src/soak.rs");
    }

    /// Requires retained reports to be refused once one input has changed.
    ///
    /// Simulated by recording an identity the path does not have, which is
    /// indistinguishable to the verifier from the path having changed since.
    fn rejects_a_changed_semantics_path(path: &str) {
        let reports = with_manifest(path, &json!("0".repeat(40)));
        assert!(
            reports[0].1["observation"]["execution_manifest"]["objects"]
                .as_object()
                .expect("objects")
                .contains_key(path),
            "{path} is not declared as campaign semantics",
        );
        let violations = semantics_of(&provenance(), &reports);
        assert!(
            violations
                .iter()
                .any(|violation| violation.starts_with(path)),
            "{path}: {violations:?}",
        );
    }

    #[test]
    fn accepts_exact_postgres_15_and_18_matrix() {
        let document = provenance();
        let entries = document["evidence"].as_array().expect("evidence").clone();
        assert_eq!(
            super::verify_matrix(&document, &entries),
            Vec::<String>::new()
        );
    }

    #[test]
    fn rejects_duplicate_matrix_point() {
        let document = provenance();
        let entry = entry();
        let violations = super::verify_matrix(&document, &[entry.clone(), entry]);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("is covered by 2 retained reports")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_unexpected_matrix_point() {
        let document = provenance();
        let mut extra = entry();
        extra["matrix_point"] = json!("postgres-17");
        let mut entries = document["evidence"].as_array().expect("evidence").clone();
        entries.push(extra);
        let violations = super::verify_matrix(&document, &entries);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("postgres-17 is retained as official evidence")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_missing_matrix_point() {
        let document = provenance();
        let violations = super::verify_matrix(&document, std::slice::from_ref(&entry()));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("postgres-18 is a required matrix point")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_an_empty_required_field() {
        for pointer in super::REQUIRED_STRINGS {
            let violations = verify(&with(pointer, json!("")));
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(&format!("empty {pointer}"))),
                "{pointer}: {violations:?}",
            );
        }
    }

    #[test]
    fn rejects_a_job_that_did_not_succeed() {
        let violations = verify(&with("/producing_job/conclusion", json!("failure")));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("producing job did not succeed")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_half_matrix_is_rejected() {
        let document = json!({ "required_matrix": ["postgres-15", "postgres-18"] });
        let violations = super::verify_matrix(&document, std::slice::from_ref(&entry()));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("postgres-18 is a required matrix point")),
            "{violations:?}",
        );
    }
}
