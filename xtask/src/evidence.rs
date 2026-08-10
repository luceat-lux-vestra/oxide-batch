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

    for entry in entries {
        violations.extend(verify_report(&root, &directory, entry));
    }
    violations.extend(verify_matrix(&document, entries));
    violations.extend(verify_semantics(&root, &document));

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
        entry.get("artifact_git_blob").and_then(Value::as_str),
    ) {
        (Some(observed), Some(recorded)) if observed == recorded => {}
        (Some(observed), Some(recorded)) => violations.push(format!(
            "{name} has git blob {observed} and its provenance records {recorded}, so the \
             retained artifact was modified after it was recorded"
        )),
        (_, None) => violations.push(format!("{name} records no artifact identity")),
        (None, _) => violations.push(format!("{name} could not be hashed")),
    }

    // The artifact names the tree it ran on. That value is the merge ref, and
    // the provenance has to agree with it rather than substitute the branch
    // head, which is a different commit and is recorded separately.
    let observed = report
        .pointer("/environment/source_commit")
        .and_then(Value::as_str);
    let recorded = entry
        .pointer("/producer/merge_ref_sha")
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
    "/producer/merge_ref_sha",
    "/workflow_run/workflow",
    "/workflow_run/workflow_file",
    "/workflow_run/job",
    "/workflow_run/artifact_name",
    "/workflow_run/event",
    "/workflow_run/conclusion",
    "/workflow_run/github_artifact_digest",
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
    if entry
        .pointer("/workflow_run/conclusion")
        .and_then(Value::as_str)
        .is_some_and(|conclusion| conclusion != "success")
    {
        violations.push(format!(
            "{name} is retained as official evidence and its producing job did not succeed"
        ));
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
    for field in ["method", "verified_at"] {
        match remote.get(field).and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => {}
            _ => violations.push(format!("{name} records no remote verification {field}")),
        }
    }
    let checked = remote.get("run_id").and_then(Value::as_u64);
    let producing = entry
        .pointer("/workflow_run/run_id")
        .and_then(Value::as_u64);
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
    for field in ["run_id", "job_id", "run_attempt"] {
        if entry
            .pointer(&format!("/workflow_run/{field}"))
            .and_then(Value::as_u64)
            .is_none()
        {
            violations.push(format!("{name} records no workflow {field}"));
        }
    }
    for field in ["workflow", "job", "artifact_name"] {
        if entry
            .pointer(&format!("/workflow_run/{field}"))
            .and_then(Value::as_str)
            .is_none()
        {
            violations.push(format!("{name} records no workflow {field}"));
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
/// This is the check the whole verifier is for, and it is a three-way identity
/// rather than a two-way one:
///
/// ```text
/// producer:<path>  ==  recorded object  ==  HEAD:<path>
/// ```
///
/// The middle term alone would be worthless. An attacker — or, far more likely,
/// someone tidying up after a change they did not think was material — can
/// change a campaign path, leave the producer SHA alone, and refresh the
/// recorded object identities from the current tree. Recorded and HEAD then
/// agree perfectly, and the retained report describes a campaign nobody runs
/// any more. Resolving the objects at the recorded producer commit is what
/// makes that visible, and it is why the producer commit is required to be
/// resolvable rather than treated as a label.
fn verify_semantics(root: &Path, document: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(objects) = document
        .pointer("/campaign_semantics_at_producer/objects")
        .and_then(Value::as_object)
    else {
        return vec![
            "the provenance document records no campaign-semantics identities, so nothing says \
             the retained reports describe the campaign this tree runs"
                .to_owned(),
        ];
    };
    let declared = document
        .pointer("/campaign_semantics/paths")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if declared.is_empty() {
        violations.push("the provenance document declares no campaign-semantics paths".to_owned());
    }

    // Every declared path must be recorded, and every recorded path declared.
    // Otherwise the set can shrink on one side and still verify.
    let recorded = objects.keys().cloned().collect::<BTreeSet<_>>();
    for path in declared.difference(&recorded) {
        violations.push(format!(
            "{path} defines campaign semantics and no identity was recorded for it"
        ));
    }
    for path in recorded.difference(&declared) {
        violations.push(format!(
            "an identity is recorded for {path}, which the contract does not declare as campaign \
             semantics"
        ));
    }

    let producer = document
        .pointer("/campaign_semantics_at_producer/producer_commit")
        .and_then(Value::as_str);
    violations.extend(verify_producer_agreement(document, producer));

    let resolvable = producer.filter(|commit| resolve(root, commit));
    if let Some(producer) = producer {
        if resolvable.is_none() {
            violations.push(format!(
                "the recorded producer commit {producer} cannot be resolved in this repository, \
                 so the recorded object identities cannot be checked against the tree the \
                 campaign actually ran on; without that they only say the provenance agrees with \
                 itself"
            ));
        }
    } else {
        violations.push(
            "the provenance document records no producer commit for its campaign-semantics \
             identities"
                .to_owned(),
        );
    }

    for (path, expected) in objects {
        violations.extend(verify_object(root, path, expected, resolvable));
    }

    // HEAD is only a truthful stand-in for the working tree while the two
    // agree on these paths.
    let mut dirty = vec![
        "status".to_owned(),
        "--porcelain".to_owned(),
        "--".to_owned(),
    ];
    dirty.extend(declared.iter().cloned());
    let arguments = dirty.iter().map(String::as_str).collect::<Vec<_>>();
    match git(root, &arguments) {
        Some(status) if status.is_empty() => {}
        Some(status) => violations.push(format!(
            "the working tree differs from HEAD in a campaign-semantics path, so the identities \
             above were checked against something that is not what would run: {}",
            status.replace('\n', "; "),
        )),
        None => violations.push("could not inspect the working tree".to_owned()),
    }

    violations
}

/// Requires one path's identity to hold at the producer and at `HEAD`.
fn verify_object(root: &Path, path: &str, expected: &Value, producer: Option<&str>) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(expected) = expected.as_str() else {
        return vec![format!("{path} records an identity that is not a string")];
    };

    // The recorded identity must be the producer's, not merely today's.
    if let Some(producer) = producer {
        match git(root, &["rev-parse", &format!("{producer}:{path}")]) {
            Some(observed) if observed == expected => {}
            Some(observed) => violations.push(format!(
                "{path} is recorded as {expected} and was {observed} at the producer commit \
                 {producer}; the recorded identity does not belong to the tree the campaign ran on"
            )),
            None => violations.push(format!(
                "{path} is declared as campaign semantics and did not exist at the producer \
                 commit {producer}"
            )),
        }
    }

    match git(root, &["rev-parse", &format!("HEAD:{path}")]) {
        Some(observed) if observed == expected => {}
        Some(observed) => violations.push(format!(
            "{path} was {expected} when the retained evidence was produced and is {observed} now; \
             the reports describe a campaign this tree no longer runs, so the campaign has to be \
             run again rather than the evidence re-promoted"
        )),
        None => violations.push(format!(
            "{path} is declared as campaign semantics and is not in this tree"
        )),
    }

    violations
}

/// Requires every entry to name the producer the semantics were recorded at.
///
/// The reports are one campaign run, so they share a producer tree. An entry
/// that named a different one would mean the recorded object identities belong
/// to some other report's tree than its own.
fn verify_producer_agreement(document: &Value, producer: Option<&str>) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(producer) = producer else {
        return violations;
    };
    for entry in document
        .get("evidence")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = entry
            .get("report")
            .and_then(Value::as_str)
            .unwrap_or("an unnamed report");
        match entry
            .pointer("/producer/branch_head_sha")
            .and_then(Value::as_str)
        {
            Some(head) if head == producer => {}
            Some(head) => violations.push(format!(
                "{name} was produced from branch head {head} and the campaign-semantics \
                 identities were recorded at {producer}, so they describe a different tree from \
                 the one that produced this report"
            )),
            None => violations.push(format!("{name} records no producer branch head")),
        }
    }
    violations
}

/// Reports whether a commit resolves here, fetching it once if it does not.
///
/// A pull-request branch head stops being reachable once the branch is deleted,
/// and a shallow checkout may never have had it. Fetching the object by name is
/// the difference between a verifier that keeps working after a merge and one
/// that has to be switched off — but an unfetchable commit is a failure, never
/// a pass, because the check it blocks is the one that matters.
fn resolve(root: &Path, commit: &str) -> bool {
    if git(root, &["cat-file", "-e", &format!("{commit}^{{commit}}")]).is_some() {
        return true;
    }
    git(root, &["fetch", "--quiet", "--depth=1", "origin", commit]);
    git(root, &["cat-file", "-e", &format!("{commit}^{{commit}}")]).is_some()
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

    /// The provenance entry the repository actually ships for `PostgreSQL` 15.
    ///
    /// Read from the shipped document rather than restated here. An earlier
    /// version of these tests hardcoded the blob and the producer SHA, which
    /// made every negative case below depend on values that change whenever
    /// evidence is retained — and duplicated state in a test is exactly the
    /// failure this whole verifier exists to catch, so it should not be the
    /// shape of the verifier's own tests.
    fn entry() -> Value {
        let root = crate::suite::workspace_root().expect("workspace root");
        let path = root.join(super::DIRECTORY).join(super::PROVENANCE);
        let document: Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("provenance document"))
                .expect("provenance json");
        document
            .get("evidence")
            .and_then(Value::as_array)
            .expect("evidence")
            .iter()
            .find(|entry| entry.get("matrix_point").and_then(Value::as_str) == Some("postgres-15"))
            .expect("a postgres-15 entry")
            .clone()
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
        let violations = verify(&with("/artifact_git_blob", json!("0".repeat(40))));
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
            "/producer/merge_ref_sha",
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
        for field in ["run_id", "job_id", "run_attempt", "workflow", "job"] {
            let mut entry = entry();
            entry["workflow_run"]
                .as_object_mut()
                .expect("workflow_run")
                .remove(field);
            let violations = verify(&entry);
            assert!(
                violations
                    .iter()
                    .any(|violation| violation.contains(&format!("records no workflow {field}"))),
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

    /// Runs the semantics check over a mutated provenance document.
    fn semantics(document: &Value) -> Vec<String> {
        let root = crate::suite::workspace_root().expect("workspace root");
        super::verify_semantics(&root, document)
    }

    #[test]
    fn rejects_semantics_objects_not_owned_by_recorded_producer() {
        // An identity that is a real object, but not the one that path had at
        // the producer commit.
        let mut document = provenance();
        let objects = document["campaign_semantics_at_producer"]["objects"]
            .as_object_mut()
            .expect("objects");
        let borrowed = objects["xtask/src/suite.rs"].clone();
        objects.insert("xtask/src/soak.rs".to_owned(), borrowed);
        let violations = semantics(&document);
        assert!(
            violations
                .iter()
                .any(|violation| violation
                    .contains("does not belong to the tree the campaign ran on")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_producer_commit_mismatch_between_entries() {
        let mut document = provenance();
        document["evidence"][0]["producer"]["branch_head_sha"] =
            json!("0000000000000000000000000000000000000000");
        let violations = semantics(&document);
        assert!(
            violations.iter().any(|violation| violation
                .contains("describe a different tree from the one that produced this report")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_provenance_rewritten_to_current_head() {
        // The attack this three-way binding exists for, reproduced exactly:
        // keep the old producer SHA, change a campaign path, and refresh the
        // recorded identity from the current tree. Recorded and HEAD agree
        // perfectly; only the producer disagrees.
        let root = crate::suite::workspace_root().expect("workspace root");
        let mut document = provenance();
        let rewritten = super::git(&root, &["rev-parse", "HEAD:xtask/src/suite.rs"])
            .expect("an object for the path");
        document["campaign_semantics_at_producer"]["objects"]["xtask/src/soak.rs"] =
            json!(rewritten);
        let violations = semantics(&document);
        assert!(
            violations
                .iter()
                .any(|violation| violation
                    .contains("does not belong to the tree the campaign ran on")),
            "{violations:?}",
        );
    }

    #[test]
    fn rejects_an_unresolvable_producer_commit() {
        // Without the producer tree the recorded identities only say the
        // provenance agrees with itself, so this must fail rather than skip.
        let mut document = provenance();
        document["campaign_semantics_at_producer"]["producer_commit"] =
            json!("0123456789012345678901234567890123456789");
        for entry in document["evidence"].as_array_mut().expect("evidence") {
            entry["producer"]["branch_head_sha"] =
                json!("0123456789012345678901234567890123456789");
        }
        let violations = semantics(&document);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("cannot be resolved in this repository")),
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

    /// Requires a retained report to be refused once one input has changed.
    ///
    /// Simulated by recording an identity the path does not have, which is
    /// indistinguishable to the verifier from the path having changed since.
    fn rejects_a_changed_semantics_path(path: &str) {
        let mut document = provenance();
        let objects = document["campaign_semantics_at_producer"]["objects"]
            .as_object_mut()
            .expect("objects");
        assert!(
            objects.contains_key(path),
            "{path} is not declared as campaign semantics",
        );
        objects.insert(path.to_owned(), json!("0".repeat(40)));
        let violations = semantics(&document);
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
        let violations = verify(&with("/workflow_run/conclusion", json!("failure")));
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

    #[test]
    fn a_changed_campaign_is_rejected() {
        // The failure the verifier exists for: last week's green report kept
        // beside a rule that has since been changed.
        let root = crate::suite::workspace_root().expect("workspace root");
        let document = json!({
            "campaign_semantics": { "paths": ["tests/fixtures/soak/campaign-scope.json"] },
            "campaign_semantics_at_producer": {
                "objects": { "tests/fixtures/soak/campaign-scope.json": "0".repeat(40) },
            },
        });
        let violations = super::verify_semantics(&root, &document);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("no longer runs")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_semantics_path_with_no_recorded_identity_is_rejected() {
        let root = crate::suite::workspace_root().expect("workspace root");
        let document = json!({
            "campaign_semantics": { "paths": ["xtask/src/soak.rs", "xtask/src/suite.rs"] },
            "campaign_semantics_at_producer": { "objects": {} },
        });
        let violations = super::verify_semantics(&root, &document);
        for path in ["xtask/src/soak.rs", "xtask/src/suite.rs"] {
            assert!(
                violations.iter().any(|violation| violation.contains(path)
                    && violation.contains("no identity was recorded")),
                "{path}: {violations:?}",
            );
        }
    }

    #[test]
    fn recording_an_identity_for_an_undeclared_path_is_rejected() {
        let root = crate::suite::workspace_root().expect("workspace root");
        let document = json!({
            "campaign_semantics": { "paths": [] },
            "campaign_semantics_at_producer": {
                "objects": { "xtask/src/soak.rs": "0".repeat(40) },
            },
        });
        let violations = super::verify_semantics(&root, &document);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("does not declare as campaign semantics")),
            "{violations:?}",
        );
    }
}
