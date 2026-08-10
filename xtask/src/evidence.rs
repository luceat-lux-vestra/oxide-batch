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

use std::collections::BTreeSet;
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
    let present = entries
        .iter()
        .filter_map(|entry| entry.get("matrix_point").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    for point in required.difference(&present) {
        violations.push(format!(
            "{point} is a required matrix point and no retained report covers it, so the evidence \
             is half a matrix presented as a whole one"
        ));
    }
    violations
}

/// Requires the campaign the reports describe to be the campaign this tree runs.
///
/// This is the check the whole verifier is for. Keeping a green report while
/// changing the rule that made it green is the failure it exists to prevent,
/// and it is the one that would otherwise be invisible in review.
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

    for (path, expected) in objects {
        let Some(expected) = expected.as_str() else {
            violations.push(format!("{path} records an identity that is not a string"));
            continue;
        };
        let Some(observed) = git(root, &["rev-parse", &format!("HEAD:{path}")]) else {
            violations.push(format!(
                "{path} is declared as campaign semantics and is not in this tree"
            ));
            continue;
        };
        if observed != expected {
            violations.push(format!(
                "{path} was {expected} when the retained evidence was produced and is {observed} \
                 now; the reports describe a campaign this tree no longer runs, so the campaign \
                 has to be run again rather than the evidence re-promoted"
            ));
        }
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

    /// One provenance entry shaped like the real ones.
    fn entry() -> Value {
        json!({
            "report": "soak-campaign-postgres-15.json",
            "matrix_point": "postgres-15",
            "postgres_major_version": "15",
            "producer": {
                "branch_head_sha": "82627f72a5bb3d6d069827ee8d890a5f7dcd66f6",
                "merge_ref_sha": "4c535639b5eb8ee8cd018e64013c24cbf48a18b4",
                "source_tree_clean": true,
            },
            "workflow_run": {
                "workflow": "Rust",
                "job": "postgres-15-soak-campaign",
                "run_id": 31_357_073_834_u64,
                "run_attempt": 1,
                "job_id": 93_358_646_500_u64,
                "artifact_name": "soak-campaign-postgres-15",
            },
            "artifact_git_blob": "fc9ee69a50b2fc7e768fadb3b49aa27d749d6156",
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
