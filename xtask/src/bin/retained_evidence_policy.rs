//! Repository-wide retained-evidence governance verifier.
//!
//! `cargo xtask evidence` remains the byte-identity and semantic-closure
//! authority for promoted campaign evidence. This companion verifier checks
//! the cross-milestone contract around that mechanism: complete inventory,
//! minimum provenance fields, canonical verdict semantics, and bounded Git /
//! GitHub Actions retention.
//!
//! A producer-owned `passed=true` is never authority. The canonical verdict is
//! the report's `violations` collection; any rendered `passed` / `result` value
//! may only mirror that collection.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde_json::Value;

const POLICY: &str = "docs/engineering/retained-evidence-policy.json";
const WORKFLOWS: &str = ".github/workflows";
const UPLOAD_ACTION: &str = "uses: actions/upload-artifact@";

// Code ceilings stop a policy-only edit from silently widening the retained
// evidence budget. Raising one therefore requires changing executable policy.
const ACTION_RETENTION_CEILING_DAYS: u64 = 30;
const REPORT_SIZE_CEILING_BYTES: u64 = 6 * 1024 * 1024;
const REPORT_COUNT_CEILING: usize = 64;
const EXPECTED_COMMITTED_SIZE_CEILING_BYTES: u64 = 14 * 1024 * 1024;
const COMMITTED_SIZE_CEILING_BYTES: u64 = 16 * 1024 * 1024;

struct Verification {
    violations: Vec<String>,
    reports: usize,
    report_bytes: u64,
    committed_bytes: u64,
    artifact_producers: usize,
}

struct Limits {
    artifact_days: u64,
    max_report_bytes: u64,
    max_report_count: usize,
    expected_report_count: usize,
    measured_adoption_bytes: u64,
    expected_committed_bytes: u64,
    max_committed_bytes: u64,
}

struct SetStats {
    violations: Vec<String>,
    reports: usize,
    report_bytes: u64,
    committed_bytes: u64,
    campaigns: BTreeSet<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(verification) if verification.violations.is_empty() => {
            eprintln!(
                "retained evidence policy holds: {} report(s), {} report byte(s), {} committed evidence byte(s), {} artifact producer workflow(s)",
                verification.reports,
                verification.report_bytes,
                verification.committed_bytes,
                verification.artifact_producers,
            );
            ExitCode::SUCCESS
        }
        Ok(verification) => {
            for violation in verification.violations {
                eprintln!("retained evidence policy violation: {violation}");
            }
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("could not verify retained evidence policy: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<Verification, String> {
    let root = workspace_root()?;
    let policy = read_json(&root.join(POLICY))?;
    let limits = read_limits(&policy)?;
    let mut violations = verify_policy_contract(&policy, &limits);

    let retained_sets = array_at(&policy, "/retained_sets")?;
    let mut reports = 0;
    let mut report_bytes = 0;
    let mut committed_bytes = 0;
    let mut retained_ids = BTreeSet::new();
    let mut retained_campaigns = BTreeSet::new();

    for retained_set in retained_sets {
        let Some(id) = string_at(retained_set, "/id") else {
            violations.push("a retained set has no id".to_owned());
            continue;
        };
        if !retained_ids.insert(id.to_owned()) {
            violations.push(format!("retained set id {id} is declared more than once"));
        }
        if string_at(retained_set, "/storage") != Some("git") {
            violations.push(format!(
                "retained set {id} is not classified as Git storage"
            ));
        }
        if string_at(retained_set, "/classification") != Some("merge-blocking") {
            violations.push(format!(
                "retained set {id} is not merge-blocking even though evidence-provenance is a required gate"
            ));
        }

        match verify_retained_set(&root, retained_set, &limits) {
            Ok(stats) => {
                violations.extend(stats.violations);
                reports += stats.reports;
                report_bytes += stats.report_bytes;
                committed_bytes += stats.committed_bytes;
                for campaign in stats.campaigns {
                    if retained_campaigns.contains(&campaign) {
                        violations.push(format!(
                            "campaign {campaign} is declared by more than one retained set"
                        ));
                    } else {
                        retained_campaigns.insert(campaign);
                    }
                }
            }
            Err(error) => violations.push(format!("retained set {id}: {error}")),
        }
    }

    if reports != limits.expected_report_count {
        violations.push(format!(
            "policy expects {} committed report(s) and inventory contains {reports}",
            limits.expected_report_count
        ));
    }
    if reports > limits.max_report_count {
        violations.push(format!(
            "{reports} committed reports exceed the policy maximum of {}",
            limits.max_report_count
        ));
    }
    if committed_bytes > limits.expected_committed_bytes {
        violations.push(format!(
            "{committed_bytes} committed evidence bytes exceed the expected {} byte budget; move growth-causing raw evidence to durable immutable storage",
            limits.expected_committed_bytes
        ));
    }
    if committed_bytes > limits.max_committed_bytes {
        violations.push(format!(
            "{committed_bytes} committed evidence bytes exceed the hard policy maximum of {}",
            limits.max_committed_bytes
        ));
    }

    let producer_result = verify_artifact_producers(&root, &policy, &limits, &retained_ids)?;
    violations.extend(producer_result.0);
    for missing in retained_campaigns.difference(&producer_result.2) {
        violations.push(format!(
            "retained campaign {missing} has no inventoried external artifact producer"
        ));
    }
    for extra in producer_result.2.difference(&retained_campaigns) {
        violations.push(format!(
            "artifact producer claims campaign {extra}, which no retained set declares"
        ));
    }

    Ok(Verification {
        violations,
        reports,
        report_bytes,
        committed_bytes,
        artifact_producers: producer_result.1,
    })
}

fn read_limits(policy: &Value) -> Result<Limits, String> {
    Ok(Limits {
        artifact_days: required_u64(policy, "/retention/actions_artifact_days")?,
        max_report_bytes: required_u64(policy, "/retention/max_report_bytes")?,
        max_report_count: required_usize(policy, "/retention/max_committed_report_count")?,
        expected_report_count: required_usize(
            policy,
            "/retention/expected_committed_report_count",
        )?,
        measured_adoption_bytes: required_u64(
            policy,
            "/retention/measured_committed_evidence_bytes_at_adoption",
        )?,
        expected_committed_bytes: required_u64(
            policy,
            "/retention/expected_committed_evidence_bytes",
        )?,
        max_committed_bytes: required_u64(policy, "/retention/max_committed_evidence_bytes")?,
    })
}

fn verify_policy_contract(policy: &Value, limits: &Limits) -> Vec<String> {
    let mut violations = Vec::new();
    if policy.get("schema_version").and_then(Value::as_u64) != Some(1) {
        violations.push("retained-evidence policy schema_version must be 1".to_owned());
    }
    if string_at(policy, "/contract/canonical_verdict") != Some("violations") {
        violations.push("canonical verdict must remain the violations collection".to_owned());
    }
    if policy
        .pointer("/contract/producer_passed_is_authoritative")
        .and_then(Value::as_bool)
        != Some(false)
    {
        violations.push("producer passed=true must remain explicitly non-authoritative".to_owned());
    }
    if string_at(policy, "/retention/git_age_policy") != Some("semantic-closure") {
        violations.push("Git-retained evidence age must remain semantic-closure-bound".to_owned());
    }
    if limits.artifact_days == 0 || limits.artifact_days > ACTION_RETENTION_CEILING_DAYS {
        violations.push(format!(
            "Actions artifact retention is {} day(s); code ceiling is {ACTION_RETENTION_CEILING_DAYS}",
            limits.artifact_days
        ));
    }
    if limits.max_report_bytes == 0 || limits.max_report_bytes > REPORT_SIZE_CEILING_BYTES {
        violations.push(format!(
            "per-report limit is {} byte(s); code ceiling is {REPORT_SIZE_CEILING_BYTES}",
            limits.max_report_bytes
        ));
    }
    if limits.max_report_count == 0 || limits.max_report_count > REPORT_COUNT_CEILING {
        violations.push(format!(
            "report-count limit is {}; code ceiling is {REPORT_COUNT_CEILING}",
            limits.max_report_count
        ));
    }
    if limits.expected_report_count == 0 || limits.expected_report_count > limits.max_report_count {
        violations.push(format!(
            "expected report count {} is zero or exceeds maximum {}",
            limits.expected_report_count, limits.max_report_count
        ));
    }
    if limits.measured_adoption_bytes == 0
        || limits.measured_adoption_bytes > limits.expected_committed_bytes
    {
        violations.push(format!(
            "adoption baseline {} must be positive and no larger than expected budget {}",
            limits.measured_adoption_bytes, limits.expected_committed_bytes
        ));
    }
    if limits.expected_committed_bytes == 0
        || limits.expected_committed_bytes > EXPECTED_COMMITTED_SIZE_CEILING_BYTES
    {
        violations.push(format!(
            "expected committed-evidence budget is {} byte(s); code ceiling is {EXPECTED_COMMITTED_SIZE_CEILING_BYTES}",
            limits.expected_committed_bytes
        ));
    }
    if limits.max_committed_bytes < limits.expected_committed_bytes
        || limits.max_committed_bytes > COMMITTED_SIZE_CEILING_BYTES
    {
        violations.push(format!(
            "hard committed-evidence maximum {} must be at least expected budget {} and no larger than code ceiling {COMMITTED_SIZE_CEILING_BYTES}",
            limits.max_committed_bytes, limits.expected_committed_bytes
        ));
    }
    violations
}

fn verify_retained_set(
    root: &Path,
    retained_set: &Value,
    limits: &Limits,
) -> Result<SetStats, String> {
    let id = required_string(retained_set, "/id")?;
    let directory_name = required_string(retained_set, "/directory")?;
    let provenance_name = required_string(retained_set, "/provenance")?;
    if !is_file_name(provenance_name) {
        return Err(format!(
            "{id} provenance path is not a file name: {provenance_name}"
        ));
    }
    let directory = root.join(directory_name);
    if !directory.is_dir() {
        return Err(format!("{} does not exist", directory.display()));
    }

    let provenance = read_json(&directory.join(provenance_name))?;
    let entries = array_at(&provenance, "/evidence")?;
    let expected_reports = required_usize(retained_set, "/expected_reports")?;
    let mut violations = Vec::new();
    if entries.len() != expected_reports {
        violations.push(format!(
            "retained set {id} expects {expected_reports} report(s), provenance contains {}",
            entries.len()
        ));
    }

    let policy_campaigns = string_set_at(retained_set, "/campaigns")?;
    let manifest_campaigns = declared_campaigns(&provenance)?;
    if policy_campaigns != manifest_campaigns {
        violations.push(format!(
            "retained set {id} policy campaigns {policy_campaigns:?} do not match provenance campaigns {manifest_campaigns:?}"
        ));
    }

    let mut reports = BTreeSet::new();
    let mut report_bytes = 0;
    for entry in entries {
        let Some(name) = string_at(entry, "/report") else {
            violations.push(format!(
                "retained set {id} has a provenance entry with no report"
            ));
            continue;
        };
        if !is_file_name(name) {
            violations.push(format!(
                "retained set {id} report path is not a file name: {name}"
            ));
            continue;
        }
        if !reports.insert(name.to_owned()) {
            violations.push(format!(
                "retained set {id} report {name} is inventoried more than once"
            ));
            continue;
        }

        let report_path = directory.join(name);
        match fs::metadata(&report_path) {
            Ok(metadata) => {
                report_bytes += metadata.len();
                if metadata.len() > limits.max_report_bytes {
                    violations.push(format!(
                        "{} is {} byte(s), above the {} byte per-report limit; raw bytes must move to durable immutable external storage",
                        report_path.display(),
                        metadata.len(),
                        limits.max_report_bytes
                    ));
                }
            }
            Err(error) => {
                violations.push(format!("could not stat {}: {error}", report_path.display()));
                continue;
            }
        }
        match read_json(&report_path) {
            Ok(report) => violations.extend(verify_common_contract(name, entry, &report)),
            Err(error) => violations.push(error),
        }
    }

    let actual_reports = json_report_files(&directory, provenance_name)?;
    for missing in reports.difference(&actual_reports) {
        violations.push(format!(
            "retained set {id} inventories {missing} but the retained file is absent"
        ));
    }
    for untracked in actual_reports.difference(&reports) {
        violations.push(format!(
            "retained set {id} contains JSON report {untracked} that provenance does not inventory"
        ));
    }

    Ok(SetStats {
        violations,
        reports: reports.len(),
        report_bytes,
        committed_bytes: directory_bytes(&directory)?,
        campaigns: manifest_campaigns,
    })
}

fn verify_common_contract(name: &str, entry: &Value, report: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    verify_required_provenance_fields(name, entry, &mut violations);
    verify_provenance_shapes(name, entry, &mut violations);
    violations.extend(verify_report_identity(name, entry, report));
    violations.extend(verify_canonical_verdict(name, report));
    violations
}

fn verify_required_provenance_fields(name: &str, entry: &Value, violations: &mut Vec<String>) {
    for pointer in [
        "/campaign",
        "/matrix_point",
        "/postgres_major_version",
        "/producer/execution_commit",
        "/producer/branch_head_sha",
        "/producer/rustc",
        "/producer/os",
        "/producer/arch",
        "/workflow_run/workflow",
        "/workflow_run/workflow_file",
        "/workflow_run/event",
        "/workflow_run/conclusion",
        "/producing_job/name",
        "/producing_job/conclusion",
        "/artifact/name",
        "/artifact/digest",
        "/retained_report_git_blob",
        "/remote_verification/verified_at",
    ] {
        match string_at(entry, pointer) {
            Some(value) if !value.trim().is_empty() => {}
            _ => violations.push(format!("{name} records no non-empty {pointer}")),
        }
    }
    for pointer in [
        "/workflow_run/id",
        "/workflow_run/attempt",
        "/producing_job/id",
        "/artifact/id",
        "/artifact/size_bytes",
        "/remote_verification/run_id",
    ] {
        match entry.pointer(pointer).and_then(Value::as_u64) {
            Some(value) if value > 0 => {}
            _ => violations.push(format!("{name} records no positive {pointer}")),
        }
    }
}

fn verify_provenance_shapes(name: &str, entry: &Value, violations: &mut Vec<String>) {
    if entry
        .pointer("/producer/source_tree_clean")
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!("{name} does not record a clean producer tree"));
    }
    for pointer in ["/workflow_run/conclusion", "/producing_job/conclusion"] {
        if string_at(entry, pointer) != Some("success") {
            violations.push(format!("{name} records non-success {pointer}"));
        }
    }
    if entry
        .pointer("/remote_verification/verified")
        .and_then(Value::as_bool)
        != Some(true)
    {
        violations.push(format!("{name} remote verification is not confirmed"));
    }
    for field in [
        "workflow_run_identity",
        "workflow_run_conclusion",
        "producing_job_identity",
        "producing_job_conclusion",
        "artifact_digest",
        "artifact_bytes_match_retained_report",
        "execution_commit_matches_report",
    ] {
        if entry
            .pointer(&format!("/remote_verification/{field}"))
            .and_then(Value::as_bool)
            != Some(true)
        {
            violations.push(format!("{name} has no confirmed remote check for {field}"));
        }
    }
    if entry
        .pointer("/remote_verification/run_id")
        .and_then(Value::as_u64)
        != entry.pointer("/workflow_run/id").and_then(Value::as_u64)
    {
        violations.push(format!(
            "{name} remote verification run id does not match producer workflow run"
        ));
    }

    for pointer in ["/producer/execution_commit", "/producer/branch_head_sha"] {
        if let Some(value) = string_at(entry, pointer)
            && !is_hex(value, 40)
        {
            violations.push(format!("{name} records malformed git commit at {pointer}"));
        }
    }
    if let Some(value) = string_at(entry, "/retained_report_git_blob")
        && !is_hex(value, 40)
    {
        violations.push(format!(
            "{name} records malformed retained git blob identity"
        ));
    }
    if let Some(digest) = string_at(entry, "/artifact/digest")
        && !is_sha256(digest)
    {
        violations.push(format!("{name} artifact digest is not a full sha256"));
    }
}

fn verify_report_identity(name: &str, entry: &Value, report: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let execution = string_at(entry, "/producer/execution_commit");
    for pointer in [
        "/environment/source_commit",
        "/observation/execution_manifest/execution_commit",
    ] {
        if string_at(report, pointer) != execution {
            violations.push(format!(
                "{name} {pointer} does not match producer execution commit"
            ));
        }
    }
    if report
        .pointer("/environment/source_tree_clean")
        .and_then(Value::as_bool)
        != Some(true)
        || report
            .pointer("/observation/execution_manifest/tree_clean")
            .and_then(Value::as_bool)
            != Some(true)
    {
        violations.push(format!(
            "{name} report does not bind a clean execution tree"
        ));
    }

    for (entry_pointer, report_pointer, label) in [
        ("/matrix_point", "/environment/matrix", "matrix point"),
        ("/producer/rustc", "/environment/rustc", "rustc"),
        ("/producer/os", "/environment/os", "operating system"),
        ("/producer/arch", "/environment/arch", "architecture"),
        (
            "/postgres_major_version",
            "/postgresql_major_version",
            "PostgreSQL runtime axis",
        ),
    ] {
        if string_at(entry, entry_pointer) != string_at(report, report_pointer) {
            violations.push(format!(
                "{name} {label} disagrees between provenance and report"
            ));
        }
    }

    let Some(objects) = report
        .pointer("/observation/execution_manifest/objects")
        .and_then(Value::as_object)
    else {
        violations.push(format!("{name} records no execution-manifest object map"));
        return violations;
    };
    for required in ["Cargo.lock", "rust-toolchain.toml", "xtask/src/evidence.rs"] {
        if !objects.contains_key(required) {
            violations.push(format!(
                "{name} execution manifest does not bind required identity {required}"
            ));
        }
    }
    if let Some(workflow) = string_at(entry, "/workflow_run/workflow_file")
        && !objects.contains_key(workflow)
    {
        violations.push(format!(
            "{name} execution manifest does not bind producer workflow {workflow}"
        ));
    }

    let has_campaign_verifier = objects.keys().any(|path| {
        path.starts_with("xtask/src/")
            && Path::new(path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("rs"))
            && !matches!(
                path.as_str(),
                "xtask/src/evidence.rs" | "xtask/src/main.rs" | "xtask/src/suite.rs"
            )
    });
    if !has_campaign_verifier {
        violations.push(format!(
            "{name} execution manifest binds no campaign-specific verifier identity"
        ));
    }
    let has_semantics = objects.keys().any(|path| {
        path.starts_with("tests/fixtures/") && path.ends_with("/campaign-semantics.json")
    });
    let has_execution_contract = objects.keys().any(|path| {
        path.starts_with("tests/fixtures/") && path.ends_with("/execution-contract.json")
    });
    if !has_semantics || !has_execution_contract {
        violations.push(format!(
            "{name} execution manifest does not bind both campaign semantics and execution contract"
        ));
    }
    violations
}

fn verify_canonical_verdict(name: &str, report: &Value) -> Vec<String> {
    let Some(canonical) = report.get("violations").and_then(Value::as_array) else {
        return vec![format!(
            "{name} has no canonical violations collection; producer verdict is not sufficient proof"
        )];
    };

    let mut violations = Vec::new();
    let canonical_pass = canonical.is_empty();
    if !canonical_pass {
        violations.push(format!(
            "{name} carries {} canonical violation(s) and cannot be official retained evidence regardless of producer passed",
            canonical.len()
        ));
    }
    match report.get("passed").and_then(Value::as_bool) {
        Some(rendered) if rendered == canonical_pass => {}
        Some(rendered) => violations.push(format!(
            "{name} renders passed={rendered} while canonical violations imply passed={canonical_pass}"
        )),
        None => violations.push(format!(
            "{name} records no boolean passed mirror of the canonical verdict"
        )),
    }
    if let Some(result) = report.get("result") {
        let expected = if canonical_pass { "passed" } else { "failed" };
        if result.as_str() != Some(expected) {
            violations.push(format!(
                "{name} renders result={result} while canonical violations require {expected}"
            ));
        }
    }
    violations
}

fn verify_artifact_producers(
    root: &Path,
    policy: &Value,
    limits: &Limits,
    retained_ids: &BTreeSet<String>,
) -> Result<(Vec<String>, usize, BTreeSet<String>), String> {
    let producers = array_at(policy, "/artifact_producers")?;
    let mut violations = Vec::new();
    let mut policy_workflows = BTreeSet::new();
    let mut campaigns = BTreeSet::new();

    for producer in producers {
        let Some(workflow) = string_at(producer, "/workflow") else {
            violations.push("an artifact producer has no workflow".to_owned());
            continue;
        };
        if !policy_workflows.insert(workflow.to_owned()) {
            violations.push(format!(
                "artifact producer workflow {workflow} is declared twice"
            ));
        }
        let Some(campaign) = string_at(producer, "/campaign") else {
            violations.push(format!("artifact producer {workflow} names no campaign"));
            continue;
        };
        if !campaigns.insert(campaign.to_owned()) {
            violations.push(format!(
                "campaign {campaign} is assigned to more than one external artifact producer"
            ));
        }
        if string_at(producer, "/storage") != Some("github-actions") {
            violations.push(format!(
                "artifact producer {workflow} is not GitHub Actions storage"
            ));
        }
        if string_at(producer, "/classification") != Some("informational") {
            violations.push(format!(
                "artifact producer {workflow} is not informational; raw producer artifacts must never be authoritative"
            ));
        }
        match string_at(producer, "/promotes_to") {
            Some(id) if retained_ids.contains(id) => {}
            Some(id) => violations.push(format!(
                "artifact producer {workflow} promotes to unknown retained set {id}"
            )),
            None => violations.push(format!(
                "artifact producer {workflow} has no promotion target"
            )),
        }

        let path = root.join(workflow);
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        violations.extend(verify_upload_workflow(
            workflow,
            &source,
            limits.artifact_days,
        ));
    }

    let actual_workflows = upload_artifact_workflows(root)?;
    for missing in policy_workflows.difference(&actual_workflows) {
        violations.push(format!(
            "policy inventories artifact producer {missing}, but it no longer uploads an Actions artifact"
        ));
    }
    for unknown in actual_workflows.difference(&policy_workflows) {
        violations.push(format!(
            "workflow {unknown} uploads an Actions artifact and is absent from retained-evidence inventory"
        ));
    }

    Ok((violations, producers.len(), campaigns))
}

fn verify_upload_workflow(workflow: &str, source: &str, retention_days: u64) -> Vec<String> {
    let upload_lines = source
        .lines()
        .filter(|line| line.trim().starts_with(UPLOAD_ACTION))
        .collect::<Vec<_>>();
    let mut violations = Vec::new();
    if upload_lines.len() != 1 {
        violations.push(format!(
            "{workflow} has {} upload-artifact invocation(s); inventory expects exactly one evidence artifact producer",
            upload_lines.len()
        ));
    }
    for line in upload_lines {
        let value = line.trim().trim_start_matches(UPLOAD_ACTION);
        match value.split_whitespace().next() {
            Some(pin) if is_hex(pin, 40) => {}
            _ => violations.push(format!(
                "{workflow} upload-artifact action is not pinned to a full commit SHA"
            )),
        }
    }

    let required = format!("retention-days: {retention_days}");
    if !source.lines().any(|line| line.trim() == required) {
        violations.push(format!(
            "{workflow} does not set the required literal Actions artifact {required}"
        ));
    }
    violations
}

fn upload_artifact_workflows(root: &Path) -> Result<BTreeSet<String>, String> {
    let directory = root.join(WORKFLOWS);
    let entries = fs::read_dir(&directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?;
    let mut workflows = BTreeSet::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("could not enumerate workflows: {error}"))?
            .path();
        let extension = path.extension().and_then(|value| value.to_str());
        if extension != Some("yml") && extension != Some("yaml") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        if !source.contains("actions/upload-artifact@") {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|error| format!("could not relativize {}: {error}", path.display()))?;
        workflows.insert(relative.to_string_lossy().replace('\\', "/"));
    }
    Ok(workflows)
}

fn declared_campaigns(provenance: &Value) -> Result<BTreeSet<String>, String> {
    let declared = array_at(provenance, "/campaigns/declared")?;
    declared
        .iter()
        .map(|campaign| required_string(campaign, "/campaign").map(str::to_owned))
        .collect()
}

fn string_set_at(document: &Value, pointer: &str) -> Result<BTreeSet<String>, String> {
    array_at(document, pointer)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{pointer} contains a non-string value"))
        })
        .collect()
}

fn json_report_files(directory: &Path, provenance: &str) -> Result<BTreeSet<String>, String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?;
    let mut reports = BTreeSet::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("could not enumerate {}: {error}", directory.display()))?
            .path();
        if !path.is_file() || path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name != provenance {
            reports.insert(name.to_owned());
        }
    }
    Ok(reports)
}

fn directory_bytes(directory: &Path) -> Result<u64, String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?;
    let mut bytes = 0;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("could not enumerate {}: {error}", directory.display()))?
            .path();
        if path.is_dir() {
            return Err(format!(
                "{} contains subdirectory {}; retained evidence footprint accounting is intentionally flat",
                directory.display(),
                path.display()
            ));
        }
        if path.is_file() {
            bytes += fs::metadata(&path)
                .map_err(|error| format!("could not stat {}: {error}", path.display()))?
                .len();
        }
    }
    Ok(bytes)
}

fn workspace_root() -> Result<PathBuf, String> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|error| format!("could not invoke git: {error}"))?;
    if !output.status.success() {
        return Err("git rev-parse --show-toplevel failed".to_owned());
    }
    let root = String::from_utf8(output.stdout)
        .map_err(|error| format!("git returned a non-UTF-8 workspace root: {error}"))?;
    Ok(PathBuf::from(root.trim()))
}

fn read_json(path: &Path) -> Result<Value, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))
}

fn required_string<'a>(document: &'a Value, pointer: &str) -> Result<&'a str, String> {
    string_at(document, pointer)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing non-empty {pointer}"))
}

fn required_u64(document: &Value, pointer: &str) -> Result<u64, String> {
    document
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("missing unsigned integer {pointer}"))
}

fn required_usize(document: &Value, pointer: &str) -> Result<usize, String> {
    let value = required_u64(document, pointer)?;
    usize::try_from(value).map_err(|_| format!("{pointer} does not fit usize: {value}"))
}

fn string_at<'a>(document: &'a Value, pointer: &str) -> Option<&'a str> {
    document.pointer(pointer).and_then(Value::as_str)
}

fn array_at<'a>(document: &'a Value, pointer: &str) -> Result<&'a [Value], String> {
    document
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("missing array {pointer}"))
}

fn is_sha256(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| is_hex(hex, 64))
}

fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_file_name(value: &str) -> bool {
    Path::new(value).file_name().and_then(|name| name.to_str()) == Some(value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn producer_true_cannot_override_canonical_violations() {
        let report = json!({"violations": ["real failure"], "passed": true});
        let violations = super::verify_canonical_verdict("report.json", &report);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("cannot be official retained evidence"))
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("renders passed=true"))
        );
    }

    #[test]
    fn producer_false_cannot_disagree_with_empty_canonical_violations() {
        let report = json!({"violations": [], "passed": false});
        let violations = super::verify_canonical_verdict("report.json", &report);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("renders passed=false"))
        );
    }

    #[test]
    fn rendered_result_cannot_diverge_from_canonical_verdict() {
        let report = json!({"violations": [], "passed": true, "result": "failed"});
        let violations = super::verify_canonical_verdict("report.json", &report);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("canonical violations require passed"))
        );
    }

    #[test]
    fn missing_canonical_violations_fails_closed() {
        let report = json!({"passed": true});
        let violations = super::verify_canonical_verdict("report.json", &report);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("producer verdict is not sufficient proof"))
        );
    }

    #[test]
    fn upload_artifact_requires_literal_bounded_retention() {
        let source = "uses: actions/upload-artifact@0123456789012345678901234567890123456789\nwith:\n  name: evidence\n";
        let violations = super::verify_upload_workflow("workflow.yml", source, 30);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("retention-days: 30"))
        );
    }

    #[test]
    fn upload_artifact_rejects_movable_action_reference() {
        let source = "uses: actions/upload-artifact@v7\nwith:\n  retention-days: 30\n";
        let violations = super::verify_upload_workflow("workflow.yml", source, 30);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("full commit SHA"))
        );
    }

    #[test]
    fn exact_upload_artifact_contract_is_accepted() {
        let source = "uses: actions/upload-artifact@0123456789012345678901234567890123456789\nwith:\n  retention-days: 30\n";
        assert!(super::verify_upload_workflow("workflow.yml", source, 30).is_empty());
    }

    #[test]
    fn policy_ceiling_rejects_widened_report_limit() {
        let policy = json!({
            "schema_version": 1,
            "contract": {
                "canonical_verdict": "violations",
                "producer_passed_is_authoritative": false
            },
            "retention": {"git_age_policy": "semantic-closure"}
        });
        let limits = super::Limits {
            artifact_days: 30,
            max_report_bytes: super::REPORT_SIZE_CEILING_BYTES + 1,
            max_report_count: 64,
            expected_report_count: 21,
            measured_adoption_bytes: 1,
            expected_committed_bytes: 14 * 1024 * 1024,
            max_committed_bytes: 16 * 1024 * 1024,
        };
        let violations = super::verify_policy_contract(&policy, &limits);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("per-report limit"))
        );
    }

    #[test]
    fn sha256_requires_full_hex_digest() {
        assert!(super::is_sha256(&format!("sha256:{}", "a".repeat(64))));
        assert!(!super::is_sha256("sha256:"));
        assert!(!super::is_sha256(&format!("sha256:{}", "z".repeat(64))));
    }

    #[test]
    fn report_inventory_rejects_path_traversal() {
        assert!(super::is_file_name("report.json"));
        assert!(!super::is_file_name("../report.json"));
        assert!(!super::is_file_name("nested/report.json"));
    }
}
