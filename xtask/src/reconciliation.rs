//! The #102 reconciliation drift check.
//!
//! [`crate::evidence`] verifies that each retained campaign report is still
//! the untouched output of the CI run it names. This verifies something
//! narrower and specific to issue #102: that
//! `docs/project/m5-102-reconciliation.md` — the document that claims #102's
//! exit criteria are satisfied — still agrees with the repository it
//! describes.
//!
//! It exists because a reconciliation document has the same failure mode as a
//! retained report: it can be written once, be true at the time, and then
//! silently stop matching reality as a later change lands. Three things are
//! checked, all objective and all cheap to keep honest by hand:
//!
//! - the criterion table names exactly the ten criteria #102 reconciliation
//!   owes, each with exactly one disposition;
//! - the campaign and report counts the document states match what
//!   `evidence-provenance.json` and the retained-report directory actually
//!   contain;
//! - every repository-relative path the document cites as evidence resolves
//!   to a real file.
//!
//! What this does not attempt is judging whether a disposition is *correct*
//! — whether a `SATISFIED` row is honestly argued is a human-review
//! conclusion, not a parseable fact, and is not encoded here.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::evidence;
use crate::suite;

/// The document this check reads.
const DOCUMENT: &str = "docs/project/m5-102-reconciliation.md";

/// The criterion IDs the reconciliation document must carry, in order, each
/// with exactly one disposition. This mirrors the ten-row breakdown the #102
/// closure task itself defines: conformance, crash/restore, upgrade,
/// security, performance, soak, cancellation, resource bounds, reference
/// workload, and the P0/P1 correctness bar.
const REQUIRED_CRITERIA: &[&str] = &["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];

/// The accepted disposition tokens. A row must carry exactly one.
const DISPOSITIONS: &[&str] = &["SATISFIED", "BLOCKED", "NOT APPLICABLE"];

/// One verification pass and everything it found.
pub struct Verification {
    /// Every failure, as a human-readable line.
    pub violations: Vec<String>,
}

/// Verifies the #102 reconciliation document against the repository it
/// describes.
///
/// # Errors
///
/// Returns the failure that prevents verification from running at all, such
/// as an unreadable document or an unparsable provenance contract.
pub fn run() -> Result<Verification, String> {
    let root = suite::workspace_root()?;
    let path = root.join(DOCUMENT);
    let source =
        fs::read_to_string(&path).map_err(|error| format!("could not read {DOCUMENT}: {error}"))?;

    let mut violations = Vec::new();
    violations.extend(verify_criterion_table(&source));
    violations.extend(verify_declared_counts(&root, &source)?);
    violations.extend(verify_links(&root, &source));

    Ok(Verification { violations })
}

/// Requires the criterion table to name every required criterion exactly
/// once, each with exactly one recognized disposition.
fn verify_criterion_table(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();

    for line in source.lines() {
        let Some(id) = criterion_row_id(line) else {
            continue;
        };
        if !seen.insert(id) {
            violations.push(format!(
                "{DOCUMENT} names criterion {id} more than once in its criterion table"
            ));
        }

        let dispositions_present: Vec<&str> = DISPOSITIONS
            .iter()
            .copied()
            .filter(|token| line.contains(token))
            .collect();
        match dispositions_present.as_slice() {
            [] => violations.push(format!(
                "{DOCUMENT}'s row for criterion {id} names no recognized disposition \
                 (expected one of {DISPOSITIONS:?})"
            )),
            [_single] => {}
            multiple => violations.push(format!(
                "{DOCUMENT}'s row for criterion {id} names more than one disposition: \
                 {multiple:?}"
            )),
        }
    }

    for required in REQUIRED_CRITERIA {
        if !seen.contains(required) {
            violations.push(format!(
                "{DOCUMENT}'s criterion table does not name required criterion {required}"
            ));
        }
    }

    violations
}

/// Reads a criterion table row's leading `| <ID> |` cell, if the line is one.
///
/// Restricted to the single-letter IDs this document uses, so an unrelated
/// table row (the evidence-identity table, for instance) is never mistaken
/// for a criterion row.
fn criterion_row_id(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix('|')?;
    let cell = rest.split('|').next()?.trim();
    (cell.len() == 1 && cell.chars().next().is_some_and(|c| c.is_ascii_uppercase())).then_some(cell)
}

/// Requires the document's stated declared-campaign and retained-report
/// counts to match what the provenance contract and the retained-report
/// directory actually contain.
fn verify_declared_counts(root: &Path, source: &str) -> Result<Vec<String>, String> {
    let mut violations = Vec::new();

    let verification = evidence::run()?;
    let directory = evidence::directory();
    let provenance_path = root.join(&directory).join("evidence-provenance.json");
    let provenance_source = fs::read_to_string(&provenance_path)
        .map_err(|error| format!("could not read {}: {error}", provenance_path.display()))?;
    let provenance: Value = serde_json::from_str(&provenance_source)
        .map_err(|error| format!("could not parse {}: {error}", provenance_path.display()))?;
    let declared_campaigns = provenance
        .pointer("/campaigns/declared")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);

    let retained_reports = fs::read_dir(root.join(&directory))
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.ends_with(".json") && name != "evidence-provenance.json"
        })
        .count();

    if retained_reports != verification.reports {
        violations.push(format!(
            "{} contains {retained_reports} retained report file(s), but the provenance \
             contract's own declared evidence names {} — a file was added or removed without \
             updating its provenance entry",
            directory.display(),
            verification.reports
        ));
    }

    if let Some(stated) = stated_count(source, "Declared campaigns")
        && stated != declared_campaigns
    {
        violations.push(format!(
            "{DOCUMENT} states {stated} declared campaign(s), but \
             evidence-provenance.json's campaigns.declared has {declared_campaigns}"
        ));
    }
    if let Some(stated) = stated_count(source, "Retained reports")
        && stated != retained_reports
    {
        violations.push(format!(
            "{DOCUMENT} states {stated} retained report(s), but {} contains {retained_reports}",
            directory.display()
        ));
    }

    Ok(violations)
}

/// Reads the leading integer the document states for a `| <label> | <N> ...`
/// evidence-identity row.
fn stated_count(source: &str, label: &str) -> Option<usize> {
    for line in source.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('|') || !trimmed.contains(label) {
            continue;
        }
        let mut cells = trimmed.split('|').skip(1);
        let cell_label = cells.next()?.trim();
        if cell_label != label {
            continue;
        }
        let value_cell = cells.next()?.trim();
        let digits: String = value_cell
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(char::is_ascii_digit)
            .collect();
        return digits.parse().ok();
    }
    None
}

/// Requires every repository-relative markdown link the document cites to
/// resolve to a real file.
fn verify_links(root: &Path, source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let document_dir = root
        .join(DOCUMENT)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();

    for target in markdown_link_targets(source) {
        let without_fragment = target.split('#').next().unwrap_or(target);
        if without_fragment.is_empty() {
            continue;
        }
        if without_fragment.contains("://") {
            continue;
        }
        if !without_fragment.starts_with('.') && !without_fragment.starts_with('/') {
            continue;
        }
        let resolved = document_dir.join(without_fragment);
        if !resolved.exists() {
            violations.push(format!(
                "{DOCUMENT} links to {target}, which does not resolve to a file on disk \
                 ({})",
                resolved.display()
            ));
        }
    }

    violations
}

/// Extracts every markdown link target `(...)` in `](...)` position.
fn markdown_link_targets(source: &str) -> Vec<&str> {
    let mut targets = Vec::new();
    let mut rest = source;
    while let Some(start) = rest.find("](") {
        let after = &rest[start + 2..];
        let Some(end) = after.find(')') else {
            break;
        };
        targets.push(&after[..end]);
        rest = &after[end + 1..];
    }
    targets
}
