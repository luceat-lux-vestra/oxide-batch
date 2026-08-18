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
//!   owes — no more, no fewer — each with exactly one recognized disposition,
//!   parsed structurally from the table's own `Result` column rather than by
//!   scanning row text for a substring;
//! - the declared-campaign and retained-report counts the document states are
//!   each named exactly once, parse as an integer, and match what
//!   `evidence-provenance.json` and the retained-report directory actually
//!   contain — a missing row, a duplicated row, or an unparsable value is a
//!   violation in its own right, never a silently skipped comparison;
//! - every repository-relative path the document cites as evidence resolves
//!   to a real file.
//!
//! What this does not attempt is judging whether a disposition is *correct*
//! — whether a `SATISFIED` row is honestly argued is a human-review
//! conclusion, not a parseable fact, and is not encoded here.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::evidence;
use crate::suite;

/// The document this check reads.
const DOCUMENT: &str = "docs/project/m5-102-reconciliation.md";

/// Where retained evidence lives, mirrored from [`evidence::directory`] so
/// messages can name it without another lookup.
const EVIDENCE_DIRECTORY: &str = "docs/engineering/campaigns/m5";

/// The criterion IDs the reconciliation document must carry, in order, each
/// with exactly one disposition — no more, no fewer. This mirrors the
/// ten-row breakdown the #102 closure task itself defines: conformance,
/// crash/restore, upgrade, security, performance, soak, cancellation,
/// resource bounds, reference workload, and the P0/P1 correctness bar.
const REQUIRED_CRITERIA: &[&str] = &["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];

/// The accepted disposition tokens. A `Result` cell must start with exactly
/// one of these (after stripping markdown emphasis), so `NOT SATISFIED` — a
/// string that contains `SATISFIED` as a substring but starts with neither
/// `SATISFIED`, `BLOCKED`, nor the two-word literal `NOT APPLICABLE` — is
/// correctly rejected rather than loosely matched.
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

    let directory = root.join(evidence::directory());
    let provenance_path = directory.join("evidence-provenance.json");
    let provenance_source = fs::read_to_string(&provenance_path)
        .map_err(|error| format!("could not read {}: {error}", provenance_path.display()))?;
    let provenance: Value = serde_json::from_str(&provenance_source)
        .map_err(|error| format!("could not parse {}: {error}", provenance_path.display()))?;
    let declared_campaigns = provenance
        .pointer("/campaigns/declared")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let declared_reports = provenance
        .pointer("/evidence")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let retained_reports = fs::read_dir(&directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.ends_with(".json") && name != "evidence-provenance.json"
        })
        .count();

    let document_dir = root
        .join(DOCUMENT)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();

    let mut violations = Vec::new();
    violations.extend(verify_criterion_table(&source));
    violations.extend(verify_declared_counts(
        &source,
        declared_campaigns,
        declared_reports,
        retained_reports,
    ));
    violations.extend(verify_links(&source, |target| {
        document_dir.join(target).exists()
    }));

    Ok(Verification { violations })
}

/// One parsed markdown table: header cells and each data row's cells, in
/// column order.
struct Table<'a> {
    header: Vec<&'a str>,
    rows: Vec<Vec<&'a str>>,
}

/// Extracts the body of the `## <heading>` section: everything after that
/// exact heading line up to (not including) the next `## ` heading or the
/// end of the document.
///
/// Scoping every table lookup to its own section is what stops a
/// same-shaped row in an unrelated table — the evidence-identity table's
/// `Field`/`Value` rows, for instance — from ever being read as a criterion
/// row or vice versa.
fn section(source: &str, heading: &str) -> Option<String> {
    let marker = format!("## {heading}");
    let lines: Vec<&str> = source.lines().collect();
    let start = lines.iter().position(|line| *line == marker)?;
    let end = lines[start + 1..]
        .iter()
        .position(|line| line.starts_with("## "))
        .map_or(lines.len(), |offset| start + 1 + offset);
    Some(lines[start + 1..end].join("\n"))
}

/// Parses the first markdown table (header, separator, contiguous data
/// rows) found in `text`.
fn first_table(text: &str) -> Option<Table<'_>> {
    let lines: Vec<&str> = text.lines().collect();
    let header_index = lines
        .iter()
        .position(|line| line.trim_start().starts_with('|'))?;
    let separator = lines.get(header_index + 1)?;
    if !is_table_separator(separator) {
        return None;
    }
    let header = split_row(lines[header_index]);
    let mut rows = Vec::new();
    for line in &lines[header_index + 2..] {
        if !line.trim_start().starts_with('|') {
            break;
        }
        rows.push(split_row(line));
    }
    Some(Table { header, rows })
}

/// Whether `line` is a markdown table separator row (`| --- | --- |`, with
/// optional `:` alignment markers).
fn is_table_separator(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with('|') && trimmed.chars().all(|c| matches!(c, '|' | '-' | ':' | ' '))
}

/// Splits one markdown table row into trimmed cells, dropping the leading
/// and trailing empty cells the boundary `|` characters produce.
fn split_row(line: &str) -> Vec<&str> {
    let trimmed = line.trim();
    let trimmed = trimmed.strip_prefix('|').unwrap_or(trimmed);
    let trimmed = trimmed.strip_suffix('|').unwrap_or(trimmed);
    trimmed.split('|').map(str::trim).collect()
}

/// Requires the criterion table — scoped to the `## Criterion
/// reconciliation` section, never the whole document — to name exactly the
/// required criteria, each exactly once, each with exactly one recognized
/// disposition in its `Result` cell.
fn verify_criterion_table(source: &str) -> Vec<String> {
    let mut violations = Vec::new();

    let Some(section) = section(source, "Criterion reconciliation") else {
        violations.push(format!(
            "{DOCUMENT} has no \"## Criterion reconciliation\" section"
        ));
        return violations;
    };

    let Some(table) = first_table(&section) else {
        violations.push(format!(
            "{DOCUMENT}'s \"Criterion reconciliation\" section has no markdown table"
        ));
        return violations;
    };

    let Some(id_index) = table.header.iter().position(|cell| *cell == "#") else {
        violations.push(format!(
            "{DOCUMENT}'s criterion table has no \"#\" column to hold the criterion ID"
        ));
        return violations;
    };
    let Some(result_index) = table.header.iter().position(|cell| *cell == "Result") else {
        violations.push(format!(
            "{DOCUMENT}'s criterion table has no \"Result\" column"
        ));
        return violations;
    };

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in &table.rows {
        let Some(id_cell) = row.get(id_index).copied() else {
            continue;
        };
        if !is_criterion_id(id_cell) {
            continue;
        }
        *counts.entry(id_cell).or_insert(0) += 1;

        match row.get(result_index).copied() {
            Some(result_cell) if disposition_of(result_cell).is_some() => {}
            Some(result_cell) => violations.push(format!(
                "{DOCUMENT}'s row for criterion {id_cell} names no recognized disposition at \
                 the start of its Result cell (expected one of {DISPOSITIONS:?}): \
                 {result_cell:?}"
            )),
            None => violations.push(format!(
                "{DOCUMENT}'s row for criterion {id_cell} has no Result cell"
            )),
        }
    }

    for (id, count) in &counts {
        if *count > 1 {
            violations.push(format!(
                "{DOCUMENT} names criterion {id} {count} times in its criterion table; expected \
                 exactly once"
            ));
        }
    }

    let required: BTreeSet<&str> = REQUIRED_CRITERIA.iter().copied().collect();
    let found: BTreeSet<&str> = counts.keys().copied().collect();

    for missing in required.difference(&found) {
        violations.push(format!(
            "{DOCUMENT}'s criterion table does not name required criterion {missing}"
        ));
    }
    for extra in found.difference(&required) {
        violations.push(format!(
            "{DOCUMENT}'s criterion table names {extra}, which is not one of the required \
             criteria {REQUIRED_CRITERIA:?}"
        ));
    }

    violations
}

/// Whether `cell` is a bare single uppercase letter — the shape every
/// criterion ID in the `#` column takes.
fn is_criterion_id(cell: &str) -> bool {
    cell.len() == 1 && cell.chars().next().is_some_and(|c| c.is_ascii_uppercase())
}

/// Reads the disposition token a `Result` cell starts with, after stripping
/// a leading markdown emphasis marker.
///
/// Matching is a prefix check followed by a word-boundary check on what
/// follows, not a substring search: `NOT SATISFIED` starts with none of
/// `SATISFIED`, `BLOCKED`, or the two-word literal `NOT APPLICABLE`, so it
/// is correctly rejected rather than matching `SATISFIED` because that word
/// happens to appear later in the cell.
fn disposition_of(cell: &str) -> Option<&'static str> {
    let cleaned = cell.trim().trim_start_matches("**").trim_start();
    DISPOSITIONS.iter().copied().find(|token| {
        cleaned.strip_prefix(token).is_some_and(|rest| {
            rest.chars()
                .next()
                .is_none_or(|c| !c.is_ascii_alphanumeric())
        })
    })
}

/// Requires the document's stated declared-campaign and retained-report
/// counts, read from the `## Evidence identity` section, to match the real
/// counts computed from the repository.
///
/// A missing row, a duplicated row, or an unparsable value is itself a
/// violation and short-circuits the corresponding comparison — deleting a
/// row or corrupting its value can never silently suppress this check the
/// way an early `?`-propagated `None` could.
fn verify_declared_counts(
    source: &str,
    declared_campaigns: usize,
    declared_reports: usize,
    retained_reports: usize,
) -> Vec<String> {
    let mut violations = Vec::new();

    if retained_reports != declared_reports {
        violations.push(format!(
            "{EVIDENCE_DIRECTORY} contains {retained_reports} retained report file(s), but \
             evidence-provenance.json's own declared evidence names {declared_reports} — a \
             file was added or removed without updating its provenance entry"
        ));
    }

    let Some(section) = section(source, "Evidence identity") else {
        violations.push(format!(
            "{DOCUMENT} has no \"## Evidence identity\" section"
        ));
        return violations;
    };

    if let Some(stated) = required_stated_count(&section, "Declared campaigns", &mut violations)
        && stated != declared_campaigns
    {
        violations.push(format!(
            "{DOCUMENT} states {stated} declared campaign(s), but evidence-provenance.json's \
             campaigns.declared has {declared_campaigns}"
        ));
    }
    if let Some(stated) = required_stated_count(&section, "Retained reports", &mut violations)
        && stated != retained_reports
    {
        violations.push(format!(
            "{DOCUMENT} states {stated} retained report(s), but {EVIDENCE_DIRECTORY} contains \
             {retained_reports}"
        ));
    }

    violations
}

/// Reads exactly one `| <label> | <N> ... |` row's integer value from
/// `section`.
///
/// Fails closed: a missing row, more than one matching row, or a value that
/// does not parse as an integer each push a violation onto `violations` and
/// this returns `None`, so the caller's `if let Some(...)` cannot be
/// mistaken for an optional check — every one of those cases already
/// produced a violation before the caller sees `None`.
fn required_stated_count(
    section: &str,
    label: &str,
    violations: &mut Vec<String>,
) -> Option<usize> {
    let matches: Vec<&str> = section
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            if !trimmed.starts_with('|') {
                return None;
            }
            let mut cells = trimmed.split('|').skip(1);
            let cell_label = cells.next()?.trim();
            if cell_label != label {
                return None;
            }
            Some(cells.next()?.trim())
        })
        .collect();

    match matches.len() {
        0 => {
            violations.push(format!(
                "{DOCUMENT}'s evidence-identity table has no \"{label}\" row"
            ));
            None
        }
        1 => {
            let value_cell = matches[0];
            let digits: String = value_cell
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(value) = digits.parse::<usize>() {
                Some(value)
            } else {
                violations.push(format!(
                    "{DOCUMENT}'s \"{label}\" row does not state a parseable integer: \
                     {value_cell:?}"
                ));
                None
            }
        }
        n => {
            violations.push(format!(
                "{DOCUMENT}'s evidence-identity table has {n} \"{label}\" rows; expected \
                 exactly one"
            ));
            None
        }
    }
}

/// Requires every repository-relative markdown link the document cites to
/// resolve, per `exists`, to a real file.
///
/// `exists` is injected rather than resolved against the real filesystem
/// here so the check's logic — which links are repository-relative, which
/// are external or pure anchors — can be unit tested without touching disk.
fn verify_links(source: &str, mut exists: impl FnMut(&str) -> bool) -> Vec<String> {
    let mut violations = Vec::new();

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
        if !exists(without_fragment) {
            violations.push(format!(
                "{DOCUMENT} links to {target}, which does not resolve to a file on disk"
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::*;

    /// A minimal, well-formed reconciliation document: every required
    /// criterion, each `SATISFIED`, plus a matching evidence-identity table
    /// and one repository-relative link. Every mutation test below starts
    /// from this and breaks exactly one thing, so a synthetic string stands
    /// in for the sandbox/fixture a real file would otherwise need —
    /// nothing here ever touches the real document or the real repository.
    fn sample_document() -> String {
        let mut criterion_rows = String::new();
        for id in REQUIRED_CRITERIA {
            criterion_rows.push_str(&criterion_row(id, "**SATISFIED**"));
        }
        format!(
            "# Fixture\n\
             \n\
             ## Evidence identity\n\
             \n\
             | Field | Value |\n\
             | --- | --- |\n\
             | Declared campaigns | `8` (eight campaigns) |\n\
             | Retained reports | `16` (sixteen reports) |\n\
             \n\
             ## Criterion reconciliation\n\
             \n\
             | # | Requirement | Evidence | Result |\n\
             | --- | --- | --- | --- |\n\
             {criterion_rows}\
             \n\
             ## What is machine-checked, and what is not\n\
             \n\
             prose\n"
        )
    }

    /// One criterion-table row, matching [`sample_document`]'s 4-column
    /// `# | Requirement | Evidence | Result` shape exactly.
    fn criterion_row(id: &str, result: &str) -> String {
        format!(
            "| {id} | requirement | [evidence](../engineering/campaigns/m5/report.json) | \
             {result} |\n"
        )
    }

    /// The set of paths [`sample_document`]'s one link may resolve against.
    fn sample_exists(target: &str) -> bool {
        target == "../engineering/campaigns/m5/report.json"
    }

    fn verify_full(source: &str) -> Vec<String> {
        let mut violations = Vec::new();
        violations.extend(verify_criterion_table(source));
        violations.extend(verify_declared_counts(source, 8, 16, 16));
        violations.extend(verify_links(source, sample_exists));
        violations
    }

    #[test]
    fn the_sample_document_verifies() {
        assert_eq!(verify_full(&sample_document()), Vec::<String>::new());
    }

    #[test]
    fn a_missing_declared_campaigns_row_is_rejected() {
        let source =
            sample_document().replace("| Declared campaigns | `8` (eight campaigns) |\n", "");
        let violations = verify_declared_counts(&source, 8, 16, 16);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("no \"Declared campaigns\" row")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_missing_retained_reports_row_is_rejected() {
        let source =
            sample_document().replace("| Retained reports | `16` (sixteen reports) |\n", "");
        let violations = verify_declared_counts(&source, 8, 16, 16);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("no \"Retained reports\" row")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_duplicated_declared_campaigns_row_is_rejected() {
        let source = sample_document().replace(
            "| Declared campaigns | `8` (eight campaigns) |\n",
            "| Declared campaigns | `8` (eight campaigns) |\n\
             | Declared campaigns | `9` (a second, contradicting row) |\n",
        );
        let violations = verify_declared_counts(&source, 8, 16, 16);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("2 \"Declared campaigns\" rows")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_unparsable_declared_campaigns_value_is_rejected() {
        let source = sample_document().replace(
            "| Declared campaigns | `8` (eight campaigns) |\n",
            "| Declared campaigns | many |\n",
        );
        let violations = verify_declared_counts(&source, 8, 16, 16);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("does not state a parseable integer")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_unparsable_retained_reports_value_is_rejected() {
        let source = sample_document().replace(
            "| Retained reports | `16` (sixteen reports) |\n",
            "| Retained reports | none |\n",
        );
        let violations = verify_declared_counts(&source, 8, 16, 16);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("does not state a parseable integer")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_declared_campaigns_mismatch_is_rejected() {
        // The document states 8; the repository actually declares 9.
        let violations = verify_declared_counts(&sample_document(), 9, 16, 16);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("states 8 declared campaign(s)") && v.contains("has 9")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_retained_reports_mismatch_is_rejected() {
        // The document states 16; the repository actually retains 15.
        let violations = verify_declared_counts(&sample_document(), 8, 15, 15);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("states 16 retained report(s)") && v.contains("contains 15")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_retained_directory_count_disagreeing_with_provenance_is_rejected() {
        // The provenance contract declares 16 entries but the retained
        // directory actually holds 15 files — an orphan-provenance-entry
        // shape the document's own stated numbers cannot catch.
        let violations = verify_declared_counts(&sample_document(), 8, 16, 15);
        assert!(
            violations.iter().any(
                |v| v.contains("contains 15 retained report file(s)") && v.contains("names 16")
            ),
            "{violations:?}",
        );
    }

    #[test]
    fn a_missing_criterion_is_rejected() {
        let source = sample_document().replace(&criterion_row("A", "**SATISFIED**"), "");
        let violations = verify_criterion_table(&source);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("does not name required criterion A")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_duplicated_criterion_is_rejected() {
        let row = criterion_row("A", "**SATISFIED**");
        let source = sample_document().replacen(&row, &format!("{row}{row}"), 1);
        let violations = verify_criterion_table(&source);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("names criterion A 2 times")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_unexpected_criterion_is_rejected() {
        let source = sample_document().replace(
            "\n## What is machine-checked",
            &format!(
                "{}\n## What is machine-checked",
                criterion_row("K", "**SATISFIED**").trim_end()
            ),
        );
        let violations = verify_criterion_table(&source);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("names K, which is not one of the required criteria")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_removed_disposition_is_rejected() {
        let source = sample_document().replace(
            &criterion_row("A", "**SATISFIED**"),
            &criterion_row("A", ""),
        );
        let violations = verify_criterion_table(&source);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("row for criterion A names no recognized disposition")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_invalid_disposition_is_rejected() {
        let source = sample_document().replace("**SATISFIED** |\n", "**NOT SATISFIED** |\n");
        let violations = verify_criterion_table(&source);
        // Every one of the ten rows uses the same disposition text in this
        // fixture, so every row is flagged; the point under test is that
        // `NOT SATISFIED` is rejected at all, not merely once.
        assert!(
            violations
                .iter()
                .any(|v| v.contains("names no recognized disposition")
                    && v.contains("NOT SATISFIED")),
            "{violations:?}",
        );
    }

    #[test]
    fn a_nonexistent_evidence_link_is_rejected() {
        let source = sample_document().replace(
            "../engineering/campaigns/m5/report.json",
            "../engineering/campaigns/m5/does-not-exist.json",
        );
        let violations = verify_links(&source, sample_exists);
        assert!(
            violations.iter().any(|v| v.contains("does-not-exist.json")
                && v.contains("does not resolve to a file on disk")),
            "{violations:?}",
        );
    }

    #[test]
    fn an_external_link_is_never_checked_against_the_filesystem() {
        let source = sample_document().replace(
            "[evidence](../engineering/campaigns/m5/report.json)",
            "[evidence](https://example.com/report.json)",
        );
        // `sample_exists` only recognizes the repository-relative fixture
        // path, so if the external link were checked this would fail.
        let violations = verify_links(&source, sample_exists);
        assert_eq!(violations, Vec::<String>::new());
    }

    #[test]
    fn disposition_of_rejects_not_satisfied() {
        assert_eq!(disposition_of("NOT SATISFIED"), None);
        assert_eq!(disposition_of("**NOT SATISFIED**"), None);
    }

    #[test]
    fn disposition_of_accepts_every_recognized_token_with_trailing_prose() {
        assert_eq!(disposition_of("**SATISFIED**"), Some("SATISFIED"));
        assert_eq!(
            disposition_of("**BLOCKED** — missing evidence"),
            Some("BLOCKED")
        );
        assert_eq!(
            disposition_of("**NOT APPLICABLE** — out of scope"),
            Some("NOT APPLICABLE"),
        );
    }
}
