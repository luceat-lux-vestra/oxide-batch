//! Structural integrity of the shared M5 evidence record.
//!
//! The six campaigns each have their own reconciliation test, and each holds
//! its own campaign to its own denominator. What none of them holds is the
//! document all six write into, and that document was where the damage went
//! unnoticed: a merge left every campaign section after the conformance one
//! present twice, with the second copy describing a superseded memory rule. The
//! duplicate carried its own results tables and its own findings, so a reader
//! who scrolled to the wrong copy would have read a different algorithm, a
//! different threshold, and a different account of what the campaign proved —
//! all of it in the canonical record, none of it flagged.
//!
//! Nothing caught it because nothing was looking. Every individual claim in the
//! stale copy had been true when it was written, so no assertion about
//! *content* would have fired; what was wrong was that the file said two things
//! at once. That is a property of the document's shape, which is what these
//! tests hold.
//!
//! The checks are deliberately structural rather than textual. Forbidding the
//! superseded rule's vocabulary outright would be the obvious reflex and the
//! wrong one, because the record is required to explain the rules it discarded
//! — F20, F21 and F22 exist precisely to describe algorithms that are no longer
//! in force, and a test that banned the words would force the history to be
//! deleted to stay green.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;

/// The canonical evidence record every M5 campaign writes into.
const RECORD: &str = "docs/project/m5-campaign-evidence.md";

/// The declared semantic closure of the soak campaign.
const SEMANTICS: &str = "tests/fixtures/soak/campaign-semantics.json";

/// The campaign sections the record is required to carry, exactly once each.
const CAMPAIGNS: &[&str] = &[
    "## Conformance campaign",
    "## Crash and restore campaign",
    "## Upgrade campaign",
    "## Security campaign",
    "## Resource-bound campaign",
    "## Soak campaign",
    "## Cancellation campaign",
    "## Performance and reference-workload campaign",
];

/// Subsections that belong to exactly one campaign apiece.
///
/// These are the headings a duplicated campaign block brings with it, so their
/// count is the count of campaign blocks whether or not the `##` headings
/// themselves were disturbed.
const PER_CAMPAIGN_SUBSECTIONS: &[&str] =
    &["### What this campaign does not establish", "### Findings"];

#[test]
fn every_campaign_section_appears_exactly_once() -> Result<(), Box<dyn Error>> {
    let record = read(RECORD)?;

    for heading in CAMPAIGNS {
        let count = record.lines().filter(|line| line == heading).count();
        assert_eq!(
            count, 1,
            "the evidence record carries {heading:?} {count} times; a campaign described twice \
             lets a reader reach a superseded account of the same run without knowing they did",
        );
    }

    for heading in PER_CAMPAIGN_SUBSECTIONS {
        let count = record.lines().filter(|line| line == heading).count();
        assert_eq!(
            count,
            CAMPAIGNS.len(),
            "the evidence record carries {heading:?} {count} times against {} campaigns, so a \
             campaign block is duplicated or missing one",
            CAMPAIGNS.len(),
        );
    }

    Ok(())
}

#[test]
fn every_finding_is_numbered_once_and_without_a_gap() -> Result<(), Box<dyn Error>> {
    let record = read(RECORD)?;

    let mut seen: BTreeMap<u32, usize> = BTreeMap::new();
    for line in record.lines() {
        let Some(rest) = line.strip_prefix("**F") else {
            continue;
        };
        let digits = rest
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        if digits.is_empty() {
            continue;
        }
        *seen.entry(digits.parse()?).or_default() += 1;
    }

    assert!(
        !seen.is_empty(),
        "the evidence record declares no findings, so this test is checking nothing",
    );
    for (number, count) in &seen {
        assert_eq!(
            *count, 1,
            "finding F{number} is declared {count} times; the same identifier appearing twice \
             means two campaigns claim it or one block was copied",
        );
    }

    let highest = *seen.keys().next_back().unwrap_or(&0);
    let missing = (1..=highest)
        .filter(|number| !seen.contains_key(number))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the record numbers findings up to F{highest} but does not declare {missing:?}, so a \
         finding was dropped rather than superseded in place",
    );

    Ok(())
}

#[test]
fn the_record_agrees_with_the_declared_semantic_closure() -> Result<(), Box<dyn Error>> {
    let record = read(RECORD)?;
    let semantics = read(SEMANTICS)?;

    // The closure is the authority on what invalidates retained evidence, and
    // the record explains it in prose. The prose outlived the closure once: the
    // workflow file was added to the closure and the record went on saying it
    // was deliberately outside, which tells a reader the opposite of what the
    // mechanism does.
    let workflow_in_closure = semantics.contains(".github/workflows/ci.yml");
    let record_excludes_workflow = record.contains("the workflow file is not in the")
        || record.contains("The workflow file is deliberately outside");

    assert!(
        !(workflow_in_closure && record_excludes_workflow),
        "the declared closure contains .github/workflows/ci.yml and the evidence record says the \
         workflow file is outside it; retained evidence would be invalidated by a change the \
         record tells the reader is harmless",
    );
    assert!(
        workflow_in_closure || !record.contains("`.github/workflows/ci.yml` is in\nthe closure"),
        "the evidence record says the workflow file is in the closure and the closure does not \
         list it",
    );

    Ok(())
}

#[test]
fn the_recorded_memory_threshold_is_the_declared_one() -> Result<(), Box<dyn Error>> {
    let scope: serde_json::Value =
        serde_json::from_str(&read("tests/fixtures/soak/campaign-scope.json")?)?;
    let record = read(RECORD)?;

    let decay = scope["growth_rules"]["rules"]
        .as_array()
        .ok_or_else(|| Failure("the scope declares no growth rules".to_owned()))?
        .iter()
        .find_map(|rule| rule["decay_percent"].as_i64())
        .ok_or_else(|| Failure("no declared rule carries a decay percent".to_owned()))?;

    // Stated as a ratio in the record and as a percent in the scope. A record
    // that quotes a limit the campaign does not enforce is the same defect as a
    // duplicated section, reached by a shorter route.
    let limit = format!("`0.{decay}`");
    assert!(
        record.contains(&limit),
        "the campaign enforces a decay of {decay}% and the evidence record never states {limit} \
         as the limit",
    );

    Ok(())
}

/// Reads one canonical document from the workspace.
fn read(relative: &str) -> Result<String, Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    Ok(fs::read_to_string(root.join(relative))?)
}

/// A reconciliation failure with the sentence that explains it.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
