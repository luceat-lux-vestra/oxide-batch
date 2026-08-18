//! Release crate-set regression check.
//!
//! The accepted release set is five crates, published in dependency order:
//! `oxide-batch-core`, `oxide-batch-repository`, `oxide-batch-plan`,
//! `oxide-batch`, `oxide-batch-cli`. [RFC-0011](../../docs/rfcs/0011-publication-of-extracted-implementation-crates.md)
//! names this order explicitly, and
//! [ADR-0010](../../docs/architecture/decisions/0010-extracted-crate-publication.md)
//! calls the release "a five-crate ordered operation". A release-relevant
//! manifest or workflow file can drift from that decision independently of
//! any of the others, and nothing before this check compared them against
//! each other: this closes that gap.

use std::fs;
use std::process::Command;

use serde_json::Value;

use crate::suite;

const RELEASE_DRAFT_WORKFLOW: &str = ".github/workflows/release-draft.yml";
const RELEASE_WORKFLOW: &str = ".github/workflows/release.yml";

/// The accepted release order, per RFC-0011's "Version and release coupling".
const EXPECTED_RELEASED_CRATES: &[&str] = &[
    "oxide-batch-core",
    "oxide-batch-repository",
    "oxide-batch-plan",
    "oxide-batch",
    "oxide-batch-cli",
];

/// Runs the release crate-set regression check.
///
/// Returns every violation as a human-readable line. An empty result means
/// every source names the same five crates in the same accepted order.
pub fn check() -> Result<Vec<String>, String> {
    let mut violations = Vec::new();
    let root = suite::workspace_root()?;

    // `cargo metadata` does not preserve dependency order across packages,
    // so the manifest source is checked as a set only; the two workflow
    // files below are the sources that also owe the accepted publish order.
    let manifest_set = published_crates_from_manifests()?;
    violations.extend(compare(
        "published Cargo.toml manifests (publish = [\"crates-io\"])",
        &manifest_set,
        EnforceOrder::No,
    ));

    let draft_path = root.join(RELEASE_DRAFT_WORKFLOW);
    let draft_text = fs::read_to_string(&draft_path)
        .map_err(|error| format!("could not read {RELEASE_DRAFT_WORKFLOW}: {error}"))?;
    let draft_occurrences = extract_env_list(&draft_text, "RELEASED_CRATES");
    if draft_occurrences.is_empty() {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} declares no RELEASED_CRATES environment variable"
        ));
    }
    for (index, crates) in draft_occurrences.iter().enumerate() {
        violations.extend(compare(
            &format!(
                "{RELEASE_DRAFT_WORKFLOW} RELEASED_CRATES (occurrence {})",
                index + 1
            ),
            crates,
            EnforceOrder::Yes,
        ));
    }
    if draft_occurrences.len() > 1 && draft_occurrences.iter().any(|c| c != &draft_occurrences[0]) {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} declares RELEASED_CRATES more than once with \
             disagreeing values"
        ));
    }

    let release_path = root.join(RELEASE_WORKFLOW);
    let release_text = fs::read_to_string(&release_path)
        .map_err(|error| format!("could not read {RELEASE_WORKFLOW}: {error}"))?;
    let verify_crates = extract_step_packages(&release_text, "Verify packages")?;
    violations.extend(compare(
        &format!("{RELEASE_WORKFLOW} \"Verify packages\" step"),
        &verify_crates,
        EnforceOrder::Yes,
    ));
    let publish_crates = extract_step_packages(&release_text, "Publish to crates.io")?;
    violations.extend(compare(
        &format!("{RELEASE_WORKFLOW} \"Publish to crates.io\" step"),
        &publish_crates,
        EnforceOrder::Yes,
    ));

    Ok(violations)
}

/// Whether a source's declared order is itself part of what is checked.
///
/// `cargo metadata` does not preserve the workspace's declared member order,
/// so a manifest-derived crate set carries no ordering claim to check; the
/// release workflow files are hand-written and do owe the accepted
/// dependency order.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EnforceOrder {
    Yes,
    No,
}

/// Reports how `found` differs from [`EXPECTED_RELEASED_CRATES`].
///
/// A missing crate, an extra crate, and — when `enforce_order` requires it —
/// a crate present but out of order are each reported distinctly, because a
/// regression that only reorders the set is otherwise invisible to a simple
/// set-equality check.
fn compare(source: &str, found: &[String], enforce_order: EnforceOrder) -> Vec<String> {
    let mut violations = Vec::new();

    for expected in EXPECTED_RELEASED_CRATES {
        if !found.iter().any(|c| c == expected) {
            violations.push(format!("{source} is missing released crate \"{expected}\""));
        }
    }
    for crate_name in found {
        if !EXPECTED_RELEASED_CRATES.contains(&crate_name.as_str()) {
            violations.push(format!(
                "{source} names \"{crate_name}\", which is not in the accepted release set"
            ));
        }
    }

    if enforce_order == EnforceOrder::No {
        return violations;
    }

    let filtered: Vec<&str> = found
        .iter()
        .map(String::as_str)
        .filter(|c| EXPECTED_RELEASED_CRATES.contains(c))
        .collect();
    if filtered != EXPECTED_RELEASED_CRATES {
        violations.push(format!(
            "{source} lists {found:?}, which is not in the accepted dependency order {EXPECTED_RELEASED_CRATES:?}"
        ));
    }

    violations
}

/// Reads every publishable workspace member from `cargo metadata`.
///
/// A crate whose `publish` field is a non-empty registry list is published;
/// `publish = false` (an empty list) and the default-but-unpublished spikes
/// and `xtask` are excluded.
fn published_crates_from_manifests() -> Result<Vec<String>, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps", "--locked"])
        .output()
        .map_err(|error| format!("could not run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let metadata: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("could not parse cargo metadata: {error}"))?;

    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or_else(|| "cargo metadata returned no packages array".to_owned())?;

    let mut released = Vec::new();
    for package in packages {
        let name = package
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "a package in cargo metadata has no name".to_owned())?;
        let is_published = package
            .get("publish")
            .and_then(Value::as_array)
            .is_some_and(|registries| !registries.is_empty());
        if is_published {
            released.push(name.to_owned());
        }
    }
    // `cargo metadata` does not order packages by workspace declaration
    // order, so this list is compared as a set by `compare` rather than
    // trusted for dependency order; the workflow files are the ordered
    // sources.
    Ok(released)
}

/// Extracts every `key: value` line's whitespace-separated tokens.
///
/// Matches a GitHub Actions `env:` entry written as `KEY: a b c` on one
/// line, which is how this workspace declares `RELEASED_CRATES`. Returns one
/// entry per occurrence, so a key declared twice (once per job) is reported
/// separately rather than merged.
fn extract_env_list(text: &str, key: &str) -> Vec<Vec<String>> {
    let prefix = format!("{key}:");
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            trimmed.strip_prefix(&prefix).map(|rest| {
                rest.split_whitespace()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
        })
        .collect()
}

/// Extracts every `--package <name>` token from a named step's `run:` block.
///
/// Finds the `- name: <step_name>` line, then scans forward until the next
/// `- name:` line (or end of file), collecting `--package` arguments from
/// whatever it finds in between. This is a plain-text scan rather than a
/// YAML parse, matching this workspace's existing convention of comparing
/// workflow values as text (see `tests/fixtures/*/verify-ci-contract.sh`)
/// rather than adding a YAML-parsing dependency.
fn extract_step_packages(text: &str, step_name: &str) -> Result<Vec<String>, String> {
    let marker = format!("- name: {step_name}");
    let start = text
        .find(&marker)
        .ok_or_else(|| format!("no \"{marker}\" step found"))?;
    let after = &text[start + marker.len()..];
    let end = after.find("\n      - name:").unwrap_or(after.len());
    let block = &after[..end];

    let mut packages = Vec::new();
    let mut tokens = block.split_whitespace();
    while let Some(token) = tokens.next() {
        if token == "--package"
            && let Some(name) = tokens.next()
        {
            packages.push(name.to_owned());
        }
    }
    Ok(packages)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn the_real_workflows_and_manifests_agree_on_the_release_set() {
        let violations = check().expect("release crate check runs");
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn extract_env_list_reads_one_line_per_occurrence() {
        let text = "steps:\n  - env:\n      RELEASED_CRATES: a b c\n  - env:\n      RELEASED_CRATES: a b c\n";
        let found = extract_env_list(text, "RELEASED_CRATES");
        assert_eq!(
            found,
            vec![
                vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
                vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            ]
        );
    }

    #[test]
    fn extract_step_packages_stops_at_the_next_step() {
        let text = "jobs:\n  publish:\n    steps:\n      - name: Verify packages\n        run: >-\n          cargo publish --package a\n          --package b --locked --dry-run\n      - name: Authenticate\n        run: echo hi\n      - name: Publish to crates.io\n        run: >-\n          cargo publish --package a --package b --package c --locked\n";
        let verify = extract_step_packages(text, "Verify packages").expect("step found");
        assert_eq!(verify, vec!["a".to_owned(), "b".to_owned()]);
        let publish = extract_step_packages(text, "Publish to crates.io").expect("step found");
        assert_eq!(
            publish,
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
    }

    #[test]
    fn extract_step_packages_rejects_a_missing_step() {
        let error = extract_step_packages(
            "steps:\n  - name: Other\n    run: echo hi\n",
            "Verify packages",
        )
        .expect_err("missing step is an error");
        assert!(error.contains("Verify packages"));
    }

    #[test]
    fn compare_reports_a_missing_crate() {
        let found: Vec<String> = EXPECTED_RELEASED_CRATES[..4]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let violations = compare("test source", &found, EnforceOrder::Yes);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("missing released crate") && v.contains("oxide-batch-cli")),
            "{violations:#?}"
        );
    }

    #[test]
    fn compare_reports_an_unexpected_extra_crate() {
        let mut found: Vec<String> = EXPECTED_RELEASED_CRATES
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        found.push("oxide-batch-repository-postgres".to_owned());
        let violations = compare("test source", &found, EnforceOrder::Yes);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("not in the accepted release set")
                    && v.contains("oxide-batch-repository-postgres")),
            "{violations:#?}"
        );
    }

    #[test]
    fn compare_reports_an_out_of_order_set() {
        let mut found: Vec<String> = EXPECTED_RELEASED_CRATES
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        found.swap(0, 1);
        let violations = compare("test source", &found, EnforceOrder::Yes);
        assert!(
            violations
                .iter()
                .any(|v| v.contains("not in the accepted dependency order")),
            "{violations:#?}"
        );
    }

    #[test]
    fn compare_accepts_the_exact_expected_order() {
        let found: Vec<String> = EXPECTED_RELEASED_CRATES
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert!(compare("test source", &found, EnforceOrder::Yes).is_empty());
    }
}
