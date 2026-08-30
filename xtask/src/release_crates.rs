//! Release crate-set regression check.
//!
//! The accepted release set is six crates, published in dependency order:
//! `oxide-batch-core`, `oxide-batch-repository`, `oxide-batch-plan`,
//! `oxide-batch`, `oxide-batch-cli`, `oxide-batch-test`. [RFC-0011](../../docs/rfcs/0011-publication-of-extracted-implementation-crates.md)
//! named the original five explicitly, and
//! [ADR-0010](../../docs/architecture/decisions/0010-extracted-crate-publication.md)
//! called that release "a five-crate ordered operation";
//! [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) added
//! `oxide-batch-test` as the sixth, per the M6 Gate G decision that the
//! application test kit "shares `oxide-batch`'s release line/version
//! cadence" and the crate-publishing governance doc's own forecast that it
//! is "Likely public". A release-relevant manifest or workflow file can
//! drift from that decision independently of any of the others, and nothing
//! before this check compared them against each other: this closes that gap.

use std::collections::HashMap;
use std::fs;
use std::process::Command;

use serde_json::Value;

use crate::suite;

const RELEASE_DRAFT_WORKFLOW: &str = ".github/workflows/release-draft.yml";
const RELEASE_WORKFLOW: &str = ".github/workflows/release.yml";

/// The `jq` filter every dynamically-derived `RELEASED_CRATES` computation
/// uses, matching [`published_crates_from_manifests`]'s own selection rule.
///
/// `release-draft.yml`'s two steps and `release.yml`'s "Verify manually
/// bootstrapped crates.io archives" step derive their crate list from the
/// checked-out tag's own manifests at run time instead of a hardcoded list,
/// specifically so a `workflow_dispatch` recovery/audit run against an
/// *older* tag (one that predates a later-added released crate, as v0.5.0
/// predates `oxide-batch-test`) names exactly what that tag released, not
/// the current accepted set. A hardcoded list there would silently break
/// that recovery path the next time the accepted set grows. This constant
/// lets the check below confirm the dynamic derivation is still present
/// rather than having quietly reverted to a hardcoded list.
const DYNAMIC_RELEASE_SET_FILTER: &str =
    r".packages[] | select((.publish // []) | length > 0) | .name";

/// The accepted release order, per RFC-0011's "Version and release coupling".
const EXPECTED_RELEASED_CRATES: &[&str] = &[
    "oxide-batch-core",
    "oxide-batch-repository",
    "oxide-batch-plan",
    "oxide-batch",
    "oxide-batch-cli",
    "oxide-batch-test",
];

/// The release candidate prepared by this change. A stale lockstep update
/// must fail before packaging or publication.
const EXPECTED_RELEASE_VERSION: &str = "0.6.0";

/// Runs the release crate-set regression check.
///
/// Returns every violation as a human-readable line. An empty result means
/// every source names the same six crates in the same accepted order.
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
    match release_order() {
        Ok(order) => violations.extend(compare(
            "Cargo metadata dependency order",
            &order,
            EnforceOrder::Yes,
        )),
        Err(error) => violations.push(error),
    }

    // release-draft.yml's two steps derive their crate list dynamically
    // (see DYNAMIC_RELEASE_SET_FILTER) rather than declaring it statically,
    // so a workflow_dispatch recovery/audit run against an older tag names
    // exactly what that tag released. Dynamic derivation is self-verifying
    // by construction (it reads the checked-out tree's real manifests at
    // run time), so what this checks is that the derivation itself is still
    // present rather than having quietly reverted to a hardcoded list.
    let draft_path = root.join(RELEASE_DRAFT_WORKFLOW);
    let draft_text = fs::read_to_string(&draft_path)
        .map_err(|error| format!("could not read {RELEASE_DRAFT_WORKFLOW}: {error}"))?;
    if !draft_text.contains("test \"$(git rev-parse HEAD)\" = \"${tag_commit}\"") {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} must verify that checkout HEAD is the immutable tag commit"
        ));
    }
    let draft_dynamic_occurrences = draft_text.matches(DYNAMIC_RELEASE_SET_FILTER).count();
    if draft_dynamic_occurrences != 2 {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} has {draft_dynamic_occurrences} dynamic release-set \
             derivation(s) (expected 2, one per step that packages/publishes the release set); \
             a hardcoded RELEASED_CRATES list there would break workflow_dispatch recovery \
             against a tag that predates a later-added released crate"
        ));
    }
    if extract_env_list(&draft_text, "RELEASED_CRATES")
        .iter()
        .any(|crates| !crates.is_empty())
    {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} declares a static RELEASED_CRATES environment variable; \
             it must derive the release set dynamically instead (see DYNAMIC_RELEASE_SET_FILTER)"
        ));
    }

    let sbom_attestation_crates = extract_sbom_attestation_crates(&draft_text)?;
    violations.extend(compare_exact_set(
        &format!("{RELEASE_DRAFT_WORKFLOW} SBOM attestations"),
        &sbom_attestation_crates,
        &manifest_set,
    ));
    if !draft_text.contains("- name: Attest oxide-batch-test SBOM\n        if: steps.package.outputs.has_test == 'true'") {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} must skip the M6 test-kit attestation when recovering a pre-M6 tag"
        ));
    }

    let release_path = root.join(RELEASE_WORKFLOW);
    let release_text = fs::read_to_string(&release_path)
        .map_err(|error| format!("could not read {RELEASE_WORKFLOW}: {error}"))?;
    if release_text.contains("\n  id-token: write")
        || release_text.matches("id-token: write").count() != 1
    {
        violations.push(format!(
            "{RELEASE_WORKFLOW} must grant id-token: write only to the OIDC publication job"
        ));
    }
    if release_text
        .matches("git rev-parse \"${RELEASE_TAG}^{commit}\"")
        .count()
        != 2
    {
        violations.push(format!(
            "{RELEASE_WORKFLOW} must verify the checked-out immutable tag in both verification and publication jobs"
        ));
    }
    let verify_block = extract_step_block(&release_text, "Verify packages")?;
    if !verify_block.contains("cargo publish --workspace --locked --dry-run") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify packages\" must dry-run the metadata-derived workspace release set"
        ));
    }
    let publish_block = extract_step_block(&release_text, "Publish to crates.io")?;
    if publish_block.contains("PENDING_CRATES")
        || !publish_block.contains("cargo publish -p")
        || !publish_block.contains("version_status")
        || !publish_block.contains("registry_sha")
        || !publish_block.contains("already published with matching checksum")
    {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Publish to crates.io\" must recheck each exact registry version/checksum immediately before publishing in metadata-derived order"
        ));
    }
    if !release_text.contains("release-order") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} must obtain publication order from cargo xtask release-order"
        ));
    }
    let bootstrap_dynamic_occurrences = release_text.matches(DYNAMIC_RELEASE_SET_FILTER).count();
    if bootstrap_dynamic_occurrences != 1 {
        violations.push(format!(
            "{RELEASE_WORKFLOW} has {bootstrap_dynamic_occurrences} dynamic release-set \
             derivation(s) in its bootstrapped-archive verification step (expected 1); a \
             hardcoded RELEASED_CRATES list there would break workflow_dispatch recovery \
             against the v0.5.0 tag, which predates oxide-batch-test"
        ));
    }
    for required in [
        "publish-registered",
        "PUBLISH_REGISTERED_ONLY",
        "bootstrap_required",
        "RECOVERY_MODE",
    ] {
        if !release_text.contains(required) {
            violations.push(format!(
                "{RELEASE_WORKFLOW} is missing the fail-closed registered-only bootstrap contract marker {required:?}"
            ));
        }
    }

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
    Ok(published_package_metadata()?
        .into_iter()
        .map(|package| package.name)
        .collect())
}

#[derive(Debug)]
struct WorkspaceDependency {
    name: String,
    requirement: String,
}

#[derive(Debug)]
struct PublishedPackage {
    name: String,
    version: String,
    path_dependencies: Vec<WorkspaceDependency>,
}

/// Loads the publishable workspace packages and their publishable path edges.
fn published_package_metadata() -> Result<Vec<PublishedPackage>, String> {
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
            let version = package
                .get("version")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("published package {name} has no version"))?;
            let path_dependencies = package
                .get("dependencies")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("published package {name} has no dependencies"))?
                .iter()
                .filter(|dependency| dependency.get("path").is_some())
                .map(|dependency| {
                    let dependency_name = dependency
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            format!("package {name} has a path dependency without a name")
                        })?;
                    let requirement = dependency
                        .get("req")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            format!(
                                "package {name} path dependency {dependency_name} has no requirement"
                            )
                        })?;
                    Ok(WorkspaceDependency {
                        name: dependency_name.to_owned(),
                        requirement: requirement.to_owned(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            released.push(PublishedPackage {
                name: name.to_owned(),
                version: version.to_owned(),
                path_dependencies,
            });
        }
    }
    Ok(released)
}

/// Derives and validates the current publication DAG from Cargo metadata.
///
/// A published workspace edge must use the exact candidate version. The
/// stable accepted order is only a tie-breaker for independent packages; it
/// cannot override a real dependency edge.
pub fn release_order() -> Result<Vec<String>, String> {
    let packages = published_package_metadata()?;
    let names: Vec<String> = packages
        .iter()
        .map(|package| package.name.clone())
        .collect();
    if names.len() != EXPECTED_RELEASED_CRATES.len()
        || EXPECTED_RELEASED_CRATES
            .iter()
            .any(|expected| !names.iter().any(|name| name == expected))
        || names
            .iter()
            .any(|name| !EXPECTED_RELEASED_CRATES.contains(&name.as_str()))
    {
        return Err(format!(
            "Cargo metadata publishable set is {names:?}, expected exactly {EXPECTED_RELEASED_CRATES:?}"
        ));
    }

    let by_name: HashMap<&str, &PublishedPackage> = packages
        .iter()
        .map(|package| (package.name.as_str(), package))
        .collect();
    for package in &packages {
        if package.version != EXPECTED_RELEASE_VERSION {
            return Err(format!(
                "publishable package {} has version {}, expected lockstep {}",
                package.name, package.version, EXPECTED_RELEASE_VERSION
            ));
        }
    }

    let mut indegree: HashMap<&str, usize> = names.iter().map(|name| (name.as_str(), 0)).collect();
    let mut dependents: HashMap<&str, Vec<&str>> = names
        .iter()
        .map(|name| (name.as_str(), Vec::new()))
        .collect();
    for package in &packages {
        for dependency in &package.path_dependencies {
            let Some(target) = by_name.get(dependency.name.as_str()) else {
                return Err(format!(
                    "publishable package {} has a path dependency {:?} outside the accepted release set",
                    package.name, dependency.name
                ));
            };
            if target.version != EXPECTED_RELEASE_VERSION {
                return Err(format!(
                    "publishable package {} depends on {} at version {}, expected exact ={}",
                    package.name, dependency.name, target.version, EXPECTED_RELEASE_VERSION
                ));
            }
            let expected_requirement = format!("={EXPECTED_RELEASE_VERSION}");
            if dependency.requirement != expected_requirement {
                return Err(format!(
                    "publishable package {} depends on {} with requirement {:?}, expected {:?}",
                    package.name, dependency.name, dependency.requirement, expected_requirement
                ));
            }
            *indegree
                .get_mut(package.name.as_str())
                .ok_or_else(|| format!("missing indegree for {}", package.name))? += 1;
            dependents
                .get_mut(dependency.name.as_str())
                .ok_or_else(|| format!("missing dependent list for {}", dependency.name))?
                .push(package.name.as_str());
        }
    }

    let order_index = |name: &str| {
        EXPECTED_RELEASED_CRATES
            .iter()
            .position(|expected| *expected == name)
            .unwrap_or(usize::MAX)
    };
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter_map(|(name, degree)| (*degree == 0).then_some(*name))
        .collect();
    let mut order = Vec::with_capacity(names.len());
    while !ready.is_empty() {
        ready.sort_by_key(|name| order_index(name));
        let next = ready.remove(0);
        order.push(next.to_owned());
        for dependent in dependents
            .get(next)
            .ok_or_else(|| format!("missing dependents for {next}"))?
        {
            let degree = indegree
                .get_mut(dependent)
                .ok_or_else(|| format!("missing indegree for {dependent}"))?;
            *degree -= 1;
            if *degree == 0 {
                ready.push(dependent);
            }
        }
    }
    if order.len() != names.len() {
        return Err("published package dependency graph contains a cycle".to_owned());
    }
    Ok(order)
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
#[cfg(test)]
fn extract_step_packages(text: &str, step_name: &str) -> Result<Vec<String>, String> {
    let block = extract_step_block(text, step_name)?;

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

/// Returns the text belonging to one workflow step.
fn extract_step_block<'a>(text: &'a str, step_name: &str) -> Result<&'a str, String> {
    let marker = format!("- name: {step_name}");
    let start = text
        .find(&marker)
        .ok_or_else(|| format!("no \"{marker}\" step found"))?;
    let after = &text[start + marker.len()..];
    let end = after.find("\n      - name:").unwrap_or(after.len());
    Ok(&after[..end])
}

/// Extracts the crate names from release-draft's explicit SBOM attestation
/// steps and verifies that each step names the matching archive and SBOM.
///
/// actions/attest is intentionally kept as one explicit step per crate: the
/// action's subject/SBOM inputs are security-sensitive release evidence, and a
/// static contract makes a newly publishable crate fail in CI until its
/// attestation is reviewed and added. The check compares the result with the
/// publishable manifests rather than only with the accepted historical list,
/// so a future manifest addition cannot silently omit release evidence.
fn extract_sbom_attestation_crates(text: &str) -> Result<Vec<String>, String> {
    const SUBJECT_PREFIX: &str = "target/package/";
    const SUBJECT_SUFFIX: &str = "-${{ steps.package.outputs.version }}.crate";
    const SBOM_SUFFIX: &str = "-${{ steps.package.outputs.version }}.cdx.json";

    let mut crates = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("- name: Attest ") || !trimmed.ends_with(" SBOM") {
            continue;
        }

        let mut subject_path = None;
        let mut sbom_path = None;
        let mut uses = None;
        for following in text.lines().skip(index + 1) {
            let following_trimmed = following.trim_start();
            if following_trimmed.starts_with("- name:") {
                break;
            }
            if let Some(action) = following_trimmed.strip_prefix("uses:") {
                if uses.is_some() {
                    return Err(format!(
                        "{RELEASE_DRAFT_WORKFLOW} has an SBOM attestation with multiple action references"
                    ));
                }
                uses = Some(action.trim());
            }
            if let Some(path) = following_trimmed.strip_prefix("subject-path:") {
                subject_path = Some(path.trim());
            }
            if let Some(path) = following_trimmed.strip_prefix("sbom-path:") {
                sbom_path = Some(path.trim());
            }
        }

        let uses = uses.ok_or_else(|| {
            format!("{RELEASE_DRAFT_WORKFLOW} has an SBOM attestation without an action reference")
        })?;
        validate_sbom_attestation_action(uses)?;

        let subject_path = subject_path.ok_or_else(|| {
            format!("{RELEASE_DRAFT_WORKFLOW} has an SBOM attestation without subject-path")
        })?;
        let sbom_path = sbom_path.ok_or_else(|| {
            format!("{RELEASE_DRAFT_WORKFLOW} has an SBOM attestation without sbom-path")
        })?;
        let crate_name = subject_path
            .strip_prefix(SUBJECT_PREFIX)
            .and_then(|path| path.strip_suffix(SUBJECT_SUFFIX))
            .ok_or_else(|| {
                format!("{RELEASE_DRAFT_WORKFLOW} has an SBOM attestation with an unexpected subject-path: {subject_path}")
            })?;
        let expected_sbom = format!("{SUBJECT_PREFIX}{crate_name}{SBOM_SUFFIX}");
        if sbom_path != expected_sbom {
            return Err(format!(
                "{RELEASE_DRAFT_WORKFLOW} has an SBOM attestation for {crate_name} with sbom-path {sbom_path:?}; expected {expected_sbom:?}"
            ));
        }
        crates.push(crate_name.to_owned());
    }

    if crates.is_empty() {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} has no explicit SBOM attestation steps"
        ));
    }
    Ok(crates)
}

/// Requires the release-evidence action to be the reviewed attestation action
/// at an immutable full commit SHA. Version tags and other action identities
/// are not an acceptable release contract, even when their inputs are valid.
fn validate_sbom_attestation_action(uses: &str) -> Result<(), String> {
    let action_ref = uses
        .split_once(" #")
        .map_or(uses, |(action, _)| action)
        .trim();
    let (action, reference) = action_ref.split_once('@').ok_or_else(|| {
        format!(
            "{RELEASE_DRAFT_WORKFLOW} SBOM attestation must use actions/attest@<40-hex-SHA>; found {uses:?}"
        )
    })?;
    if action != "actions/attest"
        || reference.len() != 40
        || !reference
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} SBOM attestation must use actions/attest@<40-hex-SHA>; found {uses:?}"
        ));
    }
    Ok(())
}

/// Reports missing, extra, or duplicate entries when a source must match a
/// dynamically discovered set exactly.
fn compare_exact_set(source: &str, found: &[String], expected: &[String]) -> Vec<String> {
    let mut violations = Vec::new();
    for expected_crate in expected {
        if !found.iter().any(|crate_name| crate_name == expected_crate) {
            violations.push(format!(
                "{source} is missing publishable crate {expected_crate:?}"
            ));
        }
    }
    for found_crate in found {
        if !expected.iter().any(|crate_name| crate_name == found_crate) {
            violations.push(format!(
                "{source} names {found_crate:?}, which is not publishable"
            ));
        }
    }
    if found.len() != expected.len()
        || expected
            .iter()
            .any(|expected_crate| found.iter().filter(|c| *c == expected_crate).count() != 1)
    {
        violations.push(format!(
            "{source} lists {found:?}, expected exactly {expected:?}"
        ));
    }
    violations
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
    fn extract_sbom_attestation_crates_requires_matching_archive_and_sbom() {
        let text = "steps:\n  - name: Attest a SBOM\n    uses: actions/attest@0123456789abcdef0123456789abcdef01234567 # v4\n    with:\n      subject-path: target/package/a-${{ steps.package.outputs.version }}.crate\n      sbom-path: target/package/a-${{ steps.package.outputs.version }}.cdx.json\n  - name: Attest b SBOM\n    uses: actions/attest@0123456789abcdef0123456789abcdef01234567\n    with:\n      subject-path: target/package/b-${{ steps.package.outputs.version }}.crate\n      sbom-path: target/package/b-${{ steps.package.outputs.version }}.cdx.json\n";
        assert_eq!(
            extract_sbom_attestation_crates(text).expect("attestation steps parse"),
            vec!["a".to_owned(), "b".to_owned()]
        );
    }

    fn one_sbom_attestation(uses: Option<&str>) -> String {
        let uses_line = uses.map_or(String::new(), |value| format!("    uses: {value}\n"));
        format!(
            "steps:\n  - name: Attest a SBOM\n{uses_line}    with:\n      subject-path: target/package/a-${{ steps.package.outputs.version }}.crate\n      sbom-path: target/package/a-${{ steps.package.outputs.version }}.cdx.json\n"
        )
    }

    #[test]
    fn extract_sbom_attestation_crates_rejects_a_version_tag() {
        let error =
            extract_sbom_attestation_crates(&one_sbom_attestation(Some("actions/attest@v4")))
                .expect_err("mutable action ref is rejected");
        assert!(error.contains("actions/attest@<40-hex-SHA>"), "{error}");
    }

    #[test]
    fn extract_sbom_attestation_crates_rejects_a_truncated_sha() {
        let error = extract_sbom_attestation_crates(&one_sbom_attestation(Some(
            "actions/attest@0123456789abcdef",
        )))
        .expect_err("truncated action ref is rejected");
        assert!(error.contains("40-hex-SHA"), "{error}");
    }

    #[test]
    fn extract_sbom_attestation_crates_rejects_a_different_action() {
        let error = extract_sbom_attestation_crates(&one_sbom_attestation(Some(
            "some-other/action@0123456789abcdef0123456789abcdef01234567",
        )))
        .expect_err("different action is rejected");
        assert!(error.contains("actions/attest@<40-hex-SHA>"), "{error}");
    }

    #[test]
    fn extract_sbom_attestation_crates_rejects_a_missing_action() {
        let error = extract_sbom_attestation_crates(&one_sbom_attestation(None))
            .expect_err("missing action is rejected");
        assert!(error.contains("without an action reference"), "{error}");
    }

    #[test]
    fn compare_exact_set_reports_a_missing_publishable_crate() {
        let found = vec!["a".to_owned()];
        let expected = vec!["a".to_owned(), "b".to_owned()];
        let violations = compare_exact_set("test source", &found, &expected);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("missing publishable crate")
                    && violation.contains('b')),
            "{violations:#?}"
        );
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
