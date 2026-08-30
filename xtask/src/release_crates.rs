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

/// The in-toto predicate type GitHub's `actions/attest` records for a plain
/// `subject-path`-only call (no `sbom-path`), confirmed against a real
/// attestation from the `v0.6.0` tag run.
const PROVENANCE_PREDICATE_TYPE: &str = "https://slsa.dev/provenance/v1";

/// The in-toto predicate type GitHub's `actions/attest` records when called
/// with `sbom-path` pointing at a `CycloneDX` JSON document, confirmed against
/// a real attestation from the `v0.6.0` tag run.
const SBOM_PREDICATE_TYPE: &str = "https://cyclonedx.org/bom";

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
    if draft_dynamic_occurrences != 4 {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} has {draft_dynamic_occurrences} dynamic release-set \
             derivation(s) (expected 4: package/publish, SBOM generation, #212's existing-SBOM- \
             coverage check, and #212's attestation-coverage verification); a hardcoded \
             RELEASED_CRATES list there would break workflow_dispatch recovery against a tag \
             that predates a later-added released crate"
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
    if !draft_text.contains(
        "- name: Attest oxide-batch-test SBOM\n        if: steps.package.outputs.has_test == 'true' && steps.sbom_coverage.outputs.oxide_batch_test_sbom_missing == 'true'",
    ) {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} must skip the M6 test-kit attestation both when recovering \
             a pre-M6 tag and when its SBOM attestation already exists"
        ));
    }
    if let Err(violation) = check_verify_tag_and_package_not_flattened(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_attestation_coverage_verification(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_provenance_gated_to_tag_push(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_sbom_coverage_gating(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_provenance_source_verification(&draft_text) {
        violations.push(violation);
    }

    violations.extend(check_release_workflow(&root)?);

    Ok(violations)
}

/// Checks the immutable-tag, bootstrap, and idempotent publication contract.
fn check_release_workflow(root: &std::path::Path) -> Result<Vec<String>, String> {
    let release_path = root.join(RELEASE_WORKFLOW);
    let release_text = fs::read_to_string(&release_path)
        .map_err(|error| format!("could not read {RELEASE_WORKFLOW}: {error}"))?;
    let mut violations = Vec::new();
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
    let rechecks_exact_versions = publish_block.contains("version_status")
        && publish_block.contains("registry_sha")
        && publish_block.contains("already published with matching checksum");
    if publish_block.contains("PENDING_CRATES")
        || !publish_block.contains("cargo publish -p")
        || !rechecks_exact_versions
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

/// Checks that `RELEASED_CRATES` is not flattened to one line before the
/// exact-line `grep -Fxq` membership check in "Verify tag and package".
///
/// #212: `RELEASED_CRATES` was piped through `tr '\n' ' '`, collapsing every
/// released crate name onto a single space-joined line. `grep -Fxq
/// "oxide-batch-test"` requires an *entire line* to equal that one crate
/// name, which a multi-crate joined line can never do, so `has_test` was
/// always false and the crate's SBOM attestation step always silently
/// skipped. The fix keeps the `jq` output newline-separated (`for crate in
/// ${RELEASED_CRATES}` still splits correctly on embedded newlines via the
/// default `IFS`), so this checks the flattening pipe has not crept back in
/// on the derivation that feeds that exact-line match.
fn check_verify_tag_and_package_not_flattened(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Verify tag and package")?;
    let unflattened_derivation = format!("{DYNAMIC_RELEASE_SET_FILTER}')\"");
    if !block.contains(&unflattened_derivation) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify tag and package\" must not flatten \
             RELEASED_CRATES to one line (e.g. via `tr '\\n' ' '`) before the exact-line \
             `grep -Fxq` membership check (#212): a space-joined line can never equal a single \
             crate name, so `has_test` would always be false"
        ));
    }
    Ok(())
}

/// Checks the runtime fail-closed attestation-coverage step added for #212.
///
/// A crate's `Attest ... SBOM` step above can be skipped by a broken `if:`
/// condition and still exit the job successfully — exactly what happened in
/// #212 — so a step *running without error* is not evidence that its
/// attestation exists. This checks that a step queries the recorded
/// evidence itself, GitHub's attestations API keyed by each released
/// crate's own archive digest, for every crate this run actually released
/// (derived the same dynamic way as the packaging steps, so an older
/// recovery run only requires coverage for what it actually released), and
/// fails the job before the draft-release step if any crate's evidence is
/// short.
fn check_attestation_coverage_verification(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Verify attestation coverage for every released crate")?;
    if !block.contains(DYNAMIC_RELEASE_SET_FILTER) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must derive its crate set the same dynamic way as the packaging steps, not a \
             hardcoded list, so a future publishable crate is checked automatically"
        ));
    }
    if !block.contains("attestations/sha256:") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must query the GitHub attestations API for each released crate's own archive digest"
        ));
    }
    // An undifferentiated total is not sufficient: a recovery rerun repeats
    // "Attest package provenance" for every crate, so a digest can reach two
    // provenance attestations while its crate-specific SBOM attestation is
    // still entirely missing, and a bare `count >= 2` would still pass
    // (exactly the gap independent review found in this check's first
    // version). Provenance and SBOM evidence must be queried and required
    // separately, by predicate type.
    if !block.contains("predicate_type=") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must filter the attestations API query by predicate_type rather than accepting an \
             undifferentiated total count: a recovery rerun can produce duplicate provenance \
             attestations for a digest whose crate-specific SBOM attestation is still missing"
        ));
    }
    if !block.contains(PROVENANCE_PREDICATE_TYPE) || !block.contains(SBOM_PREDICATE_TYPE) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must verify both the provenance predicate ({PROVENANCE_PREDICATE_TYPE:?}) and the \
             SBOM predicate ({SBOM_PREDICATE_TYPE:?}) exist for every released crate"
        ));
    }
    if block.contains("-ge 2") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must not accept an undifferentiated total attestation count of 2; it must require \
             at least 1 provenance attestation and, separately, at least 1 SBOM attestation"
        ));
    }
    if !block.contains("-ge 1") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must require at least 1 attestation of each predicate type for every released crate"
        ));
    }
    // A no-match `predicate_type` filter 404s, and `gh api` writes that
    // error body to *stdout* before exiting non-zero. A fallback written
    // *inside* the command substitution (`... || echo 0)"`) would
    // concatenate that JSON error body with a literal "0" into `count`
    // instead of replacing it, corrupting the numeric comparison this step
    // depends on to ever reach its failure branch.
    if block.contains("|| echo 0)\"") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must not fall back inside the command substitution (`... || echo 0)\"`): a 404 \
             from an unmatched predicate_type writes its error body to stdout before `gh api` \
             exits non-zero, so an inside-substitution fallback appends \"0\" to that body \
             instead of replacing it"
        ));
    }
    if !block.contains(")\" || count=0") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must fall back to count=0 outside the command substitution (`)\" || count=0`), so a \
             failed query replaces the captured count rather than appending to it"
        ));
    }
    Ok(())
}

/// Checks the #212 recovery-review fix: provenance is only ever generated
/// on the original tag `push`.
///
/// `workflow_dispatch` sets the run's own `GITHUB_REF`/`GITHUB_SHA` (and
/// therefore the OIDC claims `actions/attest` derives SLSA provenance from)
/// from whatever ref *dispatched* the run, not from the tag checked out via
/// `${{ env.RELEASE_TAG }}`. A recovery dispatched from a branch to pick up
/// a workflow fix would otherwise record new provenance whose source is
/// that branch, not the immutable release tag, even though the packaged
/// bytes are correct. So "Attest package provenance" must run only when
/// this run's own ref/sha *is* the tag by construction — the `push` event.
fn check_provenance_gated_to_tag_push(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Attest package provenance")?;
    if !block.contains("if: github.event_name == 'push'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Attest package provenance\" must run only on \
             `github.event_name == 'push'`: a `workflow_dispatch` recovery run's own ref/sha is \
             whatever ref dispatched it, not the checked-out release tag, so generating \
             provenance there would record a misleading source commit"
        ));
    }
    Ok(())
}

/// Checks the #212 recovery-review fix: a `workflow_dispatch` recovery
/// narrowly attests only the crates actually missing an SBOM attestation,
/// rather than unconditionally re-attesting every already-covered crate
/// under the dispatching run's own (possibly non-tag) identity.
fn check_sbom_coverage_gating(text: &str) -> Result<(), String> {
    let coverage_block = extract_step_block(text, "Check existing SBOM attestation coverage")?;
    if !coverage_block.contains(DYNAMIC_RELEASE_SET_FILTER) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check existing SBOM attestation coverage\" must derive \
             its crate set the same dynamic way as the packaging steps, not a hardcoded list"
        ));
    }
    if !coverage_block.contains("predicate_type=") || !coverage_block.contains(SBOM_PREDICATE_TYPE)
    {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check existing SBOM attestation coverage\" must query \
             the attestations API filtered by the SBOM predicate type ({SBOM_PREDICATE_TYPE:?})"
        ));
    }
    if !coverage_block.contains("_sbom_missing=") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check existing SBOM attestation coverage\" must emit a \
             per-crate *_sbom_missing output that the individual Attest ... SBOM steps gate on"
        ));
    }

    for crate_name in published_crates_from_manifests()? {
        let slug = crate_name.replace('-', "_");
        let attest_block = extract_step_block(text, &format!("Attest {crate_name} SBOM"))?;
        let expected_condition =
            format!("steps.sbom_coverage.outputs.{slug}_sbom_missing == 'true'");
        if !attest_block.contains(&expected_condition) {
            return Err(format!(
                "{RELEASE_DRAFT_WORKFLOW} \"Attest {crate_name} SBOM\" must gate on \
                 {expected_condition:?} so a recovery run does not re-attest a crate whose SBOM \
                 attestation already exists"
            ));
        }
    }
    Ok(())
}

/// Checks the #212 recovery-review fix: the final guard verifies SLSA
/// provenance against this run's own tag identity, not merely predicate
/// presence, and derives that identity dynamically rather than a
/// hardcoded tag/commit that would silently stop matching for any other
/// release tag.
fn check_provenance_source_verification(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Verify attestation coverage for every released crate")?;
    if !block.contains("externalParameters.workflow.ref")
        || !block.contains("resolvedDependencies")
        || !block.contains("digest.gitCommit")
    {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must check the SLSA provenance predicate's own source ref and commit \
             (buildDefinition.externalParameters.workflow.ref and \
             buildDefinition.resolvedDependencies[].digest.gitCommit), not merely that a \
             provenance predicate exists"
        ));
    }
    if !block.contains("refs/tags/${RELEASE_TAG}") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must compare the provenance source ref against refs/tags/${{RELEASE_TAG}}, derived \
             from this run's own release tag rather than a hardcoded one"
        ));
    }
    if !block.contains("git rev-parse \"${RELEASE_TAG}^{commit}\"") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released crate\" \
             must independently re-derive the expected commit via \
             `git rev-parse \"${{RELEASE_TAG}}^{{commit}}\"` rather than trusting a value \
             computed earlier in the job"
        ));
    }
    for hardcoded in [EXPECTED_RELEASE_VERSION, "v0.6.0"] {
        if block.contains(hardcoded) {
            return Err(format!(
                "{RELEASE_DRAFT_WORKFLOW} \"Verify attestation coverage for every released \
                 crate\" must not hardcode {hardcoded:?}: that would silently stop matching for \
                 a workflow_dispatch recovery run targeting any other release tag"
            ));
        }
    }
    Ok(())
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
    fn check_verify_tag_and_package_not_flattened_accepts_the_unflattened_form() {
        let text = "steps:\n  - name: Verify tag and package\n    run: |\n      RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 \\\n        | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n  - name: Next step\n    run: echo hi\n";
        check_verify_tag_and_package_not_flattened(text).expect("unflattened derivation accepted");
    }

    #[test]
    fn check_verify_tag_and_package_not_flattened_rejects_the_212_regression() {
        let text = "steps:\n  - name: Verify tag and package\n    run: |\n      RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 \\\n        | jq -r '.packages[] | select((.publish // []) | length > 0) | .name' \\\n        | tr '\\n' ' ')\"\n  - name: Next step\n    run: echo hi\n";
        let error = check_verify_tag_and_package_not_flattened(text)
            .expect_err("flattened derivation is rejected");
        assert!(error.contains("must not flatten"), "{error}");
    }

    #[test]
    fn check_verify_tag_and_package_not_flattened_rejects_a_missing_step() {
        let error = check_verify_tag_and_package_not_flattened(
            "steps:\n  - name: Other\n    run: echo hi\n",
        )
        .expect_err("missing step is an error");
        assert!(error.contains("Verify tag and package"), "{error}");
    }

    fn attestation_coverage_step(body: &str) -> String {
        format!(
            "steps:\n  - name: Verify attestation coverage for every released crate\n    run: |\n{body}\n  - name: Create or refresh draft release\n    run: echo hi\n"
        )
    }

    const DYNAMIC_CRATES_LINE: &str = "      RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n";
    const HARDCODED_CRATES_LINE: &str = "      RELEASED_CRATES=\"oxide-batch-core oxide-batch-repository oxide-batch-plan oxide-batch oxide-batch-cli oxide-batch-test\"\n";
    const PER_PREDICATE_QUERY_LINES: &str = "      for predicate_type in \"https://slsa.dev/provenance/v1\" \"https://cyclonedx.org/bom\"; do\n        count=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=${predicate_type} --jq '.attestations | length')\" || count=0\n        if [ \"${count}\" -ge 1 ]; then break; fi\n      done\n";

    #[test]
    fn check_attestation_coverage_verification_accepts_a_complete_step() {
        let text =
            attestation_coverage_step(&format!("{DYNAMIC_CRATES_LINE}{PER_PREDICATE_QUERY_LINES}"));
        check_attestation_coverage_verification(&text).expect("complete step accepted");
    }

    #[test]
    fn check_attestation_coverage_verification_rejects_a_missing_step() {
        let error =
            check_attestation_coverage_verification("steps:\n  - name: Other\n    run: echo hi\n")
                .expect_err("missing step is an error");
        assert!(error.contains("Verify attestation coverage"), "{error}");
    }

    #[test]
    fn check_attestation_coverage_verification_rejects_a_hardcoded_crate_set() {
        let text = attestation_coverage_step(&format!(
            "{HARDCODED_CRATES_LINE}{PER_PREDICATE_QUERY_LINES}"
        ));
        let error = check_attestation_coverage_verification(&text)
            .expect_err("hardcoded crate set is rejected");
        assert!(error.contains("dynamic"), "{error}");
    }

    #[test]
    fn check_attestation_coverage_verification_rejects_a_missing_api_query() {
        let text = attestation_coverage_step(&format!("{DYNAMIC_CRATES_LINE}      echo skip\n"));
        let error = check_attestation_coverage_verification(&text)
            .expect_err("missing API query is rejected");
        assert!(error.contains("attestations API"), "{error}");
    }

    /// The exact gap independent review found: a digest can carry two
    /// *provenance* attestations (a recovery rerun repeats "Attest package
    /// provenance") while its crate-specific SBOM attestation is entirely
    /// missing, and an undifferentiated `count >= 2` guard would still pass.
    #[test]
    fn check_attestation_coverage_verification_rejects_a_total_count_only_guard() {
        let text = attestation_coverage_step(&format!(
            "{DYNAMIC_CRATES_LINE}      count=\"$(gh api repos/${{GITHUB_REPOSITORY}}/attestations/sha256:${{digest}} --jq '.attestations | length')\"\n      if [ \"${{count}}\" -ge 2 ]; then break; fi\n",
        ));
        let error = check_attestation_coverage_verification(&text).expect_err(
            "a total count without predicate_type filtering must be rejected: it would accept \
             duplicate provenance attestations plus a missing SBOM attestation",
        );
        assert!(error.contains("predicate_type"), "{error}");
    }

    #[test]
    fn check_attestation_coverage_verification_rejects_predicate_type_present_but_still_gated_on_a_total_of_two()
     {
        let text = attestation_coverage_step(&format!(
            "{DYNAMIC_CRATES_LINE}      count=\"$(gh api --method GET repos/${{GITHUB_REPOSITORY}}/attestations/sha256:${{digest}} -f predicate_type=https://slsa.dev/provenance/v1 --jq '.attestations | length')\"\n      count=\"$((count + $(gh api --method GET repos/${{GITHUB_REPOSITORY}}/attestations/sha256:${{digest}} -f predicate_type=https://cyclonedx.org/bom --jq '.attestations | length')))\"\n      if [ \"${{count}}\" -ge 2 ]; then break; fi\n",
        ));
        let error = check_attestation_coverage_verification(&text).expect_err(
            "summing per-predicate counts back into one -ge 2 gate reopens the duplicate-\
             provenance-plus-missing-SBOM gap and must be rejected",
        );
        assert!(error.contains("undifferentiated total"), "{error}");
    }

    /// `gh api` writes a 404 error body to *stdout* before exiting
    /// non-zero, so a fallback written *inside* the command substitution
    /// concatenates that JSON body with a literal "0" instead of replacing
    /// it, corrupting the numeric comparisons below it so the failure
    /// branch is never reliably reached.
    #[test]
    fn check_attestation_coverage_verification_rejects_an_inside_substitution_fallback() {
        let query_with_inside_fallback = "      for predicate_type in \"https://slsa.dev/provenance/v1\" \"https://cyclonedx.org/bom\"; do\n        count=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=${predicate_type} --jq '.attestations | length' || echo 0)\"\n        if [ \"${count}\" -ge 1 ]; then break; fi\n      done\n";
        let text = attestation_coverage_step(&format!(
            "{DYNAMIC_CRATES_LINE}{query_with_inside_fallback}"
        ));
        let error = check_attestation_coverage_verification(&text)
            .expect_err("an inside-substitution fallback is rejected");
        assert!(error.contains("inside the command substitution"), "{error}");
    }

    #[test]
    fn check_attestation_coverage_verification_rejects_a_missing_fallback() {
        let query_without_fallback = "      for predicate_type in \"https://slsa.dev/provenance/v1\" \"https://cyclonedx.org/bom\"; do\n        count=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=${predicate_type} --jq '.attestations | length')\"\n        if [ \"${count}\" -ge 1 ]; then break; fi\n      done\n";
        let text =
            attestation_coverage_step(&format!("{DYNAMIC_CRATES_LINE}{query_without_fallback}"));
        let error = check_attestation_coverage_verification(&text)
            .expect_err("a query with no 404 fallback at all is rejected");
        assert!(error.contains("fall back to count=0"), "{error}");
    }

    #[test]
    fn check_attestation_coverage_verification_rejects_provenance_only_coverage() {
        let text = attestation_coverage_step(&format!(
            "{DYNAMIC_CRATES_LINE}      count=\"$(gh api --method GET repos/${{GITHUB_REPOSITORY}}/attestations/sha256:${{digest}} -f predicate_type=https://slsa.dev/provenance/v1 --jq '.attestations | length')\"\n      if [ \"${{count}}\" -ge 1 ]; then break; fi\n",
        ));
        let error = check_attestation_coverage_verification(&text)
            .expect_err("checking only the provenance predicate is rejected");
        assert!(error.contains("SBOM predicate"), "{error}");
    }

    #[test]
    fn check_provenance_gated_to_tag_push_accepts_the_push_only_gate() {
        let text = "steps:\n      - name: Attest package provenance\n        if: github.event_name == 'push'\n        uses: actions/attest@x\n      - name: Next step\n        run: echo hi\n";
        check_provenance_gated_to_tag_push(text).expect("push-only gate accepted");
    }

    #[test]
    fn check_provenance_gated_to_tag_push_rejects_an_unconditional_step() {
        let text = "steps:\n      - name: Attest package provenance\n        uses: actions/attest@x\n      - name: Next step\n        run: echo hi\n";
        let error = check_provenance_gated_to_tag_push(text).expect_err(
            "an unconditional provenance step is rejected: a workflow_dispatch \
                         recovery run would then record misleading provenance",
        );
        assert!(error.contains("github.event_name == 'push'"), "{error}");
    }

    #[test]
    fn check_provenance_gated_to_tag_push_rejects_a_missing_step() {
        let error =
            check_provenance_gated_to_tag_push("steps:\n  - name: Other\n    run: echo hi\n")
                .expect_err("missing step is an error");
        assert!(error.contains("Attest package provenance"), "{error}");
    }

    /// Every published crate name gated on its own `sbom_coverage` output,
    /// matching what `check_sbom_coverage_gating` requires. `published_
    /// crates_from_manifests` reads this workspace's real manifests, so the
    /// crate list here must track the six real published crates, not a
    /// value independent of them.
    fn complete_sbom_coverage_fixture() -> String {
        use std::fmt::Write as _;

        let mut text = String::from(
            "steps:\n      - name: Check existing SBOM attestation coverage\n        run: |\n          RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n          count=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=https://cyclonedx.org/bom --jq '.attestations | length')\" || count=0\n          echo \"${slug}_sbom_missing=${missing}\" >> \"${GITHUB_OUTPUT}\"\n",
        );
        for crate_name in [
            "oxide-batch-core",
            "oxide-batch-repository",
            "oxide-batch-plan",
            "oxide-batch",
            "oxide-batch-cli",
            "oxide-batch-test",
        ] {
            let slug = crate_name.replace('-', "_");
            let _ = write!(
                text,
                "      - name: Attest {crate_name} SBOM\n        if: steps.sbom_coverage.outputs.{slug}_sbom_missing == 'true'\n        uses: actions/attest@x\n"
            );
        }
        text
    }

    #[test]
    fn check_sbom_coverage_gating_accepts_a_complete_fixture() {
        let text = complete_sbom_coverage_fixture();
        check_sbom_coverage_gating(&text).expect("complete gating fixture accepted");
    }

    #[test]
    fn check_sbom_coverage_gating_rejects_a_missing_coverage_step() {
        let error = check_sbom_coverage_gating("steps:\n  - name: Other\n    run: echo hi\n")
            .expect_err("missing coverage step is an error");
        assert!(
            error.contains("Check existing SBOM attestation coverage"),
            "{error}"
        );
    }

    #[test]
    fn check_sbom_coverage_gating_rejects_a_static_crate_set_in_the_coverage_step() {
        let text = complete_sbom_coverage_fixture().replacen(
            "RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"",
            "RELEASED_CRATES=\"oxide-batch-core oxide-batch-repository oxide-batch-plan oxide-batch oxide-batch-cli oxide-batch-test\"",
            1,
        );
        let error = check_sbom_coverage_gating(&text)
            .expect_err("a hardcoded crate set in the coverage step is rejected");
        assert!(error.contains("dynamic"), "{error}");
    }

    #[test]
    fn check_sbom_coverage_gating_rejects_an_ungated_attest_step() {
        let text = complete_sbom_coverage_fixture().replace(
            "      - name: Attest oxide-batch-cli SBOM\n        if: steps.sbom_coverage.outputs.oxide_batch_cli_sbom_missing == 'true'\n        uses: actions/attest@x\n",
            "      - name: Attest oxide-batch-cli SBOM\n        uses: actions/attest@x\n",
        );
        let error = check_sbom_coverage_gating(&text).expect_err(
            "an attest step not gated on its own missing-coverage output is rejected: a \
             recovery run would then re-attest a crate whose SBOM attestation already exists, \
             under the dispatching run's own (possibly non-tag) identity",
        );
        assert!(error.contains("oxide-batch-cli"), "{error}");
    }

    fn complete_provenance_source_fixture() -> String {
        "steps:\n      - name: Verify attestation coverage for every released crate\n        run: |\n          tag_commit=\"$(git rev-parse \"${RELEASE_TAG}^{commit}\")\"\n          expected_source_ref=\"refs/tags/${RELEASE_TAG}\"\n          jq '.predicate.buildDefinition.externalParameters.workflow.ref == $ref and (.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit // \"\") == $sha'\n      - name: Create or refresh draft release\n        run: echo hi\n".to_owned()
    }

    #[test]
    fn check_provenance_source_verification_accepts_a_complete_fixture() {
        let text = complete_provenance_source_fixture();
        check_provenance_source_verification(&text).expect("complete fixture accepted");
    }

    #[test]
    fn check_provenance_source_verification_rejects_missing_field_checks() {
        let text = "steps:\n      - name: Verify attestation coverage for every released crate\n        run: |\n          echo skip\n      - name: Create or refresh draft release\n        run: echo hi\n";
        let error = check_provenance_source_verification(text).expect_err(
            "a guard that never inspects the provenance predicate's own source ref/commit is \
             rejected: presence of a provenance predicate alone does not prove it names the \
             right source",
        );
        assert!(error.contains("resolvedDependencies"), "{error}");
    }

    #[test]
    fn check_provenance_source_verification_rejects_a_missing_dynamic_ref_comparison() {
        let text = "steps:\n      - name: Verify attestation coverage for every released crate\n        run: |\n          tag_commit=\"$(git rev-parse \"${RELEASE_TAG}^{commit}\")\"\n          jq '.predicate.buildDefinition.externalParameters.workflow.ref == $ref and (.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit // \"\") == $sha'\n      - name: Create or refresh draft release\n        run: echo hi\n";
        let error = check_provenance_source_verification(text)
            .expect_err("a guard missing the refs/tags/${RELEASE_TAG} comparison is rejected");
        assert!(error.contains("refs/tags/${RELEASE_TAG}"), "{error}");
    }

    #[test]
    fn check_provenance_source_verification_rejects_a_missing_tag_commit_rederivation() {
        let text = "steps:\n      - name: Verify attestation coverage for every released crate\n        run: |\n          expected_source_ref=\"refs/tags/${RELEASE_TAG}\"\n          jq '.predicate.buildDefinition.externalParameters.workflow.ref == $ref and (.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit // \"\") == $sha'\n      - name: Create or refresh draft release\n        run: echo hi\n";
        let error = check_provenance_source_verification(text).expect_err(
            "a guard that trusts an earlier-computed commit instead of \
                         independently re-deriving it is rejected",
        );
        assert!(error.contains("git rev-parse"), "{error}");
    }

    #[test]
    fn check_provenance_source_verification_rejects_a_hardcoded_tag() {
        // Inserted before the step boundary (the next "- name:" line), so
        // it lands inside the extracted block rather than after it.
        let text = complete_provenance_source_fixture().replacen(
            "      - name: Create or refresh draft release",
            "          # v0.6.0\n      - name: Create or refresh draft release",
            1,
        );
        let error = check_provenance_source_verification(&text).expect_err(
            "a guard that hardcodes the current tag/version is rejected: it would silently \
             stop matching for a workflow_dispatch recovery run targeting any other tag",
        );
        assert!(error.contains("must not hardcode"), "{error}");
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
