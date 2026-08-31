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
    if draft_dynamic_occurrences != 6 {
        violations.push(format!(
            "{RELEASE_DRAFT_WORKFLOW} has {draft_dynamic_occurrences} dynamic release-set \
             derivation(s) (expected 6: package/publish, SBOM generation, #212's existing-SBOM- \
             coverage check, #212's attestation-coverage verification, #212 round 2's \
             recovered-SBOM-content verification, and #212 round 3's reviewed-evidence-manifest \
             verification); a hardcoded RELEASED_CRATES list there would break workflow_dispatch \
             recovery against a tag that predates a later-added released crate"
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
    if let Err(violation) = check_evidence_generation_gated_to_tag_push(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_draft_release_upload_gated_to_tag_push(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_recovery_downloads_existing_assets(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_recovered_sbom_content_verification(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_workflow_dispatch_originates_from_main(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_reviewed_evidence_manifest_checkout(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_reviewed_evidence_manifest_head_verification(&draft_text) {
        violations.push(violation);
    }
    if let Err(violation) = check_reviewed_evidence_manifest_verification(&draft_text) {
        violations.push(violation);
    }
    violations.extend(check_v0_6_0_evidence_manifest(&root)?);

    violations.extend(check_release_workflow(&root)?);

    Ok(violations)
}

/// Checks the immutable-tag, bootstrap, and idempotent publication contract.
fn check_release_workflow(root: &std::path::Path) -> Result<Vec<String>, String> {
    let release_path = root.join(RELEASE_WORKFLOW);
    let release_text = fs::read_to_string(&release_path)
        .map_err(|error| format!("could not read {RELEASE_WORKFLOW}: {error}"))?;
    check_release_workflow_text(&release_text)
}

/// The text-based half of [`check_release_workflow`], split out so fixture
/// text can be checked directly in tests without touching the filesystem.
fn check_release_workflow_text(release_text: &str) -> Result<Vec<String>, String> {
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
    let verify_block = extract_step_block(release_text, "Verify packages")?;
    if !verify_block.contains("cargo publish --workspace --locked --dry-run") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify packages\" must dry-run the metadata-derived workspace release set"
        ));
    }
    let publish_block = extract_step_block(release_text, "Publish to crates.io")?;
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

    violations.extend(check_release_permission_boundary(release_text)?);
    violations.extend(check_release_dispatch_origin(release_text)?);
    violations.extend(check_release_manual_target(release_text)?);

    Ok(violations)
}

/// Checks the #215 fix: draft-release visibility (`contents: write`) is
/// bounded to the `verify` job alone, that job holds no OIDC permission,
/// and its checkout does not persist credentials.
///
/// GitHub's Releases API returns a draft release only to an actor with
/// push access, so `verify-release` needs `contents: write` to see the
/// still-draft `v0.6.0` release in `publish-registered` mode — but only
/// that job, never the whole workflow, and only past what that visibility
/// requires.
fn check_release_permission_boundary(release_text: &str) -> Result<Vec<String>, String> {
    let mut violations = Vec::new();
    if release_text.contains("\n  contents: write") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} must not grant contents: write at the workflow level; draft- \
             release visibility is needed only by verify-release, which must set its own \
             job-scoped permissions"
        ));
    }
    // Anchored to the exact 6-space job-level `permissions:` indentation
    // (matching the existing "publish" job's own contents/id-token lines),
    // not a bare substring search: a bare search would also match either
    // phrase appearing in this job's own explanatory comments.
    let verify_job = extract_job_block(release_text, "verify", RELEASE_WORKFLOW)?;
    if !verify_job.contains("\n      contents: write") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} job \"verify\" must set its own job-scoped `permissions: \
             contents: write`: GitHub's Releases API returns a draft release only to an actor \
             with push access, and the workflow-level default is contents: read"
        ));
    }
    if verify_job.contains("\n      id-token: write") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} job \"verify\" must not hold id-token: write; only the OIDC \
             publication job needs it"
        ));
    }
    let verify_checkout = extract_step_block(release_text, "Check out release tag")?;
    if !verify_checkout.contains("persist-credentials: false") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"verify\" job's \"Check out release tag\" must set \
             persist-credentials: false, since that job now holds contents: write and this \
             checkout only ever needs to read the immutable release tag's tree"
        ));
    }
    Ok(violations)
}

/// Checks the #215 fix: a `workflow_dispatch` recovery/bootstrap run is
/// only valid when dispatched from `refs/heads/main`, checked before any
/// checkout.
///
/// `workflow_dispatch` runs whichever ref's own copy of this workflow file
/// was selected at dispatch time — the dispatch ref, not `inputs.tag`,
/// decides which workflow *definition* executes. Dispatching from an
/// unreviewed branch would therefore run that branch's own version of
/// every check in this file, defeating every other fix here regardless of
/// how carefully each is written.
fn check_release_dispatch_origin(release_text: &str) -> Result<Vec<String>, String> {
    let mut violations = Vec::new();
    let dispatch_guard_block = extract_step_block(
        release_text,
        "Verify workflow_dispatch originated from main",
    )?;
    if !dispatch_guard_block.contains("if: github.event_name == 'workflow_dispatch'") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify workflow_dispatch originated from main\" must run only \
             on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    if !dispatch_guard_block.contains("refs/heads/main") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify workflow_dispatch originated from main\" must reject \
             any dispatch ref other than refs/heads/main: workflow_dispatch runs whichever \
             ref's own copy of this file was selected at dispatch time, so dispatching from an \
             unreviewed branch would run that branch's own version of every check here"
        ));
    }
    let dispatch_guard_position = release_text
        .find("- name: Verify workflow_dispatch originated from main")
        .ok_or_else(|| {
            format!(
                "{RELEASE_WORKFLOW} has no \"Verify workflow_dispatch originated from main\" step"
            )
        })?;
    let checkout_position = release_text
        .find("- name: Check out release tag")
        .ok_or_else(|| format!("{RELEASE_WORKFLOW} has no \"Check out release tag\" step"))?;
    if dispatch_guard_position > checkout_position {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify workflow_dispatch originated from main\" must run \
             before \"Check out release tag\", so an illegitimate dispatch fails before this \
             workflow does any work"
        ));
    }
    Ok(violations)
}

/// Checks the #215 fix: the manual-dispatch preflight verifies `isDraft`
/// in both directions — `true` before `publish-registered` may proceed,
/// `false` before treating a `release`-event dispatch as already public.
///
/// Previously only `tagName` was checked in `publish-registered` mode, so
/// nothing stopped a stale or accidental re-dispatch after the Release had
/// already been published from proceeding into package classification.
fn check_release_manual_target(release_text: &str) -> Result<Vec<String>, String> {
    let mut violations = Vec::new();
    let recovery_target_block = extract_step_block(
        release_text,
        "Verify manual recovery target and explicit mode",
    )?;
    if !recovery_target_block.contains("<<<\"${view}\")\" = \"true\"") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify manual recovery target and explicit mode\" must verify \
             isDraft == true in publish-registered mode, not only tagName: publish-registered \
             must only ever run against a release still awaiting publication"
        ));
    }
    if !recovery_target_block.contains("<<<\"${view}\")\" = \"false\"") {
        violations.push(format!(
            "{RELEASE_WORKFLOW} \"Verify manual recovery target and explicit mode\" must \
             continue to verify isDraft == false in verify mode"
        ));
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

/// Returns the text belonging to one top-level workflow job (a 2-space-
/// indented key directly under `jobs:`), up to the next such job or end of
/// file.
fn extract_job_block<'a>(text: &'a str, job_name: &str, workflow: &str) -> Result<&'a str, String> {
    let marker = format!("\n  {job_name}:\n");
    let start = text
        .find(&marker)
        .ok_or_else(|| format!("no job {job_name:?} found in {workflow}"))?;
    let after = &text[start + marker.len()..];

    let mut end = after.len();
    let mut pos = 0;
    for line in after.split_inclusive('\n') {
        let content = line.trim_end_matches('\n');
        let is_new_job = !content.is_empty()
            && content.starts_with("  ")
            && !content.starts_with("   ")
            && content.trim_end().ends_with(':');
        if is_new_job {
            end = pos;
            break;
        }
        pos += line.len();
    }
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

/// Checks the #212 round-2 recovery-review fix: SBOM/checksum generation
/// (`cargo-cyclonedx`) is only ever run on the original tag `push`.
///
/// `cargo-cyclonedx` embeds a random `serialNumber` and a wall-clock
/// `metadata.timestamp` in every SBOM it generates — confirmed against the
/// real `v0.6.0` SBOM attestations — so re-running it on a
/// `workflow_dispatch` recovery would produce a different, non-reproducible
/// document even for the exact same dependency graph, silently replacing
/// already-published, already-attested release evidence.
fn check_evidence_generation_gated_to_tag_push(text: &str) -> Result<(), String> {
    for step_name in [
        "Install pinned SBOM generator",
        "Generate release SBOM and checksums",
    ] {
        let block = extract_step_block(text, step_name)?;
        if !block.contains("if: github.event_name == 'push'") {
            return Err(format!(
                "{RELEASE_DRAFT_WORKFLOW} \"{step_name}\" must run only on \
                 `github.event_name == 'push'`: cargo-cyclonedx's random serialNumber and \
                 wall-clock timestamp make its output non-reproducible, so re-running it on a \
                 workflow_dispatch recovery would silently replace already-published,\
                 already-attested SBOM/checksum evidence with different bytes"
            ));
        }
    }
    Ok(())
}

/// Checks the #212 round-2 recovery-review fix: the draft-release asset
/// upload (`gh release upload ... --clobber`) is only ever run on the
/// original tag `push`.
///
/// A `workflow_dispatch` recovery adds one missing attestation to release
/// evidence that already exists; it must never re-upload or overwrite the
/// draft release's published assets, regardless of what happens to be in
/// `target/package` at that point.
fn check_draft_release_upload_gated_to_tag_push(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Create or refresh draft release")?;
    if !block.contains("if: github.event_name == 'push'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Create or refresh draft release\" must run only on \
             `github.event_name == 'push'`: a workflow_dispatch recovery must never run `gh \
             release upload ... --clobber` against the already-published draft release assets"
        ));
    }
    if !block.contains("--clobber") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Create or refresh draft release\" no longer refreshes \
             existing assets via `gh release upload ... --clobber`; update this check alongside \
             whatever replaced it"
        ));
    }
    Ok(())
}

/// Checks the #212 round-2 recovery-review fix: a `workflow_dispatch`
/// recovery downloads the already-published SBOM/checksum assets instead
/// of regenerating them, and verifies the downloaded checksum manifest
/// against the locally rebuilt `.crate` archives.
fn check_recovery_downloads_existing_assets(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Download existing release assets for recovery")?;
    if !block.contains("if: github.event_name == 'workflow_dispatch'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Download existing release assets for recovery\" must \
             run only on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    if !block.contains("gh release download") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Download existing release assets for recovery\" must \
             download the existing release's assets via `gh release download`, not regenerate \
             them"
        ));
    }
    if block.contains("gh release upload") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Download existing release assets for recovery\" must \
             not upload to the release; it only downloads existing assets for local use"
        ));
    }
    if !block.contains("sha256sum --check --strict") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Download existing release assets for recovery\" must \
             verify the downloaded checksum manifest against the locally rebuilt archives \
             (`sha256sum --check --strict`), not merely trust the download"
        ));
    }
    if !block.contains("isDraft") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Download existing release assets for recovery\" must \
             explicitly verify the target release is still a draft before downloading from it; \
             the only other isDraft check lives in the push-only \"Create or refresh draft \
             release\" step, which never runs on a recovery"
        ));
    }
    Ok(())
}

/// Checks the #212 round-4 recovery-review fix: a `workflow_dispatch`
/// recovery is only valid when dispatched from `refs/heads/main`, checked
/// before any checkout.
///
/// `workflow_dispatch` runs whichever ref's own copy of this workflow file
/// was selected at dispatch time — the dispatch ref, not `inputs.tag`,
/// decides which workflow *definition* executes. Dispatching from an
/// unreviewed branch would therefore run that branch's own version of
/// every check in this file, including ones a PR could have quietly
/// removed, defeating every other #212 fix regardless of how carefully it
/// is written.
fn check_workflow_dispatch_originates_from_main(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Verify workflow_dispatch originated from main")?;
    if !block.contains("if: github.event_name == 'workflow_dispatch'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify workflow_dispatch originated from main\" must \
             run only on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    if !block.contains("refs/heads/main") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify workflow_dispatch originated from main\" must \
             reject any dispatch ref other than refs/heads/main"
        ));
    }
    let checkout_position = text
        .find("- name: Check out release tag")
        .ok_or_else(|| format!("{RELEASE_DRAFT_WORKFLOW} has no \"Check out release tag\" step"))?;
    let verify_position = text
        .find("- name: Verify workflow_dispatch originated from main")
        .ok_or_else(|| {
            format!(
                "{RELEASE_DRAFT_WORKFLOW} has no \"Verify workflow_dispatch originated from \
                 main\" step"
            )
        })?;
    if verify_position > checkout_position {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify workflow_dispatch originated from main\" must run \
             before \"Check out release tag\", so an illegitimate dispatch fails before this \
             workflow does any work with the checked-out tree"
        ));
    }
    Ok(())
}

/// Checks the #212 round-4 recovery-review fix: a `workflow_dispatch`
/// recovery verifies the reviewed evidence manifest checkout's own `HEAD`
/// equals the dispatched commit, not merely trusting that `actions/
/// checkout`'s `ref:` input did what it was asked.
fn check_reviewed_evidence_manifest_head_verification(text: &str) -> Result<(), String> {
    let block = extract_step_block(
        text,
        "Verify reviewed evidence manifest checkout is pinned to the dispatched commit",
    )?;
    if !block.contains("if: github.event_name == 'workflow_dispatch'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify reviewed evidence manifest checkout is pinned to \
             the dispatched commit\" must run only on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    if !block.contains("git -C release-evidence-manifest rev-parse HEAD") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify reviewed evidence manifest checkout is pinned to \
             the dispatched commit\" must independently read the checked-out manifest \
             directory's own HEAD rather than trusting the checkout step's `ref:` input"
        ));
    }
    if !block.contains("${GITHUB_SHA}") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify reviewed evidence manifest checkout is pinned to \
             the dispatched commit\" must compare against ${{GITHUB_SHA}}, the dispatched commit"
        ));
    }
    Ok(())
}

/// Checks the #212 round-3 recovery-review fix: a `workflow_dispatch`
/// recovery checks out the reviewed, merged-to-`main` evidence manifest for
/// this exact tag into a separate directory, leaving the primary checkout
/// pinned to the immutable release tag.
///
/// Internal consistency between the downloaded SBOM and the downloaded
/// checksum manifest proves nothing on its own: both live in the same
/// mutable draft Release, so tampering with them together still passes
/// that cross-check (confirmed by simulating the tamper before writing
/// this check: `sha256sum --check --strict` passed against a forged SBOM
/// once the checksum manifest was regenerated to match it). The reviewed
/// manifest on `main` is a separate trust domain from that mutable draft.
fn check_reviewed_evidence_manifest_checkout(text: &str) -> Result<(), String> {
    let block = extract_step_block(text, "Check out reviewed release evidence manifest")?;
    if !block.contains("if: github.event_name == 'workflow_dispatch'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check out reviewed release evidence manifest\" must run \
             only on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    // #212 round 4: `ref: main` is a moving target — by the time this step
    // runs, or on a later re-run of the same workflow_dispatch run, `main`
    // may have advanced past the commit that was actually reviewed and
    // dispatched. `github.sha` is fixed to the commit that triggered the
    // run and does not move on a re-run, so it must be used instead.
    if block.contains("ref: main") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check out reviewed release evidence manifest\" must not \
             check out the moving `ref: main`; it must pin to `ref: ${{{{ github.sha }}}}`, the \
             exact commit that triggered this run"
        ));
    }
    if !block.contains("ref: ${{ github.sha }}") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check out reviewed release evidence manifest\" must \
             check out `ref: ${{{{ github.sha }}}}`, not the release tag, as the reviewed \
             evidence manifest is a trust anchor independent of the immutable tag's own \
             (pre-#212) tree"
        ));
    }
    if !block.contains("path: release-evidence-manifest") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check out reviewed release evidence manifest\" must \
             check out into a separate path so it does not disturb the primary checkout, which \
             stays pinned to the immutable release tag"
        ));
    }
    if !block.contains("docs/release/evidence") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Check out reviewed release evidence manifest\" must \
             sparse-checkout docs/release/evidence"
        ));
    }
    Ok(())
}

/// Checks the #212 round-3 recovery-review fix: a `workflow_dispatch`
/// recovery verifies the tag identity, the checksum manifest, and every
/// released crate's `.crate`/SBOM digest against the reviewed evidence
/// manifest on `main` — not merely against each other inside the mutable
/// draft Release.
fn check_reviewed_evidence_manifest_verification(text: &str) -> Result<(), String> {
    let block = extract_step_block(
        text,
        "Verify downloaded evidence against the reviewed manifest",
    )?;
    if !block.contains("if: github.event_name == 'workflow_dispatch'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify downloaded evidence against the reviewed \
             manifest\" must run only on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    if !block.contains(DYNAMIC_RELEASE_SET_FILTER) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify downloaded evidence against the reviewed \
             manifest\" must derive its crate set the same dynamic way as the packaging steps, \
             not a hardcoded list"
        ));
    }
    if !block.contains("docs/release/evidence/${RELEASE_TAG}.json") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify downloaded evidence against the reviewed \
             manifest\" must locate the manifest by this run's own release tag, not a hardcoded \
             one, so this generalizes to a recovery run against any tag that has a manifest"
        ));
    }
    for required in [
        "tagObject",
        "commit",
        "tree",
        "checksumManifestSha256",
        ".crates[$c].crateSha256",
        ".crates[$c].sbomSha256",
    ] {
        if !block.contains(required) {
            return Err(format!(
                "{RELEASE_DRAFT_WORKFLOW} \"Verify downloaded evidence against the reviewed \
                 manifest\" must check {required:?} from the reviewed manifest"
            ));
        }
    }
    for hardcoded in [EXPECTED_RELEASE_VERSION, "v0.6.0"] {
        if block.contains(hardcoded) {
            return Err(format!(
                "{RELEASE_DRAFT_WORKFLOW} \"Verify downloaded evidence against the reviewed \
                 manifest\" must not hardcode {hardcoded:?}: that would silently stop matching \
                 for a workflow_dispatch recovery run targeting any other release tag"
            ));
        }
    }
    Ok(())
}

/// The path convention `check_reviewed_evidence_manifest_verification`
/// assumes: one manifest file per release tag, named after that tag.
const EVIDENCE_MANIFEST_DIR: &str = "docs/release/evidence";

/// Checks that the committed `v0.6.0` evidence manifest itself still names
/// exactly the real, independently-confirmed digests recorded from the
/// tag-push run's own attestations and release assets — a data file is
/// just as capable of silently drifting as workflow logic, and nothing
/// else in this repository re-derives these values to catch that.
fn check_v0_6_0_evidence_manifest(root: &std::path::Path) -> Result<Vec<String>, String> {
    let path = root.join(EVIDENCE_MANIFEST_DIR).join("v0.6.0.json");
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let manifest: Value = serde_json::from_str(&text)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

    let mut violations = Vec::new();
    let expect_field = |violations: &mut Vec<String>, field: &str, expected: &str| {
        let actual = manifest.get(field).and_then(Value::as_str);
        if actual != Some(expected) {
            violations.push(format!(
                "{} field {field:?} is {actual:?}, expected {expected:?}",
                path.display()
            ));
        }
    };
    expect_field(&mut violations, "tag", "v0.6.0");
    expect_field(
        &mut violations,
        "tagObject",
        "d23955f56c48dcce089203330b5999a36ceb2029",
    );
    expect_field(
        &mut violations,
        "commit",
        "e9ce3891a9c37959ad1022a62dbf723c9edd2d65",
    );
    expect_field(
        &mut violations,
        "tree",
        "81746a7560a1465a5f8d79655e0282e60d7d3d12",
    );
    expect_field(
        &mut violations,
        "checksumManifestSha256",
        "163c878b9a6ce21660a6257f435f5e05775f86d2e1bb9a7618553a5ee42b653a",
    );

    let expected_digests: &[(&str, &str, &str)] = &[
        (
            "oxide-batch-core",
            "3b63311721adf30b20ef5293a2ee9fe1759211464a833295e6d490fda8d0c631",
            "7dc67d5f9f5e91ede4a3b55ae18f97dc98d29eac76a99c597c195f13c94eb9b5",
        ),
        (
            "oxide-batch-repository",
            "cb2e0f79a4387331f7a78a4a3d9ebfcd0e7eb02856ea3df4e7a11ae9d62dbe79",
            "fe8c35c63fa2e7edd4b518d907a716ab0fcc4b1b112803b25af7be7a1968434f",
        ),
        (
            "oxide-batch-plan",
            "8cbeeac623fdc091dd12ee81d1d2a5bb6b1edba3522f5cade85960179ebf1c49",
            "91debc386ff552149ce7d3bbd458401faf346a144a86ca89bf81359ee82ef996",
        ),
        (
            "oxide-batch",
            "eb44a43551fbf5c70d11cc54d4c14ec8a40ebaf917a1d5ff1a158556e1fe093b",
            "c17f39c67eba56f3b6e418e08436831a2a9d1cb3da98948ba58f3ccb649f03ce",
        ),
        (
            "oxide-batch-cli",
            "5a819b8c97926ed6b1c810f93ebe1985f0c2b4a94e253414b2c7ceec656ec2ef",
            "e0c32a135aaac49d45defd8be21e2fc588c729962b4cd1523b5213edfd5609e2",
        ),
        (
            "oxide-batch-test",
            "8bad1e49c191f276402c7092b441c2a38dc33ff57b49d5911c801eafa42cb49e",
            "f36c41cbbb9655b74e5689e69167ecf27d6b5508d0b9dbdd5cb07d3e254a1446",
        ),
    ];
    let crates = manifest.get("crates").and_then(Value::as_object);
    for (crate_name, crate_sha256, sbom_sha256) in expected_digests {
        let Some(entry) = crates.and_then(|crates| crates.get(*crate_name)) else {
            violations.push(format!(
                "{} has no entry for crate {crate_name:?}",
                path.display()
            ));
            continue;
        };
        let actual_crate_sha256 = entry.get("crateSha256").and_then(Value::as_str);
        if actual_crate_sha256 != Some(*crate_sha256) {
            violations.push(format!(
                "{} crates.{crate_name}.crateSha256 is {actual_crate_sha256:?}, expected {crate_sha256:?}",
                path.display()
            ));
        }
        let actual_sbom_sha256 = entry.get("sbomSha256").and_then(Value::as_str);
        if actual_sbom_sha256 != Some(*sbom_sha256) {
            violations.push(format!(
                "{} crates.{crate_name}.sbomSha256 is {actual_sbom_sha256:?}, expected {sbom_sha256:?}",
                path.display()
            ));
        }
    }

    Ok(violations)
}

/// Checks the #212 round-2 recovery-review fix: a recovery run verifies
/// that whichever SBOM attestation it just recorded carries, byte for
/// byte (content-wise), the same predicate as the pre-existing release
/// asset it was supposed to attest — a direct check on the recorded
/// evidence rather than trusting that every upstream gate worked.
fn check_recovered_sbom_content_verification(text: &str) -> Result<(), String> {
    let block = extract_step_block(
        text,
        "Verify recovered SBOM attestation matches the existing release asset",
    )?;
    if !block.contains("if: github.event_name == 'workflow_dispatch'") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify recovered SBOM attestation matches the existing \
             release asset\" must run only on `github.event_name == 'workflow_dispatch'`"
        ));
    }
    if !block.contains(DYNAMIC_RELEASE_SET_FILTER) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify recovered SBOM attestation matches the existing \
             release asset\" must derive its crate set the same dynamic way as the packaging \
             steps, not a hardcoded list"
        ));
    }
    if !block.contains(SBOM_PREDICATE_TYPE) {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify recovered SBOM attestation matches the existing \
             release asset\" must inspect the SBOM predicate type ({SBOM_PREDICATE_TYPE:?})"
        ));
    }
    if !block.contains("@base64d") || !block.contains(".predicate") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify recovered SBOM attestation matches the existing \
             release asset\" must decode the attestation's own predicate rather than trusting \
             predicate-type presence alone"
        ));
    }
    if !block.contains(". == $local[0]") {
        return Err(format!(
            "{RELEASE_DRAFT_WORKFLOW} \"Verify recovered SBOM attestation matches the existing \
             release asset\" must compare the decoded predicate against the local downloaded \
             SBOM file's own content"
        ));
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

    fn simple_step(name: &str, condition: Option<&str>, body: &str) -> String {
        use std::fmt::Write as _;

        let mut text = format!("steps:\n      - name: {name}\n");
        if let Some(condition) = condition {
            let _ = writeln!(text, "        if: {condition}");
        }
        let _ = writeln!(text, "        run: |\n{body}");
        text.push_str("      - name: Next step\n        run: echo hi\n");
        text
    }

    #[test]
    fn check_evidence_generation_gated_to_tag_push_accepts_both_gated_steps() {
        let text = "steps:\n      - name: Install pinned SBOM generator\n        if: github.event_name == 'push'\n        run: echo hi\n      - name: Generate release SBOM and checksums\n        if: github.event_name == 'push'\n        run: echo hi\n";
        check_evidence_generation_gated_to_tag_push(text).expect("both steps gated to push");
    }

    #[test]
    fn check_evidence_generation_gated_to_tag_push_rejects_an_ungated_generator_install() {
        let text = "steps:\n      - name: Install pinned SBOM generator\n        run: echo hi\n      - name: Generate release SBOM and checksums\n        if: github.event_name == 'push'\n        run: echo hi\n";
        let error = check_evidence_generation_gated_to_tag_push(text).expect_err(
            "an ungated cargo-cyclonedx install is rejected: it would run its non-reproducible \
             SBOM generation on a workflow_dispatch recovery too",
        );
        assert!(error.contains("Install pinned SBOM generator"), "{error}");
    }

    #[test]
    fn check_evidence_generation_gated_to_tag_push_rejects_an_ungated_sbom_generation() {
        let text = "steps:\n      - name: Install pinned SBOM generator\n        if: github.event_name == 'push'\n        run: echo hi\n      - name: Generate release SBOM and checksums\n        run: echo hi\n";
        let error = check_evidence_generation_gated_to_tag_push(text).expect_err(
            "an ungated SBOM/checksum generation step is rejected: cargo-cyclonedx's random \
             serialNumber and wall-clock timestamp make re-running it on recovery silently \
             replace already-published, already-attested evidence with different bytes",
        );
        assert!(
            error.contains("Generate release SBOM and checksums"),
            "{error}"
        );
    }

    #[test]
    fn check_draft_release_upload_gated_to_tag_push_accepts_the_gated_step() {
        let text = simple_step(
            "Create or refresh draft release",
            Some("github.event_name == 'push'"),
            "          gh release upload \"${RELEASE_TAG}\" \"${assets[@]}\" --clobber",
        );
        check_draft_release_upload_gated_to_tag_push(&text).expect("gated upload step accepted");
    }

    #[test]
    fn check_draft_release_upload_gated_to_tag_push_rejects_an_ungated_clobber_upload() {
        let text = simple_step(
            "Create or refresh draft release",
            None,
            "          gh release upload \"${RELEASE_TAG}\" \"${assets[@]}\" --clobber",
        );
        let error = check_draft_release_upload_gated_to_tag_push(&text).expect_err(
            "an ungated draft-release upload is rejected: a workflow_dispatch recovery must \
             never run `gh release upload ... --clobber` against the published assets",
        );
        assert!(error.contains("github.event_name == 'push'"), "{error}");
    }

    #[test]
    fn check_draft_release_upload_gated_to_tag_push_rejects_a_missing_step() {
        let error = check_draft_release_upload_gated_to_tag_push(
            "steps:\n  - name: Other\n    run: echo hi\n",
        )
        .expect_err("missing step is an error");
        assert!(error.contains("Create or refresh draft release"), "{error}");
    }

    #[test]
    fn check_recovery_downloads_existing_assets_accepts_a_complete_step() {
        let text = simple_step(
            "Download existing release assets for recovery",
            Some("github.event_name == 'workflow_dispatch'"),
            "          is_draft=\"$(gh release view \"${RELEASE_TAG}\" --json isDraft --jq .isDraft)\"\n          test \"${is_draft}\" = \"true\"\n          gh release download \"${RELEASE_TAG}\" --pattern '*.cdx.json' --pattern '*.sha256' --dir target/package --clobber\n          (cd target/package; sha256sum --check --strict \"$(basename \"${checksum_path}\")\")",
        );
        check_recovery_downloads_existing_assets(&text).expect("complete download step accepted");
    }

    #[test]
    fn check_recovery_downloads_existing_assets_rejects_a_missing_draft_check() {
        let text = simple_step(
            "Download existing release assets for recovery",
            Some("github.event_name == 'workflow_dispatch'"),
            "          gh release download \"${RELEASE_TAG}\" --pattern '*.cdx.json' --pattern '*.sha256' --dir target/package --clobber\n          (cd target/package; sha256sum --check --strict \"$(basename \"${checksum_path}\")\")",
        );
        let error = check_recovery_downloads_existing_assets(&text).expect_err(
            "a download step that never checks isDraft is rejected: the only other isDraft \
             check lives in the push-only \"Create or refresh draft release\" step, which never \
             runs on a recovery",
        );
        assert!(error.contains("isDraft"), "{error}");
    }

    #[test]
    fn check_recovery_downloads_existing_assets_rejects_a_missing_step() {
        let error =
            check_recovery_downloads_existing_assets("steps:\n  - name: Other\n    run: echo hi\n")
                .expect_err("missing step is an error");
        assert!(
            error.contains("Download existing release assets for recovery"),
            "{error}"
        );
    }

    #[test]
    fn check_recovery_downloads_existing_assets_rejects_regeneration_instead_of_download() {
        let text = simple_step(
            "Download existing release assets for recovery",
            Some("github.event_name == 'workflow_dispatch'"),
            "          cargo cyclonedx --format json > sbom.json",
        );
        let error = check_recovery_downloads_existing_assets(&text)
            .expect_err("a step that regenerates instead of downloading is rejected");
        assert!(error.contains("gh release download"), "{error}");
    }

    #[test]
    fn check_recovery_downloads_existing_assets_rejects_an_unverified_download() {
        let text = simple_step(
            "Download existing release assets for recovery",
            Some("github.event_name == 'workflow_dispatch'"),
            "          gh release download \"${RELEASE_TAG}\" --pattern '*.cdx.json' --pattern '*.sha256' --dir target/package --clobber",
        );
        let error = check_recovery_downloads_existing_assets(&text).expect_err(
            "a download with no checksum verification against the rebuilt archives is rejected",
        );
        assert!(error.contains("sha256sum --check --strict"), "{error}");
    }

    #[test]
    fn check_recovery_downloads_existing_assets_rejects_an_upload_path() {
        let text = simple_step(
            "Download existing release assets for recovery",
            Some("github.event_name == 'workflow_dispatch'"),
            "          gh release download \"${RELEASE_TAG}\" --dir target/package\n          sha256sum --check --strict manifest.sha256\n          gh release upload \"${RELEASE_TAG}\" extra.txt",
        );
        let error = check_recovery_downloads_existing_assets(&text)
            .expect_err("a step that also uploads to the release is rejected");
        assert!(error.contains("must not upload"), "{error}");
    }

    fn complete_recovered_sbom_verification_fixture() -> String {
        simple_step(
            "Verify recovered SBOM attestation matches the existing release asset",
            Some("github.event_name == 'workflow_dispatch'"),
            "          RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n          predicate_json=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=https://cyclonedx.org/bom)\"\n          matches=\"$(printf '%s' \"${predicate_json}\" | jq --slurpfile local \"${sbom_path}\" '[.attestations[]? | (.bundle.dsseEnvelope.payload | @base64d | fromjson | .predicate) | select(. == $local[0])] | length')\"",
        )
    }

    #[test]
    fn check_recovered_sbom_content_verification_accepts_a_complete_fixture() {
        let text = complete_recovered_sbom_verification_fixture();
        check_recovered_sbom_content_verification(&text).expect("complete fixture accepted");
    }

    #[test]
    fn check_recovered_sbom_content_verification_rejects_a_missing_step() {
        let error = check_recovered_sbom_content_verification(
            "steps:\n  - name: Other\n    run: echo hi\n",
        )
        .expect_err("missing step is an error");
        assert!(
            error.contains("Verify recovered SBOM attestation"),
            "{error}"
        );
    }

    #[test]
    fn check_recovered_sbom_content_verification_rejects_a_missing_predicate_decode() {
        let text = simple_step(
            "Verify recovered SBOM attestation matches the existing release asset",
            Some("github.event_name == 'workflow_dispatch'"),
            "          RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n          count=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=https://cyclonedx.org/bom --jq '.attestations | length')\"",
        );
        let error = check_recovered_sbom_content_verification(&text).expect_err(
            "a step that only checks predicate-type presence, without decoding and comparing \
             the actual predicate content, is rejected",
        );
        assert!(
            error.contains("decode the attestation's own predicate"),
            "{error}"
        );
    }

    #[test]
    fn check_recovered_sbom_content_verification_rejects_a_missing_local_comparison() {
        let text = simple_step(
            "Verify recovered SBOM attestation matches the existing release asset",
            Some("github.event_name == 'workflow_dispatch'"),
            "          RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n          predicate_json=\"$(gh api --method GET repos/${GITHUB_REPOSITORY}/attestations/sha256:${digest} -f predicate_type=https://cyclonedx.org/bom)\"\n          count=\"$(printf '%s' \"${predicate_json}\" | jq '[.attestations[]? | (.bundle.dsseEnvelope.payload | @base64d | fromjson | .predicate)] | length')\"",
        );
        let error = check_recovered_sbom_content_verification(&text).expect_err(
            "a step that decodes the predicate but never compares it against the local \
             downloaded asset is rejected",
        );
        assert!(error.contains("local downloaded SBOM file"), "{error}");
    }

    #[test]
    fn check_recovered_sbom_content_verification_rejects_a_hardcoded_crate_set() {
        let text = complete_recovered_sbom_verification_fixture()
            .replace("$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')", "oxide-batch-core oxide-batch-repository oxide-batch-plan oxide-batch oxide-batch-cli oxide-batch-test");
        let error = check_recovered_sbom_content_verification(&text)
            .expect_err("a hardcoded crate set is rejected");
        assert!(error.contains("dynamic"), "{error}");
    }

    #[test]
    fn check_workflow_dispatch_originates_from_main_accepts_a_correctly_ordered_gate() {
        let text = "steps:\n      - name: Verify workflow_dispatch originated from main\n        if: github.event_name == 'workflow_dispatch'\n        run: |\n          test \"${GITHUB_REF}\" = \"refs/heads/main\"\n      - name: Check out release tag\n        uses: actions/checkout@x\n";
        check_workflow_dispatch_originates_from_main(text)
            .expect("a gate before checkout is accepted");
    }

    #[test]
    fn check_workflow_dispatch_originates_from_main_rejects_a_missing_step() {
        let error = check_workflow_dispatch_originates_from_main(
            "steps:\n  - name: Check out release tag\n    uses: actions/checkout@x\n",
        )
        .expect_err("missing step is an error");
        assert!(
            error.contains("Verify workflow_dispatch originated from main"),
            "{error}"
        );
    }

    #[test]
    fn check_workflow_dispatch_originates_from_main_rejects_no_ref_check() {
        let text = "steps:\n      - name: Verify workflow_dispatch originated from main\n        if: github.event_name == 'workflow_dispatch'\n        run: |\n          echo skip\n      - name: Check out release tag\n        uses: actions/checkout@x\n";
        let error = check_workflow_dispatch_originates_from_main(text).expect_err(
            "a gate that never checks the dispatch ref is rejected: dispatching from an \
             unreviewed branch would run that branch's own copy of every other #212 check",
        );
        assert!(error.contains("refs/heads/main"), "{error}");
    }

    #[test]
    fn check_workflow_dispatch_originates_from_main_rejects_running_after_checkout() {
        let text = "steps:\n      - name: Check out release tag\n        uses: actions/checkout@x\n      - name: Verify workflow_dispatch originated from main\n        if: github.event_name == 'workflow_dispatch'\n        run: |\n          test \"${GITHUB_REF}\" = \"refs/heads/main\"\n";
        let error = check_workflow_dispatch_originates_from_main(text).expect_err(
            "a gate that runs after checkout is rejected: an illegitimate dispatch must fail \
             before this workflow does any work with the checked-out tree",
        );
        assert!(error.contains("must run before"), "{error}");
    }

    #[test]
    fn check_reviewed_evidence_manifest_head_verification_accepts_a_complete_step() {
        let text = simple_step(
            "Verify reviewed evidence manifest checkout is pinned to the dispatched commit",
            Some("github.event_name == 'workflow_dispatch'"),
            "          actual_head=\"$(git -C release-evidence-manifest rev-parse HEAD)\"\n          test \"${actual_head}\" = \"${GITHUB_SHA}\"",
        );
        check_reviewed_evidence_manifest_head_verification(&text)
            .expect("complete HEAD verification step accepted");
    }

    #[test]
    fn check_reviewed_evidence_manifest_head_verification_rejects_a_missing_step() {
        let error = check_reviewed_evidence_manifest_head_verification(
            "steps:\n  - name: Other\n    run: echo hi\n",
        )
        .expect_err("missing step is an error");
        assert!(
            error.contains("Verify reviewed evidence manifest checkout is pinned"),
            "{error}"
        );
    }

    #[test]
    fn check_reviewed_evidence_manifest_head_verification_rejects_trusting_the_checkout_input() {
        let text = simple_step(
            "Verify reviewed evidence manifest checkout is pinned to the dispatched commit",
            Some("github.event_name == 'workflow_dispatch'"),
            "          echo trusting the checkout step blindly",
        );
        let error = check_reviewed_evidence_manifest_head_verification(&text).expect_err(
            "a step that never independently re-reads the checked-out manifest's own HEAD is \
             rejected",
        );
        assert!(
            error.contains("independently read the checked-out manifest directory's own HEAD"),
            "{error}"
        );
    }

    #[test]
    fn check_reviewed_evidence_manifest_checkout_accepts_a_complete_step() {
        let text = simple_step(
            "Check out reviewed release evidence manifest",
            Some("github.event_name == 'workflow_dispatch'"),
            "          echo placeholder body",
        )
        .replace(
            "        run: |\n",
            "        uses: actions/checkout@x\n        with:\n          ref: ${{ github.sha }}\n          path: release-evidence-manifest\n          sparse-checkout: |\n            docs/release/evidence\n        run: |\n",
        );
        check_reviewed_evidence_manifest_checkout(&text).expect("complete checkout step accepted");
    }

    #[test]
    fn check_reviewed_evidence_manifest_checkout_rejects_a_moving_main_ref() {
        let text = "steps:\n      - name: Check out reviewed release evidence manifest\n        if: github.event_name == 'workflow_dispatch'\n        uses: actions/checkout@x\n        with:\n          ref: main\n          path: release-evidence-manifest\n          sparse-checkout: |\n            docs/release/evidence\n      - name: Next step\n        run: echo hi\n";
        let error = check_reviewed_evidence_manifest_checkout(text).expect_err(
            "ref: main is a moving target: main may advance past the reviewed/dispatched commit \
             before this step runs, or before a later re-run of the same workflow_dispatch run",
        );
        assert!(error.contains("moving `ref: main`"), "{error}");
    }

    #[test]
    fn check_reviewed_evidence_manifest_checkout_rejects_a_missing_step() {
        let error = check_reviewed_evidence_manifest_checkout(
            "steps:\n  - name: Other\n    run: echo hi\n",
        )
        .expect_err("missing step is an error");
        assert!(
            error.contains("Check out reviewed release evidence manifest"),
            "{error}"
        );
    }

    #[test]
    fn check_reviewed_evidence_manifest_checkout_rejects_checking_out_the_release_tag() {
        let text = "steps:\n      - name: Check out reviewed release evidence manifest\n        if: github.event_name == 'workflow_dispatch'\n        uses: actions/checkout@x\n        with:\n          ref: ${{ env.RELEASE_TAG }}\n          path: release-evidence-manifest\n          sparse-checkout: |\n            docs/release/evidence\n      - name: Next step\n        run: echo hi\n";
        let error = check_reviewed_evidence_manifest_checkout(text).expect_err(
            "checking out the release tag instead of the dispatched commit defeats the point: \
             the manifest must be an independent trust anchor, not part of the same tree being \
             verified",
        );
        assert!(error.contains("ref: ${{ github.sha }}"), "{error}");
    }

    fn complete_reviewed_evidence_manifest_verification_fixture() -> String {
        simple_step(
            "Verify downloaded evidence against the reviewed manifest",
            Some("github.event_name == 'workflow_dispatch'"),
            "          RELEASED_CRATES=\"$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select((.publish // []) | length > 0) | .name')\"\n          manifest_path=\"release-evidence-manifest/docs/release/evidence/${RELEASE_TAG}.json\"\n          tag_object=\"$(jq -r '.tagObject' \"${manifest_path}\")\"\n          commit=\"$(jq -r '.commit' \"${manifest_path}\")\"\n          tree=\"$(jq -r '.tree' \"${manifest_path}\")\"\n          checksumManifestSha256=\"$(jq -r '.checksumManifestSha256' \"${manifest_path}\")\"\n          expected_crate_sha=\"$(jq -r --arg c \"${crate}\" '.crates[$c].crateSha256' \"${manifest_path}\")\"\n          expected_sbom_sha=\"$(jq -r --arg c \"${crate}\" '.crates[$c].sbomSha256' \"${manifest_path}\")\"",
        )
    }

    #[test]
    fn check_reviewed_evidence_manifest_verification_accepts_a_complete_fixture() {
        let text = complete_reviewed_evidence_manifest_verification_fixture();
        check_reviewed_evidence_manifest_verification(&text).expect("complete fixture accepted");
    }

    #[test]
    fn check_reviewed_evidence_manifest_verification_rejects_a_missing_step() {
        let error = check_reviewed_evidence_manifest_verification(
            "steps:\n  - name: Other\n    run: echo hi\n",
        )
        .expect_err("missing step is an error");
        assert!(
            error.contains("Verify downloaded evidence against the reviewed manifest"),
            "{error}"
        );
    }

    #[test]
    fn check_reviewed_evidence_manifest_verification_rejects_a_hardcoded_manifest_path() {
        let text = complete_reviewed_evidence_manifest_verification_fixture().replace(
            "release-evidence-manifest/docs/release/evidence/${RELEASE_TAG}.json",
            "release-evidence-manifest/docs/release/evidence/v0.6.0.json",
        );
        let error = check_reviewed_evidence_manifest_verification(&text).expect_err(
            "a manifest path hardcoded to the current tag is rejected: it would silently stop \
             matching for a recovery run against any other release tag",
        );
        assert!(
            error.contains("must not hardcode") || error.contains("own release tag"),
            "{error}"
        );
    }

    #[test]
    fn check_reviewed_evidence_manifest_verification_rejects_a_missing_crate_digest_check() {
        let text = complete_reviewed_evidence_manifest_verification_fixture().replace(
            "expected_sbom_sha=\"$(jq -r --arg c \"${crate}\" '.crates[$c].sbomSha256' \"${manifest_path}\")\"",
            "",
        );
        let error = check_reviewed_evidence_manifest_verification(&text)
            .expect_err("a fixture missing the per-crate SBOM digest check is rejected");
        assert!(error.contains("sbomSha256"), "{error}");
    }

    #[test]
    fn check_reviewed_evidence_manifest_verification_rejects_a_missing_checksum_check() {
        let text = complete_reviewed_evidence_manifest_verification_fixture().replace(
            "checksumManifestSha256=\"$(jq -r '.checksumManifestSha256' \"${manifest_path}\")\"",
            "",
        );
        let error = check_reviewed_evidence_manifest_verification(&text)
            .expect_err("a fixture missing the checksum-manifest digest check is rejected");
        assert!(error.contains("checksumManifestSha256"), "{error}");
    }

    #[test]
    fn check_v0_6_0_evidence_manifest_accepts_the_real_file() {
        let root = suite::workspace_root().expect("workspace root resolves");
        let violations = check_v0_6_0_evidence_manifest(&root).expect("manifest reads and parses");
        assert!(violations.is_empty(), "{violations:#?}");
    }

    /// The real, committed `release.yml`, used as a known-good baseline for
    /// `#215` regression tests: each test mutates one specific piece of it
    /// via `.replace()` and asserts `check_release_workflow_text` catches
    /// exactly that regression, rather than hand-maintaining a separate
    /// synthetic fixture that could drift from the real file's structure.
    const REAL_RELEASE_WORKFLOW: &str = include_str!("../../.github/workflows/release.yml");

    #[test]
    fn check_release_workflow_text_accepts_the_real_file() {
        let violations =
            check_release_workflow_text(REAL_RELEASE_WORKFLOW).expect("release.yml checks run");
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn check_release_workflow_text_rejects_workflow_wide_contents_write() {
        let text = REAL_RELEASE_WORKFLOW.replacen(
            "permissions:\n  contents: read\n",
            "permissions:\n  contents: write\n",
            1,
        );
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("must not grant contents: write at the workflow level")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_verify_job_missing_contents_write() {
        let text = REAL_RELEASE_WORKFLOW.replacen("      contents: write\n", "", 1);
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("job \"verify\" must set its own job-scoped")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_verify_job_id_token_write() {
        let text = REAL_RELEASE_WORKFLOW.replacen(
            "    permissions:\n      contents: write\n",
            "    permissions:\n      contents: write\n      id-token: write\n",
            1,
        );
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("job \"verify\" must not hold id-token: write")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_missing_persist_credentials_false() {
        let text = REAL_RELEASE_WORKFLOW.replacen("\n          persist-credentials: false", "", 1);
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("must set persist-credentials: false")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_a_missing_dispatch_origin_guard() {
        let text = REAL_RELEASE_WORKFLOW.replacen(
            "- name: Verify workflow_dispatch originated from main",
            "- name: Renamed step",
            1,
        );
        let error = check_release_workflow_text(&text).expect_err(
            "a missing dispatch-origin guard step is a hard error, matching how \
                         every other extract_step_block call in this function propagates a \
                         missing step via `?`",
        );
        assert!(
            error.contains("Verify workflow_dispatch originated from main"),
            "{error}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_a_dispatch_origin_guard_not_checking_main() {
        let text = REAL_RELEASE_WORKFLOW.replace("refs/heads/main", "refs/heads/anywhere");
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("must reject any dispatch ref other than refs/heads/main")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_a_dispatch_origin_guard_running_after_checkout() {
        let guard_step = "      - name: Verify workflow_dispatch originated from main\n        if: github.event_name == 'workflow_dispatch'\n        run: |\n          set -euo pipefail\n          if [ \"${GITHUB_REF}\" != \"refs/heads/main\" ]; then\n            echo \"::error::workflow_dispatch publish-registered bootstrap must be dispatched from refs/heads/main, not ${GITHUB_REF}: dispatching from any other ref would execute that ref's own (possibly unreviewed) copy of this workflow file\" >&2\n            exit 1\n          fi\n\n";
        let without_guard = REAL_RELEASE_WORKFLOW.replacen(guard_step, "", 1);
        assert_ne!(
            without_guard, REAL_RELEASE_WORKFLOW,
            "fixture's guard_step text must exactly match the real file"
        );
        let text = without_guard.replacen(
            "      - name: Verify manual recovery target and explicit mode",
            &format!("{guard_step}      - name: Verify manual recovery target and explicit mode"),
            1,
        );
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("must run before \"Check out release tag\"")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_missing_publish_registered_draft_check() {
        let text = REAL_RELEASE_WORKFLOW.replacen(
            "            test \"$(jq -r '.isDraft' <<<\"${view}\")\" = \"true\"\n",
            "",
            1,
        );
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("must verify isDraft == true in publish-registered mode")),
            "{violations:#?}"
        );
    }

    #[test]
    fn check_release_workflow_text_rejects_missing_verify_mode_draft_check() {
        let text = REAL_RELEASE_WORKFLOW.replacen(
            "            test \"$(jq -r '.isDraft' <<<\"${view}\")\" = \"false\"\n",
            "",
            1,
        );
        let violations = check_release_workflow_text(&text).expect("checks run");
        assert!(
            violations
                .iter()
                .any(|v| v.contains("must continue to verify isDraft == false in verify mode")),
            "{violations:#?}"
        );
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
