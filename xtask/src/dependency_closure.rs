//! Campaign-scoped resolved dependency closures.
//!
//! A retained campaign used to bind the workspace-wide `Cargo.lock` object.
//! That is reproducible but too coarse: a lockfile movement in a package the
//! campaign never builds invalidates every retained report. This module keeps
//! the lockfile as the resolver authority while binding only the resolved
//! packages that each campaign actually builds.
//!
//! The canonical campaign-semantics document declares the workspace package
//! roots exactly once. A `run` root mirrors `cargo run -p ...`; a `test`
//! root mirrors the shared campaign runner's `cargo test -p ... --all-features`
//! and therefore includes dev-dependencies. Cargo itself computes the graph via
//! `cargo tree --locked --offline`; this module does not implement a second
//! feature/dependency resolver.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

const SIDECAR: &str = "dependency-closure.json";

type PackageIdentity = (String, String, String);
type LockIdentities = BTreeMap<PackageIdentity, Option<String>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Root {
    package: String,
    kind: String,
}

/// Recomputes every committed campaign dependency closure.
///
/// # Errors
///
/// Returns an error when the campaign inventory, Cargo resolver, or sidecar
/// cannot be read. A stale sidecar is returned as a violation rather than an
/// error so callers can report every stale campaign together.
pub fn check_all(root: &Path) -> Result<Vec<String>, String> {
    let metadata = Metadata::load(root)?;
    let mut violations = Vec::new();
    for semantics in semantics_files(root)? {
        violations.extend(check_one_with(root, &semantics, &metadata));
    }
    Ok(violations)
}

/// Writes every canonical campaign dependency closure.
///
/// This command exists for deliberate closure refreshes. CI verification never
/// writes these files; it only recomputes and compares them.
///
/// # Errors
///
/// Returns an error when Cargo resolution or filesystem access fails.
pub fn write_all(root: &Path) -> Result<Vec<PathBuf>, String> {
    let metadata = Metadata::load(root)?;
    let mut written = Vec::new();
    for semantics in semantics_files(root)? {
        let expected = expected(root, &semantics, &metadata)?;
        let sidecar = sidecar_path(root, &semantics)?;
        let rendered = serde_json::to_string_pretty(&expected)
            .map_err(|error| format!("could not render {}: {error}", sidecar.display()))?;
        fs::write(&sidecar, format!("{rendered}\n"))
            .map_err(|error| format!("could not write {}: {error}", sidecar.display()))?;
        written.push(sidecar);
    }
    Ok(written)
}

/// Verifies one campaign's committed dependency closure.
///
/// The retained-evidence verifier calls this using the same semantics file the
/// producer report names. This keeps producer/verifier dependency boundaries
/// anchored in one checked-in declaration.
///
/// # Errors
///
/// Returns the resolver/read failure; a content mismatch is represented as one
/// human-readable violation.
pub fn check_one(root: &Path, semantics: &str) -> Result<Vec<String>, String> {
    let metadata = Metadata::load(root)?;
    Ok(check_one_with(root, semantics, &metadata))
}

fn check_one_with(root: &Path, semantics: &str, metadata: &Metadata) -> Vec<String> {
    match expected(root, semantics, metadata) {
        Ok(expected) => {
            let sidecar = match sidecar_path(root, semantics) {
                Ok(path) => path,
                Err(error) => return vec![error],
            };
            let source = match fs::read_to_string(&sidecar) {
                Ok(source) => source,
                Err(error) => {
                    return vec![format!(
                        "{} has no readable dependency closure: {error}",
                        relative(root, &sidecar)
                    )];
                }
            };
            let actual: Value = match serde_json::from_str(&source) {
                Ok(actual) => actual,
                Err(error) => {
                    return vec![format!(
                        "{} is not valid JSON: {error}",
                        relative(root, &sidecar)
                    )];
                }
            };
            if actual == expected {
                Vec::new()
            } else {
                vec![format!(
                    "{} is stale for {}; run `cargo xtask dependency-closures-write` only after reviewing the resolved graph change",
                    relative(root, &sidecar),
                    semantics
                )]
            }
        }
        Err(error) => vec![format!("{semantics} dependency closure: {error}")],
    }
}

fn expected(root: &Path, semantics: &str, metadata: &Metadata) -> Result<Value, String> {
    let source_path = root.join(semantics);
    let source = fs::read_to_string(&source_path)
        .map_err(|error| format!("could not read {}: {error}", source_path.display()))?;
    let document: Value = serde_json::from_str(&source)
        .map_err(|error| format!("could not parse {}: {error}", source_path.display()))?;

    let roots = roots(&document)?;
    let declared_manifests = semantic_manifest_paths(&document);
    for campaign_root in &roots {
        let manifest = metadata
            .workspace_manifest(&campaign_root.package)
            .ok_or_else(|| {
                format!(
                    "dependency root {} is not a unique workspace package",
                    campaign_root.package
                )
            })?;
        if !declared_manifests.contains(manifest) {
            return Err(format!(
                "dependency root {} is not bound by its manifest {} in campaign semantics",
                campaign_root.package, manifest
            ));
        }
    }

    let mut selected = BTreeSet::new();
    for campaign_root in &roots {
        selected.extend(tree_packages(root, campaign_root)?);
    }

    verify_selected_workspace_manifests(&selected, metadata, &declared_manifests)?;
    let packages = resolved_external_packages(&selected, metadata)?;

    let root_values = roots
        .iter()
        .map(|entry| json!({"package": entry.package, "kind": entry.kind}))
        .collect::<Vec<_>>();

    Ok(json!({
        "schema": 1,
        "semantics": semantics,
        "derivation": {
            "resolver": "cargo tree --locked --offline",
            "test_roots": "cargo test --package <root> --all-features; normal+build+dev edges",
            "run_roots": "cargo run --package <root>; normal+build edges",
            "roots": root_values,
        },
        "packages": packages,
    }))
}

fn verify_selected_workspace_manifests(
    selected: &BTreeSet<(String, String)>,
    metadata: &Metadata,
    declared_manifests: &BTreeSet<String>,
) -> Result<(), String> {
    for (name, version) in selected {
        if !metadata.is_workspace_package(name, version) {
            continue;
        }
        let manifest = metadata.workspace_manifest(name).ok_or_else(|| {
            format!("selected workspace package {name} {version} has no canonical manifest")
        })?;
        if !declared_manifests.contains(manifest) {
            return Err(format!(
                "selected workspace package {name} {version} is not bound by its manifest {manifest} in campaign semantics"
            ));
        }
    }
    Ok(())
}

fn resolved_external_packages(
    selected: &BTreeSet<(String, String)>,
    metadata: &Metadata,
) -> Result<Vec<Value>, String> {
    let mut packages = Vec::new();
    for (name, version) in selected {
        let package = metadata.unique(name, version)?;
        let Some(source) = package.get("source").and_then(Value::as_str) else {
            if metadata.is_workspace_package(name, version) {
                continue;
            }
            let manifest = package
                .get("manifest_path")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            return Err(format!(
                "resolved non-workspace path package {name} {version} at {manifest}; campaign dependency closure cannot omit an unbound path dependency"
            ));
        };
        let checksum = metadata.lock_checksum(name, version, source)?;
        packages.push(json!({
            "name": name,
            "version": version,
            "source": source,
            "checksum": checksum,
        }));
    }
    packages.sort_by_key(package_key);
    Ok(packages)
}

fn package_key(value: &Value) -> (String, String, String) {
    (
        value
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        value
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        value
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    )
}

fn roots(document: &Value) -> Result<Vec<Root>, String> {
    let values = document
        .get("dependency_roots")
        .and_then(Value::as_array)
        .ok_or_else(|| "campaign semantics declares no dependency_roots".to_owned())?;
    if values.is_empty() {
        return Err("campaign semantics declares an empty dependency_roots list".to_owned());
    }

    let mut roots = BTreeSet::new();
    for value in values {
        let package = value
            .get("package")
            .and_then(Value::as_str)
            .ok_or_else(|| "dependency root has no package".to_owned())?;
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("dependency root {package} has no kind"))?;
        if !matches!(kind, "run" | "test") {
            return Err(format!(
                "dependency root {package} has unsupported kind {kind}; expected run or test"
            ));
        }
        if !roots.insert(Root {
            package: package.to_owned(),
            kind: kind.to_owned(),
        }) {
            return Err(format!(
                "dependency root {package}/{kind} is declared twice"
            ));
        }
    }
    Ok(roots.into_iter().collect())
}

fn semantic_manifest_paths(document: &Value) -> BTreeSet<String> {
    let mut manifests = BTreeSet::new();
    let Some(categories) = document.get("categories").and_then(Value::as_object) else {
        return manifests;
    };
    for category in categories.values() {
        for path in category
            .get("paths")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            if path.ends_with("Cargo.toml") {
                manifests.insert(path.to_owned());
            }
        }
    }
    manifests
}

fn tree_packages(root: &Path, campaign_root: &Root) -> Result<BTreeSet<(String, String)>, String> {
    let mut command = Command::new("cargo");
    command.current_dir(root).args([
        "tree",
        "--locked",
        "--offline",
        "--package",
        &campaign_root.package,
        "--prefix",
        "none",
        "--no-dedupe",
        "--format",
        "{p}",
    ]);
    match campaign_root.kind.as_str() {
        "test" => {
            command.args(["--all-features", "--edges", "normal,build,dev"]);
        }
        "run" => {
            command.args(["--edges", "normal,build"]);
        }
        _ => unreachable!("validated dependency root kind"),
    }

    let output = command.output().map_err(|error| {
        format!(
            "could not run cargo tree for {}: {error}",
            campaign_root.package
        )
    })?;
    if !output.status.success() {
        return Err(format!(
            "cargo tree for {} failed with {}: {}",
            campaign_root.package,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let mut packages = BTreeSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        let Some(version) = fields.next().and_then(|field| field.strip_prefix('v')) else {
            return Err(format!(
                "cargo tree emitted an unparseable package line for {}: {line}",
                campaign_root.package
            ));
        };
        packages.insert((name.to_owned(), version.to_owned()));
    }
    Ok(packages)
}

fn semantics_files(root: &Path) -> Result<Vec<String>, String> {
    let fixtures = root.join("tests/fixtures");
    let mut found = Vec::new();
    collect_semantics(root, &fixtures, &mut found)?;
    found.sort();
    if found.is_empty() {
        return Err("no campaign-semantics.json files were found".to_owned());
    }
    Ok(found)
}

fn collect_semantics(root: &Path, directory: &Path, found: &mut Vec<String>) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "could not read an entry under {}: {error}",
                directory.display()
            )
        })?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|error| format!("could not inspect {}: {error}", path.display()))?;
        if kind.is_dir() {
            collect_semantics(root, &path, found)?;
        } else if path.file_name().and_then(|name| name.to_str()) == Some("campaign-semantics.json")
        {
            found.push(relative(root, &path));
        }
    }
    Ok(())
}

fn sidecar_path(root: &Path, semantics: &str) -> Result<PathBuf, String> {
    let path = root.join(semantics);
    let parent = path
        .parent()
        .ok_or_else(|| format!("{semantics} has no parent directory"))?;
    Ok(parent.join(SIDECAR))
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn parse_lockfile(root: &Path) -> Result<LockIdentities, String> {
    let path = root.join("Cargo.lock");
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let mut identities = BTreeMap::new();
    let mut current: BTreeMap<&str, String> = BTreeMap::new();

    let flush = |current: &mut BTreeMap<&str, String>,
                 identities: &mut LockIdentities|
     -> Result<(), String> {
        if current.is_empty() {
            return Ok(());
        }
        let name = current
            .remove("name")
            .ok_or_else(|| "Cargo.lock package has no name".to_owned())?;
        let version = current
            .remove("version")
            .ok_or_else(|| format!("Cargo.lock package {name} has no version"))?;
        let Some(source) = current.remove("source") else {
            current.clear();
            return Ok(());
        };
        let checksum = current.remove("checksum");
        let key = (name.clone(), version.clone(), source.clone());
        if identities.insert(key, checksum).is_some() {
            return Err(format!(
                "Cargo.lock contains duplicate external identity {name} {version} from {source}"
            ));
        }
        current.clear();
        Ok(())
    };

    for raw in source.lines() {
        let line = raw.trim();
        if line == "[[package]]" {
            flush(&mut current, &mut identities)?;
            continue;
        }
        for field in ["name", "version", "source", "checksum"] {
            let prefix = format!("{field} = \"");
            if let Some(value) = line
                .strip_prefix(&prefix)
                .and_then(|value| value.strip_suffix('"'))
            {
                current.insert(field, value.to_owned());
                break;
            }
        }
    }
    flush(&mut current, &mut identities)?;
    if identities.is_empty() {
        return Err("Cargo.lock contains no external package identities".to_owned());
    }
    Ok(identities)
}

struct Metadata {
    packages: BTreeMap<(String, String), Vec<Value>>,
    workspace_manifests: BTreeMap<String, String>,
    workspace_identities: BTreeSet<(String, String)>,
    lock_identities: LockIdentities,
}

impl Metadata {
    fn load(root: &Path) -> Result<Self, String> {
        let output = Command::new("cargo")
            .current_dir(root)
            .args([
                "metadata",
                "--locked",
                "--offline",
                "--format-version",
                "1",
                "--all-features",
            ])
            .output()
            .map_err(|error| format!("could not run cargo metadata: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "cargo metadata failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let document: Value = serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("could not parse cargo metadata: {error}"))?;
        let workspace_root = document
            .get("workspace_root")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .ok_or_else(|| "cargo metadata returned no workspace_root".to_owned())?;
        let workspace_members = document
            .get("workspace_members")
            .and_then(Value::as_array)
            .ok_or_else(|| "cargo metadata returned no workspace_members".to_owned())?
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();

        let mut packages: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
        let mut workspace_manifests = BTreeMap::new();
        let mut workspace_identities = BTreeSet::new();
        for package in document
            .get("packages")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(name) = package.get("name").and_then(Value::as_str) else {
                continue;
            };
            let Some(version) = package.get("version").and_then(Value::as_str) else {
                continue;
            };
            packages
                .entry((name.to_owned(), version.to_owned()))
                .or_default()
                .push(package.clone());

            let Some(id) = package.get("id").and_then(Value::as_str) else {
                continue;
            };
            if !workspace_members.contains(id) {
                continue;
            }
            let Some(manifest) = package.get("manifest_path").and_then(Value::as_str) else {
                continue;
            };
            let relative = Path::new(manifest)
                .strip_prefix(&workspace_root)
                .map_err(|_| format!("workspace manifest {manifest} is outside workspace root"))?
                .to_string_lossy()
                .replace('\\', "/");
            if workspace_manifests
                .insert(name.to_owned(), relative)
                .is_some()
            {
                return Err(format!("workspace contains duplicate package name {name}"));
            }
            if !workspace_identities.insert((name.to_owned(), version.to_owned())) {
                return Err(format!(
                    "workspace contains duplicate package identity {name} {version}"
                ));
            }
        }

        let lock_identities = parse_lockfile(root)?;

        Ok(Self {
            packages,
            workspace_manifests,
            workspace_identities,
            lock_identities,
        })
    }

    fn workspace_manifest(&self, package: &str) -> Option<&str> {
        self.workspace_manifests.get(package).map(String::as_str)
    }

    fn is_workspace_package(&self, name: &str, version: &str) -> bool {
        self.workspace_identities
            .contains(&(name.to_owned(), version.to_owned()))
    }

    fn lock_checksum(&self, name: &str, version: &str, source: &str) -> Result<Value, String> {
        let key = (name.to_owned(), version.to_owned(), source.to_owned());
        let Some(checksum) = self.lock_identities.get(&key) else {
            return Err(format!(
                "resolved package {name} {version} from {source} has no matching Cargo.lock identity"
            ));
        };
        Ok(checksum
            .as_ref()
            .map_or(Value::Null, |value| Value::String(value.clone())))
    }

    fn unique(&self, name: &str, version: &str) -> Result<&Value, String> {
        let Some(candidates) = self.packages.get(&(name.to_owned(), version.to_owned())) else {
            return Err(format!(
                "cargo tree selected {name} {version}, absent from cargo metadata"
            ));
        };
        if candidates.len() != 1 {
            return Err(format!(
                "cargo tree selected ambiguous package {name} {version} from {} sources",
                candidates.len()
            ));
        }
        Ok(&candidates[0])
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Metadata, Root, package_key, resolved_external_packages, roots,
        verify_selected_workspace_manifests,
    };
    use serde_json::json;

    #[test]
    fn roots_are_canonical_and_reject_duplicates() -> Result<(), String> {
        let document = json!({
            "dependency_roots": [
                {"package": "oxide-batch-xtask", "kind": "run"},
                {"package": "oxide-batch", "kind": "test"}
            ]
        });
        let parsed = roots(&document)?;
        assert_eq!(
            parsed,
            vec![
                Root {
                    package: "oxide-batch".into(),
                    kind: "test".into()
                },
                Root {
                    package: "oxide-batch-xtask".into(),
                    kind: "run".into()
                },
            ]
        );

        let duplicate = json!({
            "dependency_roots": [
                {"package": "oxide-batch", "kind": "test"},
                {"package": "oxide-batch", "kind": "test"}
            ]
        });
        assert!(roots(&duplicate).is_err());
        Ok(())
    }

    #[test]
    fn package_key_changes_when_relevant_version_changes() {
        let before = json!({
            "name": "syn",
            "version": "3.0.5",
            "source": "registry+https://github.com/rust-lang/crates.io-index"
        });
        let after = json!({
            "name": "syn",
            "version": "3.0.6",
            "source": "registry+https://github.com/rust-lang/crates.io-index"
        });
        assert_ne!(package_key(&before), package_key(&after));
    }

    fn metadata(packages: &[serde_json::Value]) -> Result<Metadata, String> {
        let mut indexed = std::collections::BTreeMap::new();
        let mut lock_identities = std::collections::BTreeMap::new();
        for package in packages {
            let name = package
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "test package has no name".to_owned())?
                .to_owned();
            let version = package
                .get("version")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("test package {name} has no version"))?
                .to_owned();
            indexed
                .entry((name.clone(), version.clone()))
                .or_insert_with(Vec::new)
                .push(package.clone());

            let source = package
                .get("source")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("test package {name} {version} has no source"))?
                .to_owned();
            let checksum = package
                .get("checksum")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            lock_identities.insert((name, version, source), checksum);
        }
        Ok(Metadata {
            packages: indexed,
            workspace_manifests: std::collections::BTreeMap::new(),
            workspace_identities: std::collections::BTreeSet::new(),
            lock_identities,
        })
    }

    fn package(name: &str, version: &str, source: &str, checksum: &str) -> serde_json::Value {
        json!({
            "name": name,
            "version": version,
            "source": source,
            "checksum": checksum,
        })
    }

    #[test]
    fn rejects_selected_non_workspace_path_dependency() -> Result<(), String> {
        let path_package = json!({
            "name": "local-helper",
            "version": "0.1.0",
            "manifest_path": "/tmp/local-helper/Cargo.toml",
        });
        let mut packages = std::collections::BTreeMap::new();
        packages.insert(
            ("local-helper".to_owned(), "0.1.0".to_owned()),
            vec![path_package],
        );
        let metadata = Metadata {
            packages,
            workspace_manifests: std::collections::BTreeMap::new(),
            workspace_identities: std::collections::BTreeSet::new(),
            lock_identities: std::collections::BTreeMap::new(),
        };
        let selected = [("local-helper".to_owned(), "0.1.0".to_owned())]
            .into_iter()
            .collect();
        let error = resolved_external_packages(&selected, &metadata)
            .err()
            .ok_or_else(|| "unbound path dependency unexpectedly passed".to_owned())?;
        assert!(error.contains("non-workspace path package"));
        Ok(())
    }

    #[test]
    fn requires_every_selected_workspace_manifest_in_semantics() -> Result<(), String> {
        let mut workspace_manifests = std::collections::BTreeMap::new();
        workspace_manifests.insert(
            "oxide-batch-core".to_owned(),
            "crates/oxide-batch-core/Cargo.toml".to_owned(),
        );
        let workspace_identities = [("oxide-batch-core".to_owned(), "0.6.0".to_owned())]
            .into_iter()
            .collect();
        let metadata = Metadata {
            packages: std::collections::BTreeMap::new(),
            workspace_manifests,
            workspace_identities,
            lock_identities: std::collections::BTreeMap::new(),
        };
        let selected = [("oxide-batch-core".to_owned(), "0.6.0".to_owned())]
            .into_iter()
            .collect();
        let declared = std::collections::BTreeSet::new();
        let error = verify_selected_workspace_manifests(&selected, &metadata, &declared)
            .err()
            .ok_or_else(|| "unbound workspace manifest unexpectedly passed".to_owned())?;
        assert!(error.contains("oxide-batch-core"));
        assert!(error.contains("crates/oxide-batch-core/Cargo.toml"));
        Ok(())
    }

    #[test]
    fn unrelated_resolved_package_movement_does_not_change_the_closure() -> Result<(), String> {
        let selected = [("syn".to_owned(), "3.0.5".to_owned())]
            .into_iter()
            .collect();
        let before = metadata(&[
            package(
                "syn",
                "3.0.5",
                "registry+https://example.invalid/index",
                "aaa",
            ),
            package(
                "unrelated",
                "1.0.0",
                "registry+https://example.invalid/index",
                "old",
            ),
        ])?;
        let after = metadata(&[
            package(
                "syn",
                "3.0.5",
                "registry+https://example.invalid/index",
                "aaa",
            ),
            package(
                "unrelated",
                "2.0.0",
                "registry+https://example.invalid/index",
                "new",
            ),
        ])?;

        assert_eq!(
            resolved_external_packages(&selected, &before)?,
            resolved_external_packages(&selected, &after)?,
        );
        Ok(())
    }

    #[test]
    fn reachable_version_source_and_checksum_changes_change_the_closure() -> Result<(), String> {
        let registry = "registry+https://example.invalid/index";
        let git = "git+https://example.invalid/repo";
        let before_selected = [("syn".to_owned(), "3.0.5".to_owned())]
            .into_iter()
            .collect();
        let after_selected = [("syn".to_owned(), "3.0.6".to_owned())]
            .into_iter()
            .collect();

        let before = metadata(&[package("syn", "3.0.5", registry, "aaa")])?;
        let version = metadata(&[package("syn", "3.0.6", registry, "aaa")])?;
        assert_ne!(
            resolved_external_packages(&before_selected, &before)?,
            resolved_external_packages(&after_selected, &version)?,
        );

        let source = metadata(&[package("syn", "3.0.5", git, "aaa")])?;
        assert_ne!(
            resolved_external_packages(&before_selected, &before)?,
            resolved_external_packages(&before_selected, &source)?,
        );

        let checksum = metadata(&[package("syn", "3.0.5", registry, "bbb")])?;
        assert_ne!(
            resolved_external_packages(&before_selected, &before)?,
            resolved_external_packages(&before_selected, &checksum)?,
        );
        Ok(())
    }
}
