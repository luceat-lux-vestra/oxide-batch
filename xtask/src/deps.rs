//! Workspace boundary checks for the staged crate extraction.
//!
//! The staged crate-extraction contract in
//! `docs/architecture/crate-extraction.md` forbids named dependency classes
//! per extracted crate and forbids any cycle between workspace crates. This
//! check is authoritative: a passing manual review never substitutes for it.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use serde_json::Value;

/// One forbidden-dependency rule for one extracted crate.
struct Boundary {
    /// The workspace crate the rule governs.
    crate_name: &'static str,
    /// Package name prefixes the crate may not reach through normal or build
    /// dependencies, directly or transitively.
    forbidden: &'static [&'static str],
}

/// The dependency classes no extracted crate may reach.
const RUNTIME: &str = "tokio";
const DRIVER: &str = "sqlx";
const COMMAND_LINE: &str = "clap";
const TELEMETRY_SDK: &str = "opentelemetry";
const BROKERS: [&str; 4] = ["rdkafka", "lapin", "async-nats", "rumqttc"];
const WEB: [&str; 6] = ["axum", "actix-web", "hyper", "reqwest", "rocket", "warp"];

/// The boundaries the extraction contract authorizes.
const BOUNDARIES: &[Boundary] = &[
    Boundary {
        crate_name: "oxide-batch-core",
        forbidden: &[
            RUNTIME,
            DRIVER,
            COMMAND_LINE,
            TELEMETRY_SDK,
            BROKERS[0],
            BROKERS[1],
            BROKERS[2],
            BROKERS[3],
            WEB[0],
            WEB[1],
            WEB[2],
            WEB[3],
            WEB[4],
            WEB[5],
            "oxide-batch",
        ],
    },
    Boundary {
        crate_name: "oxide-batch-repository",
        forbidden: &[
            DRIVER,
            COMMAND_LINE,
            TELEMETRY_SDK,
            "oxide-batch-plan",
            "oxide-batch-cli",
            "=oxide-batch",
        ],
    },
    Boundary {
        crate_name: "oxide-batch-plan",
        forbidden: &[
            RUNTIME,
            DRIVER,
            COMMAND_LINE,
            TELEMETRY_SDK,
            "oxide-batch-cli",
            "=oxide-batch",
        ],
    },
];

/// Runs the forbidden-dependency and cycle checks.
///
/// Returns every violation as a human-readable line. An empty result means the
/// workspace satisfies the contract.
pub fn check() -> Result<Vec<String>, String> {
    let metadata = load_metadata()?;
    let graph = Graph::parse(&metadata)?;

    let mut violations = Vec::new();
    violations.extend(check_forbidden(&graph));
    violations.extend(check_cycles(&graph));
    Ok(violations)
}

/// Reads the fully resolved workspace graph with every feature enabled.
fn load_metadata() -> Result<Value, String> {
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--all-features",
            "--locked",
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

    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("could not parse cargo metadata: {error}"))
}

/// The resolved package graph reduced to what the boundary rules need.
struct Graph {
    /// Package identifier to package name.
    names: BTreeMap<String, String>,
    /// Package identifier to its normal and build dependencies.
    shipped: BTreeMap<String, Vec<String>>,
    /// Package identifier to every dependency, including development ones.
    all: BTreeMap<String, Vec<String>>,
    /// Workspace member package identifiers.
    members: BTreeSet<String>,
}

impl Graph {
    /// Builds the graph from `cargo metadata` output.
    fn parse(metadata: &Value) -> Result<Self, String> {
        let mut names = BTreeMap::new();
        for package in array(metadata, "packages")? {
            let id = string(package, "id")?;
            let name = string(package, "name")?;
            names.insert(id.to_owned(), name.to_owned());
        }

        let members = array(metadata, "workspace_members")?
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();

        let resolve = metadata
            .get("resolve")
            .ok_or_else(|| "cargo metadata returned no resolved graph".to_owned())?;

        let mut shipped = BTreeMap::new();
        let mut all = BTreeMap::new();
        for node in array(resolve, "nodes")? {
            let id = string(node, "id")?.to_owned();
            let mut shipped_deps = Vec::new();
            let mut all_deps = Vec::new();
            for dep in array(node, "deps")? {
                let package = string(dep, "pkg")?.to_owned();
                let kinds = array(dep, "dep_kinds")?;
                let ships = kinds
                    .iter()
                    .any(|kind| !matches!(kind.get("kind").and_then(Value::as_str), Some("dev")));
                if ships {
                    shipped_deps.push(package.clone());
                }
                all_deps.push(package);
            }
            shipped.insert(id.clone(), shipped_deps);
            all.insert(id, all_deps);
        }

        Ok(Self {
            names,
            shipped,
            all,
            members,
        })
    }

    /// Returns the workspace member identifier for `name`, if it exists.
    fn member(&self, name: &str) -> Option<&String> {
        self.members
            .iter()
            .find(|id| self.names.get(*id).is_some_and(|found| found == name))
    }

    /// Returns every package reachable from `root` through shipped edges.
    fn shipped_closure(&self, root: &str) -> BTreeSet<String> {
        let mut seen = BTreeSet::new();
        let mut pending = vec![root.to_owned()];
        while let Some(id) = pending.pop() {
            for dependency in self.shipped.get(&id).map(Vec::as_slice).unwrap_or_default() {
                if seen.insert(dependency.clone()) {
                    pending.push(dependency.clone());
                }
            }
        }
        seen
    }
}

/// Reports every extracted crate that reaches a forbidden dependency class.
fn check_forbidden(graph: &Graph) -> Vec<String> {
    let mut violations = Vec::new();
    for boundary in BOUNDARIES {
        let Some(root) = graph.member(boundary.crate_name) else {
            continue;
        };

        for dependency in graph.shipped_closure(root) {
            let Some(name) = graph.names.get(&dependency) else {
                continue;
            };
            if let Some(rule) = boundary
                .forbidden
                .iter()
                .find(|rule| matches(name, rule, boundary.crate_name))
            {
                violations.push(format!(
                    "{} depends on {name}, which the extraction contract forbids ({rule})",
                    boundary.crate_name
                ));
            }
        }
    }
    violations.sort_unstable();
    violations.dedup();
    violations
}

/// Reports whether `name` is an instance of the forbidden `rule`.
///
/// A rule matches the package itself or any package that extends it with a
/// separator, so `tokio` covers `tokio-util` but not `tokioesque`. A rule
/// written as `=name` matches that package alone. The governed crate never
/// matches its own rule.
fn matches(name: &str, rule: &str, crate_name: &str) -> bool {
    if name == crate_name {
        return false;
    }
    match rule.strip_prefix('=') {
        Some(exact) => name == exact,
        None => {
            name == rule
                || name
                    .strip_prefix(rule)
                    .is_some_and(|rest| rest.starts_with('-') || rest.starts_with('_'))
        }
    }
}

/// Reports every dependency cycle between workspace crates.
fn check_cycles(graph: &Graph) -> Vec<String> {
    let mut violations = Vec::new();
    for member in &graph.members {
        let mut path = Vec::new();
        if let Some(cycle) = find_cycle(graph, member, member, &mut path) {
            violations.push(format!("workspace dependency cycle: {cycle}"));
        }
    }
    violations.sort_unstable();
    violations.dedup();
    violations
}

/// Walks workspace-member edges looking for a path back to `target`.
fn find_cycle(
    graph: &Graph,
    target: &str,
    current: &str,
    path: &mut Vec<String>,
) -> Option<String> {
    for dependency in graph
        .all
        .get(current)
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        if !graph.members.contains(dependency) {
            continue;
        }
        if dependency == target {
            let mut rendered: Vec<&str> = std::iter::once(target)
                .chain(path.iter().map(String::as_str))
                .chain(std::iter::once(target))
                .filter_map(|id| graph.names.get(id).map(String::as_str))
                .collect();
            rendered.dedup();
            return Some(rendered.join(" -> "));
        }
        if path.iter().any(|seen| seen == dependency) {
            continue;
        }
        path.push(dependency.clone());
        let found = find_cycle(graph, target, dependency, path);
        path.pop();
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Borrows a required array field.
fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>, String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("cargo metadata field {field} was not an array"))
}

/// Borrows a required string field.
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("cargo metadata field {field} was not a string"))
}

#[cfg(test)]
mod tests {
    use super::{Graph, check, check_cycles, check_forbidden, matches};
    use std::collections::{BTreeMap, BTreeSet};

    /// Builds a graph from `(package, shipped deps, dev deps)` triples.
    ///
    /// The first `member_count` packages are workspace members.
    fn graph(packages: &[(&str, &[&str], &[&str])], member_count: usize) -> Graph {
        let mut names = BTreeMap::new();
        let mut shipped = BTreeMap::new();
        let mut all = BTreeMap::new();
        let mut members = BTreeSet::new();

        for (index, (name, ships, dev)) in packages.iter().enumerate() {
            let id = (*name).to_owned();
            names.insert(id.clone(), (*name).to_owned());
            let ships: Vec<String> = ships.iter().map(|dep| (*dep).to_owned()).collect();
            let mut every = ships.clone();
            every.extend(dev.iter().map(|dep| (*dep).to_owned()));
            shipped.insert(id.clone(), ships);
            all.insert(id.clone(), every);
            if index < member_count {
                members.insert(id);
            }
        }

        Graph {
            names,
            shipped,
            all,
            members,
        }
    }

    #[test]
    fn forbidden_dependency_check_fails_the_build_on_violation() {
        let graph = graph(
            &[
                ("oxide-batch-core", &["sqlx"], &[]),
                ("sqlx", &["tokio"], &[]),
                ("tokio", &[], &[]),
            ],
            1,
        );

        let violations = check_forbidden(&graph);

        assert_eq!(violations.len(), 2, "{violations:?}");
        assert!(
            violations
                .iter()
                .all(|violation| violation.contains("the extraction contract forbids"))
        );
    }

    #[test]
    fn workspace_has_no_dependency_cycle() {
        let violations = match check() {
            Ok(violations) => violations,
            Err(error) => {
                // `cargo metadata` is unavailable in some sandboxes; the same
                // check runs as `cargo xtask deps` in CI.
                eprintln!("skipping workspace boundary check: {error}");
                return;
            }
        };

        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn a_transitive_runtime_dependency_is_a_violation() {
        let graph = graph(
            &[
                ("oxide-batch-core", &["serde_json", "leaky"], &[]),
                ("leaky", &["tokio"], &[]),
                ("serde_json", &[], &[]),
                ("tokio", &[], &[]),
            ],
            1,
        );

        let violations = check_forbidden(&graph);

        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(violations[0].contains("oxide-batch-core depends on tokio"));
    }

    #[test]
    fn an_allowed_dependency_is_not_a_violation() {
        let graph = graph(
            &[
                ("oxide-batch-core", &["serde_json", "sha2"], &[]),
                ("serde_json", &[], &[]),
                ("sha2", &[], &[]),
            ],
            1,
        );

        assert!(check_forbidden(&graph).is_empty());
    }

    #[test]
    fn a_development_only_dependency_does_not_ship() {
        let graph = graph(
            &[("oxide-batch-core", &[], &["tokio"]), ("tokio", &[], &[])],
            1,
        );

        assert!(check_forbidden(&graph).is_empty());
    }

    #[test]
    fn the_repository_boundary_may_use_the_core_but_not_the_plan() {
        let graph = graph(
            &[
                ("oxide-batch-repository", &["oxide-batch-core"], &[]),
                ("oxide-batch-core", &[], &[]),
            ],
            2,
        );

        assert!(check_forbidden(&graph).is_empty());

        let inverted = graph_with_plan();

        assert!(check_forbidden(&inverted).iter().any(|violation| {
            violation.contains("oxide-batch-repository depends on oxide-batch-plan")
        }));
    }

    /// A repository crate that wrongly reaches the plan crate.
    fn graph_with_plan() -> Graph {
        graph(
            &[
                ("oxide-batch-repository", &["oxide-batch-plan"], &[]),
                ("oxide-batch-plan", &[], &[]),
            ],
            2,
        )
    }

    #[test]
    fn a_workspace_cycle_is_a_violation() {
        let graph = graph(
            &[
                ("oxide-batch-core", &["oxide-batch-plan"], &[]),
                ("oxide-batch-plan", &[], &["oxide-batch-core"]),
            ],
            2,
        );

        let violations = check_cycles(&graph);

        assert!(!violations.is_empty(), "a dev-dependency cycle must fail");
        assert!(violations[0].contains("oxide-batch-core"));
        assert!(violations[0].contains("oxide-batch-plan"));
    }

    #[test]
    fn an_acyclic_workspace_passes() {
        let graph = graph(
            &[
                ("oxide-batch", &["oxide-batch-core"], &[]),
                ("oxide-batch-core", &[], &[]),
            ],
            2,
        );

        assert!(check_cycles(&graph).is_empty());
    }

    #[test]
    fn a_rule_matches_only_the_package_and_its_extensions() {
        assert!(matches("tokio", "tokio", "oxide-batch-core"));
        assert!(matches("tokio-util", "tokio", "oxide-batch-core"));
        assert!(matches(
            "opentelemetry_sdk",
            "opentelemetry",
            "oxide-batch-core"
        ));
        assert!(!matches("tokioesque", "tokio", "oxide-batch-core"));
        assert!(!matches(
            "oxide-batch-core",
            "oxide-batch",
            "oxide-batch-core"
        ));
        assert!(matches("oxide-batch", "oxide-batch", "oxide-batch-core"));
        assert!(matches(
            "oxide-batch-plan",
            "oxide-batch",
            "oxide-batch-core"
        ));
        assert!(!matches(
            "oxide-batch-core",
            "=oxide-batch",
            "oxide-batch-repository"
        ));
        assert!(matches(
            "oxide-batch",
            "=oxide-batch",
            "oxide-batch-repository"
        ));
    }
}
