//! Scope reconciliation for the M5 conformance campaign.
//!
//! The [design-gate evidence](../../../docs/project/m5-design-gate-evidence.md)
//! names `full_embedded_conformance_suite_passes_on_the_accepted_scope` for the
//! evidence-campaign workstream. That scenario has two halves, and only one of
//! them can run inside a test process:
//!
//! - **which rows the campaign owes, and which scenario proves each one.** That
//!   is a reconciliation between the [ledger](../../../docs/compatibility/conformance-matrix.md),
//!   the committed scope document, and the tests this workspace declares. It
//!   runs here, in an ordinary `cargo test`, so drift is caught in review
//!   rather than in the campaign.
//! - **whether the suite passes.** A test cannot observe the result of the
//!   binaries it is not running in, and a database-backed scenario returns
//!   green without a database because it skips. That half is `cargo xtask
//!   conformance`, which runs the suite, requires the fixtures, and writes the
//!   retained report.
//!
//! Splitting it this way is deliberate. A single in-process test that claimed
//! the whole scenario would report success on a host with no database, which
//! is exactly the forged pass the campaign exists to rule out.
//!
//! The scope document is `tests/fixtures/conformance/accepted-scope.json` at
//! the workspace root. Both halves read it, so the row set, the scenario
//! names, and the fixture each scenario needs are stated once.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// The disposition the M5 design gate closed, as counts per status.
///
/// The gate reviewed `83` rows and fixed how many sit in each status. A ledger
/// edit that moves a row without revisiting the gate fails here, which is the
/// point: the campaign's denominator cannot drift silently.
const CLOSED_DISPOSITION: &[(&str, usize)] = &[
    ("Verified", 0),
    ("Implemented", 29),
    ("Partial", 13),
    ("Planned", 39),
    ("Unknown", 2),
];

/// The statuses that make a row part of the accepted M0-M4 scope.
const ACCEPTED_STATUSES: &[&str] = &["Implemented", "Partial"];

#[test]
fn accepted_scope_matches_the_ledger_disposition() -> Result<(), Box<dyn Error>> {
    let ledger = Ledger::read()?;

    let mut population: BTreeMap<&str, usize> = CLOSED_DISPOSITION
        .iter()
        .map(|(status, _)| (*status, 0))
        .collect();
    for status in ledger.rows.values() {
        let rows = population
            .get_mut(status.as_str())
            .ok_or_else(|| Failure(format!("ledger row carries unknown status {status}")))?;
        *rows += 1;
    }
    assert_eq!(
        population.into_iter().collect::<Vec<_>>(),
        CLOSED_DISPOSITION
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect::<Vec<_>>(),
        "the ledger disposition must stay the one the M5 design gate closed",
    );

    assert_eq!(
        ledger.with_status("Implemented"),
        ledger.advertised()?,
        "the advertised embedded-kernel set and the `Implemented` rows are two \
         statements of one fact, and the ledger must not disagree with itself",
    );
    assert_eq!(
        ledger.with_status("Partial"),
        ledger.published_partial()?,
        "the rows published as preview limitations are exactly the `Partial` \
         rows",
    );

    let scope = Scope::read()?;
    let accepted = ACCEPTED_STATUSES
        .iter()
        .flat_map(|status| ledger.with_status(status))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        scope.rows.keys().cloned().collect::<BTreeSet<_>>(),
        accepted,
        "the campaign runs the accepted M0-M4 scope, so its scope document \
         covers every `Implemented` and `Partial` row and no other",
    );
    for (id, row) in &scope.rows {
        assert_eq!(
            Some(&row.status),
            ledger.rows.get(id),
            "{id} records a status the ledger does not give it",
        );
    }

    Ok(())
}

#[test]
fn every_accepted_row_names_a_declared_conformance_scenario() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let declared = declared_tests(&workspace_root())?;

    for (id, row) in &scope.rows {
        assert!(
            row.scenarios
                .iter()
                .any(|scenario| scenario.class == "conformance"),
            "{id} has no conformance-class scenario, so nothing observes the \
             behavior the ledger row describes",
        );

        let mut seen = BTreeSet::new();
        for scenario in &row.scenarios {
            assert!(
                seen.insert((scenario.target.as_str(), scenario.name.as_str())),
                "{id} names {}::{} twice",
                scenario.target,
                scenario.name,
            );
            assert!(
                scope.classes.contains(&scenario.class),
                "{id} gives {} an evidence class the ledger does not use: {}",
                scenario.name,
                scenario.class,
            );
            assert!(
                scope.fixtures.contains(&scenario.fixture),
                "{id} gives {} an undeclared fixture: {}",
                scenario.name,
                scenario.fixture,
            );

            let key = (
                scenario.package.clone(),
                scenario.target.clone(),
                leaf(&scenario.name).to_owned(),
            );
            assert!(
                declared.contains(&key),
                "{id} names {}::{} in package {}, which declares no such test",
                scenario.target,
                scenario.name,
                scenario.package,
            );
        }
    }

    Ok(())
}

/// The ledger rows and the prose sets that must agree with them.
struct Ledger {
    /// Row identifier to status.
    rows: BTreeMap<String, String>,
    /// The rendered document, kept for the prose sets.
    source: String,
}

impl Ledger {
    /// Reads and parses the canonical feature ledger.
    fn read() -> Result<Self, Box<dyn Error>> {
        let source = fs::read_to_string(
            workspace_root()
                .join("docs")
                .join("compatibility")
                .join("conformance-matrix.md"),
        )?;

        let mut rows = BTreeMap::new();
        for line in source.lines() {
            let Some(cells) = table_row(line) else {
                continue;
            };
            let (Some(id), Some(status)) = (cells.first(), cells.get(6)) else {
                continue;
            };
            if !is_row_id(id) {
                continue;
            }
            if rows
                .insert((*id).to_owned(), (*status).to_owned())
                .is_some()
            {
                return Err(Box::new(Failure(format!("ledger declares {id} twice"))));
            }
        }

        Ok(Self { rows, source })
    }

    /// Returns every row identifier carrying one status.
    fn with_status(&self, status: &str) -> BTreeSet<String> {
        self.rows
            .iter()
            .filter(|(_, row_status)| row_status.as_str() == status)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns the advertised embedded-kernel set the ledger publishes.
    fn advertised(&self) -> Result<BTreeSet<String>, Box<dyn Error>> {
        self.listed("**Advertised embedded-kernel set.**")
    }

    /// Returns the rows the ledger publishes as preview limitations.
    fn published_partial(&self) -> Result<BTreeSet<String>, Box<dyn Error>> {
        self.listed("**Rows that stay `Partial`.**")
    }

    /// Collects the row identifiers a marked prose list names.
    ///
    /// A marker either introduces its list in the same paragraph or in the one
    /// that follows, and the ledger does both. Collection therefore starts at
    /// the marker and stops at the end of the first paragraph that names a
    /// row, which keeps the prose after the list out of the set.
    fn listed(&self, marker: &str) -> Result<BTreeSet<String>, Box<dyn Error>> {
        let mut found = BTreeSet::new();
        let mut inside = false;

        for line in self.source.lines() {
            if line.starts_with(marker) {
                inside = true;
            }
            if !inside {
                continue;
            }
            if line.trim().is_empty() {
                if found.is_empty() {
                    continue;
                }
                return Ok(found);
            }
            found.extend(
                line.split('`')
                    .skip(1)
                    .step_by(2)
                    .filter(|token| is_row_id(token))
                    .map(str::to_owned),
            );
        }

        if found.is_empty() {
            return Err(Box::new(Failure(format!(
                "the ledger has no list under {marker}"
            ))));
        }
        Ok(found)
    }
}

/// The committed campaign scope document.
struct Scope {
    /// Row identifier to the row the campaign runs for it.
    rows: BTreeMap<String, ScopeRow>,
    /// The evidence classes the document declares.
    classes: BTreeSet<String>,
    /// The fixture names the document declares.
    fixtures: BTreeSet<String>,
}

/// One accepted row and the scenarios that prove it.
struct ScopeRow {
    /// The ledger status the document records for the row.
    status: String,
    /// The executable scenarios assigned to the row.
    scenarios: Vec<ScopeScenario>,
}

/// One executable scenario the campaign runs.
struct ScopeScenario {
    /// The workspace package that declares the test.
    package: String,
    /// The test target that contains it.
    target: String,
    /// The test path libtest reports, including any module prefix.
    name: String,
    /// The ledger evidence class the scenario contributes.
    class: String,
    /// The fixture the scenario needs in order to observe anything.
    fixture: String,
}

impl Scope {
    /// Reads and parses the committed scope document.
    fn read() -> Result<Self, Box<dyn Error>> {
        let document: Value = serde_json::from_str(&fs::read_to_string(scope_path())?)?;

        let classes = keys(&document, "classes")?;
        let fixtures = keys(&document, "fixtures")?;

        let mut rows = BTreeMap::new();
        for row in document
            .get("rows")
            .and_then(Value::as_array)
            .ok_or_else(|| Failure("the scope document has no rows".to_owned()))?
        {
            let id = field(row, "id")?;
            let mut scenarios = Vec::new();
            for scenario in row
                .get("scenarios")
                .and_then(Value::as_array)
                .ok_or_else(|| Failure(format!("{id} lists no scenario")))?
            {
                scenarios.push(ScopeScenario {
                    package: field(scenario, "package")?,
                    target: field(scenario, "target")?,
                    name: field(scenario, "name")?,
                    class: field(scenario, "class")?,
                    fixture: field(scenario, "fixture")?,
                });
            }
            if scenarios.is_empty() {
                return Err(Box::new(Failure(format!("{id} lists no scenario"))));
            }

            let status = field(row, "status")?;
            if rows
                .insert(id.clone(), ScopeRow { status, scenarios })
                .is_some()
            {
                return Err(Box::new(Failure(format!(
                    "the scope document declares {id} twice"
                ))));
            }
        }

        Ok(Self {
            rows,
            classes,
            fixtures,
        })
    }
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Returns the committed scope document.
fn scope_path() -> PathBuf {
    workspace_root()
        .join("tests")
        .join("fixtures")
        .join("conformance")
        .join("accepted-scope.json")
}

/// Reads one required string field.
fn field(value: &Value, name: &str) -> Result<String, Box<dyn Error>> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Box::new(Failure(format!("a scope entry has no {name}"))) as Box<dyn Error>)
}

/// Reads the key set of one required object field.
fn keys(document: &Value, name: &str) -> Result<BTreeSet<String>, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_object)
        .map(|object| object.keys().cloned().collect())
        .ok_or_else(|| {
            Box::new(Failure(format!("the scope document has no {name}"))) as Box<dyn Error>
        })
}

/// Splits one Markdown table row into trimmed cells.
fn table_row(line: &str) -> Option<Vec<&str>> {
    let line = line.trim();
    if !line.starts_with('|') || !line.ends_with('|') {
        return None;
    }
    Some(
        line.trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect::<Vec<_>>(),
    )
}

/// Reports whether a token has the shape of a ledger row identifier.
fn is_row_id(token: &str) -> bool {
    let Some((prefix, ordinal)) = token.rsplit_once('-') else {
        return false;
    };
    ordinal.len() == 3
        && ordinal.bytes().all(|byte| byte.is_ascii_digit())
        && !prefix.is_empty()
        && prefix
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

/// Returns the final segment of a libtest path.
fn leaf(name: &str) -> &str {
    name.rsplit("::").next().unwrap_or(name)
}

/// One declared test, identified the way the scope document names it.
type DeclaredTest = (String, String, String);

/// Collects every test this workspace declares, as package, target, and name.
///
/// A target's sources are its root file plus anything under a directory of the
/// same name, which is where the nested `cases` modules live. Shared support
/// modules are reached from several targets and are deliberately not searched:
/// they declare no scenario.
fn declared_tests(root: &Path) -> Result<BTreeSet<DeclaredTest>, Box<dyn Error>> {
    let mut declared = BTreeSet::new();

    for package in fs::read_dir(root.join("crates"))? {
        let package = package?.path();
        let Some(package_name) = package.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let tests = package.join("tests");
        if !tests.is_dir() {
            continue;
        }

        for entry in fs::read_dir(&tests)? {
            let entry = entry?.path();
            let Some(stem) = entry.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            if entry.extension().and_then(|extension| extension.to_str()) != Some("rs") {
                continue;
            }

            let mut sources = vec![entry.clone()];
            collect_sources(&tests.join(stem), &mut sources)?;
            for source in sources {
                for name in tests_in(&fs::read_to_string(&source)?) {
                    declared.insert((package_name.to_owned(), stem.to_owned(), name));
                }
            }
        }
    }

    Ok(declared)
}

/// Appends every Rust source under one directory.
fn collect_sources(directory: &Path, sources: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    if !directory.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?.path();
        if entry.is_dir() {
            collect_sources(&entry, sources)?;
        } else if entry.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            sources.push(entry);
        }
    }
    Ok(())
}

/// Returns the name of every attributed test function in one source file.
///
/// A test attribute and its function are separated by however many further
/// attributes the function carries, so the scan stays open until the next
/// signature rather than looking a fixed distance ahead.
fn tests_in(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut attributed = false;

    for line in source.lines().map(str::trim) {
        if line == "#[test]" || line == "#[tokio::test]" || line.starts_with("#[tokio::test(") {
            attributed = true;
            continue;
        }
        if !attributed {
            continue;
        }

        let signature = line
            .strip_prefix("async fn ")
            .or_else(|| line.strip_prefix("fn "));
        if let Some(signature) = signature {
            let name = signature
                .split(['(', '<'])
                .next()
                .unwrap_or_default()
                .trim()
                .to_owned();
            if !name.is_empty() {
                names.push(name);
            }
            attributed = false;
        }
    }

    names
}

/// A reconciliation input the campaign could not read.
#[derive(Debug)]
struct Failure(String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Failure {}
