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
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

/// The disposition the M5 design gate closed, as counts per status.
///
/// The gate reviewed `83` rows and fixed how many sit in each status. A ledger
/// edit that moves a row without revisiting the gate fails here, which is the
/// point: the campaign's denominator cannot drift silently.
///
/// `v0.5.0`'s release promoted `28` of the `29` advertised embedded-kernel
/// rows from `Implemented` to `Verified` per the
/// [conformance matrix's promotion set](../../../docs/compatibility/conformance-matrix.md#m5-disposition-and-promotion-set)
/// and [M5 exit evidence](../../../docs/project/m5-exit-evidence.md);
/// `META-CONTEXT-001` is the one advertised row that stays `Implemented`.
///
/// A later milestone can still move one of the `39` `Planned` rows to
/// `Implemented` on its own evidence without reopening the M5 gate — that row
/// is simply outside the `29`-row advertised set (see `ACCEPTED_STATUSES`
/// below), so only the aggregate counts here move, not the M5 gate's own
/// closed population. `ITEM-STREAM-001` did exactly that in
/// [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144);
/// `TEST-JOB-001`, `TEST-STEP-001`, `TEST-SCOPE-001`, and `TEST-REPO-001` did
/// the same in [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145);
/// `ITEM-COMPOSITE-001` and `ITEM-DECORATOR-001` did the same in
/// [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146);
/// `IO-FLAT-001` did the same in
/// [#147](https://github.com/luceat-lux-vestra/oxide-batch/issues/147);
/// `IO-STRUCTURED-001` did the same (for its M6 JSON/JSONL slice only; XML
/// and Avro remain `Planned` for M13) in
/// [#148](https://github.com/luceat-lux-vestra/oxide-batch/issues/148);
/// `IO-DB-001` did the same (for its M6 `PostgreSQL` cursor/paging/batch/
/// same-resource-enlisted-writer slice only; upsert, stored-procedure,
/// ORM/repository forms, other backends, and generic portability remain
/// `Planned` for M8) in
/// [#149](https://github.com/luceat-lux-vestra/oxide-batch/issues/149);
/// `ITEM-MULTI-001` and `IO-OBJECT-001` did the same (the latter for its M6
/// provider-neutral object-store capability basics slice only; S3/Azure/GCS
/// certification remains `Planned` for M9) in
/// [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150).
const CLOSED_DISPOSITION: &[(&str, usize)] = &[
    ("Verified", 28),
    ("Implemented", 13),
    ("Partial", 13),
    ("Planned", 27),
    ("Unknown", 2),
];

/// The statuses that make a row part of the accepted M0-M4 scope.
///
/// `Verified` rows stay in scope: a regression of a `Verified` row is a
/// compatibility defect per the ledger's row and claim rules, so the
/// campaign keeps exercising them exactly as it did while they were
/// `Implemented`.
const ACCEPTED_STATUSES: &[&str] = &["Verified", "Implemented", "Partial"];

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

    let recognized = ledger
        .with_status("Verified")
        .into_iter()
        .chain(ledger.with_status("Implemented"))
        .collect::<BTreeSet<_>>();
    assert!(
        ledger.advertised()?.is_subset(&recognized),
        "every row in the advertised embedded-kernel set must still carry \
         `Verified` or `Implemented`; the ledger must not silently regress \
         an advertised row. (A row outside the advertised set may also \
         reach `Implemented` on its own milestone's evidence without \
         joining this set — see `ITEM-STREAM-001`.)",
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
    assert!(
        scope
            .rows
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            .is_subset(&accepted),
        "every row the M5 campaign's frozen scope document names must still \
         carry an accepted ledger status; a row is allowed to reach an \
         accepted status after the M5 gate closed (via a later milestone's \
         own evidence, e.g. `ITEM-STREAM-001` in #144) without joining this \
         frozen scope",
    );
    for (id, row) in &scope.rows {
        let ledger_status = ledger.rows.get(id).map(String::as_str);
        // The scope document records the pre-release status a retained
        // report's provenance is bound to; editing it to say `Verified`
        // would change its git blob and invalidate that retained evidence
        // for no behavioral reason. A ledger promotion from `Implemented`
        // to `Verified` is therefore an accepted refinement here: the same
        // scope row, the same scenarios, a stronger released disposition.
        let compatible = match ledger_status {
            Some(status) => {
                status == row.status || (row.status == "Implemented" && status == "Verified")
            }
            None => false,
        };
        assert!(
            compatible,
            "{id} records a status the ledger does not give it (scope: \
             {:?}, ledger: {ledger_status:?})",
            row.status,
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

// ---------------------------------------------------------------------
// Semantic closure. The row-proof denominator and the execution envelope are
// distinct. The accepted scope defines 42 rows and 133 scenario assignments;
// those assignments derive the 30 unique (package, target) test binaries the
// producer selects (`required_targets`). No target outside that envelope is
// selected, but every selected target still runs in full, with no libtest
// filter, so a test inside a selected target that no assigned scenario names
// still runs and can still fail the campaign — selecting a target and
// filtering it down to only its assigned scenarios are not the same thing,
// and this campaign has never done the latter.
//
// The producer used to select every workspace test target `cargo metadata`
// reported, not only the 30 the assignments touch, which pulled workspace
// targets the accepted scope never named at all into the campaign's
// pass/fail gate — including other M5 campaigns' own reconciliation tests,
// some of which read a shared evidence document this campaign's own
// retention step rewrites after the report is produced.
// `the_semantic_closure_covers_what_the_campaign_runs` proves the closure
// covers exactly the envelope the denominator implies, and the tests after
// it lock the specific counterexample review found: `m5_campaign_record`,
// `docs/project/m5-campaign-evidence.md`, and another campaign's fixtures.
// ---------------------------------------------------------------------

/// Every other M5 campaign's own reconciliation/contract test, plus this
/// campaign's own. None of them is named by any accepted-scope scenario.
const GOVERNANCE_TARGETS: &[&str] = &[
    "m5_campaign_record",
    "m5_cancellation_campaign",
    "m5_conformance_campaign",
    "m5_crash_restore_campaign",
    "m5_performance_campaign",
    "m5_resource_bounds_campaign",
    "m5_security_campaign",
    "m5_soak_campaign",
    "m5_upgrade_campaign",
];

#[test]
fn the_semantic_closure_covers_what_the_campaign_runs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let paths = closure_paths()?;

    // The denominator-derived target set: every (package, target) the scope
    // document actually names. Each one must resolve to a file the closure
    // covers, or a change to that file could change the campaign's result
    // without invalidating the evidence it produced.
    let mut required_targets = BTreeSet::new();
    for row in scope.rows.values() {
        for scenario in &row.scenarios {
            required_targets.insert((scenario.package.clone(), scenario.target.clone()));
        }
    }
    assert!(
        !required_targets.is_empty(),
        "the accepted scope named no required target, so this test checks nothing",
    );

    for (package, target) in &required_targets {
        let relative = format!("crates/{package}/tests/{target}.rs");
        assert!(
            covered(&paths, &relative),
            "{relative} backs an accepted-scope scenario in package {package}, and is not \
             covered by any path in the campaign's semantic closure",
        );
    }

    // The governance targets a change to the closure must never bring in:
    // none of them names an accepted scenario, so none of them should be
    // resolvable as a required target, and — separately — none of the files
    // they are known to read dynamically should appear in the closure. Both
    // conditions held for the workspace enumeration this replaced only by
    // accident (the old closure listed the whole `tests` directory, so it
    // technically covered `m5_campaign_record.rs`'s own source, while the
    // producer still ran it and gated on its exit status).
    for governance in GOVERNANCE_TARGETS {
        assert!(
            !required_targets.contains(&("oxide-batch".to_owned(), (*governance).to_owned())),
            "{governance} is a governance test, not an accepted-scope scenario, and must not be \
             part of the campaign's required-target set",
        );
    }

    for excluded in [
        // The retention-time document `m5_campaign_record` reads: rewritten
        // with a report's own provenance after the report is produced, so a
        // closure that covered it could never converge.
        "docs/project/m5-campaign-evidence.md",
        // Another campaign's fixtures, also read by `m5_campaign_record`.
        // Conformance's own correctness must not depend on the soak
        // provenance verifier happening to catch drift here separately.
        "tests/fixtures/soak/campaign-scope.json",
        "tests/fixtures/soak/campaign-semantics.json",
    ] {
        assert!(
            !paths.iter().any(|path| path == excluded),
            "{excluded} must not be in the conformance closure: it is read only by a governance \
             test the campaign does not run, and including it would either create a retention-time \
             self-reference or bind conformance evidence to another campaign's fixtures",
        );
    }

    for required in [
        // Framework and adapter source every accepted scenario runs against.
        "crates/oxide-batch/src",
        "crates/oxide-batch-cli/src",
        // The denominator, which also determines the required-target set.
        "tests/fixtures/conformance/accepted-scope.json",
        // The verifier, whose verdicts are part of the result.
        "xtask/src/conformance.rs",
        "xtask/src/evidence.rs",
        // The resolved dependency graph, and the toolchain the suite is
        // built with.
        "Cargo.lock",
        "rust-toolchain.toml",
        // How the dedicated workflow runs it.
        ".github/workflows/m5-conformance.yml",
        "tests/fixtures/conformance/execution-contract.json",
        "tests/fixtures/conformance/run-ci-campaign.sh",
        "tests/fixtures/conformance/verify-ci-contract.sh",
    ] {
        assert!(
            paths.iter().any(|path| path == required),
            "{required} is not in the campaign's semantic closure, so a change to it would leave \
             retained evidence looking valid when it is evidence of something else",
        );
    }

    assert!(
        !paths.iter().any(|path| path == ".github/workflows/ci.yml"),
        "ci.yml is unrelated to the dedicated conformance campaign and must not invalidate its \
         evidence",
    );

    for path in &paths {
        assert!(
            workspace_root().join(path).exists(),
            "{path} is declared as campaign semantics and does not exist, so the producer cannot \
             record its object identity",
        );
    }
    Ok(())
}

#[test]
fn the_evidence_record_mutation_counterexample_stays_closed() -> Result<(), Box<dyn Error>> {
    // Regression lock for the exact counterexample review found: mutating
    // `docs/project/m5-campaign-evidence.md` can make `m5_campaign_record`
    // fail, and that must never be able to change what the conformance
    // campaign reports. Proven two ways: the document is not a semantic
    // input of any required target (checked structurally, since actually
    // running the suite needs PostgreSQL), and `m5_campaign_record` itself is
    // outside the required-target set, so ordinary Rust CI — not this
    // campaign — is what runs and fails on it.
    let paths = closure_paths()?;
    assert!(
        !paths
            .iter()
            .any(|path| path == "docs/project/m5-campaign-evidence.md"),
        "the evidence record is not a conformance semantic input; a mutation to it must be caught \
         only by cargo test running m5_campaign_record directly, never by this campaign",
    );

    // m5_campaign_record is a real, existing workspace test — the workspace-
    // wide scan finds its test functions same as any other target's — so its
    // absence from the campaign below is deliberate narrowing, not an
    // accident of the file not existing or declaring no tests.
    let declared = declared_tests(&workspace_root())?;
    assert!(
        declared
            .iter()
            .any(|(package, target, _)| package.as_str() == "oxide-batch"
                && target.as_str() == "m5_campaign_record"),
        "m5_campaign_record declares no test the workspace-wide scan can find, so this \
         regression lock is not exercising a real target",
    );

    let scope = Scope::read()?;
    let required_targets = scope
        .rows
        .values()
        .flat_map(|row| &row.scenarios)
        .map(|scenario| (scenario.package.as_str(), scenario.target.as_str()))
        .collect::<BTreeSet<_>>();
    assert!(
        !required_targets.contains(&("oxide-batch", "m5_campaign_record")),
        "m5_campaign_record must never become a required target: it is a governance test over a \
         retention-time document, not an accepted-scope scenario",
    );

    Ok(())
}

/// Returns every path the campaign's semantic closure declares.
fn closure_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let closure: Value = serde_json::from_str(&read_document(
        "tests/fixtures/conformance/campaign-semantics.json",
    )?)?;
    Ok(closure
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| Failure("the closure declares no categories".to_owned()))?
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>())
}

/// Reports whether a repository-relative path is covered by the closure:
/// named exactly, or nested under a closure path that names a directory.
fn covered(paths: &[String], candidate: &str) -> bool {
    paths
        .iter()
        .any(|path| path == candidate || candidate.starts_with(&format!("{path}/")))
}

// ---------------------------------------------------------------------
// Canonical contract accuracy. `execution-contract.json` is prose and
// structured fields beside the exact-identity check above, and prose can go
// stale in a way `verify-ci-contract.sh` never catches: an earlier revision
// of this contract still described the pre-narrowing whole-workspace
// enumeration ("every workspace test target cargo metadata reports (66 at
// the time this contract was written)") for months after the producer no
// longer worked that way. These tests hold the contract's structured claims
// to the real producer source, not to themselves: each assertion below reads
// `xtask/src/conformance.rs`'s actual text for the specific code shape that
// would have to exist for the claim to be true, so a contract edited to
// re-describe whole-workspace semantics without a matching producer change
// fails here, and a producer change without a matching contract update fails
// here too.
// ---------------------------------------------------------------------

#[test]
fn the_canonical_contract_describes_the_real_producer_behavior() -> Result<(), Box<dyn Error>> {
    let contract: Value = serde_json::from_str(&read_document(
        "tests/fixtures/conformance/execution-contract.json",
    )?)?;
    let producer = read_document("xtask/src/conformance.rs")?;

    let selection = contract
        .get("target_selection")
        .ok_or_else(|| Failure("the contract declares no target_selection".to_owned()))?;
    assert_eq!(
        selection.get("selected_targets_run_in_full"),
        Some(&Value::Bool(true)),
        "the contract must claim selected targets run in full: that is what the producer does",
    );
    assert_eq!(
        selection.get("unselected_workspace_targets_excluded"),
        Some(&Value::Bool(true)),
        "the contract must claim unselected workspace targets are excluded: that is what \
         required_targets narrows to",
    );

    let pass_condition = contract
        .get("pass_condition")
        .ok_or_else(|| Failure("the contract declares no pass_condition".to_owned()))?;
    for key in [
        "assigned_scenarios_must_report_ok",
        "selected_target_exit_must_succeed",
        "workspace_documentation_tests_must_pass",
    ] {
        assert_eq!(
            pass_condition.get(key),
            Some(&Value::Bool(true)),
            "the contract's pass_condition.{key} must be true, matching the producer's real \
             reconciliation logic",
        );
    }

    // selected_targets_run_in_full / selected_target_exit_must_succeed: a
    // selected target is run without a libtest filter, so every test in it
    // runs, and the process's own exit status — not only its assigned
    // scenarios' outcomes — becomes a campaign violation on failure.
    assert!(
        producer.contains("filters: &[],"),
        "the contract claims selected targets run in full (unfiltered), but run_suite no longer \
         passes an empty filter list to each target invocation",
    );
    assert!(
        producer.contains("suite.failed_targets.push(format!("),
        "the contract claims a selected target's own exit failure gates the campaign, but \
         run_suite no longer records failed targets separately from scenario outcomes",
    );
    assert!(
        producer.contains("let mut violations = suite.failed_targets.clone();"),
        "the contract claims a selected target's exit failure fails the campaign, but reconcile \
         no longer folds failed_targets into the violation list",
    );

    // assigned_scenarios_must_report_ok: reconcile still requires the exact
    // `ok` outcome for each of the 133 assignments.
    assert!(
        producer.contains("Some(\"ok\") => {}"),
        "the contract claims every assigned scenario must report ok, but reconcile no longer \
         requires that exact outcome",
    );

    // workspace_documentation_tests_must_pass: run unconditionally, not
    // gated on the accepted scope or the execution envelope.
    assert!(
        producer.contains("suite.documentation = run_documentation_tests(root)?;"),
        "the contract claims the workspace documentation tests are a required, separate \
         obligation, but run_suite no longer calls run_documentation_tests unconditionally",
    );

    Ok(())
}

#[test]
fn no_stale_whole_workspace_language_remains_in_the_contract_or_workflow()
-> Result<(), Box<dyn Error>> {
    let contract = read_document("tests/fixtures/conformance/execution-contract.json")?;
    let workflow = read_document(".github/workflows/m5-conformance.yml")?;

    for forbidden in [
        "every workspace test target",
        "unrelated target",
        "133 scenarios and nothing else",
        "gates on nothing else",
    ] {
        assert!(
            !contract.contains(forbidden),
            "{forbidden:?} in execution-contract.json describes the pre-narrowing producer, not \
             the current one, which selects a 30-target envelope derived from the accepted scope",
        );
        assert!(
            !workflow.contains(forbidden),
            "{forbidden:?} in the dedicated workflow describes the pre-narrowing producer, not \
             the current one",
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------
// Contract-check exactness. `verify-ci-contract.sh` binds two files —
// `.github/workflows/m5-conformance.yml` and `run-ci-campaign.sh` — by exact
// git blob identity, not by the literal presence checks alone. These tests
// drive the real script against an isolated sandbox copy of those files, so a
// mutation proves the checker's actual behaviour rather than one helper's
// return value, and never touches the repository working tree.
// ---------------------------------------------------------------------

#[test]
fn contract_check_passes_on_the_canonical_workflow_and_script() -> Result<(), Box<dyn Error>> {
    assert!(run_conformance_contract_check(|_sandbox| Ok(()))?);
    Ok(())
}

#[test]
fn contract_check_fails_on_an_added_trigger() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-conformance.yml"),
            "  workflow_dispatch:\n",
            "  schedule:\n    - cron: '0 0 * * *'\n",
        )
    })?;
    assert!(
        !passed,
        "an added trigger must fail the contract check even though every expected trigger is \
         still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_job_level_write_permission() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-conformance.yml"),
            "runs-on: ubuntu-24.04\n",
            "    permissions:\n      contents: write\n",
        )
    })?;
    assert!(
        !passed,
        "a job-level permission override must fail even though the workflow-level contents: \
         read line is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_widened_matrix() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-conformance.yml"),
            "postgres: [\"15\", \"18\"]\n",
            "        include:\n          - postgres: \"16\"\n",
        )
    })?;
    assert!(
        !passed,
        "an additional matrix execution point must fail even though the literal \
         postgres: [\"15\", \"18\"] declaration is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_narrowed_matrix() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-conformance.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace("postgres: [\"15\", \"18\"]", "postgres: [\"15\"]");
        assert_ne!(
            source, mutated,
            "the matrix literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a matrix reduced to one point must fail even though the literal declaration was \
         rewritten consistently",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_timeout() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-conformance.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace("timeout-minutes: 55", "timeout-minutes: 5");
        assert_ne!(
            source, mutated,
            "the timeout literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed timeout must fail the contract check even though every other literal is \
         unchanged",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_report_or_artifact_path() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        let workflow = sandbox.join(".github/workflows/m5-conformance.yml");
        let source = fs::read_to_string(&workflow)?;
        let mutated = source.replace(
            "path: target/m5-campaigns/conformance-campaign.json",
            "path: target/m5-campaigns/conformance-campaign-renamed.json",
        );
        assert_ne!(
            source, mutated,
            "the report path literal was not found to mutate"
        );
        fs::write(&workflow, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed retained-report path must fail even though the producer command is unchanged",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_an_additional_producer_command() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        insert_after(
            &sandbox.join(".github/workflows/m5-conformance.yml"),
            "run: ./tests/fixtures/conformance/run-ci-campaign.sh ${{ matrix.postgres }}\n",
            "      - name: Run something else\n        run: echo \"an extra producer step\"\n",
        )
    })?;
    assert!(
        !passed,
        "an additional step after the campaign producer must fail even though the declared \
         producer command is still present unmodified",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_an_appended_script_command() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        append_line(
            &sandbox.join("tests/fixtures/conformance/run-ci-campaign.sh"),
            "echo \"extra command\"",
        )
    })?;
    assert!(
        !passed,
        "an appended command must fail even though the expected cargo command is still present",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_changed_producer_command() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        let script = sandbox.join("tests/fixtures/conformance/run-ci-campaign.sh");
        let source = fs::read_to_string(&script)?;
        let mutated = source.replace(
            "cargo run --package oxide-batch-xtask -- conformance",
            "cargo run --package oxide-batch-xtask -- conformance --extra-flag",
        );
        assert_ne!(
            source, mutated,
            "the producer command was not found to mutate"
        );
        fs::write(&script, mutated)?;
        Ok(())
    })?;
    assert!(
        !passed,
        "a changed producer command must fail even though it still contains the declared command \
         as a substring",
    );
    Ok(())
}

#[test]
fn contract_check_fails_on_a_harmless_comment_byte() -> Result<(), Box<dyn Error>> {
    let passed = run_conformance_contract_check(|sandbox| {
        append_line(
            &sandbox.join(".github/workflows/m5-conformance.yml"),
            "# harmless comment",
        )
    })?;
    assert!(
        !passed,
        "exact git blob identity, not heuristic literal parsing, is the retained-evidence \
         boundary: even a harmless trailing comment must fail",
    );
    Ok(())
}

/// Copies the real workflow, script, and contract into an isolated sandbox,
/// applies `mutate` to that sandbox, then runs the real `verify-ci-contract.sh`
/// against the (possibly mutated) copy and reports whether it exited zero.
///
/// The sandbox mirrors the relative layout the script assumes
/// (`tests/fixtures/conformance/execution-contract.json` beside its working
/// directory), so the check runs exactly as CI runs it, and `mutate` can never
/// affect the real repository files.
fn run_conformance_contract_check(
    mutate: impl FnOnce(&Path) -> Result<(), Box<dyn Error>>,
) -> Result<bool, Box<dyn Error>> {
    let root = workspace_root();
    let sandbox = Sandbox::new("conformance-contract-check")?;

    let workflow_dir = sandbox.path().join(".github/workflows");
    fs::create_dir_all(&workflow_dir)?;
    fs::copy(
        root.join(".github/workflows/m5-conformance.yml"),
        workflow_dir.join("m5-conformance.yml"),
    )?;

    let fixture_dir = sandbox.path().join("tests/fixtures/conformance");
    fs::create_dir_all(&fixture_dir)?;
    for name in [
        "execution-contract.json",
        "run-ci-campaign.sh",
        "verify-ci-contract.sh",
    ] {
        fs::copy(
            root.join("tests/fixtures/conformance").join(name),
            fixture_dir.join(name),
        )?;
    }
    let checker = fixture_dir.join("verify-ci-contract.sh");
    let mut permissions = fs::metadata(&checker)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&checker, permissions)?;

    mutate(sandbox.path())?;

    exec_contract_checker(
        &checker,
        ".github/workflows/m5-conformance.yml",
        sandbox.path(),
    )
}

/// Runs a freshly copied and `chmod`-ed contract-check script, retrying on a
/// transient `ExecutableFileBusy`.
///
/// Immediately exec'ing a file this test process just wrote and `chmod`ed
/// can race the kernel's release of the write mapping under heavy parallel
/// `cargo test` fork/exec load, surfacing as `ETXTBSY` even though the file
/// is complete and correctly permissioned. Each sandbox path is unique per
/// test, so this is never a real conflict — retry briefly before treating it
/// as a genuine failure.
fn exec_contract_checker(checker: &Path, arg: &str, cwd: &Path) -> Result<bool, Box<dyn Error>> {
    let mut attempt = 0u32;
    loop {
        match Command::new(checker).arg(arg).current_dir(cwd).status() {
            Ok(status) => return Ok(status.success()),
            Err(err) if err.kind() == io::ErrorKind::ExecutableFileBusy && attempt < 5 => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(20 * u64::from(attempt)));
            }
            Err(err) => return Err(Box::new(err)),
        }
    }
}

/// Inserts `insertion` immediately after the first occurrence of `anchor`.
fn insert_after(path: &Path, anchor: &str, insertion: &str) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    let position = contents.find(anchor).ok_or_else(|| {
        Box::new(Failure(format!(
            "no {anchor:?} anchor found in {}",
            path.display()
        ))) as Box<dyn Error>
    })?;
    let insert_at = position + anchor.len();
    let mut mutated = String::with_capacity(contents.len() + insertion.len());
    mutated.push_str(&contents[..insert_at]);
    mutated.push_str(insertion);
    mutated.push_str(&contents[insert_at..]);
    fs::write(path, mutated)?;
    Ok(())
}

/// Appends one line to a file.
fn append_line(path: &Path, line: &str) -> Result<(), Box<dyn Error>> {
    let mut contents = fs::read_to_string(path)?;
    contents.push('\n');
    contents.push_str(line);
    contents.push('\n');
    fs::write(path, contents)?;
    Ok(())
}

/// A uniquely named temporary directory, removed when it goes out of scope
/// regardless of how the test exits.
struct Sandbox(PathBuf);

impl Sandbox {
    fn new(label: &str) -> Result<Self, Box<dyn Error>> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "oxide-batch-{label}-{}-{unique}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Reads one canonical document from the workspace.
fn read_document(path: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(workspace_root().join(path))?)
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
