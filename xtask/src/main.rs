//! Repository development tasks.

mod cancellation;
mod conformance;
mod crash_restore;
mod deps;
mod evidence;
mod gate_b;
mod gate_h;
mod performance;
mod reconciliation;
mod release_crates;
mod resource_bounds;
mod security;
mod soak;
mod soak_evidence;
mod suite;
mod surface;
mod upgrade;

use std::env;
use std::ffi::OsStr;
use std::process::{Command, ExitCode};

struct Task<'a> {
    label: &'a str,
    program: &'a str,
    args: &'a [&'a str],
    rustdoc_warnings: bool,
}

/// Formatting, lint, test, and documentation tasks.
const QUALITY: &[Task<'static>] = &[
    Task {
        label: "format",
        program: "cargo",
        args: &["fmt", "--all", "--", "--check"],
        rustdoc_warnings: false,
    },
    Task {
        label: "clippy",
        program: "cargo",
        args: &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
            // Mirrors the required `Rust / quality` CI job exactly
            // (`.github/workflows/ci.yml`), including the two narrow,
            // audit-shape exceptions that job's own follow-up step verifies
            // stay confined to `reconcile_exact_cell` and
            // `reconcile_search_bounded_construction`.
            "-A",
            "clippy::too_many_arguments",
            "-A",
            "clippy::too_many_lines",
        ],
        rustdoc_warnings: false,
    },
    Task {
        label: "tests",
        program: "cargo",
        args: &["test", "--workspace", "--all-features"],
        rustdoc_warnings: false,
    },
    Task {
        label: "documentation",
        program: "cargo",
        args: &["doc", "--workspace", "--all-features", "--no-deps"],
        rustdoc_warnings: true,
    },
];

/// Local tool versions the development environment requires.
const DOCTOR: &[Task<'static>] = &[
    Task {
        label: "Rust compiler",
        program: "rustc",
        args: &["--version"],
        rustdoc_warnings: false,
    },
    Task {
        label: "Cargo",
        program: "cargo",
        args: &["--version"],
        rustdoc_warnings: false,
    },
    Task {
        label: "Git",
        program: "git",
        args: &["--version"],
        rustdoc_warnings: false,
    },
];

/// Packaging evidence for every publishable workspace crate.
///
/// The workspace dry run orders crates by dependency and resolves unpublished
/// members through a temporary local registry, so it succeeds before the first
/// upload of an extracted crate.
const PACKAGE: &[Task<'static>] = &[
    Task {
        label: "package contents",
        program: "cargo",
        args: &["package", "--workspace", "--list"],
        rustdoc_warnings: false,
    },
    Task {
        label: "publish dry run",
        program: "cargo",
        args: &["publish", "--workspace", "--locked", "--dry-run"],
        rustdoc_warnings: false,
    },
];

fn main() -> ExitCode {
    let mut args = env::args();
    let _program = args.next();

    let succeeded = match args.next().as_deref() {
        Some("cancellation") => run_cancellation_campaign(),
        Some("check") => {
            run_all(QUALITY)
                && run_dependency_check()
                && run_surface_check()
                && run_release_crates_check()
                && run_evidence_check()
                && run_reconciliation_check()
        }
        Some("conformance") => run_conformance_campaign(),
        Some("crash-restore") => run_crash_restore_campaign(),
        Some("deps") => run_dependency_check(),
        Some("doctor") => run_all(DOCTOR),
        Some("evidence") => run_evidence_check(),
        Some("gate-b") => run_gate_b_campaign(),
        Some("gate-h") => run_gate_h_campaign(),
        Some("package") => run_all(PACKAGE),
        Some("performance") => run_performance_campaign(),
        Some("reconciliation") => run_reconciliation_check(),
        Some("release-crates") => run_release_crates_check(),
        Some("resource-bounds") => run_resource_bound_campaign(),
        Some("security") => run_security_campaign(),
        Some("soak") => run_soak_campaign(),
        Some("surface") => run_surface_check(),
        Some("upgrade") => run_upgrade_campaign(),
        Some(command) => {
            eprintln!("unknown xtask command: {command}");
            usage();
            false
        }
        None => {
            usage();
            false
        }
    };

    if succeeded {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Reports whether the cancellation campaign measured and proved what P-014
/// owes.
fn run_cancellation_campaign() -> bool {
    eprintln!("==> PostgreSQL cancellation campaign");

    match cancellation::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "the accepted operator path stopped intake and reached a durable terminal \
                     status, both latencies P-014 names were measured and ordered, every declared \
                     deadline reported the tasks it still owned, and a cancelled attempt \
                     restarted without re-running committed work"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign scope in \
                 tests/fixtures/cancellation/campaign-scope.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the cancellation campaign: {error}");
            false
        }
    }
}

/// Reports whether the workspace satisfies the crate-extraction boundaries.
fn run_dependency_check() -> bool {
    eprintln!("==> workspace boundaries");

    match deps::check() {
        Ok(violations) if violations.is_empty() => {
            eprintln!("workspace boundaries hold");
            true
        }
        Ok(violations) => {
            for violation in &violations {
                eprintln!("boundary violation: {violation}");
            }
            eprintln!(
                "see docs/architecture/crate-extraction.md for the authorized \
                 boundaries"
            );
            false
        }
        Err(error) => {
            eprintln!("could not check workspace boundaries: {error}");
            false
        }
    }
}

/// Reports whether the published manifests and both release workflows agree
/// on the accepted five-crate release set and its dependency order.
fn run_release_crates_check() -> bool {
    eprintln!("==> release crate set");

    match release_crates::check() {
        Ok(violations) if violations.is_empty() => {
            eprintln!(
                "published manifests, release-draft.yml, and release.yml all name the same \
                 five crates in the accepted dependency order"
            );
            true
        }
        Ok(violations) => {
            for violation in &violations {
                eprintln!("release crate set violation: {violation}");
            }
            eprintln!(
                "see RFC-0011 and docs/release/release-checklist.md for the accepted release set"
            );
            false
        }
        Err(error) => {
            eprintln!("could not check the release crate set: {error}");
            false
        }
    }
}

/// Reports whether the full suite proved every accepted M0-M4 ledger row.
fn run_conformance_campaign() -> bool {
    eprintln!("==> conformance campaign");

    match conformance::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!("every accepted row was proved by a scenario that ran and passed");
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the accepted scope in \
                 tests/fixtures/conformance/accepted-scope.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the conformance campaign: {error}");
            false
        }
    }
}

/// Reports whether the crash and restore campaign observed what it requires.
fn run_crash_restore_campaign() -> bool {
    eprintln!("==> crash and restore campaign");

    match crash_restore::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "every commit phase killed a live process and recovered without a forged \
                     status, and every reused scenario still passes"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign scope in \
                 tests/fixtures/crash-restore/campaign-scope.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the crash and restore campaign: {error}");
            false
        }
    }
}

/// Reports whether the Gate B typed-vs-Boxed transaction/restart
/// equivalence campaign proved what it requires.
fn run_gate_b_campaign() -> bool {
    eprintln!("==> Gate B transaction/restart equivalence campaign");

    match gate_b::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "all eight Gate B scenarios passed with representation-independent durable \
                     observations"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign semantics in \
                 tests/fixtures/gate-b/campaign-semantics.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the Gate B campaign: {error}");
            false
        }
    }
}

/// Reports whether the Gate H P-002 real-component performance campaign
/// proved what it requires.
fn run_gate_h_campaign() -> bool {
    eprintln!("==> Gate H P-002 real-component performance campaign");

    match gate_h::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "the typed path's zero-future-allocation and zero-dynamic-dispatch \
                     invariants held, and throughput/latency/allocation disclosure evidence was \
                     retained with no invented threshold"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign semantics in \
                 tests/fixtures/gate-h/campaign-semantics.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the Gate H campaign: {error}");
            false
        }
    }
}

/// Reports whether every declared ceiling held and was reached.
fn run_resource_bound_campaign() -> bool {
    eprintln!("==> PostgreSQL resource-bound campaign");

    match resource_bounds::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "every declared queue, retry cache, page, buffer, worker assignment, and \
                     result set was observed at its ceiling; every live ceiling was reached \
                     rather than merely respected; every shedding resource was offered an \
                     overload and shed under the rule it contracts for; and the stressed run \
                     left the sequential baseline's durable record"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign scope in \
                 tests/fixtures/resource-bounds/campaign-scope.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the resource-bound campaign: {error}");
            false
        }
    }
}

/// Reports whether the security campaign observed every property it owes.
fn run_security_campaign() -> bool {
    eprintln!("==> PostgreSQL security campaign");

    match security::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "the supported configuration connected only under validated TLS and refused \
                     an untrusted authority, a mismatched name, and a server without TLS; no \
                     privilege class exceeded itself; and no prohibited value class reached a \
                     diagnostic surface"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign scope in \
                 tests/fixtures/security/campaign-scope.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the security campaign: {error}");
            false
        }
    }
}

/// Reports whether the retained evidence is still what it says it is.
fn run_evidence_check() -> bool {
    eprintln!("==> retained evidence provenance");

    match evidence::run() {
        Ok(verification) => {
            let directories = verification
                .directories
                .iter()
                .map(|directory| directory.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            if verification.violations.is_empty() {
                eprintln!(
                    "{} retained report(s) across {directories} are byte-identical to what was \
                     recorded, name the run and the producer commit they came from, cover the \
                     required matrix, passed with no violations, and describe the campaign this \
                     tree still runs",
                    verification.reports,
                );
                return true;
            }
            for violation in &verification.violations {
                eprintln!("evidence gap: {violation}");
            }
            eprintln!(
                "see the provenance contract in each checked directory's \
                 evidence-provenance.json: {directories}"
            );
            false
        }
        Err(error) => {
            eprintln!("could not verify the retained evidence: {error}");
            false
        }
    }
}

/// Reports whether the #102 reconciliation document still agrees with the
/// repository it describes.
fn run_reconciliation_check() -> bool {
    eprintln!("==> #102 reconciliation drift");

    match reconciliation::run() {
        Ok(verification) => {
            if verification.violations.is_empty() {
                eprintln!(
                    "docs/project/m5-102-reconciliation.md names every required criterion \
                     exactly once with one disposition, its declared campaign and retained \
                     report counts match evidence-provenance.json and the retained-report \
                     directory, and every evidence link it cites resolves on disk"
                );
                return true;
            }
            for violation in &verification.violations {
                eprintln!("reconciliation drift: {violation}");
            }
            eprintln!("see docs/project/m5-102-reconciliation.md");
            false
        }
        Err(error) => {
            eprintln!("could not verify the #102 reconciliation document: {error}");
            false
        }
    }
}

/// Reports whether P-001, P-003, and P-010 each proved what they owe.
fn run_performance_campaign() -> bool {
    eprintln!("==> M5 performance and reference-workload campaign");

    match performance::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "P-001 measured the in-memory no-op tasklet lifecycle without opening a \
                     database connection, P-003 wrote its fixed, seeded, digest-verified CSV to \
                     PostgreSQL under AtomicSameResource and satisfies both campaign rows from \
                     one retained report, and P-010 scaled to 1, 10, and MAX_PARTITION_WORKERS \
                     local partitions with identical durable observations and enforced \
                     connection ceilings at every point"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!("see the campaign scope in tests/fixtures/performance/campaign-scope.json");
            false
        }
        Err(error) => {
            eprintln!("could not run the performance campaign: {error}");
            false
        }
    }
}

/// Reports whether the soak ran its declared window and nothing accumulated.
fn run_soak_campaign() -> bool {
    eprintln!("==> PostgreSQL soak campaign");

    match soak::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "the declared warmup and measured windows ran the declared workload, every \
                     cycle injected a fault, restarted, recovered, and drained completely, every \
                     cycle left the first measured cycle's durable record, and no declared growth \
                     rule for tasks, connections, handles, or resident memory was violated"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!("see the campaign scope in tests/fixtures/soak/campaign-scope.json");
            false
        }
        Err(error) => {
            eprintln!("could not run the soak campaign: {error}");
            false
        }
    }
}

/// Reports whether the upgrade campaign observed every schema path it owes.
fn run_upgrade_campaign() -> bool {
    eprintln!("==> PostgreSQL upgrade campaign");

    match upgrade::run() {
        Ok(campaign) => {
            eprintln!("campaign report: {}", campaign.report.display());
            if campaign.violations.is_empty() {
                eprintln!(
                    "schema 1, 2, and 3 upgraded directly to the current schema, the runtimes \
                     that shipped against schema 2 and schema 3 each refused the result without \
                     writing, and the backup taken before each upgrade restored the prior schema"
                );
                return true;
            }
            for violation in &campaign.violations {
                eprintln!("campaign gap: {violation}");
            }
            eprintln!(
                "see the campaign scope in \
                 tests/fixtures/upgrade/campaign-scope.json"
            );
            false
        }
        Err(error) => {
            eprintln!("could not run the upgrade campaign: {error}");
            false
        }
    }
}

/// Reports whether the rendered facade surface discloses only what the
/// facade review accepted.
fn run_surface_check() -> bool {
    eprintln!("==> facade surface disclosure");

    for finding in surface::accepted() {
        eprintln!("accepted finding: {finding}");
    }

    match surface::check() {
        Ok(violations) if violations.is_empty() => {
            eprintln!("facade surface discloses nothing further");
            true
        }
        Ok(violations) => {
            for violation in &violations {
                eprintln!("disclosure: {violation}");
            }
            eprintln!(
                "see the M5 preview surface and disclosure gate in \
                 docs/api/design-guidelines.md"
            );
            false
        }
        Err(error) => {
            eprintln!("could not inspect the facade surface: {error}");
            false
        }
    }
}

fn run_all(tasks: &[Task<'_>]) -> bool {
    for task in tasks {
        eprintln!("==> {}", task.label);

        let mut command = Command::new(task.program);
        command.args(task.args);

        if task.rustdoc_warnings {
            command.env("RUSTDOCFLAGS", "-D warnings");
        }

        match command.status() {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("{} failed with {status}", task.label);
                return false;
            }
            Err(error) => {
                eprintln!("could not run {}: {error}", display_command(task));
                return false;
            }
        }
    }

    true
}

fn display_command(task: &Task<'_>) -> String {
    std::iter::once(OsStr::new(task.program))
        .chain(task.args.iter().map(OsStr::new))
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

fn usage() {
    eprintln!(
        "usage: cargo xtask <command>\n\n\
         commands:\n\
           cancellation     measure P-014 stop, cancel, and drain against PostgreSQL\n\
           check            run formatting, Clippy, tests, rustdoc, and boundaries\n\
           conformance      run the full suite over the accepted M0-M4 scope\n\
           crash-restore    run the crash, restart, and logical restore campaign\n\
           deps             check extraction boundaries and workspace cycles\n\
           doctor           show required local tool versions\n\
           evidence         verify retained campaign evidence against its provenance\n\
           gate-b           run the Gate B typed-vs-Boxed transaction/restart equivalence campaign\n\
           gate-h           run the Gate H P-002 real-component performance campaign\n\
           package          inspect and dry-run every publishable crate\n\
           performance      run P-001, P-003, and P-010 in release profile against PostgreSQL\n\
           reconciliation   verify the #102 reconciliation document against the repository\n\
           release-crates   verify manifests and release workflows agree on the release set\n\
           resource-bounds  prove every declared ceiling holds and is reached under stress\n\
           security         run the TLS, least-privilege, and redaction campaign\n\
           soak             run the declared P-015 soak window and judge its growth\n\
           surface          inspect the rendered facade for disclosed dependencies\n\
           upgrade          run the PostgreSQL schema upgrade, rejection, and rollback campaign"
    );
}
