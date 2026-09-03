//! CI integration checks for the canonical merge-gate policy.

use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> Result<PathBuf, Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let parent = manifest_dir
        .parent()
        .ok_or_else(|| io::Error::other("xtask must be inside the workspace"))?;
    Ok(parent.to_path_buf())
}

fn run(command: &mut Command, description: &str) -> Result<(), Box<dyn Error>> {
    let output = command.output()?;
    if output.status.success() {
        return Ok(());
    }

    Err(io::Error::other(format!(
        "{description} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
    .into())
}

fn remove_temp(path: &Path) -> Result<(), Box<dyn Error>> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[test]
fn merge_gate_contract_negative_tests_pass_in_github_actions() -> Result<(), Box<dyn Error>> {
    if env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("skipping GitHub-specific merge-gate contract harness outside GitHub Actions");
        return Ok(());
    }

    let root = repo_root()?;
    run(
        Command::new("ruby")
            .current_dir(&root)
            .arg(".github/scripts/test-merge-gates.rb"),
        "merge-gate negative contract tests",
    )?;
    run(
        Command::new("ruby")
            .current_dir(&root)
            .arg(".github/scripts/test-evaluate-aggregate-run.rb"),
        "selective-rerun-safe aggregate evaluator contract tests",
    )
}

#[test]
fn merge_gate_policy_matches_live_ruleset_in_github_actions() -> Result<(), Box<dyn Error>> {
    if env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("skipping live GitHub ruleset readback outside GitHub Actions");
        return Ok(());
    }

    let root = repo_root()?;
    let api = env::var("GITHUB_API_URL").unwrap_or_else(|_| "https://api.github.com".to_owned());
    let repository = env::var("GITHUB_REPOSITORY")?;
    let ruleset_path =
        env::temp_dir().join(format!("oxide-batch-ruleset-{}.json", std::process::id()));
    let url = format!("{api}/repos/{repository}/rulesets/19905142");

    let output = Command::new("curl")
        .args(["-fsSL", "--retry", "3", "--retry-all-errors", &url])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "could not read live Protect main ruleset\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }
    fs::write(&ruleset_path, output.stdout)?;

    let verification = run(
        Command::new("ruby")
            .current_dir(&root)
            .arg(".github/scripts/verify-merge-gates.rb")
            .arg(".github/merge-gate-policy.json")
            .arg(&ruleset_path),
        "live merge-gate drift verification",
    );
    let cleanup = remove_temp(&ruleset_path);
    verification?;
    cleanup?;
    Ok(())
}
