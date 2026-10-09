//! CI integration checks for the canonical merge-gate policy.

use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
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
    )?;
    run(
        Command::new("python3")
            .current_dir(&root)
            .arg(".github/scripts/pr-authority-runtime.py")
            .arg("self-test"),
        "dispatched PR authority runtime contract tests",
    )
}

// Live Ruleset drift is still checked in the dedicated "quality-contracts"
// GitHub Actions job using the exact trusted-base verifier. The required
// protected merge-gate remains independent. This Rust integration shard must
// not duplicate an unauthenticated network call; HTTP 403 here previously
// failed unrelated application changes despite successful local contracts.

#[test]
fn repository_root_resolves_trusted_merge_gate_policy() -> Result<(), Box<dyn Error>> {
    let root = repo_root()?;
    assert!(root.join(".github/merge-gate-policy.json").is_file());
    Ok(())
}