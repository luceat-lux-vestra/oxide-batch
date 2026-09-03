use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be inside the workspace")
        .to_path_buf()
}

fn run(command: &mut Command, description: &str) {
    let output = command.output().unwrap_or_else(|error| {
        panic!("could not run {description}: {error}");
    });
    assert!(
        output.status.success(),
        "{description} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn merge_gate_contract_negative_tests_pass_in_github_actions() {
    if env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("skipping GitHub-specific merge-gate contract harness outside GitHub Actions");
        return;
    }

    let root = repo_root();
    run(
        Command::new("ruby")
            .current_dir(&root)
            .arg(".github/scripts/test-merge-gates.rb"),
        "merge-gate negative contract tests",
    );
}

#[test]
fn merge_gate_policy_matches_live_ruleset_in_github_actions() {
    if env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("skipping live GitHub ruleset readback outside GitHub Actions");
        return;
    }

    let root = repo_root();
    let api = env::var("GITHUB_API_URL").unwrap_or_else(|_| "https://api.github.com".to_owned());
    let repository =
        env::var("GITHUB_REPOSITORY").expect("GITHUB_REPOSITORY is set by GitHub Actions");
    let ruleset_path =
        env::temp_dir().join(format!("oxide-batch-ruleset-{}.json", std::process::id()));
    let url = format!("{api}/repos/{repository}/rulesets/19905142");

    let output = Command::new("curl")
        .args(["-fsSL", "--retry", "3", "--retry-all-errors", &url])
        .output()
        .expect("curl must be available on the GitHub-hosted runner");
    assert!(
        output.status.success(),
        "could not read live Protect main ruleset\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::write(&ruleset_path, output.stdout).expect("write temporary ruleset snapshot");

    run(
        Command::new("ruby")
            .current_dir(&root)
            .arg(".github/scripts/verify-merge-gates.rb")
            .arg(".github/merge-gate-policy.json")
            .arg(&ruleset_path),
        "live merge-gate drift verification",
    );

    let _ = fs::remove_file(ruleset_path);
}
