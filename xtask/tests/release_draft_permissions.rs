use std::fs;
use std::path::Path;

const WORKFLOW: &str = ".github/workflows/release-draft.yml";

fn workflow_text() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has a workspace parent");
    fs::read_to_string(root.join(WORKFLOW)).expect("release-draft workflow is readable")
}

fn top_level_permissions_block(text: &str) -> Option<&str> {
    let marker = "\npermissions:";
    let start = text.find(marker)? + marker.len();
    let after = &text[start..];
    let end = after
        .find("\nconcurrency:")
        .or_else(|| after.find("\nenv:"))
        .or_else(|| after.find("\njobs:"))
        .unwrap_or(after.len());
    Some(&after[..end])
}

fn job_block<'a>(text: &'a str, job: &str) -> &'a str {
    let marker = format!("\n  {job}:\n");
    let start = text.find(&marker).expect("job exists") + marker.len();
    let after = &text[start..];

    let mut end = after.len();
    let mut position = 0;
    for line in after.split_inclusive('\n') {
        let content = line.trim_end_matches('\n');
        let next_job = !content.is_empty()
            && content.starts_with("  ")
            && !content.starts_with("   ")
            && content.trim_end().ends_with(':');
        if next_job {
            end = position;
            break;
        }
        position += line.len();
    }
    &after[..end]
}

#[test]
fn release_draft_write_authority_is_prepare_job_scoped() {
    let text = workflow_text();

    if let Some(permissions) = top_level_permissions_block(&text) {
        assert!(
            !permissions.contains("write-all")
                && !permissions
                    .lines()
                    .any(|line| line.trim_end().ends_with(": write")),
            "{WORKFLOW} must not grant write authority at workflow scope; future jobs would inherit it automatically"
        );
    }

    let prepare = job_block(&text, "prepare");
    for permission in ["contents", "id-token", "attestations"] {
        assert!(
            prepare.contains(&format!("\n      {permission}: write")),
            "{WORKFLOW} prepare job must retain job-scoped {permission}: write"
        );
    }
}
