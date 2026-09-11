from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one replacement, found {count}")
    target.write_text(text.replace(old, new, 1))

replace(
    "crates/oxide-batch-repository/src/nested_job.rs",
    """    /// validating that the child is `COMPLETED`, `FAILED`, `STOPPED`, or
    /// `UNKNOWN`.
""",
    """    /// validating that the child is `COMPLETED`, `FAILED`, or `STOPPED`.
    /// `UNKNOWN` is deliberately not terminal for a nested-job parent: it
    /// remains unresolved until ordinary recovery resolves the child attempt.
""",
)

replace(
    "crates/oxide-batch/src/repository/memory.rs",
    """            if !matches!(
                status,
                BatchStatus::Completed
                    | BatchStatus::Failed
                    | BatchStatus::Stopped
                    | BatchStatus::Unknown
            ) {
""",
    """            if !matches!(
                status,
                BatchStatus::Completed | BatchStatus::Failed | BatchStatus::Stopped
            ) {
""",
)

path = Path("crates/oxide-batch/tests/repository.rs")
text = path.read_text()
anchor = """    let attempts = block_on(ambiguous.job_executions(first_link.child_job_instance_id()))?;
    assert_eq!(attempts.len(), 1);
    block_on(ambiguous.rollback())?;
    Ok(())
}
"""
replacement = """    let attempts = block_on(ambiguous.job_executions(first_link.child_job_instance_id()))?;
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        block_on(ambiguous.observe_nested_job_terminal(
            parent_execution.id(),
            &node,
            time(308),
        )),
        Err(RepositoryError::NestedJobChildUnresolved {
            child_execution_id: first_link.child_job_execution_id(),
            status: BatchStatus::Unknown,
        })
    );
    block_on(ambiguous.rollback())?;
    Ok(())
}
"""
if text.count(anchor) != 1:
    raise RuntimeError("unknown-child repository-test anchor did not match exactly once")
path.write_text(text.replace(anchor, replacement, 1))
