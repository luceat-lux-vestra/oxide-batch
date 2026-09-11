from __future__ import annotations

from pathlib import Path
import subprocess

SOURCE = "0682c4aea10699aa484e6ccf452087bdf5e9d9d5:.github/workflows/m7-265-memory-tests-helper.yml"
helper = subprocess.check_output(["git", "show", SOURCE], text=True)
begin = "          python3 - <<'PY'\n"
end = "          PY\n\n          cargo fmt --all"
body = helper.split(begin, 1)[1].split(end, 1)[0]
body = "\n".join(
    line[10:] if line.startswith("          ") else line
    for line in body.splitlines()
)
namespace: dict[str, object] = {}
exec(compile(body, "<m7-265-memory-tests-patch>", "exec"), namespace, namespace)

# Keep the test helper's mixed DomainError/DefinitionError boundary explicit.
test_path = Path("crates/oxide-batch/tests/repository.rs")
test_text = test_path.read_text()
old_fn = '''fn nested_child_definition() -> Result<DefinitionIdentity, oxide_batch::DefinitionError> {
    DefinitionIdentity::tasklet(
        &JobName::new("nested_child")?,
        &StepName::new("child_step")?,
        DefinitionRevision::new("nested-child-v1")?,
        &ComponentRevision::new("nested-child-tasklet-v1")?,
    )
}
'''
new_fn = '''fn nested_child_definition() -> Result<DefinitionIdentity, Box<dyn Error>> {
    Ok(DefinitionIdentity::tasklet(
        &JobName::new("nested_child")?,
        &StepName::new("child_step")?,
        DefinitionRevision::new("nested-child-v1")?,
        &ComponentRevision::new("nested-child-tasklet-v1")?,
    )?)
}
'''
if test_text.count(old_fn) != 1:
    raise RuntimeError("nested_child_definition body did not match exactly once")
test_text = test_text.replace(old_fn, new_fn, 1)
closure = ".map(|terminal| terminal.status())"
if test_text.count(closure) != 2:
    raise RuntimeError("expected exactly two nested-job terminal status closures")
test_text = test_text.replace(
    closure,
    ".map(oxide_batch::NestedJobTerminalObservation::status)",
)
test_path.write_text(test_text)

# Extract invariant checks from create_nested_job_link instead of suppressing
# clippy::too_many_lines.
memory_path = Path("crates/oxide-batch/src/repository/memory.rs")
memory = memory_path.read_text()
anchor = '''    fn next_recovery_decision_id(&self) -> Result<RecoveryDecisionId, RepositoryError> {
'''
helpers = '''    fn matching_nested_job_link(
        &self,
        parent_job_execution_id: JobExecutionId,
        node_id: &NodeId,
        child_definition: &DefinitionIdentity,
        child_parameters: &JobParameters,
    ) -> Result<Option<NestedJobLink>, RepositoryError> {
        let Some(existing) = self
            .staged
            .nested_job_links
            .get(&(parent_job_execution_id, node_id.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        let parameters = self
            .staged
            .job_parameters
            .get(&existing.child_job_execution_id())
            .ok_or(RepositoryError::NestedJobStateCorrupt)?;
        if existing.child_definition() != child_definition || parameters != child_parameters {
            return Err(RepositoryError::NestedJobStateCorrupt);
        }
        Ok(Some(existing))
    }

    fn store_nested_job_parameters(
        &mut self,
        child_execution_id: JobExecutionId,
        parameters: &JobParameters,
    ) -> Result<(), RepositoryError> {
        match self.staged.job_parameters.get(&child_execution_id) {
            Some(existing) if existing != parameters => Err(RepositoryError::NestedJobStateCorrupt),
            Some(_) => Ok(()),
            None => {
                self.staged
                    .job_parameters
                    .insert(child_execution_id, parameters.clone());
                Ok(())
            }
        }
    }

'''
if memory.count(anchor) != 1:
    raise RuntimeError("memory nested-job helper anchor did not match exactly once")
memory = memory.replace(anchor, helpers + anchor, 1)
old_existing = '''            let link_key = (request.parent_job_execution_id(), request.node_id().clone());
            if let Some(existing) = self.staged.nested_job_links.get(&link_key).cloned() {
                let parameters = self
                    .staged
                    .job_parameters
                    .get(&existing.child_job_execution_id())
                    .ok_or(RepositoryError::NestedJobStateCorrupt)?;
                if existing.child_definition() != request.child_definition()
                    || parameters != request.child_parameters()
                {
                    return Err(RepositoryError::NestedJobStateCorrupt);
                }
                return Ok(existing);
            }
'''
new_existing = '''            let link_key = (request.parent_job_execution_id(), request.node_id().clone());
            if let Some(existing) = self.matching_nested_job_link(
                request.parent_job_execution_id(),
                request.node_id(),
                request.child_definition(),
                request.child_parameters(),
            )? {
                return Ok(existing);
            }
'''
if memory.count(old_existing) != 1:
    raise RuntimeError("create_nested_job_link existing-link block did not match exactly once")
memory = memory.replace(old_existing, new_existing, 1)
old_parameters = '''            match self.staged.job_parameters.get(&child_execution.id()) {
                Some(existing) if existing != request.child_parameters() => {
                    return Err(RepositoryError::NestedJobStateCorrupt);
                }
                Some(_) => {}
                None => {
                    self.staged
                        .job_parameters
                        .insert(child_execution.id(), request.child_parameters().clone());
                }
            }
'''
new_parameters = '''            self.store_nested_job_parameters(
                child_execution.id(),
                request.child_parameters(),
            )?;
'''
if memory.count(old_parameters) != 1:
    raise RuntimeError("create_nested_job_link parameter block did not match exactly once")
memory_path.write_text(memory.replace(old_parameters, new_parameters, 1))
