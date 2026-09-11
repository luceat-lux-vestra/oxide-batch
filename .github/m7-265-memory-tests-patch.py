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

path = Path("crates/oxide-batch/tests/repository.rs")
text = path.read_text()
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
if text.count(old_fn) != 1:
    raise RuntimeError("nested_child_definition body did not match exactly once")
path.write_text(text.replace(old_fn, new_fn, 1))
