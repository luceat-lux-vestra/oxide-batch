from pathlib import Path

for name in [
    'crates/oxide-batch/src/repository/memory.rs',
    'crates/oxide-batch/src/repository/postgres.rs',
]:
    path = Path(name)
    s = path.read_text()
    old = '''                request.kind(),
                FlowTransitionKind::Decider | FlowTransitionKind::SplitAggregate
            ) {
'''
    new = '''                request.kind(),
                FlowTransitionKind::Decider
                    | FlowTransitionKind::SplitAggregate
                    | FlowTransitionKind::NestedJobExit
            ) {
'''
    if old not in s:
        raise SystemExit(f'flow source validation anchor not found in {name}')
    s = s.replace(old, new, 1)
    path.write_text(s)
