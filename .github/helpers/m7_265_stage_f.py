from pathlib import Path

path = Path('crates/oxide-batch/src/flow.rs')
s = path.read_text()
old = '''                    FlowTransitionKind::SplitAggregate => kind == Some("join"),
                    FlowTransitionKind::StepExit | FlowTransitionKind::CompletedStepReuse => {
                        matches!(kind, Some("step" | "partitioned_step"))
                    }
'''
new = '''                    FlowTransitionKind::SplitAggregate => kind == Some("join"),
                    FlowTransitionKind::NestedJobExit => kind == Some("nested_job"),
                    FlowTransitionKind::StepExit | FlowTransitionKind::CompletedStepReuse => {
                        matches!(kind, Some("step" | "partitioned_step"))
                    }
'''
if old not in s:
    raise SystemExit('decision manifest validator anchor not found')
s = s.replace(old, new, 1)
path.write_text(s)
