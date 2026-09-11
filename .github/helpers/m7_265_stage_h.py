from pathlib import Path

path = Path('crates/oxide-batch/tests/nested_job_runtime.rs')
s = path.read_text()
s = s.replace(
    'use std::sync::atomic::{AtomicUsize, Ordering};',
    'use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};',
    1,
)
append = r'''

#[derive(Clone, Copy)]
enum ChildTerminalMode {
    Failed,
    Stopped,
    Unknown,
}

struct TerminalChild {
    calls: Arc<AtomicUsize>,
    mode: ChildTerminalMode,
}

impl Tasklet for TerminalChild {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                ChildTerminalMode::Failed => Err(TaskletError::new()),
                ChildTerminalMode::Stopped => Ok(TaskletOutcome::Stopped),
                ChildTerminalMode::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
            }
        })
    }
}

fn terminal_child_job(
    name: &str,
    calls: Arc<AtomicUsize>,
    mode: ChildTerminalMode,
) -> Result<Arc<TaskletJob>, Box<dyn Error>> {
    Ok(Arc::new(TaskletJob::new(
        JobName::new(name)?,
        TaskletStep::new(
            StepName::new(format!("{name}-step"))?,
            Arc::new(TerminalChild { calls, mode }),
        ),
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new(format!("{name}-tasklet-v1"))?,
    )?))
}

fn single_nested_parent(
    parent_name: &str,
    child: Arc<TaskletJob>,
) -> Result<(FlowJob, NodeId), Box<dyn Error>> {
    let nested = NodeId::new("nested-child")?;
    let node = NestedJobNode::new(
        nested.clone(),
        child.definition_identity().clone(),
        ComponentRevision::new("mapping-v1")?,
        vec![payload_mapping(MissingParameterPolicy::Fail)?],
    )?;
    let plan = FlowGraph::new(nested.clone())
        .with_node(FlowNode::nested_job(node))
        .with_sequence(nested.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new(parent_name)?, DefinitionRevision::new("v1")?)?;
    Ok((
        FlowJob::new(JobName::new(parent_name)?, plan)?
            .with_nested_tasklet_job(nested.clone(), child)?,
        nested,
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn failed_child_fails_parent_node_and_commits_terminal_observation()
-> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let child = terminal_child_job("failed-child", calls.clone(), ChildTerminalMode::Failed)?;
    let (parent, nested) = single_nested_parent("failed-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("failed", Some("payload"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert!(matches!(
        report.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::NestedJobChildFailed { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("failed child remains durably linked");
    unit.rollback().await?;
    assert_eq!(
        link.terminal().map(|terminal| terminal.status()),
        Some(BatchStatus::Failed)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_child_stops_parent_and_commits_terminal_observation()
-> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let child = terminal_child_job("stopped-child", calls.clone(), ChildTerminalMode::Stopped)?;
    let (parent, nested) = single_nested_parent("stopped-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("stopped", Some("payload"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("stopped child remains durably linked");
    unit.rollback().await?;
    assert_eq!(
        link.terminal().map(|terminal| terminal.status()),
        Some(BatchStatus::Stopped)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_child_leaves_parent_unknown_without_fabricated_terminal()
-> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let child = terminal_child_job("unknown-child", calls.clone(), ChildTerminalMode::Unknown)?;
    let (parent, nested) = single_nested_parent("unknown-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("unknown", Some("payload"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Unknown);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Unknown
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("unknown child linkage is retained for recovery");
    let child_execution = unit
        .get_job_execution(link.child_job_execution_id())
        .await?
        .expect("linked child execution exists");
    unit.rollback().await?;
    assert!(link.terminal().is_none());
    assert_eq!(child_execution.metadata().status(), BatchStatus::Unknown);
    Ok(())
}

struct StopOwnedChild {
    entered: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
}

impl Tasklet for StopOwnedChild {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::Release);
            context.stop_token().cancelled().await;
            self.exited.store(true, Ordering::Release);
            Ok(TaskletOutcome::Stopped)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn parent_stop_is_owned_joined_and_propagated_to_child()
-> Result<(), Box<dyn Error>> {
    let entered = Arc::new(AtomicBool::new(false));
    let exited = Arc::new(AtomicBool::new(false));
    let child = Arc::new(TaskletJob::new(
        JobName::new("cancel-child")?,
        TaskletStep::new(
            StepName::new("cancel-child-step")?,
            Arc::new(StopOwnedChild {
                entered: entered.clone(),
                exited: exited.clone(),
            }),
        ),
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("cancel-child-v1")?,
    )?);
    let (parent, nested) = single_nested_parent("cancel-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (source, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("cancel", Some("payload"))?;

    let launch = launcher.launch(&parent, &parameters, &stop);
    let request = async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request);
    let report = report?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert!(entered.load(Ordering::Acquire));
    assert!(exited.load(Ordering::Acquire));
    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("cancelled child remains durably linked");
    unit.rollback().await?;
    assert_eq!(
        link.terminal().map(|terminal| terminal.status()),
        Some(BatchStatus::Stopped)
    );
    Ok(())
}
'''
if 'failed_child_fails_parent_node_and_commits_terminal_observation' in s:
    raise SystemExit('terminal propagation evidence already present')
path.write_text(s + append)
