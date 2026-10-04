//! M7 #301 repeat/interceptor runtime composition evidence.

#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BoxFuture, Clock, ComponentRevision, DefinitionRevision, ExecutionContext,
    FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    InMemoryJobRepository, JobName, JobParameters, JobRepository, JoinNode, ListenerContext,
    ListenerError, MAX_REPEAT_INTERCEPTORS, MAX_REPEAT_NESTING_DEPTH,
    MAX_REPEAT_SECONDARY_FAILURES, NodeId, PartitionBudget, PartitionCount, PartitionKey,
    PartitionPlanEntry, PartitionPlanFactory, PartitionTaskletFactory, PartitionedStepNode,
    RepeatCallbackError, RepeatCallbackFailureKind, RepeatCallbackPhase, RepeatContext,
    RepeatDecision, RepeatDefinition, RepeatFailureCause, RepeatId, RepeatInterceptor,
    RepeatInterceptorDefinition, RepeatInterceptorId, RepeatInterceptorKind,
    RepeatInterceptorRegistration, RepeatLineage, RepeatOrdinal, RepeatPolicy,
    RepeatPolicyConfiguration, RepeatPolicyDefinition, RepeatPolicyKind, RepeatPolicyOutcome,
    RepeatPolicyRegistration, RepeatRuntimeRegistration, RepeatStateSchema, SequentialIdGenerator,
    SplitBranch, SplitBudget, SplitNode, StateLimits, StateSchemaId, StateSchemaVersion,
    StepComponents, StepExecutionListener, StepName, StepNode, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletExecutionOutcome, TaskletOutcome, TaskletStep, TaskletStepFactory,
    TerminalKind,
};

const NODE: &str = "repeat-step";
const REPEAT: &str = "window";
const POLICY_KIND: &str = "test-count";
const POLICY_REVISION: &str = "policy-v1";
const INTERCEPTOR_KIND: &str = "test-recording";
const INTERCEPTOR_REVISION: &str = "interceptor-v1";
const STATE_SCHEMA: &str = "repeat.test.state";

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn infrastructure() -> (
    Arc<FixedClock>,
    Arc<SequentialIdGenerator>,
    InMemoryJobRepository,
) {
    let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(10)));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn record(events: &Mutex<Vec<String>>, value: impl Into<String>) {
    match events.lock() {
        Ok(mut events) => events.push(value.into()),
        Err(poisoned) => poisoned.into_inner().push(value.into()),
    }
}

fn snapshot(events: &Mutex<Vec<String>>) -> Vec<String> {
    match events.lock() {
        Ok(events) => events.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn repeat_state(ordinal: u32) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"{STATE_SCHEMA}","schema_version":1,"payload":{{"ordinal":{ordinal}}}}}"#
    );
    Ok(ExecutionContext::from_json(
        bytes.as_bytes(),
        StateLimits::default(),
    )?)
}

struct CountingPolicy {
    complete_after: u32,
    calls: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
}

impl RepeatPolicy for CountingPolicy {
    fn decide<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<RepeatPolicyOutcome, RepeatCallbackError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            record(
                self.events.as_ref(),
                format!("policy:{}", context.ordinal().get()),
            );
            let state =
                repeat_state(context.ordinal().get()).map_err(|_| RepeatCallbackError::new())?;
            if context.ordinal().get().saturating_add(1) >= self.complete_after {
                Ok(RepeatPolicyOutcome::complete_with(state))
            } else {
                Ok(RepeatPolicyOutcome::continue_with(state))
            }
        })
    }
}

#[derive(Clone, Copy)]
enum InterceptorMode {
    Pass,
    FailBefore,
    FailAfter,
    PanicBefore,
    PanicAfter,
}

struct RecordingInterceptor {
    id: &'static str,
    mode: InterceptorMode,
    events: Arc<Mutex<Vec<String>>>,
}

impl RepeatInterceptor for RecordingInterceptor {
    fn before<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>> {
        Box::pin(async move {
            record(
                self.events.as_ref(),
                format!("before:{}:{}", self.id, context.ordinal().get()),
            );
            match self.mode {
                InterceptorMode::FailBefore => Err(RepeatCallbackError::new()),
                InterceptorMode::PanicBefore => panic!("repeat-before-sensitive-payload"),
                _ => Ok(()),
            }
        })
    }

    fn after<'a>(
        &'a self,
        context: RepeatContext<'a>,
        _outcome: oxide_batch::TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>> {
        Box::pin(async move {
            record(
                self.events.as_ref(),
                format!("after:{}:{}", self.id, context.ordinal().get()),
            );
            match self.mode {
                InterceptorMode::FailAfter => Err(RepeatCallbackError::new()),
                InterceptorMode::PanicAfter => panic!("repeat-after-sensitive-payload"),
                _ => Ok(()),
            }
        })
    }
}

#[derive(Clone, Copy)]
enum BodyMode {
    Complete,
    StopOn(usize),
    FailOn(usize),
    PanicOn(usize),
}

struct RecordingTasklet {
    calls: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
    mode: BodyMode,
}

impl Tasklet for RecordingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            record(self.events.as_ref(), format!("body:{call}"));
            match self.mode {
                BodyMode::StopOn(target) if target == call => Ok(TaskletOutcome::Stopped),
                BodyMode::FailOn(target) if target == call => Err(TaskletError::new()),
                BodyMode::PanicOn(target) if target == call => {
                    panic!("repeat-body-sensitive-payload")
                }
                _ => Ok(TaskletOutcome::Completed),
            }
        })
    }
}

struct WaitForStopTasklet {
    started: Arc<tokio::sync::Notify>,
    events: Arc<Mutex<Vec<String>>>,
}

impl Tasklet for WaitForStopTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            record(self.events.as_ref(), "body:waiting-for-stop");
            self.started.notify_one();
            context.stop_token().cancelled().await;
            Ok(TaskletOutcome::Stopped)
        })
    }
}

struct CompletePolicy;

impl RepeatPolicy for CompletePolicy {
    fn decide<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<RepeatPolicyOutcome, RepeatCallbackError>> {
        Box::pin(async move {
            let state =
                repeat_state(context.ordinal().get()).map_err(|_| RepeatCallbackError::new())?;
            Ok(RepeatPolicyOutcome::complete_with(state))
        })
    }
}

struct FailAfterInterceptor;

impl RepeatInterceptor for FailAfterInterceptor {
    fn before<'a>(
        &'a self,
        _context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>> {
        Box::pin(async { Ok(()) })
    }

    fn after<'a>(
        &'a self,
        _context: RepeatContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>> {
        Box::pin(async { Err(RepeatCallbackError::new()) })
    }
}

fn bounded_definition(level: usize) -> Result<RepeatDefinition, Box<dyn Error>> {
    let interceptors = (0..MAX_REPEAT_INTERCEPTORS)
        .map(|index| {
            Ok(RepeatInterceptorDefinition::new(
                RepeatInterceptorId::new(format!("i-{index}"))?,
                RepeatInterceptorKind::new(INTERCEPTOR_KIND)?,
                ComponentRevision::new(INTERCEPTOR_REVISION)?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(RepeatDefinition::new(
        RepeatId::new(format!("bounded-{level}"))?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new(POLICY_KIND)?,
            ComponentRevision::new(POLICY_REVISION)?,
            RepeatPolicyConfiguration::new("complete")?,
        ),
        interceptors,
        RepeatStateSchema::new(
            StateSchemaId::new(STATE_SCHEMA)?,
            StateSchemaVersion::new(1)?,
        ),
    )?)
}

fn bounded_registration(level: usize) -> Result<RepeatRuntimeRegistration, Box<dyn Error>> {
    let interceptors = (0..MAX_REPEAT_INTERCEPTORS)
        .map(|index| {
            Ok(RepeatInterceptorRegistration::new(
                RepeatInterceptorId::new(format!("i-{index}"))?,
                RepeatInterceptorKind::new(INTERCEPTOR_KIND)?,
                ComponentRevision::new(INTERCEPTOR_REVISION)?,
                Arc::new(FailAfterInterceptor),
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(RepeatRuntimeRegistration::new(
        RepeatId::new(format!("bounded-{level}"))?,
        RepeatPolicyRegistration::new(
            RepeatPolicyKind::new(POLICY_KIND)?,
            ComponentRevision::new(POLICY_REVISION)?,
            RepeatPolicyConfiguration::new("complete")?,
            Arc::new(CompletePolicy),
        ),
        interceptors,
    ))
}

fn interceptor_definition(id: &str) -> Result<RepeatInterceptorDefinition, Box<dyn Error>> {
    Ok(RepeatInterceptorDefinition::new(
        RepeatInterceptorId::new(id)?,
        RepeatInterceptorKind::new(INTERCEPTOR_KIND)?,
        ComponentRevision::new(INTERCEPTOR_REVISION)?,
    ))
}

fn definition_for(
    repeat_id: &str,
    complete_after: u32,
    interceptor_ids: &[&str],
) -> Result<RepeatDefinition, Box<dyn Error>> {
    Ok(RepeatDefinition::new(
        RepeatId::new(repeat_id)?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new(POLICY_KIND)?,
            ComponentRevision::new(POLICY_REVISION)?,
            RepeatPolicyConfiguration::new(format!("limit-{complete_after}"))?,
        ),
        interceptor_ids
            .iter()
            .map(|id| interceptor_definition(id))
            .collect::<Result<Vec<_>, _>>()?,
        RepeatStateSchema::new(
            StateSchemaId::new(STATE_SCHEMA)?,
            StateSchemaVersion::new(1)?,
        ),
    )?)
}

fn definition(
    complete_after: u32,
    interceptor_ids: &[&str],
) -> Result<RepeatDefinition, Box<dyn Error>> {
    definition_for(REPEAT, complete_after, interceptor_ids)
}

fn plan(
    name: &JobName,
    complete_after: u32,
    interceptor_ids: &[&str],
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let node = NodeId::new(NODE)?;
    let step = StepNode::new(
        node.clone(),
        StepName::new(NODE)?,
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
    )
    .with_repeat_definition(definition(complete_after, interceptor_ids)?);
    Ok(FlowGraph::new(node.clone())
        .with_node(FlowNode::step(step))
        .with_sequence(node, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

fn runtime_registration_for(
    repeat_id: &str,
    complete_after: u32,
    events: &Arc<Mutex<Vec<String>>>,
    policy_calls: Arc<AtomicUsize>,
    interceptors: &[(&'static str, InterceptorMode)],
) -> Result<RepeatRuntimeRegistration, Box<dyn Error>> {
    let policy = RepeatPolicyRegistration::new(
        RepeatPolicyKind::new(POLICY_KIND)?,
        ComponentRevision::new(POLICY_REVISION)?,
        RepeatPolicyConfiguration::new(format!("limit-{complete_after}"))?,
        Arc::new(CountingPolicy {
            complete_after,
            calls: policy_calls,
            events: Arc::clone(events),
        }),
    );
    let interceptors = interceptors
        .iter()
        .map(|(id, mode)| {
            Ok(RepeatInterceptorRegistration::new(
                RepeatInterceptorId::new(*id)?,
                RepeatInterceptorKind::new(INTERCEPTOR_KIND)?,
                ComponentRevision::new(INTERCEPTOR_REVISION)?,
                Arc::new(RecordingInterceptor {
                    id,
                    mode: *mode,
                    events: Arc::clone(events),
                }),
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(RepeatRuntimeRegistration::new(
        RepeatId::new(repeat_id)?,
        policy,
        interceptors,
    ))
}

fn runtime_registration(
    complete_after: u32,
    events: &Arc<Mutex<Vec<String>>>,
    policy_calls: Arc<AtomicUsize>,
    interceptors: &[(&'static str, InterceptorMode)],
) -> Result<RepeatRuntimeRegistration, Box<dyn Error>> {
    runtime_registration_for(REPEAT, complete_after, events, policy_calls, interceptors)
}

#[derive(Clone, Copy)]
enum PolicyMode {
    Fail,
    Panic,
}

struct FailingPolicy {
    mode: PolicyMode,
    events: Arc<Mutex<Vec<String>>>,
}

impl RepeatPolicy for FailingPolicy {
    fn decide<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<RepeatPolicyOutcome, RepeatCallbackError>> {
        Box::pin(async move {
            record(
                self.events.as_ref(),
                format!("policy:{}", context.ordinal().get()),
            );
            match self.mode {
                PolicyMode::Fail => Err(RepeatCallbackError::new()),
                PolicyMode::Panic => panic!("repeat-policy-sensitive-payload"),
            }
        })
    }
}

struct StoppingInterceptor {
    source: StopSource,
    events: Arc<Mutex<Vec<String>>>,
}

impl RepeatInterceptor for StoppingInterceptor {
    fn before<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>> {
        Box::pin(async move {
            record(
                self.events.as_ref(),
                format!("before:stopper:{}", context.ordinal().get()),
            );
            self.source.request_stop();
            Ok(())
        })
    }

    fn after<'a>(
        &'a self,
        context: RepeatContext<'a>,
        outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>> {
        Box::pin(async move {
            let class = if matches!(outcome, TaskletExecutionOutcome::Stopped(_)) {
                "stopped"
            } else {
                "unexpected"
            };
            record(
                self.events.as_ref(),
                format!("after:stopper:{}:{class}", context.ordinal().get()),
            );
            Ok(())
        })
    }
}

fn job_with_policy(
    name: &str,
    events: &Arc<Mutex<Vec<String>>>,
    tasklet_calls: Arc<AtomicUsize>,
    policy: Arc<dyn RepeatPolicy>,
    interceptors: &[(&'static str, InterceptorMode)],
) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(name)?;
    let ids = interceptors.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let node = NodeId::new(NODE)?;
    let step = TaskletStep::new(
        StepName::new(NODE)?,
        Arc::new(RecordingTasklet {
            calls: tasklet_calls,
            events: Arc::clone(events),
            mode: BodyMode::Complete,
        }),
    );
    let interceptor_registrations = interceptors
        .iter()
        .map(|(id, mode)| {
            Ok(RepeatInterceptorRegistration::new(
                RepeatInterceptorId::new(*id)?,
                RepeatInterceptorKind::new(INTERCEPTOR_KIND)?,
                ComponentRevision::new(INTERCEPTOR_REVISION)?,
                Arc::new(RecordingInterceptor {
                    id,
                    mode: *mode,
                    events: Arc::clone(events),
                }),
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let registration = RepeatRuntimeRegistration::new(
        RepeatId::new(REPEAT)?,
        RepeatPolicyRegistration::new(
            RepeatPolicyKind::new(POLICY_KIND)?,
            ComponentRevision::new(POLICY_REVISION)?,
            RepeatPolicyConfiguration::new("limit-1")?,
            policy,
        ),
        interceptor_registrations,
    );
    Ok(FlowJob::new(name.clone(), plan(&name, 1, &ids)?)?
        .with_tasklet_step(node.clone(), step)?
        .with_repeat_registration(node, registration)?)
}

struct RecordingStepListener {
    events: Arc<Mutex<Vec<String>>>,
}

impl StepExecutionListener for RecordingStepListener {
    fn before_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async move {
            record(self.events.as_ref(), "listener:before");
            Ok(())
        })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async move {
            record(self.events.as_ref(), "listener:after");
            Ok(())
        })
    }
}

fn job(
    name: &str,
    complete_after: u32,
    events: &Arc<Mutex<Vec<String>>>,
    tasklet_calls: Arc<AtomicUsize>,
    policy_calls: Arc<AtomicUsize>,
    interceptors: &[(&'static str, InterceptorMode)],
    body_mode: BodyMode,
    listener: Option<Arc<dyn StepExecutionListener>>,
) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(name)?;
    let ids = interceptors.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let node = NodeId::new(NODE)?;
    let mut step = TaskletStep::new(
        StepName::new(NODE)?,
        Arc::new(RecordingTasklet {
            calls: tasklet_calls,
            events: Arc::clone(events),
            mode: body_mode,
        }),
    );
    if let Some(listener) = listener {
        step = step.with_listener(listener);
    }
    Ok(
        FlowJob::new(name.clone(), plan(&name, complete_after, &ids)?)?
            .with_tasklet_step(node.clone(), step)?
            .with_repeat_registration(
                node,
                runtime_registration(complete_after, events, policy_calls, interceptors)?,
            )?,
    )
}

async fn latest_repeat(
    repository: &InMemoryJobRepository,
    instance_id: oxide_batch::JobInstanceId,
) -> Result<Option<oxide_batch::RepeatExecution>, Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let repeat = unit
        .latest_repeat_execution(instance_id, &NodeId::new(NODE)?, &RepeatId::new(REPEAT)?)
        .await?;
    unit.rollback().await?;
    Ok(repeat)
}

#[tokio::test(flavor = "current_thread")]
async fn repeat_runs_ordered_before_body_policy_and_reverse_after_until_complete()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-ordering",
        3,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[
            ("a", InterceptorMode::Pass),
            ("b", InterceptorMode::Pass),
            ("c", InterceptorMode::Pass),
        ],
        BodyMode::Complete,
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 3);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 3);
    let mut expected = Vec::new();
    for ordinal in 0..3 {
        expected.extend([
            format!("before:a:{ordinal}"),
            format!("before:b:{ordinal}"),
            format!("before:c:{ordinal}"),
            format!("body:{ordinal}"),
            format!("policy:{ordinal}"),
            format!("after:c:{ordinal}"),
            format!("after:b:{ordinal}"),
            format!("after:a:{ordinal}"),
        ]);
    }
    assert_eq!(snapshot(events.as_ref()), expected);

    let Some(durable) = latest_repeat(&repository, report.instance().id()).await? else {
        panic!("completed repeat state must exist");
    };
    assert_eq!(durable.ordinal().get(), 2);
    assert_eq!(durable.decision(), RepeatDecision::Complete);
    assert_eq!(durable.state().schema_id().as_str(), STATE_SCHEMA);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_repeat_wraps_inner_iterations_in_declared_order() -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let outer_policy_calls = Arc::new(AtomicUsize::new(0));
    let inner_policy_calls = Arc::new(AtomicUsize::new(0));
    let name = JobName::new("nested-repeat-ordering")?;
    let node = NodeId::new(NODE)?;

    let nested_definition = definition_for("outer", 1, &["outer"])?.with_nested(definition_for(
        "inner",
        2,
        &["inner"],
    )?)?;
    let step_definition = StepNode::new(
        node.clone(),
        StepName::new(NODE)?,
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
    )
    .with_repeat_definition(nested_definition);
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(step_definition))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let registration = runtime_registration_for(
        "outer",
        1,
        &events,
        Arc::clone(&outer_policy_calls),
        &[("outer", InterceptorMode::Pass)],
    )?
    .with_nested(runtime_registration_for(
        "inner",
        2,
        &events,
        Arc::clone(&inner_policy_calls),
        &[("inner", InterceptorMode::Pass)],
    )?);
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            node.clone(),
            TaskletStep::new(
                StepName::new(NODE)?,
                Arc::new(RecordingTasklet {
                    calls: Arc::clone(&tasklet_calls),
                    events: Arc::clone(&events),
                    mode: BodyMode::Complete,
                }),
            ),
        )?
        .with_repeat_registration(node, registration)?;

    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 2);
    assert_eq!(inner_policy_calls.load(Ordering::SeqCst), 2);
    assert_eq!(outer_policy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:outer:0"),
            String::from("before:inner:0"),
            String::from("body:0"),
            String::from("policy:0"),
            String::from("after:inner:0"),
            String::from("before:inner:1"),
            String::from("body:1"),
            String::from("policy:1"),
            String::from("after:inner:1"),
            String::from("policy:0"),
            String::from("after:outer:0"),
        ]
    );

    let mut unit = repository.begin().await?;
    let outer = unit
        .latest_repeat_execution(
            report.instance().id(),
            &NodeId::new(NODE)?,
            &RepeatId::new("outer")?,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("outer repeat state missing"))?;
    let inner = unit
        .latest_repeat_execution(
            report.instance().id(),
            &NodeId::new(NODE)?,
            &RepeatId::new("inner")?,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("inner repeat state missing"))?;
    unit.rollback().await?;
    assert_eq!(outer.ordinal().get(), 0);
    assert_eq!(outer.decision(), RepeatDecision::Complete);
    assert_eq!(inner.ordinal().get(), 1);
    assert_eq!(inner.decision(), RepeatDecision::Complete);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_parent_continue_reenters_child_with_exact_parent_lineage()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let outer_policy_calls = Arc::new(AtomicUsize::new(0));
    let inner_policy_calls = Arc::new(AtomicUsize::new(0));
    let name = JobName::new("nested-repeat-parent-continue")?;
    let node = NodeId::new(NODE)?;

    let nested_definition = definition_for("outer", 2, &["outer"])?.with_nested(definition_for(
        "inner",
        2,
        &["inner"],
    )?)?;
    let step_definition = StepNode::new(
        node.clone(),
        StepName::new(NODE)?,
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
    )
    .with_repeat_definition(nested_definition);
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(step_definition))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let registration = runtime_registration_for(
        "outer",
        2,
        &events,
        Arc::clone(&outer_policy_calls),
        &[("outer", InterceptorMode::Pass)],
    )?
    .with_nested(runtime_registration_for(
        "inner",
        2,
        &events,
        Arc::clone(&inner_policy_calls),
        &[("inner", InterceptorMode::Pass)],
    )?);
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            node.clone(),
            TaskletStep::new(
                StepName::new(NODE)?,
                Arc::new(RecordingTasklet {
                    calls: Arc::clone(&tasklet_calls),
                    events,
                    mode: BodyMode::Complete,
                }),
            ),
        )?
        .with_repeat_registration(node.clone(), registration)?;

    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 4);
    assert_eq!(inner_policy_calls.load(Ordering::SeqCst), 4);
    assert_eq!(outer_policy_calls.load(Ordering::SeqCst), 2);

    let inner_lineage = RepeatLineage::root()
        .child(RepeatId::new("outer")?, RepeatOrdinal::new(1))
        .ok_or_else(|| std::io::Error::other("bounded child lineage rejected"))?;
    let mut unit = repository.begin().await?;
    let outer = unit
        .latest_repeat_execution(report.instance().id(), &node, &RepeatId::new("outer")?)
        .await?
        .ok_or_else(|| std::io::Error::other("outer repeat state missing"))?;
    let inner = unit
        .latest_repeat_execution_in_lineage(
            report.instance().id(),
            &node,
            &node,
            &inner_lineage,
            &RepeatId::new("inner")?,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("current inner repeat lineage missing"))?;
    unit.rollback().await?;
    assert_eq!(outer.ordinal().get(), 1);
    assert_eq!(outer.decision(), RepeatDecision::Complete);
    assert_eq!(inner.lineage(), &inner_lineage);
    assert_eq!(inner.ordinal().get(), 1);
    assert_eq!(inner.decision(), RepeatDecision::Complete);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn partition_workers_keep_independent_repeat_execution_owners() -> Result<(), Box<dyn Error>>
{
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let name = JobName::new("partition-repeat-lineage")?;
    let manager = NodeId::new("partitioned")?;
    let worker_node = NodeId::new("worker")?;
    let worker_name = StepName::new("worker")?;
    let worker = StepNode::new(
        worker_node.clone(),
        worker_name.clone(),
        StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
    )
    .with_repeat_definition(definition(2, &[])?);
    let plan = FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            manager.clone(),
            StepName::new("partitioned")?,
            worker,
            ComponentRevision::new("partitioner-v1")?,
            ComponentRevision::new("canonical-v1")?,
            PartitionCount::new(2)?,
            PartitionBudget::new(2, 3)?,
        )))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let partition_context = |key: &str| -> Result<PartitionPlanEntry, Box<dyn Error>> {
        let context = ExecutionContext::from_json(
            format!(
                r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"repeat.partition","schema_version":1,"payload":{{"key":"{key}"}}}}"#
            )
            .as_bytes(),
            StateLimits::default(),
        )?;
        Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
    };
    let entries = vec![partition_context("alpha")?, partition_context("beta")?];
    let factory_name = worker_name.clone();
    let factory_calls = Arc::clone(&tasklet_calls);
    let factory_events = Arc::clone(&events);
    let job = FlowJob::new(name, plan)?
        .with_partitioned_tasklet(
            manager,
            PartitionPlanFactory::new(move |_request| Ok(entries.clone())),
            PartitionTaskletFactory::new(worker_name, move |_input| {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(RecordingTasklet {
                        calls: Arc::clone(&factory_calls),
                        events: Arc::clone(&factory_events),
                        mode: BodyMode::Complete,
                    }),
                )
            }),
        )?
        .with_repeat_registration(
            worker_node.clone(),
            runtime_registration(2, &events, Arc::clone(&policy_calls), &[])?,
        )?;

    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 4);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 4);

    let parent = report
        .step_executions()
        .last()
        .ok_or_else(|| std::io::Error::other("partition parent execution missing"))?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    assert_eq!(partitions.len(), 2);
    let mut execution_nodes = Vec::new();
    for partition in partitions {
        let worker_id = partition
            .worker_step_execution_id()
            .ok_or_else(|| std::io::Error::other("partition worker execution missing"))?;
        let repeat = unit
            .repeat_execution(worker_id, &RepeatId::new(REPEAT)?)
            .await?
            .ok_or_else(|| std::io::Error::other("partition repeat state missing"))?;
        assert_eq!(repeat.definition_node_id(), &worker_node);
        assert_ne!(repeat.node_id(), &worker_node);
        assert!(repeat.lineage().is_root());
        assert_eq!(repeat.ordinal().get(), 1);
        assert_eq!(repeat.decision(), RepeatDecision::Complete);
        execution_nodes.push(repeat.node_id().clone());
    }
    unit.rollback().await?;
    assert_ne!(execution_nodes[0], execution_nodes[1]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn split_branch_repeat_keeps_advanced_flow_authority() -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let repeated_calls = Arc::new(AtomicUsize::new(0));
    let sibling_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let name = JobName::new("repeat-split-composition")?;
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let repeated = NodeId::new("repeated")?;
    let sibling = NodeId::new("sibling")?;
    let join = NodeId::new("joined")?;

    let prepare_step = StepNode::new(
        prepare.clone(),
        StepName::new("prepare")?,
        StepComponents::Tasklet(ComponentRevision::new("prepare-v1")?),
    );
    let repeated_step = StepNode::new(
        repeated.clone(),
        StepName::new("repeated")?,
        StepComponents::Tasklet(ComponentRevision::new("repeated-v1")?),
    )
    .with_repeat_definition(definition(2, &[])?);
    let sibling_step = StepNode::new(
        sibling.clone(),
        StepName::new("sibling")?,
        StepComponents::Tasklet(ComponentRevision::new("sibling-v1")?),
    );

    let plan = FlowGraph::new(prepare.clone())
        .with_node(FlowNode::step(prepare_step))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(vec![repeated_step]),
                SplitBranch::new(vec![sibling_step]),
            ],
            join.clone(),
            SplitBudget::new(1, 2)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare.clone(), FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let prepare_events = Arc::new(Mutex::new(Vec::new()));
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let repeated_name = StepName::new("repeated")?;
    let repeated_factory_name = repeated_name.clone();
    let repeated_factory_calls = Arc::clone(&repeated_calls);
    let repeated_factory_events = Arc::clone(&events);
    let sibling_name = StepName::new("sibling")?;
    let sibling_factory_name = sibling_name.clone();
    let sibling_factory_calls = Arc::clone(&sibling_calls);
    let sibling_factory_events = Arc::new(Mutex::new(Vec::new()));

    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            prepare.clone(),
            TaskletStep::new(
                StepName::new("prepare")?,
                Arc::new(RecordingTasklet {
                    calls: prepare_calls,
                    events: prepare_events,
                    mode: BodyMode::Complete,
                }),
            ),
        )?
        .with_split_tasklet_factory(
            repeated.clone(),
            TaskletStepFactory::new(repeated_name, move || {
                TaskletStep::new(
                    repeated_factory_name.clone(),
                    Arc::new(RecordingTasklet {
                        calls: Arc::clone(&repeated_factory_calls),
                        events: Arc::clone(&repeated_factory_events),
                        mode: BodyMode::Complete,
                    }),
                )
            }),
        )?
        .with_split_tasklet_factory(
            sibling,
            TaskletStepFactory::new(sibling_name, move || {
                TaskletStep::new(
                    sibling_factory_name.clone(),
                    Arc::new(RecordingTasklet {
                        calls: Arc::clone(&sibling_factory_calls),
                        events: Arc::clone(&sibling_factory_events),
                        mode: BodyMode::Complete,
                    }),
                )
            }),
        )?
        .with_repeat_registration(
            repeated,
            runtime_registration(2, &events, Arc::clone(&policy_calls), &[])?,
        )?;

    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(repeated_calls.load(Ordering::SeqCst), 2);
    assert_eq!(sibling_calls.load(Ordering::SeqCst), 1);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("body:0"),
            String::from("policy:0"),
            String::from("body:1"),
            String::from("policy:1"),
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stop_during_repeat_body_unwinds_and_does_not_commit_iteration()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let name = JobName::new("repeat-body-stop")?;
    let node = NodeId::new(NODE)?;
    let job = FlowJob::new(name.clone(), plan(&name, 1, &["a"])?)?
        .with_tasklet_step(
            node.clone(),
            TaskletStep::new(
                StepName::new(NODE)?,
                Arc::new(WaitForStopTasklet {
                    started: Arc::clone(&started),
                    events: Arc::clone(&events),
                }),
            ),
        )?
        .with_repeat_registration(
            node,
            runtime_registration(
                1,
                &events,
                Arc::clone(&policy_calls),
                &[("a", InterceptorMode::Pass)],
            )?,
        )?;

    let (clock, ids, repository) = infrastructure();
    let (source, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let launch = launcher.launch(&job, &parameters, &stop);
    let request_stop = async {
        started.notified().await;
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request_stop);
    let report = report?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("body:waiting-for-stop"),
            String::from("after:a:0"),
        ]
    );
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none(),
        "stopped body must not publish an accepted repeat iteration"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_repeat_secondary_diagnostics_are_bounded_at_structural_maximum()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let name = JobName::new("repeat-secondary-bound")?;
    let node = NodeId::new(NODE)?;

    let mut definition = bounded_definition(MAX_REPEAT_NESTING_DEPTH - 1)?;
    let mut registration = bounded_registration(MAX_REPEAT_NESTING_DEPTH - 1)?;
    for level in (0..MAX_REPEAT_NESTING_DEPTH - 1).rev() {
        definition = bounded_definition(level)?.with_nested(definition)?;
        registration = bounded_registration(level)?.with_nested(registration);
    }

    let step_definition = StepNode::new(
        node.clone(),
        StepName::new(NODE)?,
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
    )
    .with_repeat_definition(definition);
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(step_definition))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            node.clone(),
            TaskletStep::new(
                StepName::new(NODE)?,
                Arc::new(RecordingTasklet {
                    calls: Arc::clone(&tasklet_calls),
                    events,
                    mode: BodyMode::FailOn(0),
                }),
            ),
        )?
        .with_repeat_registration(node, registration)?;

    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 1);
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            assert_eq!(
                failure.primary(),
                &RepeatFailureCause::Body(oxide_batch::TaskletFailure::Error)
            );
            assert_eq!(failure.secondary().len(), MAX_REPEAT_SECONDARY_FAILURES);
            assert!(!failure.secondary_truncated());
            assert!(failure.secondary().iter().all(|failure| {
                failure.phase() == RepeatCallbackPhase::After
                    && failure.kind() == RepeatCallbackFailureKind::Error
            }));
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn before_failure_unwinds_only_successfully_entered_interceptors()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-before-failure",
        1,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[
            ("a", InterceptorMode::Pass),
            ("b", InterceptorMode::FailBefore),
            ("c", InterceptorMode::Pass),
        ],
        BodyMode::Complete,
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("before:b:0"),
            String::from("after:a:0"),
        ]
    );
    let b = RepeatInterceptorId::new("b")?;
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            match failure.primary() {
                RepeatFailureCause::Callback(callback) => {
                    assert_eq!(callback.interceptor_id(), Some(&b));
                    assert_eq!(callback.phase(), RepeatCallbackPhase::Before);
                    assert_eq!(callback.kind(), RepeatCallbackFailureKind::Error);
                }
                other => panic!("unexpected repeat primary: {other:?}"),
            }
            assert!(failure.secondary().is_empty());
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reverse_unwind_keeps_first_after_failure_primary_and_later_failures_secondary()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-after-failure",
        1,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[
            ("a", InterceptorMode::FailAfter),
            ("b", InterceptorMode::FailAfter),
        ],
        BodyMode::Complete,
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 1);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("before:b:0"),
            String::from("body:0"),
            String::from("policy:0"),
            String::from("after:b:0"),
            String::from("after:a:0"),
        ]
    );

    let a = RepeatInterceptorId::new("a")?;
    let b = RepeatInterceptorId::new("b")?;
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            match failure.primary() {
                RepeatFailureCause::Callback(callback) => {
                    assert_eq!(callback.interceptor_id(), Some(&b));
                    assert_eq!(callback.phase(), RepeatCallbackPhase::After);
                    assert_eq!(callback.kind(), RepeatCallbackFailureKind::Error);
                }
                other => panic!("unexpected repeat primary: {other:?}"),
            }
            assert_eq!(failure.secondary().len(), 1);
            assert_eq!(failure.secondary()[0].interceptor_id(), Some(&a));
            assert_eq!(failure.secondary()[0].phase(), RepeatCallbackPhase::After);
            assert_eq!(
                failure.secondary()[0].kind(),
                RepeatCallbackFailureKind::Error
            );
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none(),
        "failed after callbacks must not publish repeat state"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_uncommitted_iteration_restarts_from_last_committed_repeat_ordinal()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-restart",
        2,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[("a", InterceptorMode::Pass)],
        BodyMode::StopOn(1),
        None,
    )?;
    let (clock, ids, repository) = infrastructure();

    let (_first_source, first_stop) = StopSource::new();
    let first = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &first_stop)
        .await?;
    assert_eq!(first.outcome(), &FlowExecutionOutcome::Stopped);
    let Some(first_durable) = latest_repeat(&repository, first.instance().id()).await? else {
        panic!("first accepted iteration must exist");
    };
    assert_eq!(first_durable.ordinal().get(), 0);
    assert_eq!(first_durable.decision(), RepeatDecision::Continue);

    let (_restart_source, restart_stop) = StopSource::new();
    let restarted = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &restart_stop)
        .await?;
    assert_eq!(restarted.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(restarted.instance().id(), first.instance().id());
    let Some(completed) = latest_repeat(&repository, restarted.instance().id()).await? else {
        panic!("completed repeat state must exist");
    };
    assert_eq!(completed.ordinal().get(), 1);
    assert_eq!(completed.decision(), RepeatDecision::Complete);

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 3);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 2);
    let events = snapshot(events.as_ref());
    assert_eq!(
        events,
        vec![
            String::from("before:a:0"),
            String::from("body:0"),
            String::from("policy:0"),
            String::from("after:a:0"),
            String::from("before:a:1"),
            String::from("body:1"),
            String::from("after:a:1"),
            String::from("before:a:1"),
            String::from("body:2"),
            String::from("policy:1"),
            String::from("after:a:1"),
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn contained_before_panic_is_primary_and_unwinds_prior_entries() -> Result<(), Box<dyn Error>>
{
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-before-panic",
        1,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[
            ("a", InterceptorMode::Pass),
            ("b", InterceptorMode::PanicBefore),
            ("c", InterceptorMode::Pass),
        ],
        BodyMode::Complete,
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("before:b:0"),
            String::from("after:a:0"),
        ]
    );
    let b = RepeatInterceptorId::new("b")?;
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            match failure.primary() {
                RepeatFailureCause::Callback(callback) => {
                    assert_eq!(callback.interceptor_id(), Some(&b));
                    assert_eq!(callback.phase(), RepeatCallbackPhase::Before);
                    assert_eq!(callback.kind(), RepeatCallbackFailureKind::Panic);
                }
                other => panic!("unexpected repeat primary: {other:?}"),
            }
            assert!(failure.secondary().is_empty());
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    assert!(!format!("{report:?}").contains("repeat-before-sensitive-payload"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn body_failure_remains_primary_when_reverse_unwind_also_fails() -> Result<(), Box<dyn Error>>
{
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-body-primary",
        1,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[
            ("a", InterceptorMode::FailAfter),
            ("b", InterceptorMode::PanicAfter),
        ],
        BodyMode::FailOn(0),
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 1);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("before:b:0"),
            String::from("body:0"),
            String::from("after:b:0"),
            String::from("after:a:0"),
        ]
    );
    let a = RepeatInterceptorId::new("a")?;
    let b = RepeatInterceptorId::new("b")?;
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            assert_eq!(
                failure.primary(),
                &RepeatFailureCause::Body(oxide_batch::TaskletFailure::Error)
            );
            assert_eq!(failure.secondary().len(), 2);
            assert_eq!(failure.secondary()[0].interceptor_id(), Some(&b));
            assert_eq!(failure.secondary()[0].phase(), RepeatCallbackPhase::After);
            assert_eq!(
                failure.secondary()[0].kind(),
                RepeatCallbackFailureKind::Panic
            );
            assert_eq!(failure.secondary()[1].interceptor_id(), Some(&a));
            assert_eq!(failure.secondary()[1].phase(), RepeatCallbackPhase::After);
            assert_eq!(
                failure.secondary()[1].kind(),
                RepeatCallbackFailureKind::Error
            );
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    assert!(!format!("{report:?}").contains("repeat-after-sensitive-payload"));
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn body_panic_remains_primary_and_does_not_publish_repeat_state() -> Result<(), Box<dyn Error>>
{
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-body-panic",
        1,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[("a", InterceptorMode::Pass)],
        BodyMode::PanicOn(0),
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 1);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("body:0"),
            String::from("after:a:0"),
        ]
    );
    assert_eq!(
        report.outcome(),
        &FlowExecutionOutcome::Failed(FlowFailure::Tasklet(oxide_batch::TaskletFailure::Panic))
    );
    assert!(!format!("{report:?}").contains("repeat-body-sensitive-payload"));
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn step_listener_wraps_the_entire_repeat_without_per_iteration_reentry()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let listener: Arc<dyn StepExecutionListener> = Arc::new(RecordingStepListener {
        events: Arc::clone(&events),
    });
    let job = job(
        "repeat-listener-order",
        2,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[("a", InterceptorMode::Pass)],
        BodyMode::Complete,
        Some(listener),
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("listener:before"),
            String::from("before:a:0"),
            String::from("body:0"),
            String::from("policy:0"),
            String::from("after:a:0"),
            String::from("before:a:1"),
            String::from("body:1"),
            String::from("policy:1"),
            String::from("after:a:1"),
            String::from("listener:after"),
        ]
    );
    Ok(())
}

async fn assert_policy_failure(
    mode: PolicyMode,
    expected_kind: RepeatCallbackFailureKind,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let job = job_with_policy(
        name,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::new(FailingPolicy {
            mode,
            events: Arc::clone(&events),
        }),
        &[("a", InterceptorMode::Pass)],
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:a:0"),
            String::from("body:0"),
            String::from("policy:0"),
            String::from("after:a:0"),
        ]
    );
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            match failure.primary() {
                RepeatFailureCause::Callback(callback) => {
                    assert_eq!(callback.interceptor_id(), None);
                    assert_eq!(callback.phase(), RepeatCallbackPhase::Policy);
                    assert_eq!(callback.kind(), expected_kind);
                }
                other => panic!("unexpected repeat primary: {other:?}"),
            }
            assert!(failure.secondary().is_empty());
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none(),
        "failed repeat policy must not publish durable repeat state"
    );
    assert!(!format!("{report:?}").contains("repeat-policy-sensitive-payload"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn policy_error_is_primary_and_unwinds_entered_interceptors() -> Result<(), Box<dyn Error>> {
    assert_policy_failure(
        PolicyMode::Fail,
        RepeatCallbackFailureKind::Error,
        "repeat-policy-error",
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn policy_panic_is_primary_redacted_and_unwinds_entered_interceptors()
-> Result<(), Box<dyn Error>> {
    assert_policy_failure(
        PolicyMode::Panic,
        RepeatCallbackFailureKind::Panic,
        "repeat-policy-panic",
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn after_panic_is_primary_when_body_and_policy_succeed() -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let job = job(
        "repeat-after-panic",
        1,
        &events,
        Arc::clone(&tasklet_calls),
        Arc::clone(&policy_calls),
        &[("a", InterceptorMode::PanicAfter)],
        BodyMode::Complete,
        None,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 1);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 1);
    let a = RepeatInterceptorId::new("a")?;
    match report.outcome() {
        FlowExecutionOutcome::Failed(FlowFailure::Repeat { failure, .. }) => {
            match failure.primary() {
                RepeatFailureCause::Callback(callback) => {
                    assert_eq!(callback.interceptor_id(), Some(&a));
                    assert_eq!(callback.phase(), RepeatCallbackPhase::After);
                    assert_eq!(callback.kind(), RepeatCallbackFailureKind::Panic);
                }
                other => panic!("unexpected repeat primary: {other:?}"),
            }
            assert!(failure.secondary().is_empty());
        }
        other => panic!("unexpected flow outcome: {other:?}"),
    }
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none()
    );
    assert!(!format!("{report:?}").contains("repeat-after-sensitive-payload"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stop_requested_by_before_unwinds_without_starting_body_or_committing_repeat()
-> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let (source, stop_token) = StopSource::new();
    let name = JobName::new("repeat-stop-before-body")?;
    let node = NodeId::new(NODE)?;
    let tasklet_step = TaskletStep::new(
        StepName::new(NODE)?,
        Arc::new(RecordingTasklet {
            calls: Arc::clone(&tasklet_calls),
            events: Arc::clone(&events),
            mode: BodyMode::Complete,
        }),
    );
    let registration = RepeatRuntimeRegistration::new(
        RepeatId::new(REPEAT)?,
        RepeatPolicyRegistration::new(
            RepeatPolicyKind::new(POLICY_KIND)?,
            ComponentRevision::new(POLICY_REVISION)?,
            RepeatPolicyConfiguration::new("limit-1")?,
            Arc::new(CountingPolicy {
                complete_after: 1,
                calls: Arc::clone(&policy_calls),
                events: Arc::clone(&events),
            }),
        ),
        vec![RepeatInterceptorRegistration::new(
            RepeatInterceptorId::new("stopper")?,
            RepeatInterceptorKind::new(INTERCEPTOR_KIND)?,
            ComponentRevision::new(INTERCEPTOR_REVISION)?,
            Arc::new(StoppingInterceptor {
                source,
                events: Arc::clone(&events),
            }),
        )],
    );
    let job = FlowJob::new(name.clone(), plan(&name, 1, &["stopper"])?)?
        .with_tasklet_step(node.clone(), tasklet_step)?
        .with_repeat_registration(node, registration)?;
    let (clock, ids, repository) = infrastructure();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop_token)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(tasklet_calls.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot(events.as_ref()),
        vec![
            String::from("before:stopper:0"),
            String::from("after:stopper:0:stopped"),
        ]
    );
    assert!(
        latest_repeat(&repository, report.instance().id())
            .await?
            .is_none(),
        "cooperative stop before body must not accept the iteration"
    );
    Ok(())
}
