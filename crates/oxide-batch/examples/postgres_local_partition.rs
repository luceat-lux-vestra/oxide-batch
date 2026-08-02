//! Operates one bounded local-partition job against `PostgreSQL`.
//!
//! This is an application-owned definition. The `oxide-batch` operator CLI
//! can inspect and recover its metadata, but it does not discover or load this
//! Rust job definition.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;

use oxide_batch::{
    BatchStatus, BoxFuture, ComponentRevision, DefinitionRevision, ExecutionContext,
    FailureCategory, FailureId, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    JobInstanceKey, JobName, JobParameters, JobRepository, NodeId, PartitionBudget, PartitionCount,
    PartitionFactoryError, PartitionKey, PartitionPlanEntry, PartitionPlanFactory,
    PartitionTaskletFactory, PostgresConfig, PostgresJobRepository, PostgresMigrator,
    RecoveryRequest, SequentialIdGenerator, StateLimits, StepComponents, StepName, StepNode,
    StopSource, SystemClock, Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep,
    TerminalKind, TlsMode,
};

const JOB: &str = "postgres_local_partition_example";
const RUNTIME_URL: &str = "OXIDEBATCH_EXAMPLE_RUNTIME_URL";
const MIGRATOR_URL: &str = "OXIDEBATCH_EXAMPLE_MIGRATOR_URL";

struct PrintTasklet {
    key: String,
}

impl Tasklet for PrintTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            println!("executed partition {}", self.key);
            Ok(TaskletOutcome::Completed)
        })
    }
}

struct InterruptTasklet;

impl Tasklet for InterruptTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async {
            eprintln!("interrupting after the first durable partition commit");
            std::process::exit(75)
        })
    }
}

fn local_config(variable: &str) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(std::env::var(variable)?)?.with_tls_mode(TlsMode::Plaintext))
}

fn entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\"schema\":\"example.partition\",\"schema_version\":1,\"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

fn job(interrupt: bool) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(JOB)?;
    let manager = NodeId::new("partitioned")?;
    let worker_name = StepName::new("partition-worker")?;
    let worker = StepNode::new(
        NodeId::new("partition-worker")?,
        worker_name.clone(),
        StepComponents::Tasklet(ComponentRevision::new("example-worker-v1")?),
    );
    let plan = FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(
            oxide_batch::PartitionedStepNode::new(
                manager.clone(),
                StepName::new("partitioned")?,
                worker,
                ComponentRevision::new("example-partitioner-v1")?,
                ComponentRevision::new("example-aggregate-v1")?,
                PartitionCount::new(3)?,
                PartitionBudget::new(1, 2)?,
            ),
        ))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("example-v1")?)?;
    let entries = vec![entry("alpha")?, entry("beta")?, entry("gamma")?];
    let partitioner = PartitionPlanFactory::new(move |request| {
        if request.partition_count().get() != 3 {
            return Err(PartitionFactoryError::Rejected);
        }
        Ok(entries.clone())
    });
    let factory_name = worker_name.clone();
    let worker_factory = PartitionTaskletFactory::new(worker_name, move |input| {
        let key = input.key().as_str().to_owned();
        let tasklet: Arc<dyn Tasklet> = if interrupt && key == "beta" {
            Arc::new(InterruptTasklet)
        } else {
            Arc::new(PrintTasklet { key })
        };
        TaskletStep::new(factory_name.clone(), tasklet)
    });
    Ok(FlowJob::new(name, plan)?.with_partitioned_tasklet(manager, partitioner, worker_factory)?)
}

async fn repository() -> Result<PostgresJobRepository, Box<dyn Error>> {
    Ok(PostgresJobRepository::connect(local_config(RUNTIME_URL)?, Arc::new(SystemClock)).await?)
}

async fn launch(interrupt: bool) -> Result<(), Box<dyn Error>> {
    let repository = repository().await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, &SystemClock, &ids)
        .launch(&job(interrupt)?, &JobParameters::new(), &stop)
        .await?;
    println!(
        "job execution {} ended as {}",
        report.job_execution().id(),
        report.job_execution().metadata().status()
    );
    repository.close().await?;
    Ok(())
}

async fn latest(
    repository: &PostgresJobRepository,
) -> Result<Option<oxide_batch::JobExecution>, Box<dyn Error>> {
    let key = JobInstanceKey::new(JobName::new(JOB)?, &JobParameters::new());
    let mut unit = repository.begin().await?;
    let execution = if let Some(instance) = unit.find_job_instance(&key).await? {
        unit.job_executions(instance.id()).await?.into_iter().last()
    } else {
        None
    };
    unit.rollback().await?;
    Ok(execution)
}

async fn inspect() -> Result<(), Box<dyn Error>> {
    let repository = repository().await?;
    let execution = latest(&repository)
        .await?
        .ok_or("the example has no durable execution")?;
    let mut unit = repository.begin().await?;
    let steps = unit.step_executions(execution.id()).await?;
    println!(
        "execution {} status {} version {}",
        execution.id(),
        execution.metadata().status(),
        execution.version().get()
    );
    for step in &steps {
        if step.step_name().as_str() != "partitioned" {
            continue;
        }
        for partition in unit.step_partition_plan(step.id()).await? {
            println!(
                "partition {} status {} worker {:?}",
                partition.key(),
                partition.status(),
                partition.worker_step_execution_id()
            );
        }
    }
    unit.rollback().await?;
    repository.close().await?;
    Ok(())
}

async fn recover() -> Result<(), Box<dyn Error>> {
    let repository = repository().await?;
    let execution = latest(&repository)
        .await?
        .ok_or("the example has no durable execution")?;
    if !matches!(
        execution.metadata().status(),
        BatchStatus::Starting | BatchStatus::Started | BatchStatus::Stopping | BatchStatus::Unknown
    ) {
        return Err("the latest execution does not require recovery".into());
    }
    let request = RecoveryRequest::mark_failed(
        execution.version(),
        "EXAMPLE_PROCESS_INTERRUPTION_INSPECTED",
        "postgres-local-partition-example",
        [75; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(75)?,
    )?;
    let mut unit = repository.begin().await?;
    let recovered = unit.recover_job_execution(execution.id(), &request).await?;
    unit.commit().await?;
    println!(
        "execution {} recovered as {}",
        execution.id(),
        recovered.decision().resulting_status()
    );
    repository.close().await?;
    Ok(())
}

fn usage() {
    eprintln!("usage: postgres_local_partition <migrate|launch|inspect|interrupt|recover|restart>");
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(command) = std::env::args().nth(1) else {
        usage();
        return Err("a command is required".into());
    };
    match command.as_str() {
        "migrate" => PostgresMigrator::migrate(&local_config(MIGRATOR_URL)?).await?,
        "launch" | "restart" => launch(false).await?,
        "interrupt" => launch(true).await?,
        "inspect" => inspect().await?,
        "recover" => recover().await?,
        _ => {
            usage();
            return Err("unknown command".into());
        }
    }
    Ok(())
}
