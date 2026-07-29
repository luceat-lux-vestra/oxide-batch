//! Runs the smallest complete in-memory `OxideBatch` job.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;

use oxide_batch::{
    BoxFuture, InMemoryJobRepository, JobLauncher, JobName, JobParameter, JobParameters,
    ParameterName, ParameterRole, ParameterValue, SequentialIdGenerator, StopSource, SystemClock,
    Tasklet, TaskletContext, TaskletError, TaskletExecutionOutcome, TaskletJob, TaskletOutcome,
    TaskletStep,
};

struct ImportTasklet;

impl Tasklet for ImportTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let parameter_count = context.parameters().len();
            println!("importing with {parameter_count} launch parameter");
            Ok(TaskletOutcome::Completed)
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = JobLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    let step = TaskletStep::new(
        oxide_batch::StepName::new("import")?,
        Arc::new(ImportTasklet),
    );
    let job = TaskletJob::new(JobName::new("daily_import")?, step);
    let parameters = JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(
            ParameterValue::string("2026-07-29")?,
            ParameterRole::Identifying,
        ),
    )])?;
    let (_stop_source, stop_token) = StopSource::new();

    let report = launcher.launch(&job, &parameters, &stop_token).await?;

    assert_eq!(report.outcome(), TaskletExecutionOutcome::Completed);
    println!(
        "job execution {} completed with status {}",
        report.job_execution().id(),
        report.job_execution().metadata().status()
    );
    Ok(())
}
