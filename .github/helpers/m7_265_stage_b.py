from pathlib import Path

path = Path('crates/oxide-batch/src/runtime.rs')
s = path.read_text()
s = s.replace(
    '    JobExecutionId, JobExecutionListener, JobInstance, JobInstanceKey, JobName, JobParameters,\n',
    '    JobExecutionId, JobExecutionListener, JobInstance, JobInstanceId, JobInstanceKey, JobName,\n    JobParameters,\n',
)
impl_start = s.index("impl<'a> JobLauncher<'a> {")
start = s.index('    pub async fn launch(\n', impl_start)
end = s.index('    async fn reload_step', start)
old = s[start:end]
rest_start = old.index('        self.emit_event(LifecycleEventKind::LaunchAccepted')
rest = old[rest_start:]
if not rest.endswith('    }\n\n'):
    raise SystemExit('unexpected JobLauncher::launch tail')
rest = rest[:-7]

replacement = '''    pub async fn launch(
        &self,
        job: &TaskletJob,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<LaunchReport, LaunchError> {
        self.ensure_accepting()?;
        let key = JobInstanceKey::new(job.name.clone(), parameters);
        let graph = self
            .create_execution_graph(&key, job.step.name(), job.definition_identity())
            .await?;
        self.launch_created(job, parameters, stop, graph).await
    }

    /// Executes an already-linked `STARTING` child attempt through the same
    /// lifecycle/listener/tasklet engine as an ordinary launch.
    pub(crate) async fn launch_precreated(
        &self,
        job: &TaskletJob,
        parameters: &JobParameters,
        stop: &StopToken,
        instance_id: JobInstanceId,
        execution_id: JobExecutionId,
    ) -> Result<LaunchReport, LaunchError> {
        self.ensure_accepting()?;
        let graph = self
            .prepare_precreated_execution_graph(
                job,
                parameters,
                instance_id,
                execution_id,
            )
            .await?;
        self.launch_created(job, parameters, stop, graph).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the launch method keeps the listener nesting and commit order visible"
    )]
    async fn launch_created(
        &self,
        job: &TaskletJob,
        parameters: &JobParameters,
        stop: &StopToken,
        graph: CreatedExecutionGraph,
    ) -> Result<LaunchReport, LaunchError> {
        let plan = job.compiled_plan();
''' + rest + '''    }

'''
s = s[:start] + replacement + s[end:]

anchor = '    async fn create_execution_graph(\n'
idx = s.index(anchor, s.index('    async fn reload_step', start))
precreated = '''    async fn prepare_precreated_execution_graph(
        &self,
        job: &TaskletJob,
        parameters: &JobParameters,
        instance_id: JobInstanceId,
        execution_id: JobExecutionId,
    ) -> Result<CreatedExecutionGraph, LaunchError> {
        let key = JobInstanceKey::new(job.name.clone(), parameters);
        let mut unit = self.repository.begin().await?;
        let instance = unit
            .get_job_instance(instance_id)
            .await?
            .ok_or(RepositoryError::JobInstanceNotFound { id: instance_id })?;
        if instance.key() != &key {
            unit.rollback().await?;
            return Err(RepositoryError::FlowStateCorrupt.into());
        }
        let mut job_execution = unit
            .get_job_execution(execution_id)
            .await?
            .ok_or(RepositoryError::JobExecutionNotFound { id: execution_id })?;
        if job_execution.job_instance_id() != instance_id
            || job_execution.metadata().status() != BatchStatus::Starting
        {
            unit.rollback().await?;
            return Err(RepositoryError::FlowStateCorrupt.into());
        }
        if let Some((owner, _)) = self.execution_control {
            job_execution = unit
                .claim_execution_owner(
                    job_execution.id(),
                    job_execution.version(),
                    &owner,
                    self.clock.now(),
                )
                .await?;
        }
        let existing_steps = unit.step_executions(execution_id).await?;
        let step_execution = match existing_steps.as_slice() {
            [] => unit
                .create_step_execution(execution_id, job.step.name())
                .await?,
            [step]
                if step.step_name() == job.step.name()
                    && step.metadata().status() == BatchStatus::Starting =>
            {
                step.clone()
            }
            _ => {
                unit.rollback().await?;
                return Err(RepositoryError::FlowStateCorrupt.into());
            }
        };
        let attempt_count = unit.job_executions(instance_id).await?.len();
        let attempt = u64::try_from(attempt_count)
            .ok()
            .and_then(NonZeroU64::new)
            .map(ExecutionAttempt::new)
            .ok_or(RepositoryError::Unavailable)?;
        unit.commit().await?;
        let correlation = ExecutionCorrelation::new(
            key.job_name().clone(),
            instance_id,
            execution_id,
            attempt,
            job.step.name().clone(),
            step_execution.id(),
            attempt,
        );
        Ok(CreatedExecutionGraph {
            instance,
            job_execution,
            step_execution,
            correlation,
        })
    }

'''
s = s[:idx] + precreated + s[idx:]
path.write_text(s)
