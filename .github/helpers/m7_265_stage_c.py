from pathlib import Path

path = Path('crates/oxide-batch/src/flow.rs')
s = path.read_text()

# Classify expected nested child failures without carrying mapped values.
s = s.replace(
    '    /// A per-child component factory panicked before tasklet invocation.\n    PartitionFactoryPanic,\n',
    '    /// A per-child component factory panicked before tasklet invocation.\n    PartitionFactoryPanic,\n    /// Deterministic nested-job parameter resolution failed before linkage.\n    NestedJobMapping {\n        /// Logical nested-job node.\n        node: NodeId,\n        /// Value-redacted mapping category.\n        failure: crate::NestedJobMappingFailure,\n    },\n    /// A linked nested child committed `FAILED`.\n    NestedJobChildFailed {\n        /// Logical nested-job node.\n        node: NodeId,\n    },\n',
)

# Private preparation error preserves the distinction between expected mapping
# rejection and infrastructure failure.
anchor = '/// Async-first launcher for durable sequential, conditional, and bounded split flows.\n'
private = '''enum NestedJobPrepareError {
    Repository(RepositoryError),
    Mapping(crate::NestedJobMappingFailure),
}

impl From<RepositoryError> for NestedJobPrepareError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

impl From<crate::nested_job_runtime::NestedJobResolutionError> for NestedJobPrepareError {
    fn from(error: crate::nested_job_runtime::NestedJobResolutionError) -> Self {
        match error {
            crate::nested_job_runtime::NestedJobResolutionError::Repository(error) => {
                Self::Repository(error)
            }
            crate::nested_job_runtime::NestedJobResolutionError::Mapping(error) => {
                Self::Mapping(error)
            }
        }
    }
}

'''
s = s.replace(anchor, private + anchor)

run_scope_pos = s.index('    fn run_scope<')
split_pos = s.index('                    FlowNode::Split(split) => {', run_scope_pos)
nested_arm = '''                    FlowNode::NestedJob(compiled) => {
                        let link = match self
                            .prepare_nested_job_link(
                                job,
                                compiled,
                                instance_id,
                                execution_id,
                                attempt,
                                parameters,
                            )
                            .await
                        {
                            Ok(link) => link,
                            Err(NestedJobPrepareError::Repository(error)) => {
                                return Err(error.into());
                            }
                            Err(NestedJobPrepareError::Mapping(failure_kind)) => {
                                let failure =
                                    self.next_failure_summary(FailureCategory::InvalidDefinition)?;
                                return Ok(ScopeRun::terminal(
                                    BatchStatus::Failed,
                                    ExitStatus::failed(),
                                    Some(failure),
                                    Some(FlowFailure::NestedJobMapping {
                                        node: node_id,
                                        failure: failure_kind,
                                    }),
                                    steps,
                                    decisions,
                                    states,
                                    listener_failures,
                                ));
                            }
                        };
                        let terminal_link = self
                            .run_or_observe_nested_job(job, compiled, link, stop_token)
                            .await?;
                        let Some(terminal_link) = terminal_link else {
                            return Ok(ScopeRun::terminal(
                                BatchStatus::Unknown,
                                ExitStatus::unknown(),
                                None,
                                None,
                                steps,
                                decisions,
                                states,
                                listener_failures,
                            ));
                        };
                        let terminal = terminal_link.terminal().ok_or(
                            FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt),
                        )?;
                        match terminal.status() {
                            BatchStatus::Completed => {
                                let digest = nested_job_input_digest(
                                    job.plan.fingerprint(),
                                    &node_id,
                                    &terminal_link,
                                );
                                let reused = self
                                    .reusable_decision(
                                        instance_id,
                                        &node_id,
                                        job.plan.fingerprint(),
                                        &digest,
                                        FlowTransitionKind::NestedJobExit,
                                    )
                                    .await?;
                                (
                                    node_id.clone(),
                                    terminal.exit_status().clone(),
                                    None,
                                    FlowTransitionKind::NestedJobExit,
                                    digest,
                                    reused.map(|decision| decision.id()),
                                    None,
                                )
                            }
                            BatchStatus::Failed => {
                                let failure =
                                    self.next_failure_summary(FailureCategory::UserComponent)?;
                                return Ok(ScopeRun::terminal(
                                    BatchStatus::Failed,
                                    ExitStatus::failed(),
                                    Some(failure),
                                    Some(FlowFailure::NestedJobChildFailed { node: node_id }),
                                    steps,
                                    decisions,
                                    states,
                                    listener_failures,
                                ));
                            }
                            BatchStatus::Stopped => {
                                return Ok(ScopeRun::terminal(
                                    BatchStatus::Stopped,
                                    ExitStatus::stopped(),
                                    None,
                                    None,
                                    steps,
                                    decisions,
                                    states,
                                    listener_failures,
                                ));
                            }
                            _ => {
                                return Err(FlowRuntimeError::Repository(
                                    RepositoryError::FlowStateCorrupt,
                                ));
                            }
                        }
                    }
'''
s = s[:split_pos] + nested_arm + s[split_pos:]

# Now that NestedJob is handled explicitly, retain fail-closed handling for Join
# and any future node variant only.
s = s.replace(
    '                    FlowNode::Join(_) | _ => {\n',
    '                    FlowNode::Join(_) | _ => {\n',
    1,
)

# Repository/link helpers live in the same launcher and never spawn detached work.
insert = s.index('    #[allow(clippy::too_many_arguments)]\n    async fn run_split(', run_scope_pos)
helpers = r'''    #[allow(clippy::too_many_arguments)]
    async fn prepare_nested_job_link(
        &self,
        job: &FlowJob,
        compiled: &crate::NestedJobNode,
        parent_instance_id: JobInstanceId,
        parent_execution_id: JobExecutionId,
        attempt: ExecutionAttempt,
        parent_parameters: &JobParameters,
    ) -> Result<crate::NestedJobLink, NestedJobPrepareError> {
        if let Some(link) = self
            .read_nested_job_link(parent_execution_id, compiled.id())
            .await?
        {
            return Ok(link);
        }
        if let Some(prior) = self
            .read_latest_nested_job_link(parent_instance_id, compiled.id())
            .await?
        {
            return self
                .continue_nested_job_link(
                    parent_execution_id,
                    compiled.id(),
                    prior.parent_job_execution_id(),
                )
                .await
                .map_err(NestedJobPrepareError::Repository);
        }

        let child_parameters = crate::nested_job_runtime::resolve_nested_job_parameters(
            self.repository,
            job.compiled_plan(),
            compiled,
            parent_instance_id,
            parent_execution_id,
            attempt,
            parent_parameters,
        )
        .await?;
        let request = crate::NestedJobLinkRequest::new(
            parent_instance_id,
            parent_execution_id,
            compiled.id().clone(),
            compiled.child_definition().clone(),
            child_parameters,
            self.clock.now(),
        );
        self.create_nested_job_link(&request)
            .await
            .map_err(NestedJobPrepareError::Repository)
    }

    async fn read_nested_job_link(
        &self,
        parent_execution_id: JobExecutionId,
        node_id: &NodeId,
    ) -> Result<Option<crate::NestedJobLink>, RepositoryError> {
        let mut unit = self.repository.begin().await?;
        let link = unit.nested_job_link(parent_execution_id, node_id).await?;
        unit.rollback().await?;
        Ok(link)
    }

    async fn read_latest_nested_job_link(
        &self,
        parent_instance_id: JobInstanceId,
        node_id: &NodeId,
    ) -> Result<Option<crate::NestedJobLink>, RepositoryError> {
        let mut unit = self.repository.begin().await?;
        let link = unit
            .latest_nested_job_link(parent_instance_id, node_id)
            .await?;
        unit.rollback().await?;
        Ok(link)
    }

    async fn create_nested_job_link(
        &self,
        request: &crate::NestedJobLinkRequest,
    ) -> Result<crate::NestedJobLink, RepositoryError> {
        let mut unit = self.repository.begin().await?;
        let proposed = unit.create_nested_job_link(request).await?;
        match unit.commit().await {
            Ok(()) => Ok(proposed),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self
                    .read_nested_job_link(
                        request.parent_job_execution_id(),
                        request.node_id(),
                    )
                    .await?;
                durable
                    .filter(|link| link == &proposed)
                    .ok_or(RepositoryError::CommitOutcomeUnknown)
            }
            Err(error) => Err(error),
        }
    }

    async fn continue_nested_job_link(
        &self,
        parent_execution_id: JobExecutionId,
        node_id: &NodeId,
        prior_parent_execution_id: JobExecutionId,
    ) -> Result<crate::NestedJobLink, RepositoryError> {
        let mut unit = self.repository.begin().await?;
        let proposed = unit
            .continue_nested_job_link(
                parent_execution_id,
                node_id,
                prior_parent_execution_id,
                self.clock.now(),
            )
            .await?;
        match unit.commit().await {
            Ok(()) => Ok(proposed),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self
                    .read_nested_job_link(parent_execution_id, node_id)
                    .await?;
                durable
                    .filter(|link| link == &proposed)
                    .ok_or(RepositoryError::CommitOutcomeUnknown)
            }
            Err(error) => Err(error),
        }
    }

    async fn nested_job_parameters(
        &self,
        parent_execution_id: JobExecutionId,
        node_id: &NodeId,
    ) -> Result<JobParameters, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let parameters = unit
            .nested_job_parameters(parent_execution_id, node_id)
            .await?;
        unit.rollback().await?;
        Ok(parameters)
    }

    async fn load_job_execution(
        &self,
        execution_id: JobExecutionId,
    ) -> Result<JobExecution, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let execution = unit
            .get_job_execution(execution_id)
            .await?
            .ok_or(RepositoryError::JobExecutionNotFound { id: execution_id })?;
        unit.rollback().await?;
        Ok(execution)
    }

    async fn observe_nested_job_terminal(
        &self,
        parent_execution_id: JobExecutionId,
        node_id: &NodeId,
    ) -> Result<crate::NestedJobLink, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let proposed = unit
            .observe_nested_job_terminal(parent_execution_id, node_id, self.clock.now())
            .await?;
        match unit.commit().await {
            Ok(()) => Ok(proposed),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self
                    .read_nested_job_link(parent_execution_id, node_id)
                    .await?
                    .filter(|link| link == &proposed)
                    .ok_or(RepositoryError::CommitOutcomeUnknown)?;
                Ok(durable)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn run_or_observe_nested_job(
        &self,
        job: &FlowJob,
        compiled: &crate::NestedJobNode,
        link: crate::NestedJobLink,
        stop_token: &StopToken,
    ) -> Result<Option<crate::NestedJobLink>, FlowRuntimeError> {
        if link.terminal().is_some() {
            return Ok(Some(link));
        }
        if link.child_definition() != compiled.child_definition() {
            return Err(RepositoryError::FlowStateCorrupt.into());
        }
        let child_execution = self
            .load_job_execution(link.child_job_execution_id())
            .await?;
        match child_execution.metadata().status() {
            BatchStatus::Starting => {
                let parameters = self
                    .nested_job_parameters(link.parent_job_execution_id(), compiled.id())
                    .await?;
                let binding = job.nested_jobs.get(compiled.id()).ok_or_else(|| {
                    FlowRuntimeError::Job(FlowJobError::MissingBinding {
                        node: compiled.id().clone(),
                    })
                })?;
                match binding {
                    NestedJobBinding::Tasklet(child) => {
                        let mut launcher = crate::JobLauncher::new(
                            self.repository,
                            self.clock,
                            self.ids,
                        );
                        if let Some((owner, interval)) = self.execution_control {
                            launcher = launcher.with_execution_control(owner, interval);
                        }
                        if let Some(signal) = self.shutdown_signal {
                            launcher = launcher.with_shutdown_signal(signal);
                        }
                        launcher
                            .launch_precreated(
                                child.as_ref(),
                                &parameters,
                                stop_token,
                                link.child_job_instance_id(),
                                link.child_job_execution_id(),
                            )
                            .await
                            .map_err(nested_launch_error)?;
                    }
                }
            }
            BatchStatus::Completed | BatchStatus::Failed | BatchStatus::Stopped => {}
            BatchStatus::Unknown | BatchStatus::Started | BatchStatus::Stopping => {
                return Ok(None);
            }
            _ => return Err(RepositoryError::FlowStateCorrupt.into()),
        }

        let child_execution = self
            .load_job_execution(link.child_job_execution_id())
            .await?;
        match child_execution.metadata().status() {
            BatchStatus::Completed | BatchStatus::Failed | BatchStatus::Stopped => self
                .observe_nested_job_terminal(link.parent_job_execution_id(), compiled.id())
                .await
                .map(Some),
            BatchStatus::Unknown | BatchStatus::Started | BatchStatus::Stopping => Ok(None),
            _ => Err(RepositoryError::FlowStateCorrupt.into()),
        }
    }

'''
s = s[:insert] + helpers + s[insert:]

# Stable digest for completed-child transition reuse. It contains only durable
# identities/status/exit code, never child parameters.
fn_anchor = 'fn split_input_digest(\n'
fn_pos = s.index(fn_anchor)
digest = r'''fn nested_job_input_digest(
    fingerprint: &[u8; 32],
    node_id: &NodeId,
    link: &crate::NestedJobLink,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"oxide-batch:nested-job-exit:v1");
    hasher.update(fingerprint);
    hasher.update(node_id.as_str().as_bytes());
    hasher.update(link.child_job_instance_id().get().to_le_bytes());
    hasher.update(link.child_job_execution_id().get().to_le_bytes());
    if let Some(terminal) = link.terminal() {
        hasher.update(terminal.status().as_str().as_bytes());
        hasher.update(terminal.exit_status().code().as_str().as_bytes());
    }
    hasher.finalize().into()
}

fn nested_launch_error(error: crate::LaunchError) -> FlowRuntimeError {
    match error {
        crate::LaunchError::Repository(error) => FlowRuntimeError::Repository(error),
        crate::LaunchError::ShuttingDown => FlowRuntimeError::ShuttingDown,
        _ => FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt),
    }
}

'''
s = s[:fn_pos] + digest + s[fn_pos:]

path.write_text(s)
