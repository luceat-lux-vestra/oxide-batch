//! The bounded, redacted metadata inspection service.
//!
//! The service owns page bounds, cursor identity, traversal ceilings, and the
//! encoded response bound. The adapter owns one statement per page. The
//! projections, queries, cursors, and the port itself live in
//! `oxide-batch-repository`.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    ExplorerError, ExplorerQuery, ExplorerRepository, FlowDecision, JobExecutionId,
    JobExecutionProjection, JobInstanceId, JobInstanceProjection, JobName, MIN_UNRESOLVED_AGE,
    OperatorRecord, Page, PageRequest, QueryWindow, RecoveryDecision, StepExecutionId,
    StepExecutionProjection, StepPartitionProjection, TelemetryEventSink, TelemetryRecord,
};
use oxide_batch_repository::{page, resume_window, start_window};

/// The portable bounded inspection service.
///
/// The service owns page bounds, cursor identity, traversal ceilings, and the
/// encoded response bound. The adapter owns one statement per page.
#[derive(Clone)]
pub struct JobExplorer<S> {
    source: S,
    event_sinks: Vec<Arc<dyn TelemetryEventSink>>,
}

impl<S: fmt::Debug> fmt::Debug for JobExplorer<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobExplorer")
            .field("source", &self.source)
            .field("event_sinks", &self.event_sinks.len())
            .finish()
    }
}

impl<S: ExplorerRepository> JobExplorer<S> {
    /// Wraps one bounded read port.
    pub const fn new(source: S) -> Self {
        Self {
            source,
            event_sinks: Vec::new(),
        }
    }

    /// Attaches a non-authoritative, panic-isolated telemetry sink.
    #[must_use]
    pub fn with_event_sink(mut self, sink: Arc<dyn TelemetryEventSink>) -> Self {
        self.event_sinks.push(sink);
        self
    }

    /// Borrows the underlying read port.
    pub const fn source(&self) -> &S {
        &self.source
    }

    /// Lists registered job names in byte order.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_job_names(
        &self,
        request: &PageRequest,
    ) -> Result<Page<JobName>, ExplorerError> {
        let query = ExplorerQuery::JobNames;
        let window = self.window(&query, request).await?;
        let rows = self.source.job_names(&window).await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists instances of one job name, newest identity first.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_instances(
        &self,
        job_name: &JobName,
        request: &PageRequest,
    ) -> Result<Page<JobInstanceProjection>, ExplorerError> {
        let query = ExplorerQuery::Instances {
            job_name: job_name.clone(),
        };
        let window = self.window(&query, request).await?;
        let rows = self.source.instances(job_name, &window).await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists executions of one instance, newest attempt first.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_executions(
        &self,
        job_instance_id: JobInstanceId,
        request: &PageRequest,
    ) -> Result<Page<JobExecutionProjection>, ExplorerError> {
        let query = ExplorerQuery::Executions { job_instance_id };
        let window = self.window(&query, request).await?;
        let rows = self.source.executions(job_instance_id, &window).await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Reads one execution projection.
    ///
    /// # Errors
    ///
    /// Returns a typed timeout or repository failure.
    pub async fn get_execution(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Option<JobExecutionProjection>, ExplorerError> {
        self.source.execution(job_execution_id).await
    }

    /// Lists step executions of one job execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_step_executions(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<StepExecutionProjection>, ExplorerError> {
        let query = ExplorerQuery::StepExecutions { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .step_executions(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    /// Lists non-terminal executions older than an explicit age bound.
    ///
    /// # Errors
    ///
    /// Returns [`ExplorerError::AgeBoundTooSmall`] below [`MIN_UNRESOLVED_AGE`],
    /// or a typed cursor, bound, timeout, or repository failure.
    pub async fn list_unresolved_executions(
        &self,
        minimum_age: Duration,
        request: &PageRequest,
    ) -> Result<Page<JobExecutionProjection>, ExplorerError> {
        if minimum_age < MIN_UNRESOLVED_AGE {
            return Err(ExplorerError::AgeBoundTooSmall {
                minimum: MIN_UNRESOLVED_AGE,
            });
        }
        let query = ExplorerQuery::UnresolvedExecutions { minimum_age };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .unresolved_executions(minimum_age, &window)
            .await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists recovery decisions of one job execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_recovery_decisions(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<RecoveryDecision>, ExplorerError> {
        let query = ExplorerQuery::RecoveryDecisions { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .recovery_decisions(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    /// Lists flow decisions of one job execution in sequence order.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_flow_decisions(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<FlowDecision>, ExplorerError> {
        let query = ExplorerQuery::FlowDecisions { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .flow_decisions(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    /// Lists partitions of one partitioned step execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_step_partitions(
        &self,
        step_execution_id: StepExecutionId,
        request: &PageRequest,
    ) -> Result<Page<StepPartitionProjection>, ExplorerError> {
        let query = ExplorerQuery::StepPartitions { step_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .step_partitions(step_execution_id, &window)
            .await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists audited operator requests for one job execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_operator_requests(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<OperatorRecord>, ExplorerError> {
        let query = ExplorerQuery::OperatorRequests { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .operator_requests(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    fn finish_page<T>(
        &self,
        execution_id: Option<JobExecutionId>,
        result: Result<Page<T>, ExplorerError>,
    ) -> Result<Page<T>, ExplorerError> {
        if result.is_ok() {
            let record = TelemetryRecord::explorer(execution_id);
            for sink in &self.event_sinks {
                crate::telemetry::emit_safely(Some(sink), &record);
            }
        }
        result
    }

    async fn window(
        &self,
        query: &ExplorerQuery,
        request: &PageRequest,
    ) -> Result<QueryWindow, ExplorerError> {
        match request.cursor() {
            None => {
                let ceiling = self.source.identity_ceiling(query).await?;
                Ok(start_window(request, ceiling))
            }
            Some(cursor) => resume_window(cursor, query, request),
        }
    }
}
