//! A full-job test harness (`TEST-JOB-001`).

use oxide_batch::{
    ChunkJob, ChunkLaunchReport, Clock, IdGenerator, InMemoryJobRepository, ItemProcessor,
    ItemReader, ItemWriter, JobLauncher, JobParameters, LaunchError, StopSource, StopToken,
};

use crate::{DeterministicIds, EmbeddedRepository, ManualClock};

/// A full-job test harness that launches a
/// [`ChunkJob`](oxide_batch::ChunkJob) through the real, production
/// [`JobLauncher`](oxide_batch::JobLauncher).
///
/// `TestJob` is generic over its backing
/// [`JobRepository`](oxide_batch::JobRepository), so the same harness type
/// serves both the fast, isolated [`EmbeddedRepository`] path
/// ([`TestJob::embedded`]) and a durable adapter such as
/// [`crate::postgres::PostgresFixture`] under the `postgres` feature, which
/// [`crate::restart`] requires for a real inherited-progress restart.
///
/// It never reimplements launch semantics, the chunk runtime, commit
/// semantics, restart selection, or the `ItemStream` lifecycle: every call
/// goes through [`JobLauncher::launch_chunk`].
///
/// ```
/// use oxide_batch::{
///     ChunkJob, ChunkSize, DefinitionRevision, ItemProcessor, ItemReader, ItemWriter, JobName,
///     JobParameters, ProcessOutcome, ReadOutcome, StepName, WriteOutcome,
/// };
/// use oxide_batch_test::{NoCompletion, StandaloneTransactions, TestJob};
/// use oxide_batch_test::default_chunk_component_revisions;
/// use std::collections::VecDeque;
/// use std::sync::Arc;
///
/// struct Source(VecDeque<i64>);
/// impl ItemReader<i64> for Source {
///     async fn read(
///         &mut self,
///         _context: oxide_batch::ReadContext<'_>,
///     ) -> Result<ReadOutcome<i64>, oxide_batch::ReaderError> {
///         Ok(self.0.pop_front().map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
///     }
/// }
///
/// struct Identity;
/// impl ItemProcessor<i64, i64> for Identity {
///     async fn process(
///         &self,
///         item: &i64,
///         _context: oxide_batch::ProcessContext<'_>,
///     ) -> Result<ProcessOutcome<i64>, oxide_batch::ProcessorError> {
///         Ok(ProcessOutcome::Item(*item))
///     }
/// }
///
/// struct Sink;
/// impl ItemWriter<i64> for Sink {
///     async fn write(
///         &self,
///         _items: &[i64],
///         _context: oxide_batch::WriteContext<'_>,
///     ) -> Result<WriteOutcome, oxide_batch::WriterError> {
///         Ok(WriteOutcome::Written)
///     }
/// }
///
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// futures_executor::block_on(async {
///     let step = oxide_batch::ChunkStep::new(
///         StepName::new("load")?,
///         ChunkSize::new(2)?,
///         Source((0..5).collect()),
///         Identity,
///         Sink,
///         Arc::new(StandaloneTransactions),
///         Arc::new(NoCompletion),
///     );
///     let chunk_job = ChunkJob::new(
///         JobName::new("full_job_example")?,
///         step,
///         DefinitionRevision::new("full_job_example-v1")?,
///         &default_chunk_component_revisions(),
///     )?;
///     let mut job = TestJob::embedded(chunk_job);
///     let report = job.launch(&JobParameters::new()).await?;
///     assert!(report.launch().job_execution().id().get() > 0);
///     Ok::<(), Box<dyn std::error::Error>>(())
/// })
/// # }
/// # run().unwrap();
/// ```
pub struct TestJob<Repo, I, O, R, P, W> {
    job: ChunkJob<I, O, R, P, W>,
    repository: Repo,
    clock: ManualClock,
    ids: DeterministicIds,
}

impl<I, O, R, P, W> TestJob<InMemoryJobRepository, I, O, R, P, W>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    R: ItemReader<I> + Send + 'static,
    P: ItemProcessor<I, O> + Send + 'static,
    W: ItemWriter<O> + Send + 'static,
{
    /// Builds a full-job harness backed by a fresh, isolated
    /// [`EmbeddedRepository`].
    #[must_use]
    pub fn embedded(job: ChunkJob<I, O, R, P, W>) -> Self {
        Self::with_embedded(job, &EmbeddedRepository::new())
    }

    /// Builds a full-job harness over an explicit [`EmbeddedRepository`].
    #[must_use]
    pub fn with_embedded(job: ChunkJob<I, O, R, P, W>, embedded: &EmbeddedRepository) -> Self {
        Self::new(
            job,
            embedded.repository().clone(),
            embedded.clock().clone(),
            embedded.ids().clone(),
        )
    }
}

impl<Repo, I, O, R, P, W> TestJob<Repo, I, O, R, P, W>
where
    Repo: oxide_batch::JobRepository,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    R: ItemReader<I> + Send + 'static,
    P: ItemProcessor<I, O> + Send + 'static,
    W: ItemWriter<O> + Send + 'static,
{
    /// Builds a full-job harness over an explicit repository, clock, and ID
    /// source.
    #[must_use]
    pub const fn new(
        job: ChunkJob<I, O, R, P, W>,
        repository: Repo,
        clock: ManualClock,
        ids: DeterministicIds,
    ) -> Self {
        Self {
            job,
            repository,
            clock,
            ids,
        }
    }

    /// Borrows the harness's backing repository.
    #[must_use]
    pub const fn repository(&self) -> &Repo {
        &self.repository
    }

    /// Borrows the harness's deterministic clock.
    #[must_use]
    pub const fn clock(&self) -> &ManualClock {
        &self.clock
    }

    /// Borrows the harness's deterministic ID source.
    #[must_use]
    pub const fn ids(&self) -> &DeterministicIds {
        &self.ids
    }

    /// Borrows the wrapped production job definition.
    #[must_use]
    pub const fn job(&self) -> &ChunkJob<I, O, R, P, W> {
        &self.job
    }

    /// Launches (or restarts, if the same instance already has an
    /// unresolved or resumable execution) one attempt with an unrequested
    /// stop token.
    ///
    /// # Errors
    ///
    /// Returns [`LaunchError`] exactly as
    /// [`JobLauncher::launch_chunk`](oxide_batch::JobLauncher::launch_chunk)
    /// does.
    pub async fn launch(
        &mut self,
        parameters: &JobParameters,
    ) -> Result<ChunkLaunchReport, LaunchError> {
        let (_source, stop) = StopSource::new();
        self.launch_with_stop(parameters, &stop).await
    }

    /// Launches one attempt, observing the supplied stop token.
    ///
    /// Calling this again with the same identifying `parameters` starts a
    /// new execution attempt against the same job instance -- the real
    /// production restart path, reused rather than reimplemented.
    ///
    /// # Errors
    ///
    /// Returns [`LaunchError`] exactly as
    /// [`JobLauncher::launch_chunk`](oxide_batch::JobLauncher::launch_chunk)
    /// does.
    pub async fn launch_with_stop(
        &mut self,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<ChunkLaunchReport, LaunchError> {
        let launcher = JobLauncher::new(
            &self.repository as &dyn oxide_batch::JobRepository,
            &self.clock as &dyn Clock,
            &self.ids as &dyn IdGenerator,
        );
        launcher.launch_chunk(&mut self.job, parameters, stop).await
    }
}
