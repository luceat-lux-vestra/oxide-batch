//! A single-step test harness (`TEST-STEP-001`).

use std::num::NonZeroU64;
use std::sync::Arc;

use oxide_batch::{
    ChunkCompletion, ChunkExecutionReport, ChunkListener, ChunkSize, ChunkStep,
    ChunkTransactionManager, ComponentStreamIdentity, ExecutionAttempt, ExecutionCorrelation,
    ItemListenerSet, ItemProcessor, ItemReader, ItemStream, ItemWriter, JobName,
    StepExecutionListener, StepName, StopToken, StopSource, StreamStateContract,
};

use crate::DeterministicIds;
use crate::transactions::{NoCompletion, StandaloneTransactions};

const DEFAULT_JOB_NAME: &str = "oxide_batch_test_single_step";

#[allow(
    clippy::unwrap_used,
    reason = "fixed valid literals and a fresh deterministic sequence cannot fail"
)]
fn default_correlation(step_name: &StepName, ids: &DeterministicIds) -> ExecutionCorrelation {
    let job_name = JobName::new(DEFAULT_JOB_NAME).unwrap();
    let job_instance = ids.next_job_instance().unwrap();
    let job_execution = ids.next_job_execution().unwrap();
    let step_execution = ids.next_step_execution().unwrap();
    let attempt = ExecutionAttempt::new(NonZeroU64::MIN);
    ExecutionCorrelation::new(
        job_name,
        job_instance,
        job_execution,
        attempt,
        step_name.clone(),
        step_execution,
        attempt,
    )
}

/// A single-step chunk test harness that drives a
/// [`ChunkStep`](oxide_batch::ChunkStep) through its real, production
/// [`execute`](oxide_batch::ChunkStep::execute) path.
///
/// `TestStep` builds valid fixture context around one real step -- a
/// deterministic [`ExecutionCorrelation`], a [`StandaloneTransactions`]
/// manager, and a [`NoCompletion`] observer by default -- without a full
/// job/repository graph. It never fakes a successful step by invoking the
/// reader, processor, or writer independently of the real
/// [`ChunkStep::execute`](oxide_batch::ChunkStep::execute) driver.
///
/// ```
/// use oxide_batch::{
///     ChunkSize, ItemProcessor, ItemReader, ItemWriter, ProcessOutcome, ReadOutcome, WriteOutcome,
/// };
/// use oxide_batch_test::TestStep;
/// use std::collections::VecDeque;
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
/// struct Double;
/// impl ItemProcessor<i64, i64> for Double {
///     async fn process(
///         &self,
///         item: &i64,
///         _context: oxide_batch::ProcessContext<'_>,
///     ) -> Result<ProcessOutcome<i64>, oxide_batch::ProcessorError> {
///         Ok(ProcessOutcome::Item(item * 2))
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
///     let name = oxide_batch::StepName::new("double_step")?;
///     let mut step = TestStep::new(name, ChunkSize::new(2)?, Source((0..5).collect()), Double, Sink);
///     let report = step.run().await;
///     assert_eq!(report.outcome(), oxide_batch::ChunkExecutionOutcome::Completed);
///     Ok::<(), Box<dyn std::error::Error>>(())
/// })
/// # }
/// # run().unwrap();
/// ```
pub struct TestStep<I, O, R, P, W> {
    inner: ChunkStep<I, O, R, P, W>,
    correlation: ExecutionCorrelation,
    ids: DeterministicIds,
}

impl<I, O, R, P, W> TestStep<I, O, R, P, W>
where
    I: Send + Sync,
    O: Send + Sync,
    R: ItemReader<I>,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    /// Builds a single-step harness with a standalone transaction manager, a
    /// no-op completion observer, and a deterministic correlation/ID source.
    #[must_use]
    pub fn new(name: StepName, size: ChunkSize, reader: R, processor: P, writer: W) -> Self {
        Self::with_transactions(
            name,
            size,
            reader,
            processor,
            writer,
            Arc::new(StandaloneTransactions),
            Arc::new(NoCompletion),
        )
    }

    /// Builds a single-step harness over an explicit transaction manager and
    /// completion observer, e.g. a durable adapter under test.
    #[must_use]
    pub fn with_transactions(
        name: StepName,
        size: ChunkSize,
        reader: R,
        processor: P,
        writer: W,
        transactions: Arc<dyn ChunkTransactionManager>,
        completion: Arc<dyn ChunkCompletion>,
    ) -> Self {
        let ids = DeterministicIds::new(NonZeroU64::MIN);
        let correlation = default_correlation(&name, &ids);
        let inner = ChunkStep::new(name, size, reader, processor, writer, transactions, completion);
        Self {
            inner,
            correlation,
            ids,
        }
    }

    /// Registers a chunk listener in deterministic before-order.
    #[must_use]
    pub fn with_chunk_listener(mut self, listener: Arc<dyn ChunkListener>) -> Self {
        self.inner = self.inner.with_chunk_listener(listener);
        self
    }

    /// Installs item, retry, and skip listener families.
    #[must_use]
    pub fn with_item_listeners(mut self, listeners: ItemListenerSet<I, O>) -> Self {
        self.inner = self.inner.with_item_listeners(listeners);
        self
    }

    /// Registers a step listener in deterministic before-order.
    #[must_use]
    pub fn with_listener(mut self, listener: Arc<dyn StepExecutionListener>) -> Self {
        self.inner = self.inner.with_listener(listener);
        self
    }

    /// Registers one namespaced `ItemStream` in deterministic registration
    /// order.
    #[must_use]
    pub fn with_item_stream(
        mut self,
        identity: ComponentStreamIdentity,
        stream: impl ItemStream + 'static,
        contract: StreamStateContract,
    ) -> Self {
        self.inner = self.inner.with_item_stream(identity, stream, contract);
        self
    }

    /// Overrides the deterministic execution correlation.
    #[must_use]
    pub fn with_correlation(mut self, correlation: ExecutionCorrelation) -> Self {
        self.correlation = correlation;
        self
    }

    /// Borrows the harness's deterministic ID source.
    #[must_use]
    pub const fn ids(&self) -> &DeterministicIds {
        &self.ids
    }

    /// Runs the step to completion with an unrequested stop token.
    pub async fn run(&mut self) -> ChunkExecutionReport {
        let (_source, stop) = StopSource::new();
        self.run_with_stop(&stop).await
    }

    /// Runs the step to completion, observing the supplied stop token.
    pub async fn run_with_stop(&mut self, stop: &StopToken) -> ChunkExecutionReport {
        self.inner.execute(&self.correlation, stop).await
    }
}
