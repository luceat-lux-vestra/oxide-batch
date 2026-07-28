//! Async extension and blocking-isolation spike.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::FutureExt;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};

/// A dyn-compatible, runtime-neutral future in the public extension contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors normalized at the user-component boundary.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionError {
    /// The component returned a classified failure.
    #[error("the user component failed")]
    Component,
    /// The component panicked. Panic payloads are intentionally not exposed.
    #[error("the user component panicked")]
    Panic,
    /// A stop was observed before work started.
    #[error("stop requested before the operation started")]
    Stopped,
    /// The runtime could not join an isolated worker.
    #[error("the isolated worker could not be joined")]
    WorkerLost,
}

#[derive(Debug)]
struct StopState {
    requested: AtomicBool,
    notify: Notify,
}

/// The owner used by the runtime to request cooperative stopping.
#[derive(Clone, Debug)]
pub struct StopSource {
    state: Arc<StopState>,
}

/// A cloneable, framework-owned stop token passed to user components.
#[derive(Clone, Debug)]
pub struct StopToken {
    state: Arc<StopState>,
}

impl StopSource {
    /// Creates a stop source and its corresponding user-facing token.
    #[must_use]
    pub fn new() -> (Self, StopToken) {
        let state = Arc::new(StopState {
            requested: AtomicBool::new(false),
            notify: Notify::new(),
        });
        (
            Self {
                state: Arc::clone(&state),
            },
            StopToken { state },
        )
    }

    /// Requests a cooperative stop and wakes current waiters.
    pub fn request_stop(&self) {
        self.state.requested.store(true, Ordering::Release);
        self.state.notify.notify_waiters();
    }
}

impl StopToken {
    /// Returns whether a stop has already been requested.
    #[must_use]
    pub fn is_stop_requested(&self) -> bool {
        self.state.requested.load(Ordering::Acquire)
    }

    /// Waits until a stop is requested without exposing a Tokio type.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.notify.notified();
            if self.is_stop_requested() {
                return;
            }
            notified.await;
        }
    }
}

/// A dynamically dispatchable asynchronous processor.
///
/// The explicit boxed future keeps the trait dyn-compatible and permits the
/// future to borrow both the component and call-scoped arguments.
pub trait AsyncProcessor: Send + Sync {
    /// Processes one item.
    fn process<'a>(
        &'a self,
        item: &'a str,
        stop: &'a StopToken,
    ) -> BoxFuture<'a, Result<String, ExecutionError>>;
}

/// Invokes async user code behind a panic boundary.
///
/// # Errors
///
/// Returns the component's classified failure or [`ExecutionError::Panic`] if
/// the component unwinds.
pub async fn invoke_async(
    processor: &dyn AsyncProcessor,
    item: &str,
    stop: &StopToken,
) -> Result<String, ExecutionError> {
    match AssertUnwindSafe(processor.process(item, stop))
        .catch_unwind()
        .await
    {
        Ok(result) => result,
        Err(_) => Err(ExecutionError::Panic),
    }
}

/// A synchronous component that must not run on an async executor worker.
pub trait BlockingProcessor: Send + Sync + 'static {
    /// Processes owned input on an isolated blocking worker.
    ///
    /// # Errors
    ///
    /// Returns a classified component failure.
    fn process(&self, item: String) -> Result<String, ExecutionError>;
}

/// The outcome of a blocking call, including its cancellation limitation.
#[derive(Debug, PartialEq, Eq)]
pub struct BlockingOutcome {
    /// The successfully produced value.
    pub value: String,
    /// Whether stop arrived after the call started.
    ///
    /// A running blocking call is awaited to completion instead of being
    /// detached. The runtime may stop before the next item.
    pub stop_requested_during_run: bool,
}

/// An adapter that bounds and isolates synchronous work.
#[derive(Debug)]
pub struct BlockingAdapter<P> {
    processor: Arc<P>,
    permits: Arc<Semaphore>,
}

impl<P> BlockingAdapter<P>
where
    P: BlockingProcessor,
{
    /// Creates an adapter with a strict maximum number of blocking calls.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::Component`] when the limit is zero.
    pub fn new(processor: P, maximum_concurrency: usize) -> Result<Self, ExecutionError> {
        if maximum_concurrency == 0 {
            return Err(ExecutionError::Component);
        }
        Ok(Self {
            processor: Arc::new(processor),
            permits: Arc::new(Semaphore::new(maximum_concurrency)),
        })
    }

    /// Runs one call on Tokio's blocking pool.
    ///
    /// Stop is honored while waiting for capacity and immediately before the
    /// call starts. Once synchronous code is running it cannot be cancelled
    /// safely, so this method waits for it and reports a late stop.
    ///
    /// # Errors
    ///
    /// Returns a classified stop, component, panic, or worker-join failure.
    pub async fn process(
        &self,
        item: String,
        stop: &StopToken,
    ) -> Result<BlockingOutcome, ExecutionError> {
        if stop.is_stop_requested() {
            return Err(ExecutionError::Stopped);
        }

        let permit = tokio::select! {
            result = Arc::clone(&self.permits).acquire_owned() => {
                result.map_err(|_| ExecutionError::WorkerLost)?
            }
            () = stop.cancelled() => return Err(ExecutionError::Stopped),
        };

        if stop.is_stop_requested() {
            return Err(ExecutionError::Stopped);
        }

        let processor = Arc::clone(&self.processor);
        let joined = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            processor.process(item)
        })
        .await;

        let value = match joined {
            Ok(result) => result?,
            Err(error) if error.is_panic() => return Err(ExecutionError::Panic),
            Err(_) => return Err(ExecutionError::WorkerLost),
        };

        Ok(BlockingOutcome {
            value,
            stop_requested_during_run: stop.is_stop_requested(),
        })
    }
}

/// A core-owned transaction port used to prove borrowed transaction scopes.
pub trait BusinessTransaction: Send {
    /// Persists one business value through the currently enlisted resource.
    fn insert<'a>(
        &'a mut self,
        run_id: &'a str,
        item_key: &'a str,
        payload: &'a str,
    ) -> BoxFuture<'a, Result<(), ExecutionError>>;
}

/// A writer whose transaction implementation is selected by an adapter.
pub trait TransactionalWriter<T: ?Sized>: Send + Sync {
    /// Writes items through a borrowed transaction that cannot escape the call.
    fn write<'a>(
        &'a self,
        transaction: &'a mut T,
        run_id: &'a str,
        items: &'a [(&'a str, &'a str)],
    ) -> BoxFuture<'a, Result<(), ExecutionError>>;
}
