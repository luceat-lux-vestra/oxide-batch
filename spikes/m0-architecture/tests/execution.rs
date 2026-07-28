//! Async execution-model evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use oxide_batch_m0_spikes::execution::{
    AsyncProcessor, BlockingAdapter, BlockingProcessor, BoxFuture, ExecutionError, StopSource,
    StopToken, invoke_async,
};

struct BorrowingProcessor;

impl AsyncProcessor for BorrowingProcessor {
    fn process<'a>(
        &'a self,
        item: &'a str,
        _stop: &'a StopToken,
    ) -> BoxFuture<'a, Result<String, ExecutionError>> {
        Box::pin(async move { Ok(format!("{item}-processed")) })
    }
}

struct CancellableProcessor;

impl AsyncProcessor for CancellableProcessor {
    fn process<'a>(
        &'a self,
        _item: &'a str,
        stop: &'a StopToken,
    ) -> BoxFuture<'a, Result<String, ExecutionError>> {
        Box::pin(async move {
            tokio::select! {
                () = stop.cancelled() => Err(ExecutionError::Stopped),
                () = tokio::time::sleep(Duration::from_secs(10)) => Ok(String::from("late")),
            }
        })
    }
}

struct PanickingProcessor;

impl AsyncProcessor for PanickingProcessor {
    fn process<'a>(
        &'a self,
        _item: &'a str,
        _stop: &'a StopToken,
    ) -> BoxFuture<'a, Result<String, ExecutionError>> {
        Box::pin(async move {
            panic!("fixture panic payload must not cross the boundary");
        })
    }
}

#[tokio::test]
async fn boxed_future_trait_is_dyn_compatible_and_borrows_call_scope() {
    let processor: Box<dyn AsyncProcessor> = Box::new(BorrowingProcessor);
    let (_source, token) = StopSource::new();
    let item = String::from("borrowed");

    let output = invoke_async(processor.as_ref(), &item, &token).await;

    assert_eq!(output, Ok(String::from("borrowed-processed")));
}

#[tokio::test]
async fn cooperative_cancellation_interrupts_async_user_work() {
    let processor: Box<dyn AsyncProcessor> = Box::new(CancellableProcessor);
    let (source, token) = StopSource::new();
    let request = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        source.request_stop();
    });

    let started = Instant::now();
    let output = invoke_async(processor.as_ref(), "item", &token).await;
    request.await.expect("stop task must join");

    assert_eq!(output, Err(ExecutionError::Stopped));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn async_panic_is_classified_and_the_runtime_remains_usable() {
    let (_source, token) = StopSource::new();

    let failure = invoke_async(&PanickingProcessor, "item", &token).await;
    let next = invoke_async(&BorrowingProcessor, "next", &token).await;

    assert_eq!(failure, Err(ExecutionError::Panic));
    assert_eq!(next, Ok(String::from("next-processed")));
}

struct SleepingBlockingProcessor {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
}

impl BlockingProcessor for SleepingBlockingProcessor {
    fn process(&self, item: String) -> Result<String, ExecutionError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(item)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_work_is_bounded_and_does_not_starve_async_timers() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let adapter = Arc::new(
        BlockingAdapter::new(
            SleepingBlockingProcessor {
                active,
                peak: Arc::clone(&peak),
                delay: Duration::from_millis(100),
            },
            2,
        )
        .expect("valid adapter"),
    );
    let (_source, token) = StopSource::new();
    let timer_started = Instant::now();
    let timer = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Instant::now()
    });

    let mut calls = tokio::task::JoinSet::new();
    for index in 0..6 {
        let adapter = Arc::clone(&adapter);
        let token = token.clone();
        calls.spawn(async move { adapter.process(index.to_string(), &token).await });
    }
    while let Some(result) = calls.join_next().await {
        result
            .expect("blocking adapter task must join")
            .expect("blocking call must succeed");
    }
    let timer_finished = timer.await.expect("timer task must join");

    assert!(timer_finished.duration_since(timer_started) < Duration::from_millis(80));
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn running_blocking_work_finishes_before_late_stop_is_reported() {
    let adapter = Arc::new(
        BlockingAdapter::new(
            SleepingBlockingProcessor {
                active: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(100),
            },
            1,
        )
        .expect("valid adapter"),
    );
    let (source, token) = StopSource::new();
    let request = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        source.request_stop();
    });

    let started = Instant::now();
    let outcome = adapter
        .process(String::from("item"), &token)
        .await
        .expect("running blocking call completes");
    request.await.expect("stop task must join");

    assert!(started.elapsed() >= Duration::from_millis(90));
    assert!(outcome.stop_requested_during_run);
}

struct PanickingBlockingProcessor;

impl BlockingProcessor for PanickingBlockingProcessor {
    fn process(&self, _item: String) -> Result<String, ExecutionError> {
        panic!("blocking fixture panic");
    }
}

#[tokio::test]
async fn blocking_panic_is_classified() {
    let adapter = BlockingAdapter::new(PanickingBlockingProcessor, 1).expect("valid adapter");
    let (_source, token) = StopSource::new();

    let result = adapter.process(String::from("item"), &token).await;

    assert_eq!(result, Err(ExecutionError::Panic));
}
