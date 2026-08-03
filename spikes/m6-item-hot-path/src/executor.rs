//! A runtime-free driver for the spike's futures.
//!
//! The measurement binaries deliberately avoid Tokio. Two reasons: the timing
//! comparison should show dispatch cost rather than scheduler cost, and the
//! binary-size comparison should not be dominated by a runtime that both
//! paths link identically.
//!
//! This is sound only because every spike component completes without
//! yielding, so the top-level future is ready on its first poll. It is not a
//! general executor and must never leave this crate.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Polls `future` to completion on the calling thread.
///
/// # Panics
///
/// Panics if the future ever returns `Poll::Pending`, which for this crate
/// means a component started doing real asynchronous work and the measurement
/// is no longer meaningful.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => {
            #[allow(clippy::panic)]
            {
                panic!("a spike component yielded; the measurement harness cannot schedule it")
            }
        }
    }
}
