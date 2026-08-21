//! A bounded worker-process crash fixture.
//!
//! Reuses the project's established crash-evidence principle: the worker
//! runs as a separate OS process, a forced termination kills it, and a fresh
//! process inspects durable state afterward, so Rust transaction/pool
//! destructors never perform rollback on the crashed process's behalf. This
//! is deliberately a small set of primitives, not a general subprocess
//! framework: a downstream test combines them with
//! [`crate::postgres::PostgresFixture`] and [`crate::TestJob`] to prove real
//! crash/restart evidence, the same way [`crate::inject`] proves it for a
//! typed in-process failure.
//!
//! ```text
//! #[test]
//! fn my_worker_process() -> Result<(), Box<dyn std::error::Error>> {
//!     if !oxide_batch_test::process::is_worker() {
//!         return Ok(()); // only runs when spawned as a worker
//!     }
//!     let handshake = oxide_batch_test::process::handshake_dir().ok_or("no handshake dir")?;
//!     // ... commit some chunks, then:
//!     oxide_batch_test::process::announce(&handshake.join("reached"))?;
//!     oxide_batch_test::process::park_until_killed();
//! }
//!
//! #[tokio::test]
//! async fn my_crash_restart_test() -> Result<(), Box<dyn std::error::Error>> {
//!     let handshake = std::env::temp_dir().join("my-worker-handshake");
//!     std::fs::create_dir_all(&handshake)?;
//!     let mut child = oxide_batch_test::process::spawn_worker_test(
//!         "my_worker_process",
//!         &handshake,
//!     )?;
//!     oxide_batch_test::process::wait_for_file(&handshake.join("reached"), std::time::Duration::from_secs(10))?;
//!     let status = oxide_batch_test::process::kill_and_wait(&mut child)?;
//!     // inspect durable PostgreSQL state from this, unrelated, process
//!     Ok(())
//! }
//! ```

use std::env;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

/// The environment variable a worker checks to know it should run its
/// worker body instead of returning immediately.
pub const WORKER_ENV: &str = "OXIDE_BATCH_TEST_WORKER";

/// The environment variable carrying the handshake directory a worker was
/// given.
pub const HANDSHAKE_ENV: &str = "OXIDE_BATCH_TEST_HANDSHAKE";

/// Returns whether this process was launched as a worker by
/// [`spawn_worker_test`].
#[must_use]
pub fn is_worker() -> bool {
    env::var(WORKER_ENV).is_ok()
}

/// Returns the handshake directory this worker process was given.
#[must_use]
pub fn handshake_dir() -> Option<PathBuf> {
    env::var(HANDSHAKE_ENV).ok().map(PathBuf::from)
}

/// Re-executes the current test binary, running only the named `#[test]`,
/// with [`WORKER_ENV`] and [`HANDSHAKE_ENV`] set so
/// [`is_worker`]/[`handshake_dir`] let it recognize its role.
///
/// # Errors
///
/// Returns the OS failure when the current executable cannot be located or
/// re-launched.
pub fn spawn_worker_test(test_name: &str, handshake: &Path) -> io::Result<Child> {
    Command::new(env::current_exe()?)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(WORKER_ENV, "1")
        .env(HANDSHAKE_ENV, handshake)
        .spawn()
}

/// Creates an empty file at `path`, announcing that the worker reached a
/// point safe to kill it at.
///
/// # Errors
///
/// Returns the OS failure when the file cannot be created.
pub fn announce(path: &Path) -> io::Result<()> {
    std::fs::write(path, [])
}

/// A failure waiting for a worker's handshake.
#[derive(Debug)]
#[non_exhaustive]
pub enum ProcessFixtureError {
    /// `path` did not appear before the bound elapsed.
    HandshakeTimedOut {
        /// The bound that elapsed.
        timeout: Duration,
    },
}

impl fmt::Display for ProcessFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HandshakeTimedOut { timeout } => {
                write!(
                    formatter,
                    "worker handshake did not appear within {timeout:?}"
                )
            }
        }
    }
}

impl std::error::Error for ProcessFixtureError {}

/// Blocks the calling thread until `path` exists, bounded by `timeout`.
///
/// This is a bounded poll, not an async wait: it is meant to be called from
/// test setup code before a runtime-bound await, matching
/// [`std::process::Child::wait`]'s own blocking shape and adding no runtime
/// dependency to this crate's public surface.
///
/// # Errors
///
/// Returns [`ProcessFixtureError::HandshakeTimedOut`] when `path` does not
/// appear before `timeout` elapses.
pub fn wait_for_file(path: &Path, timeout: Duration) -> Result<(), ProcessFixtureError> {
    let start = Instant::now();
    while !path.exists() {
        if start.elapsed() >= timeout {
            return Err(ProcessFixtureError::HandshakeTimedOut { timeout });
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

/// Parks the current worker process forever, so a parent can kill it at a
/// precise, announced point rather than racing a return.
pub fn park_until_killed() -> ! {
    loop {
        std::thread::sleep(Duration::from_hours(1));
    }
}

/// Forcibly kills `child` and waits for its exit status.
///
/// # Errors
///
/// Returns the OS failure when the process cannot be signaled or reaped.
pub fn kill_and_wait(child: &mut Child) -> io::Result<ExitStatus> {
    child.kill()?;
    child.wait()
}

/// Returns whether `status` reports the process was killed by `SIGKILL`.
#[cfg(unix)]
#[must_use]
pub fn was_sigkilled(status: ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt;
    status.signal() == Some(9)
}
