//! Process/repository failure fixture mechanics: a worker runs as a
//! separate OS process, a forced `SIGKILL` terminates it, and the driving
//! process observes the exact signal -- so no Rust destructor in the
//! worker ever gets a chance to run.

#![cfg(unix)]

use std::error::Error;
use std::time::Duration;

use oxide_batch_test::process::{
    announce, handshake_dir, is_worker, kill_and_wait, park_until_killed, spawn_worker_test,
    wait_for_file, was_sigkilled,
};

#[test]
fn process_fixture_worker() -> Result<(), Box<dyn Error>> {
    if !is_worker() {
        return Ok(());
    }
    let handshake = handshake_dir().ok_or("worker has no handshake directory")?;
    announce(&handshake.join("reached"))?;
    park_until_killed();
}

#[test]
fn process_fixture_kills_and_reports_sigkill() -> Result<(), Box<dyn Error>> {
    let handshake = std::env::temp_dir().join(format!(
        "oxide-batch-test-process-fixture-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&handshake)?;

    let mut child = spawn_worker_test("process_fixture_worker", &handshake)?;
    wait_for_file(&handshake.join("reached"), Duration::from_secs(10))?;
    let status = kill_and_wait(&mut child)?;

    std::fs::remove_dir_all(&handshake)?;

    assert!(
        was_sigkilled(status),
        "the fixture must report a real SIGKILL, not a graceful exit"
    );
    Ok(())
}
