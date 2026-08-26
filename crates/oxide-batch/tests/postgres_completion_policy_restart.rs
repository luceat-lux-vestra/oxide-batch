//! Real `PostgreSQL` restart evidence for `REPEAT-POLICY-001`'s adaptive
//! completion policy.
//!
//! [`AdaptiveCompletionPolicy`] persists its confirmed target through the
//! same `ItemStream`/`commit_with_component_state` boundary every other M6
//! component state uses -- the atomicity of that boundary (a commit
//! interrupted mid-flight leaves no row, and a proven commit's row survives
//! a process crash) is already evidenced generically, for any conforming
//! `ItemStream`, by `postgres_item_stream_crash_recovery.rs`'s process-kill
//! harness; this file does not duplicate that OS-level kill, since
//! `AdaptiveCompletionPolicy` introduces no new persistence path for it to
//! apply to. What this file proves instead is specific to the policy's own
//! contract: that a *rollback* (not merely an uncommitted read) leaves the
//! previously committed target authoritative, and that a *freshly
//! constructed* policy instance -- as a real restart would create --
//! restores the committed target from `PostgreSQL` through `open`, rather
//! than reporting the default it would start at if merely rebuilt.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL` and
//! `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[path = "crash_restore/mod.rs"]
mod crash_restore;

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use clock::ManualClock;
use oxide_batch::{
    AdaptiveBounds, AdaptiveCompletionPolicy, ChunkCount, ChunkCounts, ChunkFaultProgress,
    ChunkSize, ChunkTimeThreshold, ChunkTransactionContext, ChunkTransactionManager,
    CompletionPolicy, ComponentStreamIdentity, ItemStream, PostgresJobRepository, PostgresMigrator,
    StopSource, StreamOpenContext, StreamUpdateContext,
};

use crash_restore::{
    config, create_attempt, epoch, migrator_url, prepare_fixture, remove_job, runtime_url,
};

const JOB: &str = "m6_151_adaptive_completion_restart";

#[test]
fn rollback_leaves_the_committed_adaptive_target_authoritative_and_restart_restores_it()
-> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(&runtime_url, &migrator_url))
}

async fn run(runtime_url: &str, migrator_url: &str) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator_url.to_owned())?).await?;
    prepare_fixture(migrator_url, JOB).await?;

    let repository = PostgresJobRepository::connect(
        config(runtime_url.to_owned())?,
        Arc::new(crash_restore::FixedClock(epoch(700))),
    )
    .await?;
    let key = crash_restore::instance_key(JOB)?;
    let (execution, step) = create_attempt(&repository, &key, JOB, 1).await?;
    crash_restore::start_attempt(&repository, &execution, &step, epoch(701)).await?;
    let scope = ChunkTransactionContext::new(execution.id(), step.id());
    let manager = crash_restore::transaction_manager(&repository, None);

    let identity = ComponentStreamIdentity::new("m6_151.adaptive_completion")?;
    let bounds = AdaptiveBounds::new(ChunkSize::new(1)?, ChunkSize::new(100)?)?;
    let target_duration = ChunkTimeThreshold::new(Duration::from_secs(1))?;
    let clock = ManualClock::new(std::time::UNIX_EPOCH);

    // Commit one chunk whose observed duration is well under the target, so
    // the confirmed target grows past its starting minimum -- distinct from
    // whatever a never-restored policy would report.
    let policy = AdaptiveCompletionPolicy::new(
        identity.clone(),
        bounds,
        target_duration,
        Arc::new(clock.clone()),
    );
    let (_source, stop) = StopSource::new();
    policy.begin_chunk();
    clock.advance(Duration::from_millis(100))?;
    let committed_envelope = policy.update(StreamUpdateContext::new(&stop)).await?;
    assert!(
        policy.current_target().get() > bounds.min().get(),
        "a fast chunk must grow the target past the configured minimum"
    );
    let committed_target = policy.current_target();

    let mut transaction = manager.begin_for(scope).await?;
    crash_restore::write_items(&mut *transaction, JOB, &[1]).await?;
    let count = ChunkCount::new(1);
    transaction
        .commit_with_component_state(
            ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
            ChunkFaultProgress::NONE,
            &[committed_envelope],
        )
        .await?;

    // A second, slow attempt shrinks the in-memory candidate -- but this
    // attempt rolls back instead of committing.
    policy.begin_chunk();
    clock.advance(Duration::from_secs(10))?;
    let _uncommitted_envelope = policy.update(StreamUpdateContext::new(&stop)).await?;
    assert!(
        policy.current_target().get() < committed_target.get(),
        "the in-memory candidate reflects the slow, not-yet-committed attempt"
    );

    // This attempt never calls `commit_with_component_state` at all: an
    // explicit rollback proves the candidate above was never even proposed
    // to `PostgreSQL`, let alone persisted.
    let mut rolled_back = manager.begin_for(scope).await?;
    crash_restore::write_items(&mut *rolled_back, JOB, &[2]).await?;
    rolled_back.rollback().await?;

    let inherited = manager.inherited_component_state(scope).await?;
    let restored_envelope = inherited
        .iter()
        .find(|candidate| candidate.namespace() == &identity)
        .expect("the committed chunk's envelope must be durable")
        .clone();

    // A freshly constructed policy -- exactly what a real restart builds --
    // must restore the *committed* target via `open`, not the shrunk
    // candidate the rolled-back attempt only speculated, and not the
    // configured minimum a merely-rebuilt policy would start at.
    let fresh = AdaptiveCompletionPolicy::new(
        identity.clone(),
        bounds,
        target_duration,
        Arc::new(ManualClock::new(std::time::UNIX_EPOCH)),
    );
    assert_eq!(
        fresh.current_target().get(),
        bounds.min().get(),
        "an unopened, freshly rebuilt policy starts at the configured minimum"
    );
    fresh
        .open(StreamOpenContext::new(Some(&restored_envelope), &stop))
        .await?;
    assert_eq!(
        fresh.current_target().get(),
        committed_target.get(),
        "restart must restore the committed target, not the rolled-back candidate \
         and not the unopened default"
    );

    remove_job(migrator_url, JOB).await?;
    Ok(())
}
