//! Retention purge with committed `ItemStream` component state.
//!
//! Migration `0005` (M6 `#144`) added `ob_component_state`, keyed by
//! `step_execution_id` with `ON DELETE RESTRICT`. The M5 retention purge
//! path deletes every table that references `ob_step_execution` the same
//! way -- `ob_step_partition`, before deleting the step execution itself --
//! and this target proves `ob_component_state` was added to that same
//! ordered deletion rather than left as a foreign key the purge had never
//! been taught about. Without it, purging a completed execution that had
//! ever committed component state would fail closed with a foreign-key
//! violation instead of a `RepositoryError`, and the retention contract
//! would be broken for exactly the executions the M6 addition is otherwise
//! meant to survive.

#![cfg(feature = "postgres")]
#![allow(clippy::expect_used, clippy::panic)]

mod crash_restore;

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use oxide_batch::{
    ActorRef, BatchStatus, ChunkCount, ChunkCounts, ChunkFaultProgress, ChunkTransactionContext,
    ChunkTransactionManager, CodecId, CodecVersion, ComponentStateEnvelope,
    ComponentStreamIdentity, DefaultComponentCodec, JobRepository, LifecycleTransition,
    OperationId, PostgresJobRepository, PostgresMigrator, PurgeBatchBound, PurgePlanRequest,
    ReasonCode, RestartabilityDeclaration, RetentionService, StateCodecError, StateLimits,
    StateSchemaId, StateSchemaVersion, TerminalStatusSet, VersionedStateCodec,
};
use serde_json::json;

use crash_restore::{
    FixedClock, config, create_attempt, epoch, instance_key, migrator_url, prepare_fixture,
    remove_job, runtime_url, start_attempt, transaction_manager, write_items,
};

const NAMESPACE: &str = "reader.row_count";

struct CounterSchema {
    schema: StateSchemaId,
}

impl VersionedStateCodec<u64> for CounterSchema {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }
    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).expect("nonzero")
    }
    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&json!({ "rows": value })).map_err(|_| StateCodecError::InvalidPayload)
    }
    fn decode(&self, _payload: &[u8]) -> Result<u64, StateCodecError> {
        Ok(0)
    }
}

fn envelope() -> Result<ComponentStateEnvelope, Box<dyn Error>> {
    let codec = DefaultComponentCodec::new(
        CounterSchema {
            schema: StateSchemaId::new("m6.retention.row-count")?,
        },
        CodecId::new("m6.retention.row-count-codec")?,
        CodecVersion::new(1)?,
        RestartabilityDeclaration::Restartable,
    );
    Ok(ComponentStateEnvelope::encode(
        ComponentStreamIdentity::new(NAMESPACE)?,
        &1_u64,
        &codec,
        StateLimits::default(),
    )?)
}

/// Selects whether a component-state row survives for one step execution.
async fn component_state_exists(
    runtime_url: &str,
    step_execution_id: i64,
) -> Result<bool, Box<dyn Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(runtime_url)
        .await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_component_state \
         WHERE step_execution_id = $1)",
    )
    .bind(step_execution_id)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(exists)
}

/// Selects whether a step execution row itself survives.
async fn step_execution_exists(
    runtime_url: &str,
    step_execution_id: i64,
) -> Result<bool, Box<dyn Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(runtime_url)
        .await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_step_execution WHERE id = $1)",
    )
    .bind(step_execution_id)
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(exists)
}

#[test]
fn purge_deletes_component_state_before_the_step_execution_it_references()
-> Result<(), Box<dyn Error>> {
    const JOB: &str = "m6_retention_component_state";
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
        .block_on(async {
            PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
            prepare_fixture(&migrator_url, JOB).await?;

            let repository = PostgresJobRepository::connect(
                config(runtime_url.clone())?,
                Arc::new(FixedClock(epoch(2000))),
            )
            .await?;
            let key = instance_key(JOB)?;
            let (execution, step) = create_attempt(&repository, &key, JOB, 1).await?;
            let (execution, step) =
                start_attempt(&repository, &execution, &step, epoch(2001)).await?;

            // Commit one chunk with committed component state, exactly the
            // durable shape a real `ItemStream` registration leaves behind.
            let manager = transaction_manager(&repository, None);
            let scope = ChunkTransactionContext::new(execution.id(), step.id());
            let mut transaction = manager.begin_for(scope).await?;
            write_items(&mut *transaction, JOB, &[1]).await?;
            let count = ChunkCount::new(1);
            transaction
                .commit_with_component_state(
                    ChunkCounts::new(count, count, count, ChunkCount::ZERO)?,
                    ChunkFaultProgress::NONE,
                    &[envelope()?],
                )
                .await?;

            let step_id = i64::try_from(step.id().get())?;
            assert!(
                component_state_exists(&runtime_url, step_id).await?,
                "the committed component state must be durable before the purge runs",
            );

            // Complete the attempt so it becomes purge-eligible. Re-read the
            // current versions first: `commit_with_component_state` already
            // bumped the step execution's optimistic version past what
            // `step` captured before the commit.
            let (current_execution, current_step) =
                crash_restore::latest_attempt(&repository, &key).await?;
            let completed_at = epoch(2002);
            let mut complete = repository.begin().await?;
            complete
                .transition_step_execution(
                    current_step.id(),
                    current_step.version(),
                    LifecycleTransition::new(BatchStatus::Completed, completed_at),
                )
                .await?;
            complete
                .transition_job_execution(
                    current_execution.id(),
                    current_execution.version(),
                    LifecycleTransition::new(BatchStatus::Completed, completed_at),
                )
                .await?;
            complete.commit().await?;

            // Purge from a repository opened well past the completed
            // execution's age, exactly as an operator purging old history
            // would.
            let later = FixedClock(epoch(2002) + Duration::from_hours(30 * 24));
            let purging =
                PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(later))
                    .await?;
            let retention = RetentionService::new(purging, Arc::new(later));
            let request = PurgePlanRequest::new(
                oxide_batch::JobName::new(JOB)?,
                TerminalStatusSet::new([BatchStatus::Completed])?,
                Duration::from_hours(24),
                PurgeBatchBound::new(10)?,
            )?;
            let plan = retention.plan_purge(&request).await?;
            assert_eq!(
                plan.candidates().len(),
                1,
                "the completed execution must be exactly one eligible purge candidate",
            );

            // The assertion this whole target exists for: applying the purge
            // must not fail with a foreign-key violation now that a
            // completed execution can have committed component state.
            let report = retention
                .apply_purge(
                    OperationId::new("m6-retention-component-state")?,
                    ActorRef::new("operator:m6-retention-campaign")?,
                    ReasonCode::new("SCHEDULED_PURGE")?,
                    &plan,
                )
                .await?;
            assert_eq!(report.counts().step_executions(), 1);

            assert!(
                !step_execution_exists(&runtime_url, step_id).await?,
                "the purge must actually delete the step execution, not silently skip it",
            );
            assert!(
                !component_state_exists(&runtime_url, step_id).await?,
                "the purge must delete the step execution's component state with it, leaving no \
                 orphaned row and no row a later query could disclose",
            );

            remove_job(&migrator_url, JOB).await?;
            Ok(())
        })
}
