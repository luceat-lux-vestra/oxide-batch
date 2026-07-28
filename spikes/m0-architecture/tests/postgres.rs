//! Real-PostgreSQL transaction, locking, and recovery evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::process::ExitStatus;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use oxide_batch_m0_spikes::postgres::{
    MIGRATOR, PortOnlyWriter, PostgresSpikeError, commit_enlisted_chunk, migrate_and_verify,
    verify_schema_version,
};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Acquire, Connection, Executor, PgPool};
use tokio::process::Command;
use tokio::sync::Barrier;

const INJECTED_EXIT: i32 = 86;
static RUN_SEQUENCE: AtomicU64 = AtomicU64::new(1);

async fn spike_pool() -> Option<PgPool> {
    let database_url = std::env::var("OXIDEBATCH_SPIKE_DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(24)
        .connect(&database_url)
        .await;
    let pool = pool.expect("spike PostgreSQL must be reachable");
    migrate_and_verify(&pool)
        .await
        .expect("spike migrations must apply");
    Some(pool)
}

async fn isolated_failure_pool() -> PgPool {
    let database_url =
        std::env::var("OXIDEBATCH_SPIKE_DATABASE_URL").expect("database URL remains set");
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("isolated failure pool must connect")
}

fn unique_id(prefix: &str) -> String {
    let sequence = RUN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{sequence}", std::process::id())
}

async fn row_count(pool: &PgPool, table: &str, key_column: &str, key: &str) -> i64 {
    let query = match (table, key_column) {
        ("ob_business_item", "run_id") => {
            sqlx::query_scalar("SELECT count(*) FROM ob_business_item WHERE run_id = $1")
        }
        ("ob_step_execution", "step_id") => {
            sqlx::query_scalar("SELECT count(*) FROM ob_step_execution WHERE step_id = $1")
        }
        ("ob_job_instance", "instance_key") => {
            sqlx::query_scalar("SELECT count(*) FROM ob_job_instance WHERE instance_key = $1")
        }
        _ => panic!("row_count only accepts audited table and key pairs"),
    };
    query
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("count query must succeed")
}

fn assert_injected_exit(status: ExitStatus) {
    assert_eq!(status.code(), Some(INJECTED_EXIT));
}

#[tokio::test]
async fn enlisted_business_and_checkpoint_writes_commit_or_roll_back_together() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let committed = unique_id("atomic-commit");
    commit_enlisted_chunk(
        &pool,
        &PortOnlyWriter,
        &committed,
        &[("one", "alpha"), ("two", "beta")],
        0,
        false,
    )
    .await
    .expect("enlisted chunk must commit");

    assert_eq!(
        row_count(&pool, "ob_business_item", "run_id", &committed).await,
        2
    );
    let metadata: (i64, i64, Value, i64) = sqlx::query_as(
        "SELECT checkpoint, write_count, context, version \
         FROM ob_step_execution WHERE step_id = $1",
    )
    .bind(&committed)
    .fetch_one(&pool)
    .await
    .expect("committed metadata must exist");
    assert_eq!(metadata.0, 2);
    assert_eq!(metadata.1, 2);
    assert_eq!(metadata.2["cursor"], 2);
    assert_eq!(metadata.3, 0);

    let rolled_back = unique_id("atomic-rollback");
    let failure = commit_enlisted_chunk(
        &pool,
        &PortOnlyWriter,
        &rolled_back,
        &[("one", "alpha")],
        0,
        true,
    )
    .await;
    assert!(matches!(
        failure,
        Err(PostgresSpikeError::InjectedBeforeCommit)
    ));
    assert_eq!(
        row_count(&pool, "ob_business_item", "run_id", &rolled_back).await,
        0
    );
    assert_eq!(
        row_count(&pool, "ob_step_execution", "step_id", &rolled_back).await,
        0
    );
}

#[tokio::test]
async fn unique_index_lock_serializes_duplicate_launches() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let instance_key = unique_id("locked-instance");
    let mut first = pool.begin().await.expect("first transaction must begin");
    sqlx::query("INSERT INTO ob_job_instance (job_name, instance_key) VALUES ('inventory', $1)")
        .bind(&instance_key)
        .execute(&mut *first)
        .await
        .expect("first insert must hold the unique-index lock");

    let mut contender = pool
        .begin()
        .await
        .expect("contender transaction must begin");
    contender
        .execute("SET LOCAL lock_timeout = '150ms'")
        .await
        .expect("lock timeout must be configured");
    let blocked = sqlx::query(
        "INSERT INTO ob_job_instance (job_name, instance_key) VALUES ('inventory', $1) \
         ON CONFLICT (job_name, instance_key) DO NOTHING",
    )
    .bind(&instance_key)
    .execute(&mut *contender)
    .await;
    let code = match &blocked {
        Err(sqlx::Error::Database(error)) => error.code().map(std::borrow::Cow::into_owned),
        _ => None,
    };
    assert_eq!(code.as_deref(), Some("55P03"));
    contender
        .rollback()
        .await
        .expect("contender rollback must succeed");

    first.commit().await.expect("first launch must commit");
    let retry = sqlx::query(
        "INSERT INTO ob_job_instance (job_name, instance_key) VALUES ('inventory', $1) \
         ON CONFLICT (job_name, instance_key) DO NOTHING",
    )
    .bind(&instance_key)
    .execute(&pool)
    .await
    .expect("retry must observe the committed instance");
    assert_eq!(retry.rows_affected(), 0);
    assert_eq!(
        row_count(&pool, "ob_job_instance", "instance_key", &instance_key).await,
        1
    );
}

#[tokio::test]
async fn concurrent_duplicate_launches_create_exactly_one_instance() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let instance_key = unique_id("raced-instance");
    let contenders = 12;
    let barrier = Arc::new(Barrier::new(contenders));
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..contenders {
        let pool = pool.clone();
        let instance_key = instance_key.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            sqlx::query(
                "INSERT INTO ob_job_instance (job_name, instance_key) \
                 VALUES ('inventory', $1) \
                 ON CONFLICT (job_name, instance_key) DO NOTHING",
            )
            .bind(instance_key)
            .execute(&pool)
            .await
            .map(|result| result.rows_affected())
        });
    }

    let mut inserted = 0;
    while let Some(joined) = tasks.join_next().await {
        inserted += joined
            .expect("launch task must join")
            .expect("launch query must succeed");
    }
    assert_eq!(inserted, 1);
    assert_eq!(
        row_count(&pool, "ob_job_instance", "instance_key", &instance_key).await,
        1
    );
}

#[tokio::test]
async fn optimistic_update_race_has_one_winner_and_one_conflict() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let step_id = unique_id("optimistic");
    sqlx::query(
        "INSERT INTO ob_step_execution \
         (step_id, checkpoint, write_count, context, version) \
         VALUES ($1, 0, 0, '{}'::jsonb, 0)",
    )
    .bind(&step_id)
    .execute(&pool)
    .await
    .expect("fixture step must exist");

    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let pool = pool.clone();
        let step_id = step_id.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            let observed: i64 =
                sqlx::query_scalar("SELECT version FROM ob_step_execution WHERE step_id = $1")
                    .bind(&step_id)
                    .fetch_one(&pool)
                    .await?;
            barrier.wait().await;
            sqlx::query(
                "UPDATE ob_step_execution SET checkpoint = checkpoint + 1, version = version + 1 \
                 WHERE step_id = $1 AND version = $2",
            )
            .bind(step_id)
            .bind(observed)
            .execute(&pool)
            .await
            .map(|result| result.rows_affected())
        });
    }

    let mut effects = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        effects.push(
            joined
                .expect("optimistic task must join")
                .expect("optimistic query must complete"),
        );
    }
    effects.sort_unstable();
    assert_eq!(effects, vec![0, 1]);
}

#[tokio::test]
async fn process_exit_crash_matrix_matches_the_commit_boundary() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let database_url =
        std::env::var("OXIDEBATCH_SPIKE_DATABASE_URL").expect("database URL remains set");
    let worker = env!("CARGO_BIN_EXE_crash-worker");
    let phases = [
        ("before-transaction", false),
        ("after-business-write", false),
        ("before-commit", false),
        ("after-commit", true),
    ];

    for (phase, committed) in phases {
        let run_id = unique_id(phase);
        let status = Command::new(worker)
            .args([&database_url, &run_id, phase])
            .status()
            .await
            .expect("crash worker must run");
        assert_injected_exit(status);

        let business = row_count(&pool, "ob_business_item", "run_id", &run_id).await;
        let metadata = row_count(&pool, "ob_step_execution", "step_id", &run_id).await;
        assert_eq!(business, i64::from(committed), "phase: {phase}");
        assert_eq!(metadata, i64::from(committed), "phase: {phase}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backend_termination_makes_commit_fail_and_rolls_back_both_resources() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let run_id = unique_id("disconnect");
    let failure_pool = isolated_failure_pool().await;
    let mut connection = failure_pool
        .acquire()
        .await
        .expect("connection must be acquired");
    let mut transaction = connection.begin().await.expect("transaction must begin");
    let backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *transaction)
        .await
        .expect("backend PID must be visible");
    sqlx::query(
        "INSERT INTO ob_business_item (run_id, item_key, payload) \
         VALUES ($1, 'one', '__delay_commit__')",
    )
    .bind(&run_id)
    .execute(&mut *transaction)
    .await
    .expect("business write must be pending");
    sqlx::query(
        "INSERT INTO ob_step_execution \
         (step_id, checkpoint, write_count, context, version) \
         VALUES ($1, 1, 1, '{\"cursor\":1}'::jsonb, 0)",
    )
    .bind(&run_id)
    .execute(&mut *transaction)
    .await
    .expect("metadata write must be pending");

    let commit = transaction.commit();
    let terminate_during_commit = async {
        tokio::time::sleep(Duration::from_millis(150)).await;
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(backend_pid)
            .fetch_one(&pool)
            .await
    };
    let (commit_result, terminated) = tokio::join!(commit, terminate_during_commit);
    let terminated = terminated.expect("test role must terminate its backend during commit");
    assert!(terminated);
    assert!(commit_result.is_err());
    // A commit error can leave protocol state unsuitable for reuse. Explicitly
    // evict the connection instead of returning it to the shared pool.
    connection
        .detach()
        .close_hard()
        .await
        .expect("failed connection must close hard");
    drop(failure_pool);
    assert_eq!(
        row_count(&pool, "ob_business_item", "run_id", &run_id).await,
        0
    );
    assert_eq!(
        row_count(&pool, "ob_step_execution", "step_id", &run_id).await,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_slow_query_leaves_no_committed_effect_and_pool_recovers() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    let run_id = unique_id("query-cancel");
    let failure_pool = isolated_failure_pool().await;
    let mut connection = failure_pool
        .acquire()
        .await
        .expect("connection must be acquired");
    let mut transaction = connection.begin().await.expect("transaction must begin");
    sqlx::query(
        "INSERT INTO ob_business_item (run_id, item_key, payload) VALUES ($1, 'one', 'pending')",
    )
    .bind(&run_id)
    .execute(&mut *transaction)
    .await
    .expect("business effect must be pending");
    let timed = tokio::time::timeout(
        Duration::from_millis(150),
        sqlx::query("SELECT pg_sleep(5)").execute(&mut *transaction),
    )
    .await;
    assert!(timed.is_err());
    // Dropping an in-flight protocol future is not a signal that this
    // connection is immediately reusable. Closing it lets PostgreSQL roll back
    // the open transaction and prevents pool poisoning.
    drop(transaction);
    connection
        .detach()
        .close_hard()
        .await
        .expect("cancelled connection must close hard");
    drop(failure_pool);

    let answer: i32 = sqlx::query_scalar("SELECT 42")
        .fetch_one(&pool)
        .await
        .expect("pool must remain usable after cancellation");
    assert_eq!(answer, 42);
    assert_eq!(
        row_count(&pool, "ob_business_item", "run_id", &run_id).await,
        0
    );
}

#[tokio::test]
async fn migrations_are_idempotent_and_newer_schema_versions_are_rejected() {
    let Some(pool) = spike_pool().await else {
        eprintln!("skipped: OXIDEBATCH_SPIKE_DATABASE_URL is not set");
        return;
    };
    MIGRATOR
        .run(&pool)
        .await
        .expect("second migration run must be idempotent");
    migrate_and_verify(&pool)
        .await
        .expect("current schema must verify");
    assert!(matches!(
        verify_schema_version(2),
        Err(PostgresSpikeError::NewerSchema)
    ));
}
