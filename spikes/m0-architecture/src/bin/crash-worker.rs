//! Helper process terminated at deterministic transaction phases.

use std::error::Error;

use oxide_batch_m0_spikes::postgres::migrate_and_verify;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

const INJECTED_EXIT: i32 = 86;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = std::env::args()
        .nth(1)
        .ok_or("missing database URL argument")?;
    let run_id = std::env::args().nth(2).ok_or("missing run ID argument")?;
    let phase = std::env::args().nth(3).ok_or("missing phase argument")?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await?;
    migrate_and_verify(&pool).await?;

    if phase == "before-transaction" {
        std::process::exit(INJECTED_EXIT);
    }

    let mut transaction = pool.begin().await?;
    sqlx::query(
        "INSERT INTO ob_business_item (run_id, item_key, payload) VALUES ($1, 'item-1', 'value')",
    )
    .bind(&run_id)
    .execute(&mut *transaction)
    .await?;
    if phase == "after-business-write" {
        std::process::exit(INJECTED_EXIT);
    }

    sqlx::query(
        "INSERT INTO ob_step_execution \
         (step_id, checkpoint, write_count, context, version) VALUES ($1, 1, 1, $2, 0)",
    )
    .bind(&run_id)
    .bind(json!({"cursor": 1}))
    .execute(&mut *transaction)
    .await?;
    if phase == "before-commit" {
        std::process::exit(INJECTED_EXIT);
    }

    transaction.commit().await?;
    if phase == "after-commit" {
        std::process::exit(INJECTED_EXIT);
    }

    Err(format!("unknown crash phase: {phase}").into())
}
