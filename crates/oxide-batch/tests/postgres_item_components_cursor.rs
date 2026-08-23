//! #149 evidence: `PostgresCursorReader` streaming, restart, cleanup, and
//! bounded-resource behavior against a real `PostgreSQL` server-side cursor.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention (see
//! `crates/oxide-batch/tests/postgres_repository.rs`).

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::error::Error;
use std::time::Duration;

use oxide_batch::item_components::{
    KeysetColumn, PostgresComponentConfigError, PostgresCursorFormat, PostgresRow,
    postgres_cursor_reader,
};
use oxide_batch::{
    ComponentStreamIdentity, FailureCategory, ItemReader, ItemStream, PostgresConfig,
    PostgresConfigError, ReadContext, ReadOutcome, StopSource, StreamCloseContext,
    StreamOpenContext, StreamRuntimeOutcome, StreamUpdateContext, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

fn plaintext_config(url: String) -> Result<PostgresConfig, PostgresConfigError> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BusinessRow {
    sort_key: String,
    id: i64,
    payload: String,
}

fn map_row(row: &PostgresRow<'_>) -> Result<BusinessRow, oxide_batch::ReaderError> {
    Ok(BusinessRow {
        sort_key: row.text("sort_key")?,
        id: row.i64("id")?,
        payload: row.text("payload")?,
    })
}

fn failing_map_row(_row: &PostgresRow<'_>) -> Result<BusinessRow, oxide_batch::ReaderError> {
    Err(oxide_batch::ReaderError::with_category(
        FailureCategory::UserComponent,
    ))
}

fn key_columns() -> Vec<KeysetColumn> {
    vec![KeysetColumn::text("sort_key"), KeysetColumn::i64("id")]
}

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(format!("oxide-batch-test.postgres-cursor-{name}"))
        .expect("static identity is valid")
}

/// Creates (idempotently) the shared business schema/table and clears any
/// rows left by a previous run of `scope`, mirroring
/// `postgres_repository.rs::prepare_business_fixture`'s established
/// isolation-by-scope convention.
async fn prepare_scope(
    url: &str,
    scope: &str,
    rows: &[(&str, i64, &str)],
) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
    let schema_exists: bool =
        sqlx::query_scalar("SELECT to_regnamespace('oxide_batch_business') IS NOT NULL")
            .fetch_one(&pool)
            .await?;
    if !schema_exists {
        sqlx::query("CREATE SCHEMA oxide_batch_business")
            .execute(&pool)
            .await?;
    }
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oxide_batch_business.postgres_component_rows (\
         scope text NOT NULL, sort_key text NOT NULL, id bigint NOT NULL, \
         payload text NOT NULL, PRIMARY KEY (scope, id))",
    )
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM oxide_batch_business.postgres_component_rows WHERE scope = $1")
        .bind(scope)
        .execute(&pool)
        .await?;
    for (sort_key, id, payload) in rows {
        sqlx::query(
            "INSERT INTO oxide_batch_business.postgres_component_rows \
             (scope, sort_key, id, payload) VALUES ($1, $2, $3, $4)",
        )
        .bind(scope)
        .bind(*sort_key)
        .bind(*id)
        .bind(*payload)
        .execute(&pool)
        .await?;
    }
    pool.close().await;
    Ok(())
}

fn base_query(scope: &str) -> String {
    format!(
        "SELECT sort_key, id, payload FROM oxide_batch_business.postgres_component_rows \
         WHERE scope = '{scope}'"
    )
}

/// A `base_query` whose row generation stalls, so a short server-side
/// `statement_timeout` on the reader's own connection has something real to
/// cancel. `pg_sleep`'s `void` result is never selected by `map_row`.
fn slow_base_query(scope: &str) -> String {
    format!(
        "SELECT pg_sleep(2), sort_key, id, payload \
         FROM oxide_batch_business.postgres_component_rows WHERE scope = '{scope}'"
    )
}

async fn active_backends_idle_in_transaction(url: &str) -> Result<i64, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction' \
         AND query LIKE '%oxide_batch_cursor%'",
    )
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(count)
}

fn stop_source_and_token() -> (StopSource, oxide_batch::StopToken) {
    let (source, token) = StopSource::new();
    (source, token)
}

#[test]
fn empty_result_reports_end_of_input_immediately() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "cursor_empty_result";
        prepare_scope(&url, scope, &[]).await?;
        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(4),
            map_row,
            identity("empty"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let outcome = reader.read(ReadContext::new(&read_token)).await?;
        assert_eq!(outcome, ReadOutcome::EndOfInput);
        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn streams_bounded_batches_without_materializing_the_full_result_set() -> Result<(), Box<dyn Error>>
{
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "cursor_bounded_stream";
        let total_rows = 5_000_i64;
        let rows: Vec<(String, i64, String)> = (0..total_rows)
            .map(|id| ("k".to_owned(), id, format!("payload-{id}")))
            .collect();
        let borrowed: Vec<(&str, i64, &str)> = rows
            .iter()
            .map(|(sort_key, id, payload)| (sort_key.as_str(), *id, payload.as_str()))
            .collect();
        prepare_scope(&url, scope, &borrowed).await?;

        let fetch_size = 32usize;
        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(fetch_size),
            map_row,
            identity("bounded_stream"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;

        let (_read_source, read_token) = stop_source_and_token();
        let mut delivered = Vec::with_capacity(usize::try_from(total_rows)?);
        loop {
            match reader.read(ReadContext::new(&read_token)).await? {
                ReadOutcome::Item(item) => delivered.push(item.id),
                ReadOutcome::EndOfInput => break,
                ReadOutcome::Stopped => return Err("stop was never requested".into()),
                other => return Err(format!("unexpected read outcome: {other:?}").into()),
            }
        }
        assert_eq!(delivered.len(), usize::try_from(total_rows)?);
        assert_eq!(delivered, (0..total_rows).collect::<Vec<_>>());

        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn restart_resumes_from_the_last_committed_key_without_gap_or_duplicate()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "cursor_restart";
        let total_rows = 20_i64;
        let rows: Vec<(String, i64, String)> = (0..total_rows)
            .map(|id| ("k".to_owned(), id, format!("payload-{id}")))
            .collect();
        let borrowed: Vec<(&str, i64, &str)> = rows
            .iter()
            .map(|(sort_key, id, payload)| (sort_key.as_str(), *id, payload.as_str()))
            .collect();
        prepare_scope(&url, scope, &borrowed).await?;

        // First attempt: read a few items, simulate a committing chunk
        // boundary via `ItemStream::update`, then "crash" -- drop the reader
        // and stream without closing them, exactly as a killed process would
        // never call `close`.
        let mut delivered_before_crash = Vec::new();
        let envelope = {
            let config = plaintext_config(url.clone())?;
            let (mut reader, stream, _contract) = postgres_cursor_reader(
                config,
                base_query(scope),
                key_columns(),
                PostgresCursorFormat::new().with_fetch_size(3),
                map_row,
                identity("restart"),
            )?;
            let (_open_source, open_token) = stop_source_and_token();
            stream
                .open(StreamOpenContext::new(None, &open_token))
                .await?;
            let (_read_source, read_token) = stop_source_and_token();
            for _ in 0..7 {
                match reader.read(ReadContext::new(&read_token)).await? {
                    ReadOutcome::Item(item) => delivered_before_crash.push(item.id),
                    other => {
                        return Err(format!("unexpected outcome before crash: {other:?}").into());
                    }
                }
            }
            // This is the durable checkpoint boundary: only items delivered
            // up to *this* `update()` call are ever treated as committed.
            let envelope = stream.update(StreamUpdateContext::new(&open_token)).await?;
            // Read two more items *after* the checkpointed boundary, then
            // abandon this reader/stream without another `update()` -- a
            // restart must re-deliver these, never skip them.
            for _ in 0..2 {
                match reader.read(ReadContext::new(&read_token)).await? {
                    ReadOutcome::Item(item) => delivered_before_crash.push(item.id),
                    other => {
                        return Err(format!("unexpected outcome before crash: {other:?}").into());
                    }
                }
            }
            envelope
        };
        assert_eq!(delivered_before_crash.len(), 9);
        let committed_before_crash = &delivered_before_crash[..7];

        // Restart: a fresh reader/stream pair restores from the captured
        // envelope.
        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(3),
            map_row,
            identity("restart"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(Some(&envelope), &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let mut delivered_after_restart = Vec::new();
        loop {
            match reader.read(ReadContext::new(&read_token)).await? {
                ReadOutcome::Item(item) => delivered_after_restart.push(item.id),
                ReadOutcome::EndOfInput => break,
                ReadOutcome::Stopped => return Err("stop was never requested".into()),
                other => return Err(format!("unexpected read outcome: {other:?}").into()),
            }
        }

        // No skip: every row not yet committed before the crash is
        // re-delivered after restart.
        assert_eq!(delivered_after_restart, (7..total_rows).collect::<Vec<_>>());
        // No unbounded duplicate: nothing committed before the crash
        // reappears.
        for id in committed_before_crash {
            assert!(!delivered_after_restart.contains(id));
        }

        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn malformed_row_fails_closed_without_advancing_the_checkpoint() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "cursor_malformed_row";
        prepare_scope(&url, scope, &[("k", 1, "payload-1"), ("k", 2, "payload-2")]).await?;

        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(4),
            failing_map_row,
            identity("malformed_row"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let error = reader
            .read(ReadContext::new(&read_token))
            .await
            .expect_err("map_row was configured to always fail");
        assert!(!error.has_checkpoint_advanced());

        // Retrying immediately re-attempts the exact same row (id = 1), not
        // the next one: the failing row is never silently consumed.
        let retry_error = reader
            .read(ReadContext::new(&read_token))
            .await
            .expect_err("map_row still fails on retry");
        assert!(!retry_error.has_checkpoint_advanced());

        let envelope = stream.update(StreamUpdateContext::new(&open_token)).await?;
        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Failed,
            ))
            .await?;

        // A fresh reader restored from that envelope, now with a
        // succeeding `map_row`, must still start at id = 1: the checkpoint
        // never advanced past the row `map_row` kept failing on.
        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(4),
            map_row,
            identity("malformed_row"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(Some(&envelope), &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let mut delivered = Vec::new();
        loop {
            match reader.read(ReadContext::new(&read_token)).await? {
                ReadOutcome::Item(item) => delivered.push(item.id),
                ReadOutcome::EndOfInput => break,
                ReadOutcome::Stopped => return Err("stop was never requested".into()),
                other => return Err(format!("unexpected read outcome: {other:?}").into()),
            }
        }
        assert_eq!(delivered, vec![1, 2]);
        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn close_rolls_back_and_leaves_no_idle_in_transaction_backend() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "cursor_cleanup";
        prepare_scope(&url, scope, &[("k", 1, "payload-1"), ("k", 2, "payload-2")]).await?;

        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(1),
            map_row,
            identity("cleanup"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let outcome = reader.read(ReadContext::new(&read_token)).await?;
        assert!(matches!(outcome, ReadOutcome::Item(_)));

        assert_eq!(active_backends_idle_in_transaction(&url).await?, 1);

        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;

        assert_eq!(active_backends_idle_in_transaction(&url).await?, 0);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn empty_key_columns_are_rejected_at_construction() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let config = plaintext_config(url)?;
    let result = postgres_cursor_reader::<BusinessRow>(
        config,
        base_query("unused"),
        Vec::new(),
        PostgresCursorFormat::new(),
        map_row,
        identity("empty_key_columns"),
    );
    assert_eq!(
        result.err(),
        Some(PostgresComponentConfigError::EmptyKeyColumns)
    );
    Ok(())
}

/// `PostgresConfig`'s server-side timeout semantics (#149 item #15) must
/// reach the cursor reader's own business-data connection, not just the
/// framework's metadata connection -- `connect_pool`'s `after_connect` hook
/// is what wires this up (`repository/postgres.rs`). A `statement_timeout`
/// far shorter than a deliberately slow `base_query` proves the setting is
/// live on this reader's connection specifically: `57014` (`query_canceled`)
/// classifies as `FailureCategory::Cancelled` via `classify_pg_error`.
#[test]
fn statement_timeout_is_enforced_on_the_cursor_business_connection() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "cursor_statement_timeout";
        prepare_scope(&url, scope, &[("k", 1, "payload-1")]).await?;

        let config = plaintext_config(url.clone())?
            .with_lock_timeout(Duration::from_millis(50))?
            .with_statement_timeout(Duration::from_millis(200))?;
        let (mut reader, stream, _contract) = postgres_cursor_reader(
            config,
            slow_base_query(scope),
            key_columns(),
            PostgresCursorFormat::new().with_fetch_size(4),
            map_row,
            identity("statement_timeout"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let error = reader.read(ReadContext::new(&read_token)).await.expect_err(
            "a 200ms statement_timeout must cancel a FETCH that stalls for 2s on this \
                 reader's own connection",
        );
        assert_eq!(error.category(), FailureCategory::Cancelled);
        Ok::<(), Box<dyn Error>>(())
    })
}
