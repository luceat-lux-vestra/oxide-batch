//! #149 evidence: `PostgresPagingReader` keyset paging, restart, and
//! bounded-resource behavior against a real `PostgreSQL` server.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::error::Error;
use std::time::Duration;

use oxide_batch::item_components::{
    KeysetColumn, PostgresPagingFormat, PostgresRow, postgres_paging_reader,
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
}

fn map_row(row: &PostgresRow<'_>) -> Result<BusinessRow, oxide_batch::ReaderError> {
    Ok(BusinessRow {
        sort_key: row.text("sort_key")?,
        id: row.i64("id")?,
    })
}

fn key_columns() -> Vec<KeysetColumn> {
    vec![KeysetColumn::text("sort_key"), KeysetColumn::i64("id")]
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Int4Row {
    id: i32,
}

fn map_int4_row(row: &PostgresRow<'_>) -> Result<Int4Row, oxide_batch::ReaderError> {
    Ok(Int4Row { id: row.i32("id")? })
}

fn identity(name: &str) -> ComponentStreamIdentity {
    ComponentStreamIdentity::new(format!("oxide-batch-test.postgres-paging-{name}"))
        .expect("static identity is valid")
}

async fn prepare_scope(url: &str, scope: &str, rows: &[(&str, i64)]) -> Result<(), sqlx::Error> {
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
    for (sort_key, id) in rows {
        sqlx::query(
            "INSERT INTO oxide_batch_business.postgres_component_rows \
             (scope, sort_key, id, payload) VALUES ($1, $2, $3, 'unused')",
        )
        .bind(scope)
        .bind(*sort_key)
        .bind(*id)
        .execute(&pool)
        .await?;
    }
    pool.close().await;
    Ok(())
}

fn base_query(scope: &str) -> String {
    format!(
        "SELECT sort_key, id FROM oxide_batch_business.postgres_component_rows \
         WHERE scope = '{scope}'"
    )
}

/// A `base_query` whose row generation stalls, so a short server-side
/// `statement_timeout` on the reader's own connection has something real to
/// cancel. `pg_sleep`'s `void` result is never selected by `map_row`.
fn slow_base_query(scope: &str) -> String {
    format!(
        "SELECT pg_sleep(2), sort_key, id \
         FROM oxide_batch_business.postgres_component_rows WHERE scope = '{scope}'"
    )
}

async fn active_backend_count(url: &str) -> Result<i64, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'",
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

async fn read_all(
    reader: &mut oxide_batch::item_components::PostgresPagingReader<BusinessRow>,
) -> Result<Vec<i64>, Box<dyn Error>> {
    let (_source, token) = stop_source_and_token();
    let mut delivered = Vec::new();
    loop {
        match reader.read(ReadContext::new(&token)).await? {
            ReadOutcome::Item(item) => delivered.push(item.id),
            ReadOutcome::EndOfInput => break,
            ReadOutcome::Stopped => return Err("stop was never requested".into()),
            other => return Err(format!("unexpected read outcome: {other:?}").into()),
        }
    }
    Ok(delivered)
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
        let scope = "paging_empty";
        prepare_scope(&url, scope, &[]).await?;
        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresPagingFormat::new().with_page_size(4),
            map_row,
            identity("empty"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        assert_eq!(read_all(&mut reader).await?, Vec::<i64>::new());
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
fn page_boundaries_deliver_every_row_exactly_once() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Page sizes chosen to cover: one row, less-than-a-page, exactly one
        // page, and several pages, over the same underlying row count.
        for (row_count, page_size) in [(1_i64, 4_usize), (3, 10), (10, 10), (25, 4), (1, 1)] {
            let scope = format!("paging_boundaries_{row_count}_{page_size}");
            let rows: Vec<(String, i64)> = (0..row_count).map(|id| ("k".to_owned(), id)).collect();
            let borrowed: Vec<(&str, i64)> = rows
                .iter()
                .map(|(sort_key, id)| (sort_key.as_str(), *id))
                .collect();
            prepare_scope(&url, &scope, &borrowed).await?;

            let config = plaintext_config(url.clone())?;
            let (mut reader, stream, _contract) = postgres_paging_reader(
                config,
                base_query(&scope),
                key_columns(),
                PostgresPagingFormat::new().with_page_size(page_size),
                map_row,
                identity(&format!("boundaries_{row_count}_{page_size}")),
            )?;
            let (_open_source, open_token) = stop_source_and_token();
            stream
                .open(StreamOpenContext::new(None, &open_token))
                .await?;
            let delivered = read_all(&mut reader).await?;
            assert_eq!(
                delivered,
                (0..row_count).collect::<Vec<_>>(),
                "row_count={row_count} page_size={page_size}"
            );
            stream
                .close(StreamCloseContext::new(
                    &open_token,
                    StreamRuntimeOutcome::Committed,
                ))
                .await?;
        }
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn duplicate_primary_sort_key_is_resolved_by_the_unique_tiebreaker() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "paging_duplicate_sort_key";
        // Every row shares the same `sort_key`; only `id` (the unique
        // tiebreaker) distinguishes them. A non-unique-order paging
        // implementation would skip or repeat rows across page boundaries.
        let rows: Vec<(&str, i64)> = (0..17_i64).map(|id| ("same", id)).collect();
        prepare_scope(&url, scope, &rows).await?;

        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresPagingFormat::new().with_page_size(5),
            map_row,
            identity("duplicate_sort_key"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let mut delivered = read_all(&mut reader).await?;
        delivered.sort_unstable();
        assert_eq!(delivered, (0..17_i64).collect::<Vec<_>>());
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
fn restart_resumes_from_the_last_committed_key_without_skip_or_duplicate()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "paging_restart";
        let total_rows = 23_i64;
        let rows: Vec<(String, i64)> = (0..total_rows).map(|id| ("k".to_owned(), id)).collect();
        let borrowed: Vec<(&str, i64)> = rows
            .iter()
            .map(|(sort_key, id)| (sort_key.as_str(), *id))
            .collect();
        prepare_scope(&url, scope, &borrowed).await?;

        let envelope = {
            let config = plaintext_config(url.clone())?;
            let (mut reader, stream, _contract) = postgres_paging_reader(
                config,
                base_query(scope),
                key_columns(),
                PostgresPagingFormat::new().with_page_size(4),
                map_row,
                identity("restart"),
            )?;
            let (_open_source, open_token) = stop_source_and_token();
            stream
                .open(StreamOpenContext::new(None, &open_token))
                .await?;
            let (_read_source, read_token) = stop_source_and_token();
            for _ in 0..9 {
                reader.read(ReadContext::new(&read_token)).await?;
            }
            // Commit boundary: rows read after this point are never
            // committed and must be re-delivered after restart.
            let envelope = stream.update(StreamUpdateContext::new(&open_token)).await?;
            for _ in 0..3 {
                reader.read(ReadContext::new(&read_token)).await?;
            }
            envelope
        };

        // Between the crash and the restart, insert rows into a gap that a
        // (forbidden) OFFSET-based implementation would silently skip past;
        // a keyset-based restart is unaffected because it filters on the
        // last delivered key, not a positional count.
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?;
        sqlx::query(
            "INSERT INTO oxide_batch_business.postgres_component_rows \
             (scope, sort_key, id, payload) VALUES ($1, 'k', -1, 'unused')",
        )
        .bind(scope)
        .execute(&pool)
        .await?;
        pool.close().await;

        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresPagingFormat::new().with_page_size(4),
            map_row,
            identity("restart"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(Some(&envelope), &open_token))
            .await?;
        let delivered = read_all(&mut reader).await?;
        // Exactly the rows from id 9 onward: nothing before the committed
        // boundary reappears (no duplicate), nothing from 9 up is missing
        // (no skip), and the out-of-window inserted row (-1) never appears
        // because it sorts before the restored key.
        assert_eq!(delivered, (9..total_rows).collect::<Vec<_>>());
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
fn no_server_side_resource_is_held_between_pages() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "paging_no_held_resource";
        let rows: Vec<(&str, i64)> = (0..12_i64).map(|id| ("k", id)).collect();
        prepare_scope(&url, scope, &rows).await?;

        let baseline = active_backend_count(&url).await?;
        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresPagingFormat::new().with_page_size(3),
            map_row,
            identity("no_held_resource"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        // Read across a page boundary (page size 3, read 4 rows) and assert
        // no transaction is left open in between.
        for _ in 0..4 {
            reader.read(ReadContext::new(&read_token)).await?;
        }
        assert_eq!(active_backend_count(&url).await?, baseline);
        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

/// `PostgresConfig`'s server-side timeout semantics (#149 item #15) must
/// reach the paging reader's own business-data connection, not just the
/// framework's metadata connection -- `connect_pool`'s `after_connect` hook
/// is what wires this up (`repository/postgres.rs`). A `statement_timeout`
/// far shorter than a deliberately slow `base_query` proves the setting is
/// live on this reader's connection specifically: `57014` (`query_canceled`)
/// classifies as `FailureCategory::Cancelled` via `classify_pg_error`.
#[test]
fn statement_timeout_is_enforced_on_the_paging_business_connection() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "paging_statement_timeout";
        prepare_scope(&url, scope, &[("k", 1)]).await?;

        let config = plaintext_config(url.clone())?
            .with_lock_timeout(Duration::from_millis(50))?
            .with_statement_timeout(Duration::from_millis(200))?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            slow_base_query(scope),
            key_columns(),
            PostgresPagingFormat::new().with_page_size(4),
            map_row,
            identity("statement_timeout"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let error = reader.read(ReadContext::new(&read_token)).await.expect_err(
            "a 200ms statement_timeout must cancel a page fetch that stalls for 2s on this \
                 reader's own connection",
        );
        assert_eq!(error.category(), FailureCategory::Cancelled);
        Ok::<(), Box<dyn Error>>(())
    })
}

/// Pins the paging-specific half of `postgres_keyset`'s "Paging: every page
/// is a fresh, independently visible statement" documentation directly
/// against a real server: unlike the cursor reader's held snapshot (see
/// `postgres_item_components_cursor.rs`'s sibling test), a row committed by
/// another transaction between two pages of the *same* attempt, at a key
/// past everything already delivered, is delivered by a later page --
/// because each page is its own fresh, independent statement, not a portal
/// held open across the whole attempt.
#[test]
fn insert_between_pages_is_visible_to_a_later_page_in_the_same_attempt()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let scope = "paging_insert_visibility";
        let rows: Vec<(&str, i64)> = vec![("k", 0), ("k", 1), ("k", 2), ("k", 3), ("k", 4)];
        prepare_scope(&url, scope, &rows).await?;

        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            base_query(scope),
            key_columns(),
            PostgresPagingFormat::new().with_page_size(2),
            map_row,
            identity("insert_visibility"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();

        // First page (page_size 2) delivers rows 0 and 1.
        let mut delivered = Vec::new();
        for _ in 0..2 {
            match reader.read(ReadContext::new(&read_token)).await? {
                ReadOutcome::Item(item) => delivered.push(item.id),
                other => return Err(format!("unexpected outcome: {other:?}").into()),
            }
        }
        assert_eq!(delivered, vec![0, 1]);

        // A second connection commits a row at a key past everything this
        // scope had, *between* pages of this same, still-in-progress
        // attempt.
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(
            "INSERT INTO oxide_batch_business.postgres_component_rows \
             (scope, sort_key, id, payload) VALUES ($1, 'k', 10, 'unused')",
        )
        .bind(scope)
        .execute(&admin)
        .await?;
        admin.close().await;

        // The rest of this same attempt's reads -- several more pages --
        // must deliver the 5 pre-existing rows *and* the concurrently
        // inserted row 10, in order, without needing a restart.
        loop {
            match reader.read(ReadContext::new(&read_token)).await? {
                ReadOutcome::Item(item) => delivered.push(item.id),
                ReadOutcome::EndOfInput => break,
                other => return Err(format!("unexpected outcome: {other:?}").into()),
            }
        }
        assert_eq!(
            delivered,
            vec![0, 1, 2, 3, 4, 10],
            "a row committed by another transaction between two pages of the same attempt \
             must be delivered by a later page, unlike the cursor reader's held snapshot"
        );
        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

/// `KeysetColumnKind::I64`'s doc comment promises "`bigint`/`integer`-family"
/// coverage. `sqlx` decodes strictly by wire type and never implicitly
/// widens an `int4` column into a requested `i64`, so this pins the
/// int4-specific fallback `extract_keyset` needs directly against a real
/// `integer`-typed (not `bigint`) primary key column -- a bare
/// `try_get::<i64, _>` would reject every row here.
#[test]
fn keyset_i64_column_kind_decodes_from_a_real_int4_column() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS oxide_batch_business")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS oxide_batch_business.postgres_149_int4_keyset_rows (\
             id integer PRIMARY KEY)",
        )
        .execute(&pool)
        .await?;
        sqlx::query("DELETE FROM oxide_batch_business.postgres_149_int4_keyset_rows")
            .execute(&pool)
            .await?;
        for id in 0_i32..5 {
            sqlx::query(
                "INSERT INTO oxide_batch_business.postgres_149_int4_keyset_rows (id) VALUES ($1)",
            )
            .bind(id)
            .execute(&pool)
            .await?;
        }
        pool.close().await;

        let config = plaintext_config(url.clone())?;
        let (mut reader, stream, _contract) = postgres_paging_reader(
            config,
            "SELECT id FROM oxide_batch_business.postgres_149_int4_keyset_rows".to_owned(),
            vec![KeysetColumn::i64("id")],
            PostgresPagingFormat::new().with_page_size(2),
            map_int4_row,
            identity("int4_keyset"),
        )?;
        let (_open_source, open_token) = stop_source_and_token();
        stream
            .open(StreamOpenContext::new(None, &open_token))
            .await?;
        let (_read_source, read_token) = stop_source_and_token();
        let mut delivered = Vec::new();
        loop {
            match reader.read(ReadContext::new(&read_token)).await? {
                ReadOutcome::Item(item) => delivered.push(item.id),
                ReadOutcome::EndOfInput => break,
                other => return Err(format!("unexpected outcome: {other:?}").into()),
            }
        }
        assert_eq!(
            delivered,
            vec![0, 1, 2, 3, 4],
            "KeysetColumn::i64 must decode and restart-filter correctly against a real int4 \
             column across a page boundary, not merely accept it at construction"
        );
        stream
            .close(StreamCloseContext::new(
                &open_token,
                StreamRuntimeOutcome::Committed,
            ))
            .await?;
        Ok::<(), Box<dyn Error>>(())
    })
}
