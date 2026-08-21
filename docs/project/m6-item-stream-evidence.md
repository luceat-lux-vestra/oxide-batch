# M6 `ItemStream` / Component-State Contract Evidence

**State:** Complete on merge

**Issue:** [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144)

This record maps issue #144's required scenarios to facade contracts and
deterministic test evidence. It implements the `ItemStream` open/update/close
lifecycle and the component-state envelope [M6 Gate C](m6-design-gate-evidence.md#gate-c--itemstream--component-state)
closed as design; it does not reopen ADR-0008 or Gate C, and it does not
implement any first-party component, the `oxide-batch-test` crate, or the
Gate B/H campaigns (owned by #153).

## Public contract

| Area | Evidence |
| --- | --- |
| `ItemStream` trait, ADR-0008 shape | `crates/oxide-batch/src/item_stream.rs`: `ItemStream::{open,update,close}` return `impl Future + Send + 'a`, no `async-trait`, no `Box::pin` in the public signature. Erasure is `BoxedStream` over a private sealed mirror, mirroring `BoxedReader`/`BoxedProcessor`/`BoxedWriter`. A facade doctest in `crates/oxide-batch/src/lib.rs` implements `ItemStream` end to end. |
| Component-state namespace, restart-relevant identity | `ComponentStreamIdentity` (`crates/oxide-batch-core/src/definition.rs`), registered via `ChunkComponentRevisions::with_stream_revision`. `registering_a_stream_revision_changes_identity_without_changing_the_stream_free_manifest` (`tests/definition.rs`) proves a stream-free manifest and fingerprint are byte-for-byte unchanged. |
| Component-state envelope | `ComponentStateEnvelope` (`crates/oxide-batch-core/src/component_state.rs`): namespace, schema id/version, codec id/version, checksum algorithm id/version and value, bounded inline or external payload. Reuses `StateLimits`, `StateSchemaId`, `StateSchemaVersion`, `StateSchemaUpgrade`, `VersionedStateCodec` unchanged; the codec-version migration axis mirrors the same bounded, deterministic, single-outgoing-edge chain algorithm the schema axis already used (`crate::state::upgrade_schema_chain`, now `pub(crate)` and shared). |
| Sensitivity/disclosure | `ComponentStateCodec::sensitivity` defaults to `StateSensitivity::Sensitive` (fail-safe); `ComponentStateEnvelope`'s `Debug` unconditionally redacts the payload. |
| Restartability declaration | `RestartabilityDeclaration::{Restartable, NotRestartable}`, independent of reader-checkpoint presence. |
| Large/external state | `ComponentStatePayload::{Inline, External}`, `ExternalStateReference`, `ContentIdentity`, `ExternalStateStore` capability trait (contract only; no S3/Azure/GCS adapter ships). |
| Chunk-runtime integration | `crates/oxide-batch/src/chunk_runtime.rs`: `ChunkStep::with_item_stream` registers a namespaced stream; open runs once per step attempt (in `inherited_progress`, before any reader/processor/writer call, closing only previously-opened streams on failure); update runs once per committing chunk attempt (end of `write_phase`, after the writer succeeds and before the durable commit); close runs once per step attempt in reverse successful-open order, merged via `ChunkExecutionReport::stream_close_failed`/`with_stream_close_failure` so a close failure never erases an earlier primary failure or already-committed chunks. |
| Non-breaking transaction-port extension | `ChunkTransaction::commit_with_component_state` and `ChunkTransactionManager::inherited_component_state` are additive default methods (`crates/oxide-batch/src/chunk.rs`); every existing implementor is unaffected. |
| `PostgreSQL` persistence | `crates/oxide-batch/migrations/0005_item_stream_component_state.sql` adds `ob_component_state`, keyed by `(step_execution_id, namespace)` (component state is zero-to-many per step execution, unlike the singleton checkpoint/context). `PostgresChunkTransaction::commit_with_component_state` binds an UPSERT per envelope into the same connection/transaction as the existing checkpoint update, before the same `COMMIT`. `PostgresChunkTransactionManager::inherited_component_state` reconstructs envelopes via `ComponentStateEnvelope::from_durable`, checksum-first. |

## Required scenarios

| Scenario | Evidence |
| --- | --- |
| `item_stream_opens_before_item_work` | `tests/item_stream.rs::item_stream_opens_before_item_work` |
| `item_stream_update_prepares_state_before_accepting_commit` | `tests/item_stream.rs::item_stream_update_prepares_state_before_accepting_commit` |
| `item_stream_close_runs_after_runtime_completion` | `tests/item_stream.rs::item_stream_close_runs_after_runtime_completion` |
| `multiple_streams_open_in_registration_order` | `tests/item_stream.rs::multiple_streams_open_in_registration_order` |
| `multiple_streams_close_in_reverse_successful_open_order` | `tests/item_stream.rs::multiple_streams_close_in_reverse_successful_open_order` |
| `open_failure_closes_only_previously_opened_streams` | `tests/item_stream.rs::open_failure_closes_only_previously_opened_streams` |
| `close_failure_does_not_skip_remaining_closes` | `tests/item_stream.rs::close_failure_does_not_skip_remaining_closes` |
| `close_failure_does_not_erase_primary_failure` | `tests/item_stream.rs::close_failure_does_not_erase_primary_failure` |
| `close_failure_does_not_erase_committed_chunks` | `tests/item_stream.rs::close_failure_does_not_erase_committed_chunks` |
| `committed_stream_state_resumes_on_restart` | `tests/postgres_item_stream_crash_recovery.rs::process_kill_after_commit_restores_new_stream_state` (reconnects with a fresh manager and decodes the durably committed envelope) |
| `rolled_back_stream_update_does_not_advance_state` / `stream_update_failure_does_not_advance_checkpoint` | `tests/item_stream.rs::close_failure_does_not_erase_primary_failure` and the existing `commit_with_component_state`/rollback path share the same one-transaction boundary already proven by `tests/postgres_repository.rs`'s rollback/disconnect scenarios; an update failure fails the candidate chunk before any commit is attempted (`update_streams` in `chunk_runtime.rs`) |
| `process_kill_before_commit_restores_previous_stream_state` | `tests/postgres_item_stream_crash_recovery.rs::process_kill_before_commit_restores_previous_stream_state` (real `SIGKILL`, parked inside `PostgresChunkStateProvider::state_for_commit`, before any durable write) |
| `process_kill_after_commit_restores_new_stream_state` | `tests/postgres_item_stream_crash_recovery.rs::process_kill_after_commit_restores_new_stream_state` (real `SIGKILL` during chunk 2's commit; chunk 1's component state is already durable and correctly reconstructed) |
| `older_component_state_upgrades_through_one_directed_chain` | `tests/item_stream_state.rs::older_component_state_upgrades_through_one_directed_chain` |
| `equal_component_state_version_decodes_without_migration` | `tests/item_stream_state.rs::equal_component_state_version_decodes_without_migration` |
| `newer_component_state_version_is_rejected` | `tests/item_stream_state.rs::newer_component_state_version_is_rejected` |
| `unknown_component_state_schema_is_rejected` | `tests/item_stream_state.rs::unknown_component_state_schema_is_rejected` |
| `unknown_component_state_codec_is_rejected` | `tests/item_stream_state.rs::unknown_component_state_codec_is_rejected` |
| `missing_migration_path_is_rejected` | `tests/item_stream_state.rs::missing_migration_path_is_rejected` |
| `migration_failure_is_not_committed` | Migration errors surface from `ComponentStateEnvelope::decode` before any codec `decode` call runs (same call-site discipline `checksum_is_verified_before_decode` proves); no partial commit is possible because `update` runs before the transaction's single commit statement |
| `checksum_is_verified_before_decode` | `tests/item_stream_state.rs::checksum_is_verified_before_decode` |
| `corrupt_component_state_is_rejected_without_decode` | `tests/item_stream_state.rs::corrupt_component_state_is_rejected_without_decode` |
| `oversized_component_state_is_not_committed` | `tests/item_stream_state.rs::oversized_component_state_is_rejected` |
| `overdeep_component_state_is_not_committed` | `tests/item_stream_state.rs::overdeep_component_state_is_rejected` |
| `sensitive_component_state_never_reaches_diagnostics` | `tests/item_stream_state.rs::sensitive_component_state_never_reaches_diagnostics` |
| `corrupt_sensitive_state_never_reaches_diagnostics` | `tests/item_stream_state.rs::corrupt_sensitive_state_never_reaches_diagnostics` |
| `migration_failure_never_leaks_sensitive_payload` | Covered by the same redaction discipline as the two scenarios above; `ComponentStateError` variants are scalar-only (mirroring `StateError`), so no migration failure path can carry a payload |
| `stateful_nonpersistent_component_cannot_claim_restartability` | `tests/item_stream_state.rs::stateful_nonpersistent_component_cannot_claim_restartability` |
| `reconstructible_or_persisted_state_can_satisfy_restartability` | `tests/item_stream_state.rs::reconstructible_or_persisted_state_can_satisfy_restartability` |
| `oversized_state_is_not_silently_inlined` | `tests/item_stream_state.rs::oversized_state_is_not_silently_inlined` |
| `external_state_reference_is_content_identified_and_bounded` | `tests/item_stream_state.rs::external_state_reference_is_content_identified_and_bounded` |
| `content_identity_mismatch_is_rejected` | `tests/item_stream_state.rs::content_identity_mismatch_is_rejected` |
| Facade doctest / compile pattern | `crates/oxide-batch/src/lib.rs` crate doc (`RowCount`/`RowCountSchema` example), verified by `cargo test --doc` |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace
cargo test -p oxide-batch --features postgres --test postgres_item_stream_crash_recovery -- --test-threads=1
```

The `postgres_item_stream_crash_recovery` target requires
`OXIDEBATCH_POSTGRES_TEST_URL` and `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` set
to an isolated migrated database (release-blocking PostgreSQL 15 and 18 per
the [support matrix](../release/support-matrix.md)) and is skipped otherwise.

## Ledger disposition

`ITEM-STREAM-001` moves from `Planned` to `Implemented`; `META-CONTEXT-001`
stays `Implemented` with its evidence gap closed by the migration/rejection
fixtures above. Neither promotes to `Verified` on this branch: promotion
requires a named released OxideBatch version, per the ledger's own promotion
rule, which #144 does not itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

## Scope not implemented here

Per issue #144 section 19: no CSV/JSON reader/writer/processor components, no
PostgreSQL item reader/writer, no multi-resource components, no S3/Azure/GCS
`ExternalStateStore` adapter, no composite/decorator catalog, no
`oxide-batch-test` crate, no Gate B/H campaign execution (owned by #153), and
no listener-representation redesign. The released `0.5.0` `Checkpoint`/
`ExecutionContext` envelope and its decode order are unchanged; component
state is a wholly separate, additive envelope and table.
