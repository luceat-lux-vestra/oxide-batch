//! Metadata adapters for the extracted repository ports.
//!
//! The ports themselves live in `oxide-batch-repository`. This module holds the
//! adapters that implement them: the reference in-memory implementation and the
//! `PostgreSQL` implementation behind the `postgres` feature.

mod memory;
#[cfg(feature = "postgres")]
pub(crate) mod postgres;
#[cfg(feature = "postgres")]
mod scope_postgres;

pub use memory::{InMemoryExplorer, InMemoryJobRepository};
#[cfg(feature = "postgres")]
pub use postgres::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresExplorer, PostgresFaultState, PostgresJobRepository, PostgresMigrator, TlsMode,
};

pub(crate) fn repeat_request_matches_manifest(
    manifest: &serde_json::Value,
    request: &crate::RepeatCommitRequest,
) -> bool {
    fn repeat_matches(
        repeat: &serde_json::Value,
        repeat_id: &crate::RepeatId,
        state: &crate::ExecutionContext,
    ) -> bool {
        let matches = repeat.as_object().is_some_and(|object| {
            object.get("id").and_then(serde_json::Value::as_str) == Some(repeat_id.as_str())
                && object
                    .get("state")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|state_manifest| {
                        state_manifest
                            .get("schema")
                            .and_then(serde_json::Value::as_str)
                            == Some(state.schema_id().as_str())
                            && state_manifest
                                .get("version")
                                .and_then(serde_json::Value::as_u64)
                                == Some(u64::from(state.schema_version().get()))
                    })
        });
        matches
            || repeat
                .get("nested")
                .is_some_and(|nested| repeat_matches(nested, repeat_id, state))
    }

    fn visit(value: &serde_json::Value, request: &crate::RepeatCommitRequest) -> bool {
        match value {
            serde_json::Value::Object(object) => {
                if object.get("kind").and_then(serde_json::Value::as_str) == Some("step")
                    && object.get("id").and_then(serde_json::Value::as_str)
                        == Some(request.node_id().as_str())
                    && object.get("repeat").is_some_and(|repeat| {
                        repeat_matches(repeat, request.repeat_id(), request.state())
                    })
                {
                    return true;
                }
                object.values().any(|child| visit(child, request))
            }
            serde_json::Value::Array(values) => values.iter().any(|child| visit(child, request)),
            _ => false,
        }
    }

    visit(manifest, request)
}
