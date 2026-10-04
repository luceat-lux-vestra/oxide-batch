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
    partition_manager_node_id: Option<&crate::NodeId>,
) -> bool {
    fn repeat_matches(
        repeat: &serde_json::Value,
        lineage: &crate::RepeatLineage,
        repeat_id: &crate::RepeatId,
        state: &crate::ExecutionContext,
    ) -> bool {
        let mut current = repeat;
        for (parent_id, _) in lineage.iter() {
            let Some(object) = current.as_object() else {
                return false;
            };
            if object.get("id").and_then(serde_json::Value::as_str) != Some(parent_id.as_str()) {
                return false;
            }
            let Some(nested) = object.get("nested") else {
                return false;
            };
            current = nested;
        }

        current.as_object().is_some_and(|object| {
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
        })
    }

    fn step_matches(step: &serde_json::Value, request: &crate::RepeatCommitRequest) -> bool {
        step.as_object().is_some_and(|object| {
            object.get("kind").and_then(serde_json::Value::as_str) == Some("step")
                && object.get("id").and_then(serde_json::Value::as_str)
                    == Some(request.definition_node_id().as_str())
                && object.get("repeat").is_some_and(|repeat| {
                    repeat_matches(
                        repeat,
                        request.lineage(),
                        request.repeat_id(),
                        request.state(),
                    )
                })
        })
    }

    fn partition_matches(
        value: &serde_json::Value,
        manager_node_id: &crate::NodeId,
        request: &crate::RepeatCommitRequest,
    ) -> bool {
        value.as_object().is_some_and(|object| {
            object.get("kind").and_then(serde_json::Value::as_str) == Some("partitioned_step")
                && object.get("id").and_then(serde_json::Value::as_str)
                    == Some(manager_node_id.as_str())
                && object
                    .get("worker")
                    .is_some_and(|worker| step_matches(worker, request))
        })
    }

    fn visit_step(value: &serde_json::Value, request: &crate::RepeatCommitRequest) -> bool {
        match value {
            serde_json::Value::Object(object) => {
                if step_matches(value, request) {
                    return true;
                }
                object.values().any(|child| visit_step(child, request))
            }
            serde_json::Value::Array(values) => {
                values.iter().any(|child| visit_step(child, request))
            }
            _ => false,
        }
    }

    fn visit_partition(
        value: &serde_json::Value,
        manager_node_id: &crate::NodeId,
        request: &crate::RepeatCommitRequest,
    ) -> bool {
        match value {
            serde_json::Value::Object(object) => {
                if partition_matches(value, manager_node_id, request) {
                    return true;
                }
                object
                    .values()
                    .any(|child| visit_partition(child, manager_node_id, request))
            }
            serde_json::Value::Array(values) => values
                .iter()
                .any(|child| visit_partition(child, manager_node_id, request)),
            _ => false,
        }
    }

    if request.definition_node_id() == request.node_id() {
        partition_manager_node_id.is_none() && visit_step(manifest, request)
    } else {
        partition_manager_node_id.is_some_and(|manager| visit_partition(manifest, manager, request))
    }
}
