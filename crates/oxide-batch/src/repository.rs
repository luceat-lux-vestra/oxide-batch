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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RepeatManifestOwner {
    Static,
    Partitioned(crate::NodeId),
}

pub(crate) fn repeat_request_manifest_owner(
    manifest: &serde_json::Value,
    request: &crate::RepeatCommitRequest,
) -> Option<RepeatManifestOwner> {
    fn repeat_state_matches(
        repeat: &serde_json::Value,
        repeat_id: &crate::RepeatId,
        state: &crate::ExecutionContext,
    ) -> bool {
        repeat.as_object().is_some_and(|object| {
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

    fn lineage_repeat_matches(
        repeat: &serde_json::Value,
        lineage: &[(crate::RepeatId, crate::RepeatOrdinal)],
        repeat_id: &crate::RepeatId,
        state: &crate::ExecutionContext,
    ) -> bool {
        let Some(object) = repeat.as_object() else {
            return false;
        };
        if let Some((ancestor_id, _)) = lineage.first() {
            return object.get("id").and_then(serde_json::Value::as_str)
                == Some(ancestor_id.as_str())
                && object.get("nested").is_some_and(|nested| {
                    lineage_repeat_matches(nested, &lineage[1..], repeat_id, state)
                });
        }
        repeat_state_matches(repeat, repeat_id, state)
    }

    fn step_matches(step: &serde_json::Value, request: &crate::RepeatCommitRequest) -> bool {
        let Some(object) = step.as_object() else {
            return false;
        };
        object.get("kind").and_then(serde_json::Value::as_str) == Some("step")
            && object.get("id").and_then(serde_json::Value::as_str)
                == Some(request.definition_node_id().as_str())
            && object.get("repeat").is_some_and(|repeat| {
                lineage_repeat_matches(
                    repeat,
                    request.lineage().ancestors(),
                    request.repeat_id(),
                    request.state(),
                )
            })
    }

    fn visit(
        value: &serde_json::Value,
        request: &crate::RepeatCommitRequest,
    ) -> Option<RepeatManifestOwner> {
        match value {
            serde_json::Value::Object(object) => {
                if object.get("kind").and_then(serde_json::Value::as_str)
                    == Some("partitioned_step")
                {
                    let manager_id = object.get("id").and_then(serde_json::Value::as_str)?;
                    if object
                        .get("worker")
                        .is_some_and(|worker| step_matches(worker, request))
                    {
                        return crate::NodeId::new(manager_id)
                            .ok()
                            .map(RepeatManifestOwner::Partitioned);
                    }
                }
                if step_matches(value, request) {
                    return Some(RepeatManifestOwner::Static);
                }
                object.values().find_map(|child| visit(child, request))
            }
            serde_json::Value::Array(values) => {
                values.iter().find_map(|child| visit(child, request))
            }
            _ => None,
        }
    }

    visit(manifest, request)
}

pub(crate) fn partition_worker_token(
    manager_node_id: &crate::NodeId,
    partition_key: &crate::PartitionKey,
) -> String {
    use sha2::Digest as _;

    let mut digest = sha2::Sha256::new();
    digest.update(b"oxide-batch.local-partition-worker.v1\0");
    digest.update(manager_node_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(partition_key.as_str().as_bytes());
    format!(
        "__ob_partition_worker_{}",
        oxide_batch_repository::hex_digest(&digest.finalize())
    )
}
