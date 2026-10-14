//! Restart-relevant definition identity contracts.

use std::error::Error;

use oxide_batch::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ChunkSize, ComponentRevision,
    ComponentStreamIdentity, DefinitionError, DefinitionIdentity, DefinitionNodeUpgrade,
    DefinitionRevision, DefinitionStateKind, DefinitionStateTransform,
    DefinitionStateTransformError, DefinitionStateTransformId, DefinitionStateTransformer,
    DefinitionUpgrade, DefinitionUpgradeExecution, DefinitionUpgradeKey, DefinitionUpgradeRevision,
    InFlightPolicy, JobName, MAX_DEFINITION_UPGRADE_NODES, MAX_DEFINITION_UPGRADE_TRANSFORMS,
    NodeId, StateSchemaId, StateSchemaVersion, StepDefinitionUpgrade, StepName,
};

#[test]
fn canonical_identity_changes_with_restart_relevant_inputs() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("daily_import")?;
    let step = StepName::new("load")?;
    let v1 = DefinitionIdentity::tasklet(
        &job,
        &step,
        DefinitionRevision::new("2026-07-30")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let same = DefinitionIdentity::tasklet(
        &job,
        &step,
        DefinitionRevision::new("2026-07-30")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let changed = DefinitionIdentity::tasklet(
        &job,
        &step,
        DefinitionRevision::new("2026-07-30")?,
        &ComponentRevision::new("tasklet-v2")?,
    )?;

    assert_eq!(v1, same);
    assert_ne!(v1.manifest_digest(), changed.manifest_digest());
    let diagnostic = format!("{v1:?}");
    assert!(diagnostic.contains("digest_prefix"));
    assert!(!diagnostic.contains("tasklet-v1"));
    Ok(())
}

#[test]
fn rollback_chunk_policy_changes_identity_without_changing_the_default_manifest()
-> Result<(), Box<dyn Error>> {
    let job = JobName::new("daily_import")?;
    let step = StepName::new("load")?;
    let revisions = |policy| -> Result<ChunkComponentRevisions, Box<dyn Error>> {
        Ok(ChunkComponentRevisions::new(
            ComponentRevision::new("reader-v1")?,
            ComponentRevision::new("processor-v1")?,
            ComponentRevision::new("writer-v1")?,
            ComponentRevision::new("checkpoint-v1")?,
            ChunkRestartContract::new(
                StateSchemaId::new("checkpoint-v1")?,
                StateSchemaVersion::new(1)?,
                StateSchemaId::new("context-v1")?,
                StateSchemaVersion::new(1)?,
                ChunkDeliveryMode::AtomicSameResource,
            )
            .with_in_flight_policy(policy),
        ))
    };
    let default = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(10)?,
        DefinitionRevision::new("v1")?,
        &revisions(InFlightPolicy::FinishChunk)?,
    )?;
    let rollback = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(10)?,
        DefinitionRevision::new("v1")?,
        &revisions(InFlightPolicy::RollbackChunk)?,
    )?;

    assert!(!std::str::from_utf8(default.canonical_manifest())?.contains("in_flight_policy"));
    assert!(std::str::from_utf8(rollback.canonical_manifest())?.contains("rollback_chunk"));
    assert_ne!(default.manifest_digest(), rollback.manifest_digest());
    Ok(())
}

#[test]
fn chunk_identity_includes_size_and_all_component_revisions() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("daily_import")?;
    let step = StepName::new("load")?;
    let components = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        ChunkRestartContract::new(
            StateSchemaId::new("checkpoint-v1")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("context-v1")?,
            StateSchemaVersion::new(1)?,
            ChunkDeliveryMode::AtomicSameResource,
        ),
    );
    let small = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(10)?,
        DefinitionRevision::new("v1")?,
        &components,
    )?;
    let large = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(100)?,
        DefinitionRevision::new("v1")?,
        &components,
    )?;
    let changed_state = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        ChunkRestartContract::new(
            StateSchemaId::new("checkpoint-v2")?,
            StateSchemaVersion::new(2)?,
            StateSchemaId::new("context-v1")?,
            StateSchemaVersion::new(1)?,
            ChunkDeliveryMode::AtLeastOnce,
        ),
    );
    let changed_state = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(10)?,
        DefinitionRevision::new("v2")?,
        &changed_state,
    )?;
    assert_ne!(small.manifest_digest(), large.manifest_digest());
    assert_ne!(small.manifest_digest(), changed_state.manifest_digest());
    Ok(())
}

#[test]
fn registering_a_stream_revision_changes_identity_without_changing_the_stream_free_manifest()
-> Result<(), Box<dyn Error>> {
    let job = JobName::new("daily_import")?;
    let step = StepName::new("load")?;
    let restart = ChunkRestartContract::new(
        StateSchemaId::new("checkpoint-v1")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("context-v1")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    );
    let stream_free = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        restart.clone(),
    );
    let with_stream = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        restart,
    )
    .with_stream_revision(
        ComponentStreamIdentity::new("reader.row_count")?,
        ComponentRevision::new("row-count-v1")?,
    );

    let without = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(10)?,
        DefinitionRevision::new("v1")?,
        &stream_free,
    )?;
    let with = DefinitionIdentity::chunk(
        &job,
        &step,
        ChunkSize::new(10)?,
        DefinitionRevision::new("v1")?,
        &with_stream,
    )?;

    // A stream-free chunk definition's manifest is byte-for-byte unchanged by
    // the existence of the `with_stream_revision` builder: representation
    // (registering no streams at all) is not itself restart-relevant.
    assert!(!std::str::from_utf8(without.canonical_manifest())?.contains("streams"));
    let with_manifest = std::str::from_utf8(with.canonical_manifest())?;
    assert!(with_manifest.contains("\"streams\""));
    assert!(with_manifest.contains("reader.row_count"));
    assert!(with_manifest.contains("row-count-v1"));
    assert_ne!(without.manifest_digest(), with.manifest_digest());
    Ok(())
}

#[test]
fn directed_upgrade_rejects_incomplete_or_ambiguous_mapping() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("daily_import")?;
    let old_step = StepName::new("load-v1")?;
    let new_step = StepName::new("load-v2")?;
    let from = DefinitionIdentity::tasklet(
        &job,
        &old_step,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let to = DefinitionIdentity::tasklet(
        &job,
        &new_step,
        DefinitionRevision::new("v2")?,
        &ComponentRevision::new("tasklet-v2")?,
    )?;
    let key = DefinitionUpgradeKey::new("v1-to-v2")?;

    assert_eq!(
        DefinitionUpgrade::new(key.clone(), from.clone(), to.clone(), []),
        Err(DefinitionError::EmptyStepMapping)
    );
    let upgrade = DefinitionUpgrade::new(
        key,
        from,
        to,
        [StepDefinitionUpgrade::new(old_step, new_step)],
    )?;
    assert_eq!(upgrade.key().as_str(), "v1-to-v2");
    Ok(())
}

#[test]
fn transformed_upgrade_is_injective_and_schema_explicit() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("daily_import")?;
    let from = DefinitionIdentity::tasklet(
        &job,
        &StepName::new("load-v1")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let to = DefinitionIdentity::tasklet(
        &job,
        &StepName::new("load-v2")?,
        DefinitionRevision::new("v2")?,
        &ComponentRevision::new("tasklet-v2")?,
    )?;
    let source = NodeId::new("load-v1")?;
    let target = NodeId::new("load-v2")?;
    let transform = DefinitionStateTransform::new(
        source.clone(),
        target.clone(),
        DefinitionStateKind::Checkpoint,
        DefinitionStateTransformId::new("reader-checkpoint")?,
        StateSchemaId::new("reader.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("reader.position")?,
        StateSchemaVersion::new(2)?,
        DefinitionUpgradeRevision::new("checkpoint-v1-to-v2")?,
    );
    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("v1-to-v2")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        from.clone(),
        to.clone(),
        [DefinitionNodeUpgrade::new(source.clone(), target.clone())],
        [transform],
    )?;

    assert!(edge.is_transformed());
    assert_eq!(
        edge.revision().map(DefinitionUpgradeRevision::as_str),
        Some("edge-v1")
    );
    assert_eq!(edge.node_mapping().get(&source), Some(&target));
    assert_eq!(edge.transforms().len(), 1);

    let duplicate_target = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("duplicate-target")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        from.clone(),
        to.clone(),
        [
            DefinitionNodeUpgrade::new(NodeId::new("source-a")?, target.clone()),
            DefinitionNodeUpgrade::new(NodeId::new("source-b")?, target.clone()),
        ],
        [],
    );
    assert_eq!(duplicate_target, Err(DefinitionError::DuplicateTargetNode));

    let outside = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("outside")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        from,
        to,
        [DefinitionNodeUpgrade::new(source.clone(), target.clone())],
        [DefinitionStateTransform::new(
            NodeId::new("other")?,
            target,
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("context")?,
            StateSchemaId::new("context.v1")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("context.v2")?,
            StateSchemaVersion::new(2)?,
            DefinitionUpgradeRevision::new("context-v1-to-v2")?,
        )],
    );
    assert_eq!(
        outside,
        Err(DefinitionError::StateTransformOutsideNodeMapping)
    );
    Ok(())
}

fn custom_leaf_identity(
    job: &JobName,
    revision: &str,
    schema_version: u32,
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let manifest = serde_json::json!({
        "entry": "custom",
        "format": 4,
        "job": job.as_str(),
        "nodes": [{
            "handler_revision": revision,
            "id": "custom",
            "kind": "custom_leaf",
            "leaf_kind": "example.handler",
            "listeners": [],
            "policy": null,
            "start": {
                "allow_start_if_complete": false,
                "start_limit": u32::MAX,
            },
            "state_schema": {
                "id": "example.state",
                "version": schema_version,
            },
            "step_name": "custom-step",
        }],
        "transitions": [],
    });
    let bytes = serde_json::to_vec(&manifest)?;
    Ok(DefinitionIdentity::from_flow_manifest(
        job,
        DefinitionRevision::new(revision)?,
        &bytes,
    )?)
}

#[test]
fn transformed_upgrade_requires_changed_manifest_state_schema_transform()
-> Result<(), Box<dyn Error>> {
    let job = JobName::new("state-schema-upgrade")?;
    let from = custom_leaf_identity(&job, "v1", 1)?;
    let to = custom_leaf_identity(&job, "v2", 2)?;
    let node = NodeId::new("custom")?;

    assert_eq!(
        DefinitionUpgrade::transformed(
            DefinitionUpgradeKey::new("v1-to-v2")?,
            DefinitionUpgradeRevision::new("edge-v1")?,
            from.clone(),
            to.clone(),
            [DefinitionNodeUpgrade::new(node.clone(), node.clone())],
            [],
        ),
        Err(DefinitionError::MissingRequiredStateTransform)
    );

    let wrong = DefinitionStateTransform::new(
        node.clone(),
        node.clone(),
        DefinitionStateKind::ExecutionContext,
        DefinitionStateTransformId::new("custom-state")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(3)?,
        DefinitionUpgradeRevision::new("state-v1-to-v2")?,
    );
    assert_eq!(
        DefinitionUpgrade::transformed(
            DefinitionUpgradeKey::new("v1-to-v2-wrong")?,
            DefinitionUpgradeRevision::new("edge-v1")?,
            from.clone(),
            to.clone(),
            [DefinitionNodeUpgrade::new(node.clone(), node.clone())],
            [wrong],
        ),
        Err(DefinitionError::StateTransformSchemaMismatch)
    );

    let exact = DefinitionStateTransform::new(
        node.clone(),
        node.clone(),
        DefinitionStateKind::ExecutionContext,
        DefinitionStateTransformId::new("custom-state")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(2)?,
        DefinitionUpgradeRevision::new("state-v1-to-v2")?,
    );
    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("v1-to-v2-exact")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        from,
        to,
        [DefinitionNodeUpgrade::new(node.clone(), node)],
        [exact],
    )?;
    assert_eq!(edge.transforms().len(), 1);
    Ok(())
}

fn identity_upgrade_payload(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    Ok(payload.to_vec())
}

#[test]
fn transformed_upgrade_execution_requires_exact_live_revision_set() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("upgrade-execution")?;
    let from = DefinitionIdentity::tasklet(
        &job,
        &StepName::new("source")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let to = DefinitionIdentity::tasklet(
        &job,
        &StepName::new("target")?,
        DefinitionRevision::new("v2")?,
        &ComponentRevision::new("tasklet-v2")?,
    )?;
    let source = NodeId::new("source")?;
    let target = NodeId::new("target")?;
    let transform_revision = DefinitionUpgradeRevision::new("context-v1-to-v2")?;
    let upgrade = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("v1-to-v2")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        from,
        to,
        [DefinitionNodeUpgrade::new(source.clone(), target.clone())],
        [DefinitionStateTransform::new(
            source,
            target,
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("context")?,
            StateSchemaId::new("context.v1")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("context.v2")?,
            StateSchemaVersion::new(2)?,
            transform_revision.clone(),
        )],
    )?;

    assert_eq!(
        DefinitionUpgradeExecution::new(upgrade.clone(), []),
        Err(DefinitionError::StateTransformerSetMismatch)
    );
    let execution = DefinitionUpgradeExecution::new(
        upgrade,
        [DefinitionStateTransformer::new(
            transform_revision,
            identity_upgrade_payload,
        )],
    )?;
    assert!(execution.upgrade().is_transformed());
    assert_eq!(execution.upgrade().transforms().len(), 1);
    Ok(())
}


fn two_node_upgrade_identity(
    job: &JobName,
    revision: &str,
    first: &str,
    second: &str,
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let manifest = serde_json::json!({
        "entry": first,
        "format": 4,
        "job": job.as_str(),
        "nodes": [
            {
                "handler_revision": revision,
                "id": first,
                "kind": "custom_leaf",
                "leaf_kind": "upgrade-proof",
                "listeners": [],
                "policy": null,
                "start": {
                    "allow_start_if_complete": false,
                    "start_limit": u32::MAX,
                },
                "state_schema": {
                    "id": "upgrade-proof.state",
                    "version": 1,
                },
                "step_name": first,
            },
            {
                "handler_revision": revision,
                "id": second,
                "kind": "custom_leaf",
                "leaf_kind": "upgrade-proof",
                "listeners": [],
                "policy": null,
                "start": {
                    "allow_start_if_complete": false,
                    "start_limit": u32::MAX,
                },
                "state_schema": {
                    "id": "upgrade-proof.state",
                    "version": 1,
                },
                "step_name": second,
            }
        ],
        "transitions": [],
    });
    let bytes = serde_json::to_vec(&manifest)?;
    Ok(DefinitionIdentity::from_flow_manifest(
        job,
        DefinitionRevision::new(revision)?,
        &bytes,
    )?)
}

#[test]
fn transformed_upgrade_rejects_omitted_source_node() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("upgrade-omitted-source")?;
    let from = two_node_upgrade_identity(&job, "v1", "source-a", "source-b")?;
    let to = two_node_upgrade_identity(&job, "v2", "target-a", "target-b")?;

    assert_eq!(
        DefinitionUpgrade::transformed(
            DefinitionUpgradeKey::new("v1-to-v2")?,
            DefinitionUpgradeRevision::new("edge-v1")?,
            from,
            to,
            [DefinitionNodeUpgrade::new(
                NodeId::new("source-a")?,
                NodeId::new("target-a")?,
            )],
            [],
        ),
        Err(DefinitionError::MissingUpgradeSourceNode)
    );
    Ok(())
}

#[test]
fn transformed_upgrade_enforces_node_and_transform_bounds() -> Result<(), Box<dyn Error>> {
    let job = JobName::new("upgrade-bounds")?;
    let from = DefinitionIdentity::tasklet(
        &job,
        &StepName::new("source")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let to = DefinitionIdentity::tasklet(
        &job,
        &StepName::new("target")?,
        DefinitionRevision::new("v2")?,
        &ComponentRevision::new("tasklet-v2")?,
    )?;

    let mut mappings = Vec::with_capacity(MAX_DEFINITION_UPGRADE_NODES + 1);
    for index in 0..=MAX_DEFINITION_UPGRADE_NODES {
        mappings.push(DefinitionNodeUpgrade::new(
            NodeId::new(format!("source-{index}"))?,
            NodeId::new(format!("target-{index}"))?,
        ));
    }
    assert_eq!(
        DefinitionUpgrade::transformed(
            DefinitionUpgradeKey::new("too-many-nodes")?,
            DefinitionUpgradeRevision::new("edge-v1")?,
            from.clone(),
            to.clone(),
            mappings,
            [],
        ),
        Err(DefinitionError::TooManyUpgradeNodes {
            max: MAX_DEFINITION_UPGRADE_NODES,
        })
    );

    let source = NodeId::new("source")?;
    let target = NodeId::new("target")?;
    let transform = DefinitionStateTransform::new(
        source.clone(),
        target.clone(),
        DefinitionStateKind::ExecutionContext,
        DefinitionStateTransformId::new("state")?,
        StateSchemaId::new("upgrade.state")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("upgrade.state")?,
        StateSchemaVersion::new(2)?,
        DefinitionUpgradeRevision::new("state-v1-to-v2")?,
    );
    assert_eq!(
        DefinitionUpgrade::transformed(
            DefinitionUpgradeKey::new("too-many-transforms")?,
            DefinitionUpgradeRevision::new("edge-v1")?,
            from,
            to,
            [DefinitionNodeUpgrade::new(source, target)],
            std::iter::repeat_n(transform, MAX_DEFINITION_UPGRADE_TRANSFORMS + 1),
        ),
        Err(DefinitionError::TooManyUpgradeTransforms {
            max: MAX_DEFINITION_UPGRADE_TRANSFORMS,
        })
    );
    Ok(())
}
