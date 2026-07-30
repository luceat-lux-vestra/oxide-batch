//! Restart-relevant definition identity contracts.

use std::error::Error;

use oxide_batch::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ChunkSize, ComponentRevision,
    DefinitionError, DefinitionIdentity, DefinitionRevision, DefinitionUpgrade,
    DefinitionUpgradeKey, JobName, StateSchemaId, StateSchemaVersion, StepDefinitionUpgrade,
    StepName,
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
