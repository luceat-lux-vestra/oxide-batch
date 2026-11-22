//! Gate-B manifest-format-5 projection, closed-reader, and legacy-identity evidence.

#![allow(clippy::expect_used)]

use oxide_batch::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ChunkSize, ComponentRevision,
    DefinitionIdentity, DefinitionManifest, DefinitionRevision, EffectScopeId, EffectSlotId,
    FlowGraph, FlowNode, FlowTarget, GateBAmbiguityReplay, GateBContractId, GateBContractVersion,
    GateBDefinitionContract, GateBEffectIdentity, GateBEffectJournal, GateBEffectScope,
    GateBEffectSlot, GateBInboundProtection, GateBLocalAtomicity, GateBMinimumValidityMs,
    GateBNativeMessageTransaction, GateBOutboundObligation, GateBRemoteContract, GateBRequirements,
    GateBSemanticContract, JobName, ManifestError, NodeId, PartitionBudget, PartitionCount,
    PartitionedStepNode, RestartPolicy, StateSchemaId, StateSchemaVersion, StepComponents, StepName,
    StepNode, TerminalKind,
};

const LEGACY_TASKLET: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/legacy-format1-tasklet.manifest.json");
const LEGACY_CHUNK: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/legacy-format1-chunk.manifest.json");

fn version(value: u16) -> GateBContractVersion {
    GateBContractVersion::new(value).expect("non-zero Gate-B version")
}

fn semantic(id: &str, value: u16) -> GateBSemanticContract {
    GateBSemanticContract::new(
        GateBContractId::new(id).expect("bounded Gate-B contract id"),
        version(value),
    )
}

fn gate_b_contract() -> GateBDefinitionContract {
    let alpha = GateBEffectScope::new(
        EffectScopeId::new("alpha").expect("scope id"),
        vec![
            GateBEffectSlot::new(
                EffectSlotId::new("secondary").expect("slot id"),
                None,
            ),
            GateBEffectSlot::new(
                EffectSlotId::new("primary").expect("slot id"),
                Some(GateBContractId::new("logical-destination-v1").expect("destination contract")),
            ),
        ],
    )
    .expect("canonical alpha scope");
    let zeta = GateBEffectScope::new(
        EffectScopeId::new("zeta").expect("scope id"),
        vec![GateBEffectSlot::new(
            EffectSlotId::new("emit").expect("slot id"),
            None,
        )],
    )
    .expect("canonical zeta scope");

    let identity = GateBEffectIdentity::new(
        version(1),
        version(2),
        version(3),
        Some(version(4)),
        vec![zeta, alpha],
    )
    .expect("canonical effect identity");

    let requirements = GateBRequirements::new(
        GateBLocalAtomicity::AtomicSameResource,
        GateBEffectJournal::Required,
        GateBOutboundObligation::Outbox,
        GateBInboundProtection::InboxDedup,
        GateBAmbiguityReplay::IdempotentExternalEffect,
        GateBNativeMessageTransaction::Required(semantic("native-message-tx", 5)),
    )
    .expect("valid requirements");

    let remote = GateBRemoteContract::new(
        semantic("remote-profile", 6),
        semantic("remote-ack", 7),
        semantic("remote-unknown", 8),
    )
    .with_minimum_validity(
        GateBMinimumValidityMs::new(30_000).expect("positive remote validity floor"),
    );

    GateBDefinitionContract::new(
        requirements,
        identity,
        Some(remote),
        Some(GateBMinimumValidityMs::new(60_000).expect("positive inbox floor")),
    )
    .expect("valid Gate-B definition")
}

fn legacy_tasklet() -> DefinitionIdentity {
    DefinitionIdentity::tasklet(
        &JobName::new("gate_b_v07_tasklet").expect("job"),
        &StepName::new("only").expect("step"),
        DefinitionRevision::new("v1").expect("revision"),
        &ComponentRevision::new("tasklet-v1").expect("component"),
    )
    .expect("format-1 tasklet identity")
}

fn legacy_chunk() -> DefinitionIdentity {
    let restart = ChunkRestartContract::new(
        StateSchemaId::new("test.position").expect("checkpoint schema"),
        StateSchemaVersion::new(1).expect("checkpoint schema version"),
        StateSchemaId::new("test.context").expect("context schema"),
        StateSchemaVersion::new(1).expect("context schema version"),
        ChunkDeliveryMode::AtomicSameResource,
    );
    let components = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1").expect("reader revision"),
        ComponentRevision::new("processor-v1").expect("processor revision"),
        ComponentRevision::new("writer-v1").expect("writer revision"),
        ComponentRevision::new("checkpoint-v1").expect("checkpoint revision"),
        restart,
    );
    DefinitionIdentity::chunk(
        &JobName::new("gate_b_v07_chunk").expect("job"),
        &StepName::new("only").expect("step"),
        ChunkSize::new(2).expect("chunk size"),
        DefinitionRevision::new("v1").expect("revision"),
        &components,
    )
    .expect("format-1 chunk identity")
}

fn tasklet_step(id: &str) -> StepNode {
    StepNode::new(
        NodeId::new(id).expect("node id"),
        StepName::new(id).expect("step name"),
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1").expect("component")),
    )
}

fn format2_plan() -> oxide_batch::CompiledExecutionPlan {
    let id = NodeId::new("only").expect("node");
    FlowGraph::new(id.clone())
        .with_node(FlowNode::step(tasklet_step("only")))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))
        .expect("sequence")
        .compile(
            &JobName::new("gate_b_format2").expect("job"),
            DefinitionRevision::new("v1").expect("revision"),
        )
        .expect("format-2 plan")
}

fn format3_plan() -> oxide_batch::CompiledExecutionPlan {
    let id = NodeId::new("partition").expect("partition node");
    let partition = PartitionedStepNode::new(
        id.clone(),
        StepName::new("partition").expect("partition step"),
        tasklet_step("worker"),
        ComponentRevision::new("partitioner-v1").expect("partitioner"),
        ComponentRevision::new("aggregation-v1").expect("aggregation"),
        PartitionCount::new(2).expect("partition count"),
        PartitionBudget::new(1, 2).expect("partition budget"),
    );
    FlowGraph::new(id.clone())
        .with_node(FlowNode::partitioned_step(partition))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))
        .expect("sequence")
        .compile(
            &JobName::new("gate_b_format3").expect("job"),
            DefinitionRevision::new("v1").expect("revision"),
        )
        .expect("format-3 plan")
}

fn format4_plan() -> oxide_batch::CompiledExecutionPlan {
    let id = NodeId::new("only").expect("node");
    FlowGraph::new(id.clone())
        .with_node(FlowNode::step(tasklet_step("only")))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))
        .expect("sequence")
        .with_job_restart_policy(RestartPolicy::NotRestartable)
        .compile(
            &JobName::new("gate_b_format4").expect("job"),
            DefinitionRevision::new("v1").expect("revision"),
        )
        .expect("format-4 plan")
}

fn outer(identity: &DefinitionIdentity) -> serde_json::Value {
    serde_json::from_slice(identity.canonical_manifest()).expect("canonical manifest json")
}

fn canonical(value: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("canonical json")
}

#[test]
fn v0_7_0_format1_tasklet_and_chunk_bytes_are_immutable() {
    let tasklet = legacy_tasklet();
    assert_eq!(tasklet.canonical_manifest(), LEGACY_TASKLET);
    assert_eq!(legacy_chunk().canonical_manifest(), LEGACY_CHUNK);

    let tasklet_document: serde_json::Value =
        serde_json::from_slice(tasklet.canonical_manifest()).expect("tasklet manifest");
    assert_eq!(
        tasklet_document["component"],
        serde_json::json!({ "tasklet": "tasklet-v1" })
    );
}

#[test]
fn format5_wraps_exact_format1_2_3_4_base_bytes_and_round_trips() {
    let format1_tasklet = legacy_tasklet();
    let format1_chunk = legacy_chunk();
    let format2 = format2_plan().definition_identity().clone();
    let format3 = format3_plan().definition_identity().clone();
    let format4 = format4_plan().definition_identity().clone();

    for (legacy_format, legacy) in [
        (1_u64, format1_tasklet),
        (1_u64, format1_chunk),
        (2_u64, format2),
        (3_u64, format3),
        (4_u64, format4),
    ] {
        let contract = gate_b_contract();
        let wrapped = legacy
            .clone()
            .with_gate_b(contract.clone())
            .expect("format-5 wrapper");
        assert_eq!(wrapped.manifest_format(), 5);

        let document = outer(&wrapped);
        assert_eq!(document["format"].as_u64(), Some(5));
        assert_eq!(document["base"]["format"].as_u64(), Some(legacy_format));
        assert_eq!(canonical(&document["base"]), legacy.canonical_manifest());

        let read = DefinitionManifest::read_verified(
            wrapped.canonical_manifest(),
            wrapped.manifest_digest(),
        )
        .expect("valid format-5 manifest");
        assert_eq!(read.format(), 5);
        assert_eq!(read.job_name(), legacy.job_name());
        assert_eq!(read.gate_b_definition(), Some(&contract));
    }
}

#[test]
fn compiled_plan_gate_b_opt_in_preserves_execution_topology() {
    let plan = format2_plan();
    let legacy_manifest = plan.definition_identity().canonical_manifest().to_vec();
    let entry = plan.entry().clone();
    let node_count = plan.node_count();
    let transition_count = plan.transition_count();
    let contract = gate_b_contract();

    let wrapped = plan
        .with_gate_b(contract.clone())
        .expect("plan Gate-B opt-in");

    assert_eq!(wrapped.manifest_format(), 5);
    assert_eq!(wrapped.entry(), &entry);
    assert_eq!(wrapped.node_count(), node_count);
    assert_eq!(wrapped.transition_count(), transition_count);
    assert_eq!(wrapped.gate_b_definition(), Some(&contract));

    let document = outer(wrapped.definition_identity());
    assert_eq!(canonical(&document["base"]), legacy_manifest);
}

#[test]
fn gate_b_scope_and_slot_declaration_order_is_canonical() {
    let wrapped = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 wrapper");
    let document = outer(&wrapped);
    let scopes = document["gate_b"]["effect_identity"]["scopes"]
        .as_array()
        .expect("scope array");

    assert_eq!(scopes[0]["id"].as_str(), Some("alpha"));
    assert_eq!(scopes[1]["id"].as_str(), Some("zeta"));
    let slots = scopes[0]["slots"].as_array().expect("slot array");
    assert_eq!(slots[0]["id"].as_str(), Some("primary"));
    assert_eq!(slots[1]["id"].as_str(), Some("secondary"));
}

#[test]
fn format5_reader_fails_closed_on_unknown_members_nulls_versions_and_ordering() {
    let wrapped = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 wrapper");
    let original = outer(&wrapped);

    let mut unknown_root = original.clone();
    unknown_root
        .as_object_mut()
        .expect("root object")
        .insert("unknown".to_owned(), serde_json::json!(true));
    assert_eq!(
        DefinitionManifest::read(&canonical(&unknown_root)),
        Err(ManifestError::MalformedGateB)
    );

    let mut unknown_base = original.clone();
    unknown_base["base"]
        .as_object_mut()
        .expect("base object")
        .insert("unknown".to_owned(), serde_json::json!(true));
    assert_eq!(
        DefinitionManifest::read(&canonical(&unknown_base)),
        Err(ManifestError::MalformedBase)
    );

    let mut unknown_gate_b = original.clone();
    unknown_gate_b["gate_b"]
        .as_object_mut()
        .expect("Gate-B object")
        .insert("unknown".to_owned(), serde_json::json!(true));
    assert_eq!(
        DefinitionManifest::read(&canonical(&unknown_gate_b)),
        Err(ManifestError::MalformedGateB)
    );

    let mut null_remote = original.clone();
    null_remote["gate_b"]["remote_contract"] = serde_json::Value::Null;
    assert_eq!(
        DefinitionManifest::read(&canonical(&null_remote)),
        Err(ManifestError::MalformedGateB)
    );

    let mut newer_contract = original.clone();
    newer_contract["gate_b"]["contract_version"] = serde_json::json!(2);
    assert_eq!(
        DefinitionManifest::read(&canonical(&newer_contract)),
        Err(ManifestError::UnsupportedGateBContractVersion {
            version: 2,
            supported: 1,
        })
    );

    let mut unsorted_scopes = original.clone();
    unsorted_scopes["gate_b"]["effect_identity"]["scopes"]
        .as_array_mut()
        .expect("scope array")
        .swap(0, 1);
    assert_eq!(
        DefinitionManifest::read(&canonical(&unsorted_scopes)),
        Err(ManifestError::MalformedGateB)
    );

    let mut unsorted_slots = original;
    unsorted_slots["gate_b"]["effect_identity"]["scopes"][0]["slots"]
        .as_array_mut()
        .expect("slot array")
        .swap(0, 1);
    assert_eq!(
        DefinitionManifest::read(&canonical(&unsorted_slots)),
        Err(ManifestError::MalformedGateB)
    );
}

#[test]
fn format5_reader_rejects_structurally_closed_but_impossible_legacy_semantics() {
    let tasklet = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 tasklet");
    let mut tasklet_document = outer(&tasklet);
    tasklet_document["base"]["transaction_boundary"] =
        serde_json::json!("impossible_boundary");
    assert_eq!(
        DefinitionManifest::read(&canonical(&tasklet_document)),
        Err(ManifestError::MalformedBase)
    );

    let format2 = format2_plan()
        .with_gate_b(gate_b_contract())
        .expect("format-5 format-2 plan");
    let mut format2_document = outer(format2.definition_identity());
    format2_document["base"]["nodes"][0]["step"]["declaration"]["delivery_mode"] =
        serde_json::json!("impossible_delivery");
    assert_eq!(
        DefinitionManifest::read(&canonical(&format2_document)),
        Err(ManifestError::MalformedBase)
    );

    let format4 = format4_plan()
        .with_gate_b(gate_b_contract())
        .expect("format-5 format-4 plan");
    let mut format4_document = outer(format4.definition_identity());
    format4_document["base"]["restart_policy"] = serde_json::json!("restartable");
    assert_eq!(
        DefinitionManifest::read(&canonical(&format4_document)),
        Err(ManifestError::MalformedBase)
    );
}

#[test]
fn format5_reader_rejects_unknown_nested_base_members_and_digest_drift() {
    let wrapped = format2_plan()
        .with_gate_b(gate_b_contract())
        .expect("format-5 plan");
    let mut document = outer(wrapped.definition_identity());
    document["base"]["nodes"][0]
        .as_object_mut()
        .expect("node object")
        .insert("unknown".to_owned(), serde_json::json!(true));
    assert_eq!(
        DefinitionManifest::read(&canonical(&document)),
        Err(ManifestError::MalformedBase)
    );

    let wrong_digest = [0_u8; 32];
    assert_eq!(
        DefinitionManifest::read_verified(
            wrapped.definition_identity().canonical_manifest(),
            &wrong_digest,
        ),
        Err(ManifestError::DigestMismatch)
    );
}

#[test]
fn format5_contains_no_deployment_or_runtime_effect_identity_values() {
    let wrapped = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 wrapper");
    let encoded = std::str::from_utf8(wrapped.canonical_manifest()).expect("utf8 manifest");

    for excluded in [
        "endpoint",
        "credential",
        "account_id",
        "provider_idempotency_key",
        "job_execution_id",
        "step_execution_id",
        "effect_attempt",
        "effective_inbox_dedup_window",
    ] {
        assert!(!encoded.contains(excluded), "{excluded} must stay out of definition identity");
    }
}
