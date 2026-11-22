//! Gate-B manifest-format-5 projection, closed-reader, and legacy-identity evidence.

#![allow(clippy::expect_used)]

use sha2::{Digest, Sha256};

use oxide_batch::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ChunkSize, ComponentRevision,
    DefinitionIdentity, DefinitionManifest, DefinitionRevision, EffectScopeId, EffectSlotId,
    FlowGraph, FlowNode, FlowTarget, GateBAmbiguityReplay, GateBContractId, GateBContractVersion,
    GateBDefinitionContract, GateBEffectIdentity, GateBEffectJournal, GateBEffectScope,
    GateBEffectSlot, GateBInboundProtection, GateBLocalAtomicity, GateBMinimumValidityMs,
    GateBNativeMessageTransaction, GateBOutboundObligation, GateBRemoteContract, GateBRequirements,
    GateBSemanticContract, JobName, ManifestError, MAX_NODES, MAX_TRANSITIONS, NodeId, PartitionBudget, PartitionCount,
    PartitionedStepNode, RestartPolicy, StateSchemaId, StateSchemaVersion, StepComponents, StepName,
    StepNode, TerminalKind,
};

const LEGACY_TASKLET: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/legacy-format1-tasklet.manifest.json");
const LEGACY_CHUNK: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/legacy-format1-chunk.manifest.json");
const FORMAT5_BASE1_TASKLET: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/format5-base1-tasklet.manifest.json");
const FORMAT5_BASE1_CHUNK: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/format5-base1-chunk.manifest.json");
const FORMAT5_BASE2: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/format5-base2.manifest.json");
const FORMAT5_BASE3: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/format5-base3.manifest.json");
const FORMAT5_BASE4: &[u8] =
    include_bytes!("fixtures/GATE-B-FORMAT5-001/format5-base4.manifest.json");

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

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn changed_at(
    original: &serde_json::Value,
    pointer: &str,
    value: serde_json::Value,
) -> serde_json::Value {
    let mut changed = original.clone();
    *changed
        .pointer_mut(pointer)
        .expect("fingerprint mutation path must exist") = value;
    changed
}

fn assert_digest_changed(
    label: &str,
    baseline: &[u8; 32],
    changed: &serde_json::Value,
) {
    let changed_digest = sha256(&canonical(changed));
    assert_ne!(
        baseline, &changed_digest,
        "{label} must remain definition-fingerprint material"
    );
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


#[test]
fn format5_materialized_goldens_cover_all_legacy_base_families() {
    let cases = [
        (legacy_tasklet(), FORMAT5_BASE1_TASKLET),
        (legacy_chunk(), FORMAT5_BASE1_CHUNK),
        (format2_plan().definition_identity().clone(), FORMAT5_BASE2),
        (format3_plan().definition_identity().clone(), FORMAT5_BASE3),
        (format4_plan().definition_identity().clone(), FORMAT5_BASE4),
    ];

    for (legacy, expected) in cases {
        let wrapped = legacy
            .with_gate_b(gate_b_contract())
            .expect("format-5 golden producer");
        assert_eq!(
            wrapped.canonical_manifest(),
            expected,
            "materialized format-5 golden must match the real producer"
        );
        let read = DefinitionManifest::read_verified(expected, wrapped.manifest_digest())
            .expect("materialized golden is a valid format-5 manifest");
        assert_eq!(read.format(), 5);
    }
}

#[test]
fn every_static_gate_b_projection_field_changes_the_format5_fingerprint() {
    let wrapped = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 wrapper");
    let original = outer(&wrapped);
    let baseline = sha256(&canonical(&original));

    let mutations = [
        (
            "contract_version",
            "/gate_b/contract_version",
            serde_json::json!(2),
        ),
        (
            "local_atomicity",
            "/gate_b/requirements/local_atomicity",
            serde_json::json!("none"),
        ),
        (
            "effect_journal",
            "/gate_b/requirements/effect_journal",
            serde_json::json!("not_required"),
        ),
        (
            "outbound_obligation",
            "/gate_b/requirements/outbound_obligation",
            serde_json::json!("none"),
        ),
        (
            "inbound_protection",
            "/gate_b/requirements/inbound_protection",
            serde_json::json!("none"),
        ),
        (
            "ambiguity_replay",
            "/gate_b/requirements/ambiguity_replay",
            serde_json::json!("at_least_once"),
        ),
        (
            "native_message_mode",
            "/gate_b/requirements/native_message_transaction/mode",
            serde_json::json!("none"),
        ),
        (
            "native_message_contract_id",
            "/gate_b/requirements/native_message_transaction/semantic_contract_id",
            serde_json::json!("native-message-tx-v2"),
        ),
        (
            "native_message_contract_version",
            "/gate_b/requirements/native_message_transaction/semantic_contract_version",
            serde_json::json!(6),
        ),
        (
            "effect_id_derivation_version",
            "/gate_b/effect_identity/derivation_contract_version",
            serde_json::json!(2),
        ),
        (
            "occurrence_key_version",
            "/gate_b/effect_identity/occurrence_key_contract_version",
            serde_json::json!(3),
        ),
        (
            "request_identity_version",
            "/gate_b/effect_identity/request_identity_contract_version",
            serde_json::json!(4),
        ),
        (
            "provider_key_binding_version",
            "/gate_b/effect_identity/provider_key_binding_contract_version",
            serde_json::json!(5),
        ),
        (
            "effect_scope_id",
            "/gate_b/effect_identity/scopes/0/id",
            serde_json::json!("alpha-v2"),
        ),
        (
            "effect_slot_id",
            "/gate_b/effect_identity/scopes/0/slots/0/id",
            serde_json::json!("primary-v2"),
        ),
        (
            "logical_destination_contract",
            "/gate_b/effect_identity/scopes/0/slots/0/logical_destination_contract",
            serde_json::json!("logical-destination-v2"),
        ),
        (
            "remote_semantic_profile_id",
            "/gate_b/remote_contract/semantic_profile_id",
            serde_json::json!("remote-profile-v2"),
        ),
        (
            "remote_semantic_profile_version",
            "/gate_b/remote_contract/semantic_profile_version",
            serde_json::json!(7),
        ),
        (
            "remote_ack_contract_id",
            "/gate_b/remote_contract/acknowledgement_contract_id",
            serde_json::json!("remote-ack-v2"),
        ),
        (
            "remote_ack_contract_version",
            "/gate_b/remote_contract/acknowledgement_contract_version",
            serde_json::json!(8),
        ),
        (
            "remote_unknown_contract_id",
            "/gate_b/remote_contract/unknown_outcome_contract_id",
            serde_json::json!("remote-unknown-v2"),
        ),
        (
            "remote_unknown_contract_version",
            "/gate_b/remote_contract/unknown_outcome_contract_version",
            serde_json::json!(9),
        ),
        (
            "remote_minimum_validity",
            "/gate_b/remote_contract/minimum_validity_ms",
            serde_json::json!(30_001),
        ),
        (
            "inbox_dedup_minimum_validity",
            "/gate_b/inbox_dedup_minimum_validity_ms",
            serde_json::json!(60_001),
        ),
    ];

    for (label, pointer, value) in mutations {
        assert_digest_changed(label, &baseline, &changed_at(&original, pointer, value));
    }
}

#[test]
fn format5_reader_rejects_duplicate_and_overbound_effect_identity() {
    let wrapped = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 wrapper");
    let original = outer(&wrapped);

    let first_scope = original["gate_b"]["effect_identity"]["scopes"][0].clone();
    let mut duplicate_scope = original.clone();
    duplicate_scope["gate_b"]["effect_identity"]["scopes"]
        .as_array_mut()
        .expect("scope array")
        .insert(1, first_scope);
    assert_eq!(
        DefinitionManifest::read(&canonical(&duplicate_scope)),
        Err(ManifestError::MalformedGateB)
    );

    let first_slot = original["gate_b"]["effect_identity"]["scopes"][0]["slots"][0].clone();
    let mut duplicate_slot = original.clone();
    duplicate_slot["gate_b"]["effect_identity"]["scopes"][0]["slots"]
        .as_array_mut()
        .expect("slot array")
        .insert(1, first_slot);
    assert_eq!(
        DefinitionManifest::read(&canonical(&duplicate_slot)),
        Err(ManifestError::MalformedGateB)
    );

    let mut too_many_scopes = original.clone();
    too_many_scopes["gate_b"]["effect_identity"]["scopes"] = serde_json::Value::Array(
        (0..=MAX_NODES)
            .map(|index| {
                serde_json::json!({
                    "id": format!("s{index:04}"),
                    "slots": []
                })
            })
            .collect(),
    );
    assert_eq!(
        DefinitionManifest::read(&canonical(&too_many_scopes)),
        Err(ManifestError::MalformedGateB)
    );

    let mut too_many_slots = original;
    too_many_slots["gate_b"]["effect_identity"]["scopes"] = serde_json::json!([
        {
            "id": "scope",
            "slots": (0..=MAX_TRANSITIONS)
                .map(|index| serde_json::json!({ "id": format!("{index:04x}") }))
                .collect::<Vec<_>>()
        }
    ]);
    assert_eq!(
        DefinitionManifest::read(&canonical(&too_many_slots)),
        Err(ManifestError::MalformedGateB)
    );
}

#[test]
fn format5_reader_rejects_gate_b_cross_field_and_nested_contract_violations() {
    let wrapped = legacy_tasklet()
        .with_gate_b(gate_b_contract())
        .expect("format-5 wrapper");
    let original = outer(&wrapped);

    let mut idempotent_without_journal = original.clone();
    idempotent_without_journal["gate_b"]["requirements"]["effect_journal"] =
        serde_json::json!("not_required");
    assert_eq!(
        DefinitionManifest::read(&canonical(&idempotent_without_journal)),
        Err(ManifestError::MalformedGateB)
    );

    let mut missing_remote = original.clone();
    missing_remote["gate_b"]["requirements"]["ambiguity_replay"] =
        serde_json::json!("best_effort");
    missing_remote["gate_b"]["effect_identity"]
        .as_object_mut()
        .expect("effect identity")
        .remove("provider_key_binding_contract_version");
    missing_remote["gate_b"]
        .as_object_mut()
        .expect("Gate-B object")
        .remove("remote_contract");
    assert_eq!(
        DefinitionManifest::read(&canonical(&missing_remote)),
        Err(ManifestError::MalformedGateB)
    );

    let mut forbidden_remote = original.clone();
    forbidden_remote["gate_b"]["requirements"]["ambiguity_replay"] = serde_json::json!("none");
    forbidden_remote["gate_b"]["requirements"]["native_message_transaction"] =
        serde_json::json!({ "mode": "none" });
    assert_eq!(
        DefinitionManifest::read(&canonical(&forbidden_remote)),
        Err(ManifestError::MalformedGateB)
    );

    let mut missing_provider_binding = original.clone();
    missing_provider_binding["gate_b"]["effect_identity"]
        .as_object_mut()
        .expect("effect identity")
        .remove("provider_key_binding_contract_version");
    assert_eq!(
        DefinitionManifest::read(&canonical(&missing_provider_binding)),
        Err(ManifestError::MalformedGateB)
    );

    let mut binding_without_remote = original.clone();
    binding_without_remote["gate_b"]
        .as_object_mut()
        .expect("Gate-B object")
        .remove("remote_contract");
    assert_eq!(
        DefinitionManifest::read(&canonical(&binding_without_remote)),
        Err(ManifestError::MalformedGateB)
    );

    let mut inbox_floor_without_inbox = original.clone();
    inbox_floor_without_inbox["gate_b"]["requirements"]["inbound_protection"] =
        serde_json::json!("none");
    assert_eq!(
        DefinitionManifest::read(&canonical(&inbox_floor_without_inbox)),
        Err(ManifestError::MalformedGateB)
    );

    let mut native_none_with_contract = original.clone();
    native_none_with_contract["gate_b"]["requirements"]["native_message_transaction"]["mode"] =
        serde_json::json!("none");
    assert_eq!(
        DefinitionManifest::read(&canonical(&native_none_with_contract)),
        Err(ManifestError::MalformedGateB)
    );

    let mut native_required_without_contract = original.clone();
    native_required_without_contract["gate_b"]["requirements"]["native_message_transaction"] =
        serde_json::json!({ "mode": "required" });
    assert_eq!(
        DefinitionManifest::read(&canonical(&native_required_without_contract)),
        Err(ManifestError::MalformedGateB)
    );

    let mut zero_remote_floor = original.clone();
    zero_remote_floor["gate_b"]["remote_contract"]["minimum_validity_ms"] =
        serde_json::json!(0);
    assert_eq!(
        DefinitionManifest::read(&canonical(&zero_remote_floor)),
        Err(ManifestError::MalformedGateB)
    );

    let mut zero_inbox_floor = original.clone();
    zero_inbox_floor["gate_b"]["inbox_dedup_minimum_validity_ms"] = serde_json::json!(0);
    assert_eq!(
        DefinitionManifest::read(&canonical(&zero_inbox_floor)),
        Err(ManifestError::MalformedGateB)
    );

    let mut unknown_nested = original.clone();
    unknown_nested["gate_b"]["effect_identity"]["scopes"][0]["slots"][0]
        .as_object_mut()
        .expect("slot object")
        .insert("unknown".to_owned(), serde_json::json!(true));
    assert_eq!(
        DefinitionManifest::read(&canonical(&unknown_nested)),
        Err(ManifestError::MalformedGateB)
    );

    let mut null_nested_optional = original;
    null_nested_optional["gate_b"]["effect_identity"]["scopes"][0]["slots"][0]
        ["logical_destination_contract"] = serde_json::Value::Null;
    assert_eq!(
        DefinitionManifest::read(&canonical(&null_nested_optional)),
        Err(ManifestError::MalformedGateB)
    );
}
