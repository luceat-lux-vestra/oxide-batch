//! Canonical Gate-B manifest-format-5 projection and closed parsing.
//!
//! Format 5 wraps an exact frozen format-1/2/3/4 object in `base` and adds a
//! recursively closed `gate_b` semantic object. Historical readers stay
//! unchanged; the stricter legacy grammar in this module is used only inside
//! the format-5 wrapper.

use serde_json::{Map, Value, json};

use crate::definition::{
    DefinitionManifest, ManifestError, MANIFEST_FORMAT_ADVANCED_FLOW, MANIFEST_FORMAT_FLOW,
    MANIFEST_FORMAT_GATE_B, MANIFEST_FORMAT_LOCAL_SCALE, MANIFEST_FORMAT_ONE_STEP,
};
use crate::{
    BackoffKind, ChunkDeliveryMode, ClassifierRevision, ComponentRevision,
    ComponentStreamIdentity, DefinitionRevision, DefinitionTokenKind, EffectScopeId, EffectSlotId,
    FailureCategory, FaultPhase, GateBAmbiguityReplay, GateBContractId, GateBContractVersion,
    GateBDefinitionContract, GateBEffectIdentity, GateBEffectJournal, GateBEffectScope,
    GateBEffectSlot, GateBInboundProtection, GateBLocalAtomicity, GateBMinimumValidityMs,
    GateBNativeMessageTransaction, GateBOutboundObligation, GateBRemoteContract, GateBRequirements,
    GateBSemanticContract, JobName, NodeId, ParameterName, ParameterValueKind, ScopeFrameworkSource,
    ScopeKind, ScopedComponentId, StateSchemaId, StateSchemaVersion, StepName, TerminalKind,
    GATE_B_CONTRACT_VERSION, MAX_PARTITIONS, selector_path_is_valid, validate_token,
};

const TASKLET_DELIVERY_MODE: &str = "best_effort";
const TASKLET_TRANSACTION_BOUNDARY: &str = "tasklet_completion";
const CHUNK_TRANSACTION_BOUNDARY: &str = "chunk";
const TASKLET_RESTART_STATE: &str = "none";
const ROLLBACK_CHUNK_IN_FLIGHT_POLICY: &str = "rollback_chunk";
const NON_RESTARTABLE_POLICY: &str = "not_restartable";
const LOCAL_FAILURE_POLICIES: &[&str] = &["cancel_siblings", "drain_siblings"];
const PARAMETER_COERCIONS: &[&str] = &[
    "exact",
    "string_to_i64",
    "string_to_u64",
    "string_to_bool",
    "i64_to_u64",
    "u64_to_i64",
];
const MISSING_PARAMETER_POLICIES: &[&str] = &["fail", "type_default"];
const PARAMETER_ROLES: &[&str] = &["identifying", "non_identifying"];
const NESTED_FRAMEWORK_FIELDS: &[&str] = &[
    "parent_job_instance_id",
    "parent_job_execution_id",
    "parent_attempt",
    "parent_definition_revision",
    "parent_definition_fingerprint",
    "nested_job_node_id",
];

pub(crate) struct Format5Read {
    pub(crate) job_name: Option<JobName>,
    pub(crate) node_count: Option<usize>,
    pub(crate) transition_count: Option<usize>,
    pub(crate) gate_b: GateBDefinitionContract,
}

pub(crate) fn wrap(
    base: Value,
    contract: &GateBDefinitionContract,
) -> Result<Value, ManifestError> {
    validate_legacy_base(&base)?;
    Ok(json!({
        "base": base,
        "format": MANIFEST_FORMAT_GATE_B,
        "gate_b": project_gate_b(contract)
    }))
}

pub(crate) fn read(document: &Value) -> Result<Format5Read, ManifestError> {
    let root = exact_object(document, &["base", "format", "gate_b"], &[], ManifestError::MalformedGateB)?;
    if root.get("format").and_then(Value::as_u64) != Some(u64::from(MANIFEST_FORMAT_GATE_B)) {
        return Err(ManifestError::MissingFormat);
    }

    let base = root.get("base").ok_or(ManifestError::MalformedBase)?;
    validate_legacy_base(base)?;
    let base_bytes = serde_json::to_vec(base).map_err(|_| ManifestError::MalformedBase)?;
    let legacy = DefinitionManifest::read(&base_bytes)?;
    if !matches!(
        legacy.format(),
        MANIFEST_FORMAT_ONE_STEP
            | MANIFEST_FORMAT_FLOW
            | MANIFEST_FORMAT_LOCAL_SCALE
            | MANIFEST_FORMAT_ADVANCED_FLOW
    ) {
        return Err(ManifestError::MalformedBase);
    }

    let gate_b = parse_gate_b(root.get("gate_b").ok_or(ManifestError::MalformedGateB)?)?;
    Ok(Format5Read {
        job_name: legacy.job_name().cloned(),
        node_count: legacy.node_count(),
        transition_count: legacy.transition_count(),
        gate_b,
    })
}

pub(crate) fn validate_legacy_base(value: &Value) -> Result<(), ManifestError> {
    let object = value.as_object().ok_or(ManifestError::MalformedBase)?;
    let format = object
        .get("format")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(ManifestError::MalformedBase)?;

    match format {
        MANIFEST_FORMAT_ONE_STEP => validate_format1(object),
        MANIFEST_FORMAT_FLOW | MANIFEST_FORMAT_LOCAL_SCALE | MANIFEST_FORMAT_ADVANCED_FLOW => {
            validate_graph_root(object, format)
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn project_gate_b(contract: &GateBDefinitionContract) -> Value {
    let requirements = contract.requirements();
    let native_message_transaction = match requirements.native_message_transaction() {
        GateBNativeMessageTransaction::None => json!({ "mode": "none" }),
        GateBNativeMessageTransaction::Required(semantic) => json!({
            "mode": "required",
            "semantic_contract_id": semantic.id().as_str(),
            "semantic_contract_version": semantic.version().get()
        }),
    };

    let requirements = json!({
        "ambiguity_replay": ambiguity_replay_name(requirements.ambiguity_replay()),
        "effect_journal": effect_journal_name(requirements.effect_journal()),
        "inbound_protection": inbound_protection_name(requirements.inbound_protection()),
        "local_atomicity": local_atomicity_name(requirements.local_atomicity()),
        "native_message_transaction": native_message_transaction,
        "outbound_obligation": outbound_obligation_name(requirements.outbound_obligation())
    });

    let identity = contract.effect_identity();
    let scopes = identity
        .scopes()
        .map(|scope| {
            let slots = scope
                .slots()
                .map(|slot| {
                    let mut value = json!({ "id": slot.id().as_str() });
                    if let Some(destination) = slot.logical_destination_contract()
                        && let Some(object) = value.as_object_mut()
                    {
                        object.insert(
                            "logical_destination_contract".to_owned(),
                            Value::String(destination.as_str().to_owned()),
                        );
                    }
                    value
                })
                .collect::<Vec<_>>();
            json!({
                "id": scope.id().as_str(),
                "slots": slots
            })
        })
        .collect::<Vec<_>>();

    let mut effect_identity = json!({
        "derivation_contract_version": identity.derivation_contract_version().get(),
        "occurrence_key_contract_version": identity.occurrence_key_contract_version().get(),
        "request_identity_contract_version": identity.request_identity_contract_version().get(),
        "scopes": scopes
    });
    if let Some(version) = identity.provider_key_binding_contract_version()
        && let Some(object) = effect_identity.as_object_mut()
    {
        object.insert(
            "provider_key_binding_contract_version".to_owned(),
            json!(version.get()),
        );
    }

    let mut gate_b = json!({
        "contract_version": contract.contract_version(),
        "effect_identity": effect_identity,
        "requirements": requirements
    });

    if let Some(remote) = contract.remote_contract()
        && let Some(object) = gate_b.as_object_mut()
    {
        let mut remote_value = json!({
            "acknowledgement_contract_id": remote.acknowledgement().id().as_str(),
            "acknowledgement_contract_version": remote.acknowledgement().version().get(),
            "semantic_profile_id": remote.semantic_profile().id().as_str(),
            "semantic_profile_version": remote.semantic_profile().version().get(),
            "unknown_outcome_contract_id": remote.unknown_outcome().id().as_str(),
            "unknown_outcome_contract_version": remote.unknown_outcome().version().get()
        });
        if let Some(minimum) = remote.minimum_validity_ms()
            && let Some(remote_object) = remote_value.as_object_mut()
        {
            remote_object.insert("minimum_validity_ms".to_owned(), json!(minimum.get()));
        }
        object.insert("remote_contract".to_owned(), remote_value);
    }

    if let Some(minimum) = contract.inbox_dedup_minimum_validity_ms()
        && let Some(object) = gate_b.as_object_mut()
    {
        object.insert(
            "inbox_dedup_minimum_validity_ms".to_owned(),
            json!(minimum.get()),
        );
    }

    gate_b
}

fn parse_gate_b(value: &Value) -> Result<GateBDefinitionContract, ManifestError> {
    let object = exact_object(
        value,
        &["contract_version", "effect_identity", "requirements"],
        &["inbox_dedup_minimum_validity_ms", "remote_contract"],
        ManifestError::MalformedGateB,
    )?;
    let contract_version = required_u16(object, "contract_version")?;
    if contract_version != GATE_B_CONTRACT_VERSION {
        return Err(ManifestError::UnsupportedGateBContractVersion {
            version: contract_version,
            supported: GATE_B_CONTRACT_VERSION,
        });
    }

    let requirements = parse_requirements(required(object, "requirements")?)?;
    let effect_identity = parse_effect_identity(required(object, "effect_identity")?)?;
    let remote_contract = object
        .get("remote_contract")
        .map(parse_remote_contract)
        .transpose()?;
    let inbox_floor = object
        .get("inbox_dedup_minimum_validity_ms")
        .map(parse_minimum_validity)
        .transpose()?;

    GateBDefinitionContract::new(requirements, effect_identity, remote_contract, inbox_floor)
        .map_err(|_| ManifestError::MalformedGateB)
}

fn parse_requirements(value: &Value) -> Result<GateBRequirements, ManifestError> {
    let object = exact_object(
        value,
        &[
            "ambiguity_replay",
            "effect_journal",
            "inbound_protection",
            "local_atomicity",
            "native_message_transaction",
            "outbound_obligation",
        ],
        &[],
        ManifestError::MalformedGateB,
    )?;

    let local_atomicity = match required_str(object, "local_atomicity")? {
        "none" => GateBLocalAtomicity::None,
        "atomic_same_resource" => GateBLocalAtomicity::AtomicSameResource,
        _ => return Err(ManifestError::MalformedGateB),
    };
    let effect_journal = match required_str(object, "effect_journal")? {
        "not_required" => GateBEffectJournal::NotRequired,
        "required" => GateBEffectJournal::Required,
        _ => return Err(ManifestError::MalformedGateB),
    };
    let outbound_obligation = match required_str(object, "outbound_obligation")? {
        "none" => GateBOutboundObligation::None,
        "outbox" => GateBOutboundObligation::Outbox,
        _ => return Err(ManifestError::MalformedGateB),
    };
    let inbound_protection = match required_str(object, "inbound_protection")? {
        "none" => GateBInboundProtection::None,
        "inbox_dedup" => GateBInboundProtection::InboxDedup,
        _ => return Err(ManifestError::MalformedGateB),
    };
    let ambiguity_replay = match required_str(object, "ambiguity_replay")? {
        "none" => GateBAmbiguityReplay::None,
        "best_effort" => GateBAmbiguityReplay::BestEffort,
        "at_least_once" => GateBAmbiguityReplay::AtLeastOnce,
        "idempotent_external_effect" => GateBAmbiguityReplay::IdempotentExternalEffect,
        _ => return Err(ManifestError::MalformedGateB),
    };
    let native_message_transaction =
        parse_native_message_transaction(required(object, "native_message_transaction")?)?;

    GateBRequirements::new(
        local_atomicity,
        effect_journal,
        outbound_obligation,
        inbound_protection,
        ambiguity_replay,
        native_message_transaction,
    )
    .map_err(|_| ManifestError::MalformedGateB)
}

fn parse_native_message_transaction(
    value: &Value,
) -> Result<GateBNativeMessageTransaction, ManifestError> {
    let object = value.as_object().ok_or(ManifestError::MalformedGateB)?;
    match object.get("mode").and_then(Value::as_str) {
        Some("none") => {
            exact_keys(object, &["mode"], &[], ManifestError::MalformedGateB)?;
            Ok(GateBNativeMessageTransaction::None)
        }
        Some("required") => {
            exact_keys(
                object,
                &["mode", "semantic_contract_id", "semantic_contract_version"],
                &[],
                ManifestError::MalformedGateB,
            )?;
            Ok(GateBNativeMessageTransaction::Required(
                parse_semantic_contract(
                    required_str(object, "semantic_contract_id")?,
                    required_u16(object, "semantic_contract_version")?,
                )?,
            ))
        }
        _ => Err(ManifestError::MalformedGateB),
    }
}

fn parse_effect_identity(value: &Value) -> Result<GateBEffectIdentity, ManifestError> {
    let object = exact_object(
        value,
        &[
            "derivation_contract_version",
            "occurrence_key_contract_version",
            "request_identity_contract_version",
            "scopes",
        ],
        &["provider_key_binding_contract_version"],
        ManifestError::MalformedGateB,
    )?;

    let scopes_value = required(object, "scopes")?
        .as_array()
        .ok_or(ManifestError::MalformedGateB)?;
    let mut previous_scope: Option<&str> = None;
    let mut scopes = Vec::with_capacity(scopes_value.len());
    for scope_value in scopes_value {
        let scope_object = exact_object(
            scope_value,
            &["id", "slots"],
            &[],
            ManifestError::MalformedGateB,
        )?;
        let raw_scope = required_str(scope_object, "id")?;
        if previous_scope.is_some_and(|previous| previous >= raw_scope) {
            return Err(ManifestError::MalformedGateB);
        }
        previous_scope = Some(raw_scope);

        let slots_value = required(scope_object, "slots")?
            .as_array()
            .ok_or(ManifestError::MalformedGateB)?;
        let mut previous_slot: Option<&str> = None;
        let mut slots = Vec::with_capacity(slots_value.len());
        for slot_value in slots_value {
            let slot_object = exact_object(
                slot_value,
                &["id"],
                &["logical_destination_contract"],
                ManifestError::MalformedGateB,
            )?;
            let raw_slot = required_str(slot_object, "id")?;
            if previous_slot.is_some_and(|previous| previous >= raw_slot) {
                return Err(ManifestError::MalformedGateB);
            }
            previous_slot = Some(raw_slot);
            let destination = slot_object
                .get("logical_destination_contract")
                .map(|value| {
                    value
                        .as_str()
                        .ok_or(ManifestError::MalformedGateB)
                        .and_then(parse_contract_id)
                })
                .transpose()?;
            slots.push(GateBEffectSlot::new(
                EffectSlotId::new(raw_slot.to_owned())
                    .map_err(|_| ManifestError::MalformedGateB)?,
                destination,
            ));
        }

        scopes.push(
            GateBEffectScope::new(
                EffectScopeId::new(raw_scope.to_owned())
                    .map_err(|_| ManifestError::MalformedGateB)?,
                slots,
            )
            .map_err(|_| ManifestError::MalformedGateB)?,
        );
    }

    GateBEffectIdentity::new(
        parse_version(required_u16(object, "derivation_contract_version")?)?,
        parse_version(required_u16(object, "occurrence_key_contract_version")?)?,
        parse_version(required_u16(object, "request_identity_contract_version")?)?,
        object
            .get("provider_key_binding_contract_version")
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|raw| u16::try_from(raw).ok())
                    .ok_or(ManifestError::MalformedGateB)
                    .and_then(parse_version)
            })
            .transpose()?,
        scopes,
    )
    .map_err(|_| ManifestError::MalformedGateB)
}

fn parse_remote_contract(value: &Value) -> Result<GateBRemoteContract, ManifestError> {
    let object = exact_object(
        value,
        &[
            "acknowledgement_contract_id",
            "acknowledgement_contract_version",
            "semantic_profile_id",
            "semantic_profile_version",
            "unknown_outcome_contract_id",
            "unknown_outcome_contract_version",
        ],
        &["minimum_validity_ms"],
        ManifestError::MalformedGateB,
    )?;

    let mut remote = GateBRemoteContract::new(
        parse_semantic_contract(
            required_str(object, "semantic_profile_id")?,
            required_u16(object, "semantic_profile_version")?,
        )?,
        parse_semantic_contract(
            required_str(object, "acknowledgement_contract_id")?,
            required_u16(object, "acknowledgement_contract_version")?,
        )?,
        parse_semantic_contract(
            required_str(object, "unknown_outcome_contract_id")?,
            required_u16(object, "unknown_outcome_contract_version")?,
        )?,
    );
    if let Some(minimum) = object.get("minimum_validity_ms") {
        remote = remote.with_minimum_validity(parse_minimum_validity(minimum)?);
    }
    Ok(remote)
}

fn parse_semantic_contract(
    id: &str,
    version: u16,
) -> Result<GateBSemanticContract, ManifestError> {
    Ok(GateBSemanticContract::new(
        parse_contract_id(id)?,
        parse_version(version)?,
    ))
}

fn parse_contract_id(value: &str) -> Result<GateBContractId, ManifestError> {
    GateBContractId::new(value.to_owned()).map_err(|_| ManifestError::MalformedGateB)
}

fn parse_version(value: u16) -> Result<GateBContractVersion, ManifestError> {
    GateBContractVersion::new(value).ok_or(ManifestError::MalformedGateB)
}

fn parse_minimum_validity(value: &Value) -> Result<GateBMinimumValidityMs, ManifestError> {
    let raw = value.as_u64().ok_or(ManifestError::MalformedGateB)?;
    GateBMinimumValidityMs::new(raw).ok_or(ManifestError::MalformedGateB)
}

fn local_atomicity_name(value: GateBLocalAtomicity) -> &'static str {
    match value {
        GateBLocalAtomicity::None => "none",
        GateBLocalAtomicity::AtomicSameResource => "atomic_same_resource",
    }
}

fn effect_journal_name(value: GateBEffectJournal) -> &'static str {
    match value {
        GateBEffectJournal::NotRequired => "not_required",
        GateBEffectJournal::Required => "required",
    }
}

fn outbound_obligation_name(value: GateBOutboundObligation) -> &'static str {
    match value {
        GateBOutboundObligation::None => "none",
        GateBOutboundObligation::Outbox => "outbox",
    }
}

fn inbound_protection_name(value: GateBInboundProtection) -> &'static str {
    match value {
        GateBInboundProtection::None => "none",
        GateBInboundProtection::InboxDedup => "inbox_dedup",
    }
}

fn ambiguity_replay_name(value: GateBAmbiguityReplay) -> &'static str {
    match value {
        GateBAmbiguityReplay::None => "none",
        GateBAmbiguityReplay::BestEffort => "best_effort",
        GateBAmbiguityReplay::AtLeastOnce => "at_least_once",
        GateBAmbiguityReplay::IdempotentExternalEffect => "idempotent_external_effect",
    }
}

fn validate_format1(object: &Map<String, Value>) -> Result<(), ManifestError> {
    match object.get("kind").and_then(Value::as_str) {
        Some("tasklet") => {
            exact_keys(
                object,
                &[
                    "component",
                    "delivery_mode",
                    "format",
                    "job",
                    "kind",
                    "restart_state",
                    "step",
                    "transaction_boundary",
                ],
                &[],
                ManifestError::MalformedBase,
            )?;
            validate_job_name_member(object, "job")?;
            validate_step_name_member(object, "step")?;
            require_exact_base_string(object, "kind", "tasklet")?;
            require_exact_base_string(object, "delivery_mode", TASKLET_DELIVERY_MODE)?;
            require_exact_base_string(object, "restart_state", TASKLET_RESTART_STATE)?;
            require_exact_base_string(
                object,
                "transaction_boundary",
                TASKLET_TRANSACTION_BOUNDARY,
            )?;
            let component = exact_object(
                required(object, "component")?,
                &["tasklet"],
                &[],
                ManifestError::MalformedBase,
            )?;
            validate_component_revision_member(component, "tasklet")
        }
        Some("chunk") => {
            exact_keys(
                object,
                &[
                    "checkpoint",
                    "chunk_size",
                    "components",
                    "context",
                    "delivery_mode",
                    "format",
                    "job",
                    "kind",
                    "step",
                    "transaction_boundary",
                ],
                &["completion_policy", "in_flight_policy", "streams"],
                ManifestError::MalformedBase,
            )?;
            validate_job_name_member(object, "job")?;
            validate_step_name_member(object, "step")?;
            require_exact_base_string(object, "kind", "chunk")?;
            require_exact_base_string(object, "transaction_boundary", CHUNK_TRANSACTION_BOUNDARY)?;
            validate_chunk_delivery_mode_member(object, "delivery_mode")?;
            if object
                .get("chunk_size")
                .and_then(Value::as_u64)
                .is_none_or(|value| value == 0 || value > u64::from(u32::MAX))
            {
                return Err(ManifestError::MalformedBase);
            }
            validate_schema_descriptor(required(object, "checkpoint")?)?;
            validate_component_revisions(required(object, "components")?)?;
            validate_schema_descriptor(required(object, "context")?)?;
            if let Some(value) = object.get("completion_policy") {
                validate_component_revision_value(value)?;
            }
            if let Some(value) = object.get("in_flight_policy") {
                require_exact_value_string(value, ROLLBACK_CHUNK_IN_FLIGHT_POLICY)?;
            }
            if let Some(value) = object.get("streams") {
                let streams = value.as_array().ok_or(ManifestError::MalformedBase)?;
                let mut previous: Option<&str> = None;
                for stream in streams {
                    let stream = exact_object(
                        stream,
                        &["identity", "revision"],
                        &[],
                        ManifestError::MalformedBase,
                    )?;
                    let identity = base_str(stream, "identity")?;
                    ComponentStreamIdentity::new(identity.to_owned())
                        .map_err(|_| ManifestError::MalformedBase)?;
                    if previous.is_some_and(|candidate| candidate >= identity) {
                        return Err(ManifestError::MalformedBase);
                    }
                    previous = Some(identity);
                    validate_component_revision_member(stream, "revision")?;
                }
            }
            Ok(())
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn validate_graph_root(object: &Map<String, Value>, format: u16) -> Result<(), ManifestError> {
    let optional: &[&str] = if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        &["parameter_incrementer", "restart_policy", "scoped_components"]
    } else {
        &[]
    };
    exact_keys(
        object,
        &["entry", "format", "job", "nodes", "transitions"],
        optional,
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "entry")?;
    validate_job_name_member(object, "job")?;

    let nodes = required(object, "nodes")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for node in nodes {
        validate_node(node, format)?;
    }
    let transitions = required(object, "transitions")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for transition in transitions {
        validate_transition(transition)?;
    }

    if let Some(incrementer) = object.get("parameter_incrementer") {
        let incrementer = exact_object(
            incrementer,
            &["kind", "revision"],
            &[],
            ManifestError::MalformedBase,
        )?;
        validate_definition_revision_member(incrementer, "kind")?;
        validate_definition_revision_member(incrementer, "revision")?;
    }
    if let Some(policy) = object.get("restart_policy") {
        require_exact_value_string(policy, NON_RESTARTABLE_POLICY)?;
    }
    if let Some(scoped) = object.get("scoped_components") {
        let scoped = scoped.as_array().ok_or(ManifestError::MalformedBase)?;
        for component in scoped {
            validate_scoped_component(component)?;
        }
    }
    Ok(())
}

fn validate_node(value: &Value, format: u16) -> Result<(), ManifestError> {
    let object = value.as_object().ok_or(ManifestError::MalformedBase)?;
    match object.get("kind").and_then(Value::as_str) {
        Some("step") => validate_step(object, format),
        Some("decision") => validate_decision(object, format),
        Some("join") if format >= MANIFEST_FORMAT_LOCAL_SCALE => validate_join(object, format),
        Some("split") if format >= MANIFEST_FORMAT_LOCAL_SCALE => validate_split(object, format),
        Some("partitioned_step") if format >= MANIFEST_FORMAT_LOCAL_SCALE => {
            validate_partitioned_step(object, format)
        }
        Some("nested_job") if format == MANIFEST_FORMAT_ADVANCED_FLOW => {
            validate_nested_job(object)
        }
        Some("custom_leaf") if format == MANIFEST_FORMAT_ADVANCED_FLOW => {
            validate_custom_leaf(object, format)
        }
        Some("nested_flow") if format == MANIFEST_FORMAT_ADVANCED_FLOW => {
            validate_nested_flow(object)
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn validate_step(object: &Map<String, Value>, format: u16) -> Result<(), ManifestError> {
    let optional: &[&str] = if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        &["decision_sequence", "repeat"]
    } else {
        &[]
    };
    exact_keys(
        object,
        &["id", "kind", "listeners", "policy", "start", "step"],
        optional,
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "step")?;
    validate_component_revision_array(required(object, "listeners")?)?;
    validate_fault_policy(required(object, "policy")?)?;
    validate_start(required(object, "start")?, format)?;
    validate_step_body(required(object, "step")?)?;
    if let Some(sequence) = object.get("decision_sequence")
        && sequence.as_u64().is_none()
    {
        return Err(ManifestError::MalformedBase);
    }
    if let Some(repeat) = object.get("repeat") {
        validate_repeat(repeat)?;
    }
    Ok(())
}

fn validate_step_body(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["declaration", "kind", "name"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_step_name_member(object, "name")?;
    let declaration = required(object, "declaration")?;
    match object.get("kind").and_then(Value::as_str) {
        Some("tasklet") => {
            let declaration = exact_object(
                declaration,
                &["component", "delivery_mode", "transaction_boundary"],
                &[],
                ManifestError::MalformedBase,
            )?;
            validate_component_revision_member(declaration, "component")?;
            require_exact_base_string(declaration, "delivery_mode", TASKLET_DELIVERY_MODE)?;
            require_exact_base_string(
                declaration,
                "transaction_boundary",
                TASKLET_TRANSACTION_BOUNDARY,
            )
        }
        Some("chunk") => {
            let declaration = exact_object(
                declaration,
                &[
                    "checkpoint",
                    "components",
                    "context",
                    "delivery_mode",
                    "size",
                    "transaction_boundary",
                ],
                &["in_flight_policy"],
                ManifestError::MalformedBase,
            )?;
            require_exact_base_string(
                declaration,
                "transaction_boundary",
                CHUNK_TRANSACTION_BOUNDARY,
            )?;
            validate_chunk_delivery_mode_member(declaration, "delivery_mode")?;
            if declaration
                .get("size")
                .and_then(Value::as_u64)
                .is_none_or(|value| value == 0 || value > u64::from(u32::MAX))
            {
                return Err(ManifestError::MalformedBase);
            }
            validate_schema_descriptor(required(declaration, "checkpoint")?)?;
            validate_component_revisions(required(declaration, "components")?)?;
            validate_schema_descriptor(required(declaration, "context")?)?;
            if let Some(policy) = declaration.get("in_flight_policy") {
                require_exact_value_string(policy, ROLLBACK_CHUNK_IN_FLIGHT_POLICY)?;
            }
            Ok(())
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn validate_decision(object: &Map<String, Value>, format: u16) -> Result<(), ManifestError> {
    let optional: &[&str] = if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        &["decision_sequence"]
    } else {
        &[]
    };
    exact_keys(
        object,
        &["decision", "id", "kind"],
        optional,
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "decision")?;
    let decision = exact_object(
        required(object, "decision")?,
        &["input_version", "revision"],
        &[],
        ManifestError::MalformedBase,
    )?;
    if decision
        .get("input_version")
        .and_then(Value::as_u64)
        .is_none_or(|value| value == 0 || value > u64::from(u32::MAX))
    {
        return Err(ManifestError::MalformedBase);
    }
    let revision = base_str(decision, "revision")?;
    validate_token(revision, DefinitionTokenKind::Decider)
        .map_err(|_| ManifestError::MalformedBase)?;
    validate_optional_sequence(object)
}

fn validate_join(object: &Map<String, Value>, format: u16) -> Result<(), ManifestError> {
    let optional: &[&str] = if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        &["decision_sequence"]
    } else {
        &[]
    };
    exact_keys(
        object,
        &["id", "kind"],
        optional,
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "join")?;
    validate_optional_sequence(object)
}

fn validate_split(object: &Map<String, Value>, format: u16) -> Result<(), ManifestError> {
    let optional: &[&str] = if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        &["decision_sequence"]
    } else {
        &[]
    };
    exact_keys(
        object,
        &["branches", "failure_policy", "id", "join", "kind"],
        optional,
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "id")?;
    validate_node_id_member(object, "join")?;
    require_exact_base_string(object, "kind", "split")?;
    require_one_of_base_strings(object, "failure_policy", LOCAL_FAILURE_POLICIES)?;
    let branches = required(object, "branches")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        for branch in branches {
            let branch_object = branch.as_object().ok_or(ManifestError::MalformedBase)?;
            match branch_object.get("kind").and_then(Value::as_str) {
                Some("linear") => {
                    exact_keys(
                        branch_object,
                        &["kind", "ordinal", "steps"],
                        &[],
                        ManifestError::MalformedBase,
                    )?;
                    if branch_object.get("ordinal").and_then(Value::as_u64).is_none() {
                        return Err(ManifestError::MalformedBase);
                    }
                    let steps = required(branch_object, "steps")?
                        .as_array()
                        .ok_or(ManifestError::MalformedBase)?;
                    for step in steps {
                        let step = step.as_object().ok_or(ManifestError::MalformedBase)?;
                        validate_step(step, format)?;
                    }
                }
                Some("flow") => {
                    exact_keys(
                        branch_object,
                        &["entry", "kind", "members", "ordinal"],
                        &[],
                        ManifestError::MalformedBase,
                    )?;
                    validate_node_id_member(branch_object, "entry")?;
                    require_exact_base_string(branch_object, "kind", "flow")?;
                    validate_node_id_array(required(branch_object, "members")?)?;
                    if branch_object.get("ordinal").and_then(Value::as_u64).is_none() {
                        return Err(ManifestError::MalformedBase);
                    }
                }
                _ => return Err(ManifestError::MalformedBase),
            }
        }
    } else {
        for branch in branches {
            let steps = branch.as_array().ok_or(ManifestError::MalformedBase)?;
            for step in steps {
                let step = step.as_object().ok_or(ManifestError::MalformedBase)?;
                validate_step(step, format)?;
            }
        }
    }
    validate_optional_sequence(object)
}

fn validate_partitioned_step(
    object: &Map<String, Value>,
    format: u16,
) -> Result<(), ManifestError> {
    let optional: &[&str] = if format == MANIFEST_FORMAT_ADVANCED_FLOW {
        &["decision_sequence"]
    } else {
        &[]
    };
    exact_keys(
        object,
        &[
            "aggregation",
            "failure_policy",
            "id",
            "kind",
            "partition_count",
            "partitioner",
            "start",
            "step_name",
            "worker",
        ],
        optional,
        ManifestError::MalformedBase,
    )?;
    validate_component_revision_member(object, "aggregation")?;
    require_one_of_base_strings(object, "failure_policy", LOCAL_FAILURE_POLICIES)?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "partitioned_step")?;
    validate_component_revision_member(object, "partitioner")?;
    validate_step_name_member(object, "step_name")?;
    if object
        .get("partition_count")
        .and_then(Value::as_u64)
        .is_none_or(|value| value == 0 || value > u64::try_from(MAX_PARTITIONS).unwrap_or(u64::MAX))
    {
        return Err(ManifestError::MalformedBase);
    }
    validate_start(required(object, "start")?, format)?;
    let worker = required(object, "worker")?
        .as_object()
        .ok_or(ManifestError::MalformedBase)?;
    validate_step(worker, format)?;
    validate_optional_sequence(object)
}

fn validate_nested_job(object: &Map<String, Value>) -> Result<(), ManifestError> {
    exact_keys(
        object,
        &["child", "id", "kind", "mapping_revision", "parameters"],
        &["decision_sequence"],
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "nested_job")?;
    validate_component_revision_member(object, "mapping_revision")?;
    let child = exact_object(
        required(object, "child")?,
        &["fingerprint", "format", "job", "revision"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_sha256_hex_member(child, "fingerprint")?;
    validate_job_name_member(child, "job")?;
    validate_definition_revision_member(child, "revision")?;
    if child
        .get("format")
        .and_then(Value::as_u64)
        .is_none_or(|value| !(1..=u64::from(MANIFEST_FORMAT_ADVANCED_FLOW)).contains(&value))
    {
        return Err(ManifestError::MalformedBase);
    }
    let parameters = required(object, "parameters")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for mapping in parameters {
        validate_nested_mapping(mapping)?;
    }
    validate_optional_sequence(object)
}

fn validate_nested_mapping(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["coercion", "expected", "missing", "role", "source", "target"],
        &[],
        ManifestError::MalformedBase,
    )?;
    require_one_of_base_strings(object, "coercion", PARAMETER_COERCIONS)?;
    validate_parameter_value_kind_member(object, "expected")?;
    require_one_of_base_strings(object, "missing", MISSING_PARAMETER_POLICIES)?;
    require_one_of_base_strings(object, "role", PARAMETER_ROLES)?;
    validate_parameter_name_member(object, "target")?;
    validate_nested_source(required(object, "source")?)
}

fn validate_nested_source(value: &Value) -> Result<(), ManifestError> {
    let object = value.as_object().ok_or(ManifestError::MalformedBase)?;
    match object.get("kind").and_then(Value::as_str) {
        Some("parent_parameter") => {
            exact_keys(object, &["kind", "name"], &[], ManifestError::MalformedBase)?;
            require_exact_base_string(object, "kind", "parent_parameter")?;
            validate_parameter_name_member(object, "name")
        }
        Some("parent_job_context") => {
            exact_keys(
                object,
                &["kind", "path", "schema", "schema_version"],
                &[],
                ManifestError::MalformedBase,
            )?;
            require_exact_base_string(object, "kind", "parent_job_context")?;
            validate_schema_id_member(object, "schema")?;
            validate_selector_path(required(object, "path")?)?;
            validate_schema_version_member(object, "schema_version")
        }
        Some("parent_step_context") => {
            exact_keys(
                object,
                &["kind", "node", "path", "schema", "schema_version"],
                &[],
                ManifestError::MalformedBase,
            )?;
            require_exact_base_string(object, "kind", "parent_step_context")?;
            validate_node_id_member(object, "node")?;
            validate_schema_id_member(object, "schema")?;
            validate_selector_path(required(object, "path")?)?;
            validate_schema_version_member(object, "schema_version")
        }
        Some("framework") => {
            exact_keys(object, &["field", "kind"], &[], ManifestError::MalformedBase)?;
            require_exact_base_string(object, "kind", "framework")?;
            require_one_of_base_strings(object, "field", NESTED_FRAMEWORK_FIELDS)
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn validate_custom_leaf(
    object: &Map<String, Value>,
    format: u16,
) -> Result<(), ManifestError> {
    exact_keys(
        object,
        &[
            "handler_revision",
            "id",
            "kind",
            "leaf_kind",
            "listeners",
            "policy",
            "start",
            "state_schema",
            "step_name",
        ],
        &["decision_sequence"],
        ManifestError::MalformedBase,
    )?;
    validate_component_revision_member(object, "handler_revision")?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "custom_leaf")?;
    validate_generic_definition_token_member(object, "leaf_kind", DefinitionTokenKind::Component)?;
    validate_step_name_member(object, "step_name")?;
    validate_component_revision_array(required(object, "listeners")?)?;
    validate_fault_policy(required(object, "policy")?)?;
    validate_start(required(object, "start")?, format)?;
    let schema = exact_object(
        required(object, "state_schema")?,
        &["id", "version"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_schema_id_member(schema, "id")?;
    validate_schema_version_member(schema, "version")?;
    validate_optional_sequence(object)
}

fn validate_nested_flow(object: &Map<String, Value>) -> Result<(), ManifestError> {
    exact_keys(
        object,
        &["entry", "exits", "id", "kind", "members"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_node_id_member(object, "entry")?;
    validate_node_id_member(object, "id")?;
    require_exact_base_string(object, "kind", "nested_flow")?;
    validate_node_id_array(required(object, "members")?)?;
    let exits = required(object, "exits")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for transition in exits {
        validate_transition(transition)?;
    }
    Ok(())
}

fn validate_scoped_component(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["factory", "id", "inputs", "resolver", "scope"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_scoped_component_id_member(object, "id")?;
    validate_scope_kind_member(object, "scope")?;
    for key in ["factory", "resolver"] {
        let descriptor = exact_object(
            required(object, key)?,
            &["kind", "revision"],
            &[],
            ManifestError::MalformedBase,
        )?;
        validate_generic_definition_token_member(
            descriptor,
            "kind",
            DefinitionTokenKind::Component,
        )?;
        validate_component_revision_member(descriptor, "revision")?;
    }
    let inputs = required(object, "inputs")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for input in inputs {
        let input = exact_object(
            input,
            &["coercion", "expected", "missing", "name", "source"],
            &[],
            ManifestError::MalformedBase,
        )?;
        require_one_of_base_strings(input, "coercion", PARAMETER_COERCIONS)?;
        validate_parameter_value_kind_member(input, "expected")?;
        require_one_of_base_strings(input, "missing", MISSING_PARAMETER_POLICIES)?;
        validate_parameter_name_member(input, "name")?;
        validate_scoped_source(required(input, "source")?)?;
    }
    Ok(())
}

fn validate_scoped_source(value: &Value) -> Result<(), ManifestError> {
    let object = value.as_object().ok_or(ManifestError::MalformedBase)?;
    match object.get("kind").and_then(Value::as_str) {
        Some("job_parameter") => {
            exact_keys(object, &["kind", "name"], &[], ManifestError::MalformedBase)?;
            require_exact_base_string(object, "kind", "job_parameter")?;
            validate_parameter_name_member(object, "name")
        }
        Some("job_context") => {
            exact_keys(
                object,
                &["kind", "path", "schema", "schema_version"],
                &[],
                ManifestError::MalformedBase,
            )?;
            require_exact_base_string(object, "kind", "job_context")?;
            validate_schema_id_member(object, "schema")?;
            validate_selector_path(required(object, "path")?)?;
            validate_schema_version_member(object, "schema_version")
        }
        Some("step_context") => {
            exact_keys(
                object,
                &["kind", "node", "path", "schema", "schema_version"],
                &[],
                ManifestError::MalformedBase,
            )?;
            require_exact_base_string(object, "kind", "step_context")?;
            validate_node_id_member(object, "node")?;
            validate_schema_id_member(object, "schema")?;
            validate_selector_path(required(object, "path")?)?;
            validate_schema_version_member(object, "schema_version")
        }
        Some("framework") => {
            exact_keys(object, &["field", "kind"], &[], ManifestError::MalformedBase)?;
            require_exact_base_string(object, "kind", "framework")?;
            validate_scope_framework_source_member(object, "field")
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn validate_repeat(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["id", "interceptors", "policy", "state"],
        &["nested"],
        ManifestError::MalformedBase,
    )?;
    validate_generic_definition_token_member(object, "id", DefinitionTokenKind::Repeat)?;
    let interceptors = required(object, "interceptors")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for interceptor in interceptors {
        let interceptor = exact_object(
            interceptor,
            &["id", "kind", "revision"],
            &[],
            ManifestError::MalformedBase,
        )?;
        validate_generic_definition_token_member(
            interceptor,
            "id",
            DefinitionTokenKind::RepeatInterceptor,
        )?;
        validate_generic_definition_token_member(
            interceptor,
            "kind",
            DefinitionTokenKind::RepeatInterceptor,
        )?;
        validate_component_revision_member(interceptor, "revision")?;
    }
    let policy = exact_object(
        required(object, "policy")?,
        &["configuration", "kind", "revision"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_generic_definition_token_member(
        policy,
        "configuration",
        DefinitionTokenKind::RepeatPolicy,
    )?;
    validate_generic_definition_token_member(policy, "kind", DefinitionTokenKind::RepeatPolicy)?;
    validate_component_revision_member(policy, "revision")?;
    validate_schema_descriptor(required(object, "state")?)?;
    if let Some(nested) = object.get("nested") {
        validate_repeat(nested)?;
    }
    Ok(())
}

fn validate_transition(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["pattern", "source", "target"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_generic_definition_token_member(object, "pattern", DefinitionTokenKind::Component)?;
    validate_node_id_member(object, "source")?;
    let target = required(object, "target")?
        .as_object()
        .ok_or(ManifestError::MalformedBase)?;
    match (target.get("node"), target.get("terminal")) {
        (Some(node), None) => {
            exact_keys(target, &["node"], &[], ManifestError::MalformedBase)?;
            let node = node.as_str().ok_or(ManifestError::MalformedBase)?;
            NodeId::new(node.to_owned()).map_err(|_| ManifestError::MalformedBase)?;
            Ok(())
        }
        (None, Some(terminal)) => {
            exact_keys(target, &["terminal"], &[], ManifestError::MalformedBase)?;
            let terminal = terminal.as_str().ok_or(ManifestError::MalformedBase)?;
            if [
                TerminalKind::Complete.as_str(),
                TerminalKind::Fail.as_str(),
                TerminalKind::Stop.as_str(),
            ]
            .contains(&terminal)
            {
                Ok(())
            } else {
                Err(ManifestError::MalformedBase)
            }
        }
        _ => Err(ManifestError::MalformedBase),
    }
}

fn validate_start(value: &Value, format: u16) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["allow_start_if_complete", "start_limit"],
        if format == MANIFEST_FORMAT_ADVANCED_FLOW {
            &["restart_policy"]
        } else {
            &[]
        },
        ManifestError::MalformedBase,
    )?;
    if object
        .get("allow_start_if_complete")
        .and_then(Value::as_bool)
        .is_none()
        || object
            .get("start_limit")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0 || value > u64::from(u32::MAX))
    {
        return Err(ManifestError::MalformedBase);
    }
    if let Some(policy) = object.get("restart_policy") {
        require_exact_value_string(policy, NON_RESTARTABLE_POLICY)?;
    }
    Ok(())
}

fn validate_fault_policy(value: &Value) -> Result<(), ManifestError> {
    if value.is_null() {
        return Ok(());
    }
    let object = exact_object(
        value,
        &[
            "backoff",
            "classifier",
            "retry_limit",
            "retry_state_limit",
            "skip_limit",
        ],
        &[],
        ManifestError::MalformedBase,
    )?;
    for key in ["retry_limit", "retry_state_limit", "skip_limit"] {
        require_u64_member(object, key)?;
    }
    let backoff = exact_object(
        required(object, "backoff")?,
        &["initial_ms", "kind", "maximum_ms", "multiplier"],
        &[],
        ManifestError::MalformedBase,
    )?;
    let backoff_kind = base_str(backoff, "kind")?;
    if ![
        BackoffKind::None.as_str(),
        BackoffKind::Fixed.as_str(),
        BackoffKind::Exponential.as_str(),
    ]
    .contains(&backoff_kind)
    {
        return Err(ManifestError::MalformedBase);
    }
    for key in ["initial_ms", "maximum_ms", "multiplier"] {
        if !backoff.contains_key(key) || !backoff[key].is_number() {
            return Err(ManifestError::MalformedBase);
        }
    }
    let classifier = exact_object(
        required(object, "classifier")?,
        &["revision", "rules"],
        &[],
        ManifestError::MalformedBase,
    )?;
    let classifier_revision = base_str(classifier, "revision")?;
    ClassifierRevision::new(classifier_revision.to_owned())
        .map_err(|_| ManifestError::MalformedBase)?;
    let rules = required(classifier, "rules")?
        .as_array()
        .ok_or(ManifestError::MalformedBase)?;
    for rule in rules {
        let rule = exact_object(
            rule,
            &["category", "phase", "retryable", "skip"],
            &[],
            ManifestError::MalformedBase,
        )?;
        validate_failure_category_member(rule, "category")?;
        let phase = base_str(rule, "phase")?;
        if FaultPhase::from_durable_name(phase).is_none() {
            return Err(ManifestError::MalformedBase);
        }
        if rule.get("retryable").and_then(Value::as_bool).is_none()
            || !rule
                .get("skip")
                .is_some_and(|value| value.is_null() || value.is_string())
        {
            return Err(ManifestError::MalformedBase);
        }
    }
    Ok(())
}

fn validate_schema_descriptor(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["schema", "version"],
        &[],
        ManifestError::MalformedBase,
    )?;
    validate_schema_id_member(object, "schema")?;
    validate_schema_version_member(object, "version")
}

fn validate_component_revisions(value: &Value) -> Result<(), ManifestError> {
    let object = exact_object(
        value,
        &["checkpoint", "processor", "reader", "writer"],
        &[],
        ManifestError::MalformedBase,
    )?;
    for key in ["checkpoint", "processor", "reader", "writer"] {
        validate_component_revision_member(object, key)?;
    }
    Ok(())
}

fn validate_optional_sequence(object: &Map<String, Value>) -> Result<(), ManifestError> {
    if let Some(sequence) = object.get("decision_sequence")
        && sequence.as_u64().is_none()
    {
        return Err(ManifestError::MalformedBase);
    }
    Ok(())
}

fn base_str<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ManifestError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ManifestError::MalformedBase)
}

fn require_exact_base_string(
    object: &Map<String, Value>,
    key: &str,
    expected: &str,
) -> Result<(), ManifestError> {
    let actual = base_str(object, key)?;
    if actual == expected {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn require_exact_value_string(value: &Value, expected: &str) -> Result<(), ManifestError> {
    if value.as_str() == Some(expected) {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn require_one_of_base_strings(
    object: &Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<(), ManifestError> {
    let actual = base_str(object, key)?;
    if allowed.contains(&actual) {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_job_name_member(object: &Map<String, Value>, key: &str) -> Result<(), ManifestError> {
    JobName::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_step_name_member(object: &Map<String, Value>, key: &str) -> Result<(), ManifestError> {
    StepName::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_node_id_member(object: &Map<String, Value>, key: &str) -> Result<(), ManifestError> {
    NodeId::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_parameter_name_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    ParameterName::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_definition_revision_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    DefinitionRevision::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_component_revision_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    validate_component_revision_value(
        object.get(key).ok_or(ManifestError::MalformedBase)?,
    )
}

fn validate_component_revision_value(value: &Value) -> Result<(), ManifestError> {
    let value = value.as_str().ok_or(ManifestError::MalformedBase)?;
    ComponentRevision::new(value.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_generic_definition_token_member(
    object: &Map<String, Value>,
    key: &str,
    kind: DefinitionTokenKind,
) -> Result<(), ManifestError> {
    validate_token(base_str(object, key)?, kind).map_err(|_| ManifestError::MalformedBase)
}

fn validate_schema_id_member(object: &Map<String, Value>, key: &str) -> Result<(), ManifestError> {
    StateSchemaId::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_schema_version_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let raw = object
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(ManifestError::MalformedBase)?;
    StateSchemaVersion::new(raw)
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_chunk_delivery_mode_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let actual = base_str(object, key)?;
    if [
        ChunkDeliveryMode::AtomicSameResource.manifest_name(),
        ChunkDeliveryMode::AtLeastOnce.manifest_name(),
    ]
    .contains(&actual)
    {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_parameter_value_kind_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let actual = base_str(object, key)?;
    if [
        ParameterValueKind::String.as_str(),
        ParameterValueKind::I64.as_str(),
        ParameterValueKind::U64.as_str(),
        ParameterValueKind::Bool.as_str(),
    ]
    .contains(&actual)
    {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_scope_kind_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let actual = base_str(object, key)?;
    if [ScopeKind::Job.as_str(), ScopeKind::Step.as_str()].contains(&actual) {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_scope_framework_source_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let actual = base_str(object, key)?;
    if [
        ScopeFrameworkSource::JobInstanceId.as_str(),
        ScopeFrameworkSource::JobExecutionId.as_str(),
        ScopeFrameworkSource::StepExecutionId.as_str(),
        ScopeFrameworkSource::Attempt.as_str(),
        ScopeFrameworkSource::DefinitionRevision.as_str(),
        ScopeFrameworkSource::DefinitionFingerprint.as_str(),
        ScopeFrameworkSource::LogicalNodeId.as_str(),
        ScopeFrameworkSource::StepName.as_str(),
    ]
    .contains(&actual)
    {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_scoped_component_id_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    ScopedComponentId::new(base_str(object, key)?.to_owned())
        .map(|_| ())
        .map_err(|_| ManifestError::MalformedBase)
}

fn validate_selector_path(value: &Value) -> Result<(), ManifestError> {
    let segments = value.as_array().ok_or(ManifestError::MalformedBase)?;
    let mut path = Vec::with_capacity(segments.len());
    for segment in segments {
        path.push(
            segment
                .as_str()
                .ok_or(ManifestError::MalformedBase)?
                .to_owned(),
        );
    }
    if selector_path_is_valid(&path) {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_node_id_array(value: &Value) -> Result<(), ManifestError> {
    let values = value.as_array().ok_or(ManifestError::MalformedBase)?;
    for value in values {
        let raw = value.as_str().ok_or(ManifestError::MalformedBase)?;
        NodeId::new(raw.to_owned()).map_err(|_| ManifestError::MalformedBase)?;
    }
    Ok(())
}

fn validate_component_revision_array(value: &Value) -> Result<(), ManifestError> {
    let values = value.as_array().ok_or(ManifestError::MalformedBase)?;
    for value in values {
        validate_component_revision_value(value)?;
    }
    Ok(())
}

fn validate_sha256_hex_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let value = base_str(object, key)?;
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn validate_failure_category_member(
    object: &Map<String, Value>,
    key: &str,
) -> Result<(), ManifestError> {
    let value = base_str(object, key)?;
    let known = [
        FailureCategory::InvalidDefinition,
        FailureCategory::DuplicateExecution,
        FailureCategory::IllegalTransition,
        FailureCategory::TransientInfrastructure,
        FailureCategory::PermanentInfrastructure,
        FailureCategory::UserComponent,
        FailureCategory::Cancelled,
        FailureCategory::Serialization,
        FailureCategory::Invariant,
        FailureCategory::OptimisticConflict,
        FailureCategory::Timeout,
        FailureCategory::UnsupportedCapability,
        FailureCategory::UnknownCommit,
        FailureCategory::ShutdownIncomplete,
        FailureCategory::StaleRecovered,
    ];
    if known.into_iter().any(|candidate| candidate.as_str() == value) {
        Ok(())
    } else {
        Err(ManifestError::MalformedBase)
    }
}

fn exact_object<'a>(
    value: &'a Value,
    required: &[&str],
    optional: &[&str],
    error: ManifestError,
) -> Result<&'a Map<String, Value>, ManifestError> {
    let object = value.as_object().ok_or(error)?;
    exact_keys(object, required, optional, error)?;
    Ok(object)
}

fn exact_keys(
    object: &Map<String, Value>,
    required: &[&str],
    optional: &[&str],
    error: ManifestError,
) -> Result<(), ManifestError> {
    if required.iter().any(|key| !object.contains_key(*key))
        || object
            .keys()
            .any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
    {
        return Err(error);
    }
    if optional
        .iter()
        .any(|key| object.get(*key).is_some_and(Value::is_null))
    {
        return Err(error);
    }
    Ok(())
}

fn required<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a Value, ManifestError> {
    object.get(key).ok_or(ManifestError::MalformedGateB)
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ManifestError> {
    required(object, key)?
        .as_str()
        .ok_or(ManifestError::MalformedGateB)
}

fn required_u16(object: &Map<String, Value>, key: &str) -> Result<u16, ManifestError> {
    required(object, key)?
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(ManifestError::MalformedGateB)
}

fn require_string_members(
    object: &Map<String, Value>,
    keys: &[&str],
) -> Result<(), ManifestError> {
    if keys
        .iter()
        .any(|key| object.get(*key).and_then(Value::as_str).is_none())
    {
        return Err(ManifestError::MalformedBase);
    }
    Ok(())
}

fn require_u64_member(object: &Map<String, Value>, key: &str) -> Result<(), ManifestError> {
    if object.get(key).and_then(Value::as_u64).is_none() {
        return Err(ManifestError::MalformedBase);
    }
    Ok(())
}

fn string_array(value: &Value) -> Result<(), ManifestError> {
    let values = value.as_array().ok_or(ManifestError::MalformedBase)?;
    if values.iter().any(|value| !value.is_string()) {
        return Err(ManifestError::MalformedBase);
    }
    Ok(())
}
