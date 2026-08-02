//! Redacted projections of service results.
//!
//! Every command renders its result through this module, so the redaction rules
//! live in one place rather than once per command and once per output form.
//!
//! No projection here may carry a job parameter value, an execution or step
//! context, a checkpoint payload, an item, a credential, an endpoint, SQL text,
//! or user error text. Parameter names, type tags, envelope sizes, digests, and
//! framework failure categories are observable; the values behind them are not.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use oxide_batch::{
    DefinitionDescriptor, DurableStateKind, ExecutionCounts, ExecutionTimestamps, FlowDecision,
    FlowTarget, FlowTransitionKind, JobExecutionProjection, JobInstanceProjection, OperatorRecord,
    OwnerObservation, ParameterDescriptor, PurgeCounts, PurgePlan, RecoveryDecision,
    RecoveryProposal, RetentionHold, RetentionRecord, StateEnvelopeDescriptor,
    StepExecutionProjection, StepPartitionProjection,
};

/// Renders an instant as whole seconds since the Unix epoch.
///
/// An instant before the epoch renders as null rather than as a negative value
/// that a machine consumer would have to special-case.
fn instant(value: SystemTime) -> Value {
    value
        .duration_since(UNIX_EPOCH)
        .map_or(Value::Null, |elapsed| json!(elapsed.as_secs()))
}

fn optional_instant(value: Option<SystemTime>) -> Value {
    value.map_or(Value::Null, instant)
}

/// Renders a digest as lowercase hexadecimal.
fn digest(value: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in value {
        let _ = std::fmt::Write::write_fmt(&mut encoded, format_args!("{byte:02x}"));
    }
    encoded
}

fn counts(value: ExecutionCounts) -> Value {
    json!({
        "read": value.read(),
        "processed": value.processed(),
        "written": value.written(),
        "filtered": value.filtered(),
        "committed": value.committed(),
    })
}

fn timestamps(value: ExecutionTimestamps) -> Value {
    json!({
        "created_at": instant(value.created_at()),
        "started_at": optional_instant(value.started_at()),
        "ended_at": optional_instant(value.ended_at()),
    })
}

/// Returns the stable wire name of one durable state category.
///
/// The mapping is owned here rather than derived from `Debug`, because the
/// output schema is this crate's published contract: renaming a library variant
/// must force a visible decision here instead of silently changing a wire
/// value. The wildcard keeps a newly added category renderable.
fn state_kind(value: DurableStateKind) -> &'static str {
    match value {
        DurableStateKind::Checkpoint => "checkpoint",
        DurableStateKind::ExecutionContext => "execution_context",
        _ => "other",
    }
}

/// Returns the stable wire name of one flow transition cause.
///
/// Owned here for the same reason as [`state_kind`].
fn transition_kind(value: FlowTransitionKind) -> &'static str {
    match value {
        FlowTransitionKind::StepExit => "step_exit",
        FlowTransitionKind::Decider => "decider",
        FlowTransitionKind::CompletedStepReuse => "completed_step_reuse",
        _ => "other",
    }
}

/// Projects the destination one transition selected.
///
/// The two destinations are distinct shapes rather than one opaque string, so a
/// consumer reads a node identifier or a terminal without parsing prose.
fn flow_target(value: &FlowTarget) -> Value {
    match value {
        FlowTarget::Node(id) => json!({ "node": id.as_str() }),
        FlowTarget::Terminal(kind) => json!({ "terminal": kind.as_str() }),
    }
}

/// Projects a durable state envelope without its payload.
fn envelope(value: Option<&StateEnvelopeDescriptor>) -> Value {
    value.map_or(Value::Null, |envelope| {
        json!({
            "kind": state_kind(envelope.kind()),
            "format_version": envelope.format_version(),
            "schema_id": envelope.schema_id().as_str(),
            "schema_version": envelope.schema_version().get(),
            "encoded_len": envelope.encoded_len(),
        })
    })
}

fn definition(value: Option<&DefinitionDescriptor>) -> Value {
    value.map_or(Value::Null, |descriptor| {
        json!({
            "revision": descriptor.revision().as_str(),
            "manifest_format": descriptor.manifest_format(),
            "manifest_digest": descriptor.manifest_digest_hex(),
        })
    })
}

/// Projects a parameter's name, type tag, and identity role but never a value.
fn parameter(value: &ParameterDescriptor) -> Value {
    json!({
        "name": value.name().as_str(),
        "kind": value.kind().as_str(),
        "identifying": value.is_identifying(),
    })
}

/// Projects one logical job instance.
#[must_use]
pub fn instance(value: &JobInstanceProjection) -> Value {
    json!({
        "instance_id": value.id().get(),
        "job_name": value.job_name().as_str(),
        "instance_key_digest": value.instance_key_digest_hex(),
        "created_at": optional_instant(value.created_at()),
        "parameters": value.parameters().iter().map(parameter).collect::<Vec<_>>(),
        "hold": value.hold().map_or(Value::Null, hold),
    })
}

/// Projects one job execution attempt.
#[must_use]
pub fn execution(value: &JobExecutionProjection) -> Value {
    json!({
        "execution_id": value.id().get(),
        "instance_id": value.job_instance_id().get(),
        "job_name": value.job_name().as_str(),
        "attempt": value.attempt(),
        "status": value.status().as_str(),
        "exit_code": value.exit_status().code().as_str(),
        "version": value.version().get(),
        "counts": counts(value.counts()),
        "timestamps": timestamps(value.timestamps()),
        "updated_at": instant(value.updated_at()),
        "failure": value.failure().map_or(Value::Null, |failure| {
            json!({
                "category": failure.category().as_str(),
                "failure_id": failure.failure_id().get(),
            })
        }),
        "definition": definition(value.definition()),
        "context": envelope(value.context()),
        "stop_requested_at": optional_instant(value.stop_requested_at()),
        "owner_recorded": value.owner_recorded(),
    })
}

/// Projects bounded recovery evidence and its version-bound digest.
#[must_use]
pub fn recovery_proposal(value: &RecoveryProposal) -> Value {
    let evidence = value.evidence();
    let owner = match evidence.owner() {
        OwnerObservation::Absent => "absent",
        OwnerObservation::CurrentProcess => "current_process",
        OwnerObservation::OtherProcess => "other_process",
        _ => "other",
    };
    let latest_step = evidence.latest_step().map_or(Value::Null, |step| {
        json!({
            "step_execution_id": step.id().get(),
            "status": step.status().as_str(),
            "checkpoint": envelope(step.checkpoint()),
        })
    });
    json!({
        "evidence_digest": value.digest_hex(),
        "observed_version": value.observed_version().get(),
        "status": evidence.status().as_str(),
        "attempt": evidence.attempt(),
        "owner": owner,
        "updated_at": instant(evidence.updated_at()),
        "inactivity_millis": evidence.inactivity().as_millis(),
        "server_time": instant(evidence.server_time()),
        "observed_clock_offset_millis": evidence.observed_clock_offset().as_millis(),
        "observation_window_millis": evidence.observation_window().as_millis(),
        "latest_step": latest_step,
        "unknown_commit": evidence.unknown_commit(),
        "completed_partition": evidence.completed_partition(),
        "committed_flow_decision": evidence.committed_flow_decision(),
        "ambiguous_external_effect": evidence.ambiguous_external_effect(),
    })
}

/// Projects one step execution attempt.
#[must_use]
pub fn step(value: &StepExecutionProjection) -> Value {
    json!({
        "step_execution_id": value.id().get(),
        "execution_id": value.job_execution_id().get(),
        "step_name": value.step_name().as_str(),
        "node_id": value.node_id().map_or(Value::Null, |node| json!(node.as_str())),
        "status": value.status().as_str(),
        "exit_code": value.exit_status().code().as_str(),
        "version": value.version().get(),
        "counts": counts(value.counts()),
        "timestamps": timestamps(value.timestamps()),
        "failure": value.failure().map_or(Value::Null, |failure| {
            json!({
                "category": failure.category().as_str(),
                "failure_id": failure.failure_id().get(),
            })
        }),
        "checkpoint": envelope(value.checkpoint()),
        "context": envelope(value.context()),
    })
}

/// Projects one durable step partition.
#[must_use]
pub fn partition(value: &StepPartitionProjection) -> Value {
    json!({
        "partition_id": value.id().get(),
        "step_execution_id": value.step_execution_id().get(),
        "partition_key": value.partition_key(),
        "ordinal": value.ordinal(),
        "status": value.status().as_str(),
        "exit_code": value.exit_status().code().as_str(),
        "counts": counts(value.counts()),
    })
}

/// Projects one recorded flow transition.
#[must_use]
pub fn flow_decision(value: &FlowDecision) -> Value {
    json!({
        "record": "flow_decision",
        "decision_id": value.id().get(),
        "execution_id": value.job_execution_id().get(),
        "sequence": value.sequence().get(),
        "source_node_id": value.source_node_id().as_str(),
        "source_step_execution_id": value
            .source_step_execution_id()
            .map_or(Value::Null, |id| json!(id.get())),
        "kind": transition_kind(value.kind()),
        "observed_outcome": value.observed_outcome().as_str(),
        "target": flow_target(value.target()),
        "plan_fingerprint": digest(value.plan_fingerprint()),
        "input_digest": digest(value.input_digest()),
        "reused_decision_id": value
            .reused_decision_id()
            .map_or(Value::Null, |id| json!(id.get())),
        "decided_at": instant(value.decided_at()),
    })
}

/// Projects one append-only recovery decision.
#[must_use]
pub fn recovery_decision(value: &RecoveryDecision) -> Value {
    json!({
        "record": "recovery_decision",
        "decision_id": value.id().get(),
        "execution_id": value.job_execution_id().get(),
        "execution_version": value.execution_version().get(),
        "prior_status": value.prior_status().as_str(),
        "resulting_status": value.resulting_status().as_str(),
        "reason_code": value.reason_code(),
        "operator_reference": value.operator_reference(),
        "evidence_digest": digest(value.evidence_digest()),
    })
}

/// Projects one append-only operator audit record.
#[must_use]
pub fn operator_record(value: &OperatorRecord) -> Value {
    json!({
        "record": "operator_request",
        "request_id": value.id().get(),
        "action": value.action().as_str(),
        "operation_id": value.operation_id().as_str(),
        "actor": value.actor().as_str(),
        "reason": value.reason().map_or(Value::Null, |reason| json!(reason.as_str())),
        "digest": value.digest().to_hex(),
        "instance_id": value.job_instance_id().map_or(Value::Null, |id| json!(id.get())),
        "execution_id": value.job_execution_id().map_or(Value::Null, |id| json!(id.get())),
        "observed_version": value
            .observed_version()
            .map_or(Value::Null, |version| json!(version.get())),
        "prior_status": value.prior_status().map_or(Value::Null, |status| json!(status.as_str())),
        "result_status": value.result_status().map_or(Value::Null, |status| json!(status.as_str())),
        "outcome": value.outcome().as_str(),
        "rejection": value.rejection().map_or(Value::Null, |rejection| json!(rejection.as_str())),
        "requested_at": instant(value.requested_at()),
    })
}

/// Projects one retention hold.
#[must_use]
pub fn hold(value: &RetentionHold) -> Value {
    json!({
        "instance_id": value.job_instance_id().get(),
        "actor": value.actor().as_str(),
        "reason": value.reason().as_str(),
        "placed_at": instant(value.placed_at()),
    })
}

/// Projects the durable counters of one purge.
#[must_use]
pub fn purge_counts(value: PurgeCounts) -> Value {
    json!({
        "job_instances": value.job_instances(),
        "job_executions": value.job_executions(),
        "step_executions": value.step_executions(),
        "step_partitions": value.step_partitions(),
        "flow_decisions": value.flow_decisions(),
        "recovery_decisions": value.recovery_decisions(),
        "operator_requests": value.operator_requests(),
    })
}

/// Projects one bounded purge plan and its guard digest.
#[must_use]
pub fn purge_plan(value: &PurgePlan) -> Value {
    json!({
        "job_name": value.request().job_name().as_str(),
        "minimum_age_seconds": value.request().minimum_age().as_secs(),
        "batch_bound": value.request().batch().get(),
        "statuses": value
            .request()
            .statuses()
            .iter()
            .map(oxide_batch::BatchStatus::as_str)
            .collect::<Vec<_>>(),
        "plan_digest": value.digest_hex(),
        "empty": value.is_empty(),
        "counts": purge_counts(value.counts()),
        "candidates": value
            .candidates()
            .iter()
            .map(|candidate| {
                json!({
                    "instance_id": candidate.job_instance_id().get(),
                    "execution_id": candidate.job_execution_id().get(),
                    "version": candidate.version().get(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

/// Projects one append-only retention audit record.
#[must_use]
pub fn retention_record(value: &RetentionRecord) -> Value {
    json!({
        "action_id": value.id().get(),
        "action": value.action().as_str(),
        "operation_id": value.operation_id().as_str(),
        "actor": value.actor().as_str(),
        "reason": value.reason().as_str(),
        "instance_id": value.job_instance_id().map_or(Value::Null, |id| json!(id.get())),
        "plan_digest": value.plan_digest().map_or(Value::Null, |value| json!(digest(value))),
        "batch_bound": value.batch_bound().map_or(Value::Null, |bound| json!(bound.get())),
        "counts": purge_counts(value.counts()),
        "outcome": value.outcome().as_str(),
        "applied_at": instant(value.applied_at()),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::digest;

    #[test]
    fn digests_render_as_lowercase_hexadecimal() {
        let mut value = [0_u8; 32];
        value[0] = 0xAB;
        value[31] = 0x0F;
        let rendered = digest(&value);
        assert_eq!(rendered.len(), 64);
        assert!(rendered.starts_with("ab"));
        assert!(rendered.ends_with("0f"));
    }
}
