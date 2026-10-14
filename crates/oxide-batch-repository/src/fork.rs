//! Explicit new-lineage fork identity. No raw parameter or state payload is persisted here.

use crate::{DefinitionRegistration, OperationId};
use oxide_batch_core::{
    DefinitionIdentity, DefinitionStateTransform, ExecutionVersion, JobExecutionId, JobInstanceId,
    JobInstanceKey, JobParameters, StepExecutionId,
};

/// Explicit context transformation selected by one fork request.
///
/// This is neither a definition-compatible restart nor permission to copy
/// arbitrary application context: the source step, its optimistic version and
/// the complete state schema/revision contract must match the durable source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkContextTransform {
    source_step_execution_id: StepExecutionId,
    source_step_version: ExecutionVersion,
    descriptor: DefinitionStateTransform,
}

impl ForkContextTransform {
    /// Binds one reviewed descriptor to a committed source step.
    #[must_use]
    pub const fn new(
        source_step_execution_id: StepExecutionId,
        source_step_version: ExecutionVersion,
        descriptor: DefinitionStateTransform,
    ) -> Self {
        Self {
            source_step_execution_id,
            source_step_version,
            descriptor,
        }
    }
    /// Returns the selected source step.
    #[must_use]
    pub const fn source_step_execution_id(&self) -> StepExecutionId {
        self.source_step_execution_id
    }
    /// Returns the exact committed source-step version.
    #[must_use]
    pub const fn source_step_version(&self) -> ExecutionVersion {
        self.source_step_version
    }
    /// Borrows the exact source/target node, schema and transformer revision contract.
    #[must_use]
    pub const fn descriptor(&self) -> &DefinitionStateTransform {
        &self.descriptor
    }
}

/// A caller-supplied idempotent fork command, distinct from restart and upgrade.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkRequest {
    operation_id: OperationId,
    source_execution_id: JobExecutionId,
    source_version: ExecutionVersion,
    target: DefinitionRegistration,
    parameters: JobParameters,
    context_transform: Option<ForkContextTransform>,
}
impl ForkRequest {
    /// Creates a fork from one exact committed execution.
    #[must_use]
    pub const fn new(
        operation_id: OperationId,
        source_execution_id: JobExecutionId,
        source_version: ExecutionVersion,
        target: DefinitionRegistration,
        parameters: JobParameters,
    ) -> Self {
        Self {
            operation_id,
            source_execution_id,
            source_version,
            target,
            parameters,
            context_transform: None,
        }
    }
    /// Attaches one reviewed context transform; no inferred state mapping.
    #[must_use]
    pub fn with_context_transform(mut self, selected: ForkContextTransform) -> Self {
        self.context_transform = Some(selected);
        self
    }
    /// Borrows the exact requested context transform, when present.
    #[must_use]
    pub const fn context_transform(&self) -> Option<&ForkContextTransform> {
        self.context_transform.as_ref()
    }
    /// Borrows the idempotency key.
    #[must_use]
    pub const fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }
    /// Returns the source execution identity.
    #[must_use]
    pub const fn source_execution_id(&self) -> JobExecutionId {
        self.source_execution_id
    }
    /// Returns the expected source version.
    #[must_use]
    pub const fn source_version(&self) -> ExecutionVersion {
        self.source_version
    }
    /// Borrows the exact registered target.
    #[must_use]
    pub const fn target(&self) -> &DefinitionRegistration {
        &self.target
    }
    /// Borrows the explicitly supplied target parameters.
    #[must_use]
    pub const fn parameters(&self) -> &JobParameters {
        &self.parameters
    }
    /// Derives the normal identifying-parameter key without suffixes.
    #[must_use]
    pub fn target_key(&self) -> JobInstanceKey {
        JobInstanceKey::new(self.target.job_name().clone(), &self.parameters)
    }
}

/// A durable immutable fork proof. No lifecycle or application state is inherited.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkLineage {
    operation_id: OperationId,
    source_execution_id: JobExecutionId,
    source_version: ExecutionVersion,
    source_definition_digest: [u8; 32],
    source_state_digest: [u8; 32],
    target: DefinitionRegistration,
    target_instance_id: JobInstanceId,
    target_execution_id: JobExecutionId,
    target_parameters_digest: [u8; 32],
    context_transform: Option<ForkContextTransform>,
}
impl ForkLineage {
    /// Creates a committed candidate from validated source and new target.
    #[must_use]
    pub fn new(
        request: &ForkRequest,
        source_definition: &DefinitionIdentity,
        source_state_digest: [u8; 32],
        target_instance_id: JobInstanceId,
        target_execution_id: JobExecutionId,
    ) -> Self {
        Self::from_durable(
            request.operation_id().clone(),
            request.source_execution_id(),
            request.source_version(),
            *source_definition.manifest_digest(),
            source_state_digest,
            request.target().clone(),
            target_instance_id,
            target_execution_id,
            request.parameters().flow_input_digest(),
        )
        .with_context_transform(request.context_transform().cloned())
    }

    /// Associates the exact durable transform metadata with the lineage.
    #[doc(hidden)]
    #[must_use]
    pub fn with_context_transform(mut self, value: Option<ForkContextTransform>) -> Self {
        self.context_transform = value;
        self
    }

    /// Returns the explicit transformed-fork descriptor, if any.
    #[must_use]
    pub const fn context_transform(&self) -> Option<&ForkContextTransform> {
        self.context_transform.as_ref()
    }
    /// Reconstitutes the bounded, payload-free database projection.
    #[doc(hidden)]
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn from_durable(
        operation_id: OperationId,
        source_execution_id: JobExecutionId,
        source_version: ExecutionVersion,
        source_definition_digest: [u8; 32],
        source_state_digest: [u8; 32],
        target: DefinitionRegistration,
        target_instance_id: JobInstanceId,
        target_execution_id: JobExecutionId,
        target_parameters_digest: [u8; 32],
    ) -> Self {
        Self {
            operation_id,
            source_execution_id,
            source_version,
            source_definition_digest,
            source_state_digest,
            target,
            target_instance_id,
            target_execution_id,
            target_parameters_digest,
            context_transform: None,
        }
    }
    /// Returns true only for the original, immutable command payload.
    #[must_use]
    pub fn matches_request(&self, request: &ForkRequest) -> bool {
        self.operation_id == *request.operation_id()
            && self.source_execution_id == request.source_execution_id()
            && self.source_version == request.source_version()
            && self.target == *request.target()
            && self.target_parameters_digest == request.parameters().flow_input_digest()
            && self.context_transform.as_ref() == request.context_transform()
    }
    /// Returns the operation key.
    #[must_use]
    pub const fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }
    /// Returns the source execution.
    #[must_use]
    pub const fn source_execution_id(&self) -> JobExecutionId {
        self.source_execution_id
    }
    /// Returns the source version.
    #[must_use]
    pub const fn source_version(&self) -> ExecutionVersion {
        self.source_version
    }
    /// Returns the source definition fingerprint.
    #[must_use]
    pub const fn source_definition_digest(&self) -> &[u8; 32] {
        &self.source_definition_digest
    }
    /// Returns the bounded committed-state fingerprint.
    #[must_use]
    pub const fn source_state_digest(&self) -> &[u8; 32] {
        &self.source_state_digest
    }
    /// Borrows the immutable target binding.
    #[must_use]
    pub const fn target(&self) -> &DefinitionRegistration {
        &self.target
    }
    /// Returns the new instance ID.
    #[must_use]
    pub const fn target_instance_id(&self) -> JobInstanceId {
        self.target_instance_id
    }
    /// Returns the first execution ID.
    #[must_use]
    pub const fn target_execution_id(&self) -> JobExecutionId {
        self.target_execution_id
    }
    /// Returns the complete target parameter digest.
    #[must_use]
    pub const fn target_parameters_digest(&self) -> &[u8; 32] {
        &self.target_parameters_digest
    }
}
