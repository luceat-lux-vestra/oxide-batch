//! Restart-relevant job-definition identity.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{ChunkSize, JobName, StateSchemaId, StateSchemaVersion, StepName};

const MAX_TOKEN_BYTES: usize = 128;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MANIFEST_FORMAT: u16 = 1;
const LEGACY_REVISION: &str = "__m1_repository_port_v1";
const LEGACY_MANIFEST: &[u8] =
    br#"{"format":1,"repository_port":"m1","revision":"__m1_repository_port_v1"}"#;

fn validate_token(value: &str, kind: DefinitionTokenKind) -> Result<(), DefinitionError> {
    if value.is_empty() {
        return Err(DefinitionError::EmptyToken { kind });
    }
    if value.len() > MAX_TOKEN_BYTES {
        return Err(DefinitionError::TokenTooLong {
            kind,
            max_bytes: MAX_TOKEN_BYTES,
        });
    }
    if value.trim() != value {
        return Err(DefinitionError::SurroundingWhitespace { kind });
    }
    if value.chars().any(char::is_control) {
        return Err(DefinitionError::ControlCharacter { kind });
    }
    Ok(())
}

macro_rules! definition_token {
    ($name:ident, $kind:expr, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Validates and constructs the token.
            ///
            /// # Errors
            ///
            /// Rejects empty values, values longer than 128 UTF-8 bytes,
            /// surrounding whitespace, and control characters.
            pub fn new(value: impl Into<String>) -> Result<Self, DefinitionError> {
                let value = value.into();
                validate_token(&value, $kind)?;
                Ok(Self(value))
            }

            /// Borrows the validated token.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

definition_token!(
    DefinitionRevision,
    DefinitionTokenKind::Revision,
    "An application-owned audit label for one restart-relevant definition."
);
definition_token!(
    DefinitionUpgradeKey,
    DefinitionTokenKind::Upgrade,
    "An application-owned key for one directed definition compatibility edge."
);

/// One source-to-target durable step mapping for a compatible restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepDefinitionUpgrade {
    source: StepName,
    target: StepName,
}

impl StepDefinitionUpgrade {
    /// Constructs one directed step mapping.
    #[must_use]
    pub const fn new(source: StepName, target: StepName) -> Self {
        Self { source, target }
    }

    /// Borrows the checkpoint-producing step name.
    #[must_use]
    pub const fn source(&self) -> &StepName {
        &self.source
    }

    /// Borrows the step name in the proposed definition.
    #[must_use]
    pub const fn target(&self) -> &StepName {
        &self.target
    }
}

/// One explicit, directed definition compatibility edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefinitionUpgrade {
    key: DefinitionUpgradeKey,
    from: DefinitionIdentity,
    to: DefinitionIdentity,
    step_mapping: BTreeMap<StepName, StepName>,
}

impl DefinitionUpgrade {
    /// Validates a direct, non-transitive compatibility edge.
    ///
    /// The M2 edge preserves checkpoint and context bytes unchanged, so
    /// applications may use it only when the mapped steps retain the same
    /// state schemas and semantics.
    ///
    /// # Errors
    ///
    /// Rejects self-edges, empty mappings, and mappings that reuse a target.
    pub fn new(
        key: DefinitionUpgradeKey,
        from: DefinitionIdentity,
        to: DefinitionIdentity,
        steps: impl IntoIterator<Item = StepDefinitionUpgrade>,
    ) -> Result<Self, DefinitionError> {
        if from.manifest_digest() == to.manifest_digest() {
            return Err(DefinitionError::UpgradeSelfEdge);
        }
        let mut step_mapping = BTreeMap::new();
        let mut targets = BTreeSet::new();
        for step in steps {
            if step_mapping
                .insert(step.source().clone(), step.target().clone())
                .is_some()
            {
                return Err(DefinitionError::DuplicateSourceStep);
            }
            if !targets.insert(step.target().clone()) {
                return Err(DefinitionError::DuplicateTargetStep);
            }
        }
        if step_mapping.is_empty() {
            return Err(DefinitionError::EmptyStepMapping);
        }
        Ok(Self {
            key,
            from,
            to,
            step_mapping,
        })
    }

    /// Borrows the application-owned upgrade key.
    #[must_use]
    pub const fn key(&self) -> &DefinitionUpgradeKey {
        &self.key
    }

    /// Borrows the checkpoint-producing definition.
    #[must_use]
    pub const fn from(&self) -> &DefinitionIdentity {
        &self.from
    }

    /// Borrows the proposed definition.
    #[must_use]
    pub const fn to(&self) -> &DefinitionIdentity {
        &self.to
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn step_mapping(&self) -> &BTreeMap<StepName, StepName> {
        &self.step_mapping
    }
}
definition_token!(
    ComponentRevision,
    DefinitionTokenKind::Component,
    "An application-owned revision token for one opaque executable component."
);
definition_token!(
    ClassifierRevision,
    DefinitionTokenKind::Classifier,
    "An application-owned revision token for one bounded fault classifier."
);

/// Component revisions for a one-step chunk definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkComponentRevisions {
    reader: ComponentRevision,
    processor: ComponentRevision,
    writer: ComponentRevision,
    checkpoint: ComponentRevision,
    restart: ChunkRestartContract,
}

impl ChunkComponentRevisions {
    /// Constructs the four restart-relevant chunk component revisions.
    #[must_use]
    pub const fn new(
        reader: ComponentRevision,
        processor: ComponentRevision,
        writer: ComponentRevision,
        checkpoint: ComponentRevision,
        restart: ChunkRestartContract,
    ) -> Self {
        Self {
            reader,
            processor,
            writer,
            checkpoint,
            restart,
        }
    }

    /// Returns the delivery mode declared by the restart contract.
    #[must_use]
    pub const fn delivery_mode(&self) -> ChunkDeliveryMode {
        self.restart.delivery_mode
    }
}

/// Declared delivery boundary included in a chunk definition fingerprint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChunkDeliveryMode {
    /// Business writes and progress share one `PostgreSQL` transaction.
    AtomicSameResource,
    /// The resource may observe a duplicate after restart.
    AtLeastOnce,
}

impl ChunkDeliveryMode {
    const fn manifest_name(self) -> &'static str {
        match self {
            Self::AtomicSameResource => "atomic_same_resource",
            Self::AtLeastOnce => "at_least_once",
        }
    }
}

/// Restart-state schemas and delivery mode for a chunk definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRestartContract {
    checkpoint_schema: StateSchemaId,
    checkpoint_schema_version: StateSchemaVersion,
    context_schema: StateSchemaId,
    context_schema_version: StateSchemaVersion,
    delivery_mode: ChunkDeliveryMode,
}

impl ChunkRestartContract {
    /// Constructs the restart-relevant state and delivery declaration.
    #[must_use]
    pub const fn new(
        checkpoint_schema: StateSchemaId,
        checkpoint_schema_version: StateSchemaVersion,
        context_schema: StateSchemaId,
        context_schema_version: StateSchemaVersion,
        delivery_mode: ChunkDeliveryMode,
    ) -> Self {
        Self {
            checkpoint_schema,
            checkpoint_schema_version,
            context_schema,
            context_schema_version,
            delivery_mode,
        }
    }
}

/// Canonical restart-relevant identity persisted with every execution.
#[derive(Clone, Eq, PartialEq)]
pub struct DefinitionIdentity {
    job_name: Option<JobName>,
    revision: DefinitionRevision,
    manifest_format: u16,
    manifest_digest: [u8; 32],
    canonical_manifest: Box<[u8]>,
}

impl DefinitionIdentity {
    pub(crate) fn legacy() -> Self {
        Self::from_canonical(
            None,
            DefinitionRevision(LEGACY_REVISION.to_owned()),
            LEGACY_MANIFEST.to_vec(),
        )
    }

    /// Builds the canonical identity for a one-step tasklet definition.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError::ManifestEncoding`] if the bounded canonical
    /// manifest cannot be encoded.
    pub fn tasklet(
        job_name: &JobName,
        step_name: &StepName,
        revision: DefinitionRevision,
        component_revision: &ComponentRevision,
    ) -> Result<Self, DefinitionError> {
        let manifest = json!({
            "component": {
                "tasklet": component_revision.as_str()
            },
            "delivery_mode": "best_effort",
            "format": MANIFEST_FORMAT,
            "job": job_name.as_str(),
            "kind": "tasklet",
            "restart_state": "none",
            "step": step_name.as_str(),
            "transaction_boundary": "tasklet_completion"
        });
        Self::encode(job_name.clone(), revision, &manifest)
    }

    /// Builds the canonical identity for a one-step chunk definition.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError::ManifestEncoding`] if the bounded canonical
    /// manifest cannot be encoded.
    pub fn chunk(
        job_name: &JobName,
        step_name: &StepName,
        chunk_size: ChunkSize,
        revision: DefinitionRevision,
        components: &ChunkComponentRevisions,
    ) -> Result<Self, DefinitionError> {
        let manifest = json!({
            "chunk_size": chunk_size.get(),
            "components": {
                "checkpoint": components.checkpoint.as_str(),
                "processor": components.processor.as_str(),
                "reader": components.reader.as_str(),
                "writer": components.writer.as_str()
            },
            "context": {
                "schema": components.restart.context_schema.as_str(),
                "version": components.restart.context_schema_version.get()
            },
            "checkpoint": {
                "schema": components.restart.checkpoint_schema.as_str(),
                "version": components.restart.checkpoint_schema_version.get()
            },
            "delivery_mode": components.restart.delivery_mode.manifest_name(),
            "format": MANIFEST_FORMAT,
            "job": job_name.as_str(),
            "kind": "chunk",
            "step": step_name.as_str(),
            "transaction_boundary": "chunk"
        });
        Self::encode(job_name.clone(), revision, &manifest)
    }

    fn encode(
        job_name: JobName,
        revision: DefinitionRevision,
        manifest: &serde_json::Value,
    ) -> Result<Self, DefinitionError> {
        let canonical =
            serde_json::to_vec(manifest).map_err(|_| DefinitionError::ManifestEncoding)?;
        if canonical.len() > MAX_MANIFEST_BYTES {
            return Err(DefinitionError::ManifestTooLarge {
                max_bytes: MAX_MANIFEST_BYTES,
            });
        }
        Ok(Self::from_canonical(Some(job_name), revision, canonical))
    }

    fn from_canonical(
        job_name: Option<JobName>,
        revision: DefinitionRevision,
        canonical: Vec<u8>,
    ) -> Self {
        let digest: [u8; 32] = Sha256::digest(&canonical).into();
        Self {
            job_name,
            revision,
            manifest_format: MANIFEST_FORMAT,
            manifest_digest: digest,
            canonical_manifest: canonical.into_boxed_slice(),
        }
    }

    /// Borrows the application-owned definition revision.
    #[must_use]
    pub const fn revision(&self) -> &DefinitionRevision {
        &self.revision
    }

    /// Borrows the job name bound into a framework-produced manifest.
    ///
    /// Legacy direct repository calls use an internal compatibility manifest
    /// without a bound name.
    #[must_use]
    pub const fn job_name(&self) -> Option<&JobName> {
        self.job_name.as_ref()
    }

    /// Returns the canonical manifest format version.
    #[must_use]
    pub const fn manifest_format(&self) -> u16 {
        self.manifest_format
    }

    /// Returns the framework-produced SHA-256 manifest digest.
    #[must_use]
    pub const fn manifest_digest(&self) -> &[u8; 32] {
        &self.manifest_digest
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn canonical_manifest(&self) -> &[u8] {
        &self.canonical_manifest
    }
}

impl fmt::Debug for DefinitionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DefinitionIdentity")
            .field("job_name", &self.job_name)
            .field("revision", &self.revision)
            .field("manifest_format", &self.manifest_format)
            .field(
                "digest_prefix",
                &DigestPrefix([
                    self.manifest_digest[0],
                    self.manifest_digest[1],
                    self.manifest_digest[2],
                    self.manifest_digest[3],
                ]),
            )
            .field("canonical_manifest", &"<redacted>")
            .finish()
    }
}

struct DigestPrefix([u8; 4]);

impl fmt::Debug for DigestPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Definition token category used by validation diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DefinitionTokenKind {
    /// Definition revision.
    Revision,
    /// Opaque component revision.
    Component,
    /// Directed compatibility edge key.
    Upgrade,
    /// Bounded fault-classifier revision.
    Classifier,
}

/// Failure to construct a bounded restart definition.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DefinitionError {
    /// A required token was empty.
    EmptyToken {
        /// Rejected token category.
        kind: DefinitionTokenKind,
    },
    /// A token exceeded its UTF-8 byte bound.
    TokenTooLong {
        /// Rejected token category.
        kind: DefinitionTokenKind,
        /// Maximum accepted byte length.
        max_bytes: usize,
    },
    /// A token had leading or trailing whitespace.
    SurroundingWhitespace {
        /// Rejected token category.
        kind: DefinitionTokenKind,
    },
    /// A token contained a control character.
    ControlCharacter {
        /// Rejected token category.
        kind: DefinitionTokenKind,
    },
    /// The canonical manifest could not be encoded.
    ManifestEncoding,
    /// The canonical manifest exceeded its durable bound.
    ManifestTooLarge {
        /// Maximum accepted byte length.
        max_bytes: usize,
    },
    /// A directed edge pointed from a definition to itself.
    UpgradeSelfEdge,
    /// A directed edge omitted its durable step mapping.
    EmptyStepMapping,
    /// A source step appeared more than once.
    DuplicateSourceStep,
    /// Two source steps mapped to the same target step.
    DuplicateTargetStep,
    /// A step's fault runtime declared a different delivery mode than its
    /// restart contract.
    DeliveryModeMismatch,
}

impl fmt::Display for DefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyToken { kind } => write!(formatter, "{kind:?} token must not be empty"),
            Self::TokenTooLong { kind, max_bytes } => {
                write!(formatter, "{kind:?} token exceeds {max_bytes} bytes")
            }
            Self::SurroundingWhitespace { kind } => {
                write!(formatter, "{kind:?} token has surrounding whitespace")
            }
            Self::ControlCharacter { kind } => {
                write!(formatter, "{kind:?} token contains a control character")
            }
            Self::ManifestEncoding => formatter.write_str("definition manifest encoding failed"),
            Self::ManifestTooLarge { max_bytes } => {
                write!(formatter, "definition manifest exceeds {max_bytes} bytes")
            }
            Self::UpgradeSelfEdge => formatter.write_str("definition upgrade is a self-edge"),
            Self::EmptyStepMapping => formatter.write_str("definition upgrade has no step mapping"),
            Self::DuplicateSourceStep => {
                formatter.write_str("definition upgrade repeats a source step")
            }
            Self::DuplicateTargetStep => {
                formatter.write_str("definition upgrade reuses a target step")
            }
            Self::DeliveryModeMismatch => formatter
                .write_str("fault runtime and restart contract declare different delivery modes"),
        }
    }
}

impl Error for DefinitionError {}
