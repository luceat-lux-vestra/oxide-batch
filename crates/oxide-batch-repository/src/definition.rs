//! Immutable definition-registry request and record values.

use std::error::Error;
use std::fmt;

use oxide_batch_core::{DefinitionArtifactId, DefinitionIdentity, DefinitionRevision, JobName};

/// One immutable registry binding between a canonical definition and executable artifact.
///
/// Executable paths, hosts, credentials, and deployment locations are deliberately absent.
/// The artifact identifier is application-owned, bounded, and restart-relevant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefinitionRegistration {
    job_name: JobName,
    definition: DefinitionIdentity,
    artifact: DefinitionArtifactId,
}

impl DefinitionRegistration {
    /// Creates one registry binding from a framework-produced named definition.
    ///
    /// # Errors
    ///
    /// Returns DefinitionRegistrationError::UnnamedDefinition for the legacy
    /// repository-only identity that predates named canonical manifests.
    pub fn new(
        definition: DefinitionIdentity,
        artifact: DefinitionArtifactId,
    ) -> Result<Self, DefinitionRegistrationError> {
        let job_name = definition
            .job_name()
            .cloned()
            .ok_or(DefinitionRegistrationError::UnnamedDefinition)?;
        Ok(Self {
            job_name,
            definition,
            artifact,
        })
    }

    /// Borrows the exact job name used to address this registration.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Borrows the canonical definition identity.
    #[must_use]
    pub const fn definition(&self) -> &DefinitionIdentity {
        &self.definition
    }

    /// Borrows the application-owned executable artifact identity.
    #[must_use]
    pub const fn artifact(&self) -> &DefinitionArtifactId {
        &self.artifact
    }

    /// Borrows the exact application-owned definition revision.
    #[must_use]
    pub const fn revision(&self) -> &DefinitionRevision {
        self.definition.revision()
    }
}

/// Failure to construct a public registry binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DefinitionRegistrationError {
    /// The legacy repository compatibility identity carries no job name.
    UnnamedDefinition,
}

impl fmt::Display for DefinitionRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnnamedDefinition => {
                formatter.write_str("definition registry entries require a named definition")
            }
        }
    }
}

impl Error for DefinitionRegistrationError {}
