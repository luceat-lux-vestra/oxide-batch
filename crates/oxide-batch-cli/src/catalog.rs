//! The host-supplied job definition catalog.
//!
//! A launch or a restart is guarded against the job's canonical
//! [`DefinitionIdentity`], which is derived from the live component revisions
//! of the application that owns the job. A standalone process that only reads
//! metadata cannot reconstruct one, and asserting a manifest digest from
//! configuration would let an operator claim an identity the application never
//! produced.
//!
//! A host application therefore embeds this crate and registers the same
//! definitions it launches in process. The shipped binary registers none, so it
//! serves every command that a repository alone can answer and reports a
//! deterministic rejection for the two that cannot be answered without the
//! application.
//!
//! This is not a definition registry: it resolves nothing, persists nothing,
//! and stores no component. It is the narrowest input `launch` and
//! `execution restart` require.

use std::collections::BTreeMap;
use std::fmt;

use oxide_batch::{DefinitionIdentity, JobName};

/// A rejected catalog registration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogError {
    /// The identity carries no job name, so it selects no job.
    AnonymousDefinition,
    /// The job name is already registered with a different identity.
    DuplicateJob {
        /// Conflicting job name.
        job_name: JobName,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AnonymousDefinition => {
                formatter.write_str("the definition identity carries no job name")
            }
            Self::DuplicateJob { job_name } => {
                write!(formatter, "job {job_name} is already registered")
            }
        }
    }
}

impl std::error::Error for CatalogError {}

/// The job definitions one embedding application authorizes for launch.
#[derive(Clone, Debug, Default)]
pub struct DefinitionCatalog {
    entries: BTreeMap<JobName, DefinitionIdentity>,
}

impl DefinitionCatalog {
    /// Builds an empty catalog.
    ///
    /// A CLI built on an empty catalog serves every command except `launch`
    /// and `execution restart`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Registers one job definition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::AnonymousDefinition`] when the identity carries
    /// no job name and [`CatalogError::DuplicateJob`] when the name is already
    /// registered.
    pub fn register(&mut self, identity: DefinitionIdentity) -> Result<(), CatalogError> {
        let job_name = identity
            .job_name()
            .cloned()
            .ok_or(CatalogError::AnonymousDefinition)?;
        if self.entries.contains_key(&job_name) {
            return Err(CatalogError::DuplicateJob { job_name });
        }
        self.entries.insert(job_name, identity);
        Ok(())
    }

    /// Registers one job definition and returns the catalog.
    ///
    /// # Errors
    ///
    /// Returns the same rejections as [`DefinitionCatalog::register`].
    pub fn with(mut self, identity: DefinitionIdentity) -> Result<Self, CatalogError> {
        self.register(identity)?;
        Ok(self)
    }

    /// Returns the registered identity of one job name.
    #[must_use]
    pub fn get(&self, job_name: &JobName) -> Option<&DefinitionIdentity> {
        self.entries.get(job_name)
    }

    /// Returns whether the catalog registers no definition.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of registered definitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterates registered job names in canonical order.
    #[must_use]
    pub fn job_names(&self) -> impl ExactSizeIterator<Item = &JobName> {
        self.entries.keys()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use oxide_batch::{
        ComponentRevision, DefinitionIdentity, DefinitionRevision, JobName, StepName,
    };

    use super::{CatalogError, DefinitionCatalog};

    fn identity(job: &str) -> DefinitionIdentity {
        let job_name = JobName::new(job).expect("the job name is valid");
        let step_name = StepName::new("only").expect("the step name is valid");
        let revision = DefinitionRevision::new("r1").expect("the revision is valid");
        let component = ComponentRevision::new("c1").expect("the component revision is valid");
        DefinitionIdentity::tasklet(&job_name, &step_name, revision, &component)
            .expect("the manifest encodes")
    }

    #[test]
    fn an_empty_catalog_registers_nothing() {
        let catalog = DefinitionCatalog::new();
        assert!(catalog.is_empty());
        assert_eq!(catalog.len(), 0);
    }

    #[test]
    fn a_registered_job_resolves_by_name() {
        let catalog = DefinitionCatalog::new()
            .with(identity("orders"))
            .expect("the registration succeeds");
        let job_name = JobName::new("orders").expect("the job name is valid");
        assert!(catalog.get(&job_name).is_some());
    }

    #[test]
    fn a_duplicate_job_name_is_rejected() {
        let mut catalog = DefinitionCatalog::new();
        catalog
            .register(identity("orders"))
            .expect("the first registration succeeds");
        let error = catalog
            .register(identity("orders"))
            .expect_err("the second registration is a duplicate");
        assert!(matches!(error, CatalogError::DuplicateJob { .. }));
    }
}
