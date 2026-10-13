//! Portable immutable definition-registry service.

use crate::{
    DefinitionRegistration, DefinitionRevision, JobName, JobRepository, RepositoryCapability,
    RepositoryError,
};

/// Public facade for exact, bounded definition registration and lookup.
///
/// Registration is immutable and compare-and-swap at the repository boundary.
/// Resolution requires the exact job name and revision; the service never
/// scans revisions or infers compatibility.
#[derive(Clone, Debug)]
pub struct DefinitionRegistry<R> {
    repository: R,
}

impl<R: JobRepository> DefinitionRegistry<R> {
    /// Binds one repository to the registry facade.
    pub const fn new(repository: R) -> Self {
        Self { repository }
    }

    /// Borrows the underlying repository.
    #[must_use]
    pub const fn repository(&self) -> &R {
        &self.repository
    }

    /// Registers one immutable definition/artifact binding.
    ///
    /// Exact replay returns the durable record. A reused revision with a
    /// different manifest or artifact identity fails closed.
    ///
    /// # Errors
    ///
    /// Returns a typed capability, drift, concurrency, or repository failure.
    pub async fn register(
        &self,
        registration: &DefinitionRegistration,
    ) -> Result<DefinitionRegistration, RepositoryError> {
        self.repository
            .descriptor()
            .require(RepositoryCapability::DefinitionRegistry)?;
        let mut unit = self.repository.begin().await?;
        let registered = unit.register_definition(registration).await?;
        unit.commit().await?;
        Ok(registered)
    }

    /// Resolves one exact registered definition revision.
    ///
    /// # Errors
    ///
    /// Returns a typed capability or repository failure. An absent exact
    /// revision is Ok(None); no fallback or revision-order lookup occurs.
    pub async fn resolve(
        &self,
        job_name: &JobName,
        revision: &DefinitionRevision,
    ) -> Result<Option<DefinitionRegistration>, RepositoryError> {
        self.repository
            .descriptor()
            .require(RepositoryCapability::DefinitionRegistry)?;
        let mut unit = self.repository.begin().await?;
        let registered = unit.definition(job_name, revision).await?;
        unit.rollback().await?;
        Ok(registered)
    }
}
