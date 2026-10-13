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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use futures_executor::block_on;

    use super::DefinitionRegistry;
    use crate::{
        ComponentRevision, DefinitionArtifactId, DefinitionIdentity, DefinitionRegistration,
        DefinitionRegistrationError, DefinitionRevision, InMemoryJobRepository, JobName,
        RepositoryError, SequentialIdGenerator, StepName, SystemClock,
    };

    fn registry() -> DefinitionRegistry<InMemoryJobRepository> {
        DefinitionRegistry::new(InMemoryJobRepository::new(
            Arc::new(SystemClock),
            Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
        ))
    }

    fn registration(
        component: &str,
        artifact: &str,
    ) -> Result<DefinitionRegistration, Box<dyn Error>> {
        let job = JobName::new("registry-job")?;
        let definition = DefinitionIdentity::tasklet(
            &job,
            &StepName::new("only")?,
            DefinitionRevision::new("v1")?,
            &ComponentRevision::new(component)?,
        )?;
        Ok(DefinitionRegistration::new(
            definition,
            DefinitionArtifactId::new(artifact)?,
        )?)
    }

    #[test]
    fn exact_registration_is_idempotent_and_exactly_addressable() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let registry = registry();
            let registration = registration("component-v1", "artifact-v1")?;

            let first = registry.register(&registration).await?;
            let replay = registry.register(&registration).await?;
            assert_eq!(first, replay);

            let resolved = registry
                .resolve(registration.job_name(), registration.revision())
                .await?;
            assert_eq!(resolved.as_ref(), Some(&registration));

            let missing_revision = DefinitionRevision::new("v2")?;
            let missing = registry
                .resolve(registration.job_name(), &missing_revision)
                .await?;
            assert!(missing.is_none());
            Ok::<(), Box<dyn Error>>(())
        })?;
        Ok(())
    }

    #[test]
    fn artifact_rebinding_fails_closed() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let registry = registry();
            let first = registration("component-v1", "artifact-v1")?;
            registry.register(&first).await?;

            let conflicting = registration("component-v1", "artifact-v2")?;
            let result = registry.register(&conflicting).await;
            assert_eq!(
                result,
                Err(RepositoryError::DefinitionArtifactDrift {
                    job_name: first.job_name().clone(),
                    revision: first.revision().clone(),
                })
            );
            if let Err(error) = result {
                let diagnostic = error.to_string();
                assert!(!diagnostic.contains("artifact-v1"));
                assert!(!diagnostic.contains("artifact-v2"));
            }
            Ok::<(), Box<dyn Error>>(())
        })?;
        Ok(())
    }

    #[test]
    fn manifest_rebinding_fails_closed_before_artifact_comparison() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let registry = registry();
            let first = registration("component-v1", "artifact-v1")?;
            registry.register(&first).await?;

            let conflicting = registration("component-v2", "artifact-v1")?;
            assert_eq!(
                registry.register(&conflicting).await,
                Err(RepositoryError::DefinitionDrift {
                    job_name: first.job_name().clone(),
                    revision: first.revision().clone(),
                })
            );
            Ok::<(), Box<dyn Error>>(())
        })?;
        Ok(())
    }

    #[test]
    fn legacy_unnamed_identity_is_not_registry_addressable() -> Result<(), Box<dyn Error>> {
        let artifact = DefinitionArtifactId::new("legacy-artifact")?;
        assert_eq!(
            DefinitionRegistration::new(DefinitionIdentity::legacy(), artifact),
            Err(DefinitionRegistrationError::UnnamedDefinition)
        );
        Ok(())
    }
}
