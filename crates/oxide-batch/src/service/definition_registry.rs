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
    use std::num::NonZeroU64;
    use std::sync::Arc;

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
    ) -> Result<DefinitionRegistration, Box<dyn std::error::Error>> {
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
    fn exact_registration_is_idempotent_and_exactly_addressable() {
        futures_executor::block_on(async {
            let registry = registry();
            let registration = registration("component-v1", "artifact-v1").expect("registration");

            let first = registry.register(&registration).await.expect("register");
            let replay = registry.register(&registration).await.expect("replay");
            assert_eq!(first, replay);

            let resolved = registry
                .resolve(registration.job_name(), registration.revision())
                .await
                .expect("resolve");
            assert_eq!(resolved.as_ref(), Some(&registration));

            let missing = registry
                .resolve(
                    registration.job_name(),
                    &DefinitionRevision::new("v2").expect("revision"),
                )
                .await
                .expect("missing lookup");
            assert!(missing.is_none());
        });
    }

    #[test]
    fn artifact_rebinding_fails_closed() {
        futures_executor::block_on(async {
            let registry = registry();
            let first = registration("component-v1", "artifact-v1").expect("registration");
            registry.register(&first).await.expect("register");

            let conflicting = registration("component-v1", "artifact-v2").expect("registration");
            let error = registry
                .register(&conflicting)
                .await
                .expect_err("artifact drift");
            assert_eq!(
                error,
                RepositoryError::DefinitionArtifactDrift {
                    job_name: first.job_name().clone(),
                    revision: first.revision().clone(),
                }
            );
            let diagnostic = error.to_string();
            assert!(!diagnostic.contains("artifact-v1"));
            assert!(!diagnostic.contains("artifact-v2"));
        });
    }

    #[test]
    fn manifest_rebinding_fails_closed_before_artifact_comparison() {
        futures_executor::block_on(async {
            let registry = registry();
            let first = registration("component-v1", "artifact-v1").expect("registration");
            registry.register(&first).await.expect("register");

            let conflicting = registration("component-v2", "artifact-v1").expect("registration");
            let error = registry
                .register(&conflicting)
                .await
                .expect_err("definition drift");
            assert_eq!(
                error,
                RepositoryError::DefinitionDrift {
                    job_name: first.job_name().clone(),
                    revision: first.revision().clone(),
                }
            );
        });
    }

    #[test]
    fn legacy_unnamed_identity_is_not_registry_addressable() {
        let error = DefinitionRegistration::new(
            DefinitionIdentity::legacy(),
            DefinitionArtifactId::new("legacy-artifact").expect("artifact"),
        )
        .expect_err("legacy identity");
        assert_eq!(error, DefinitionRegistrationError::UnnamedDefinition);
    }
}
