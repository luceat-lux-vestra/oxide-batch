//! The `PostgreSQL` repository backend.
//!
//! This module is the only place a resolved [`Secret`] is exposed, and it
//! exposes it solely to construct a connection. A connection string, host name,
//! user name, or certificate never leaves this module.

use std::sync::Arc;

use oxide_batch::{
    BoxFuture, CaCertificate, Clock, JobExplorer, JobOperator, PostgresConfig, PostgresExplorer,
    PostgresJobRepository, PostgresMigrator, RepositoryError, RetentionService, SystemClock,
    TlsMode,
};

use crate::config::{Configuration, TlsSetting};
use crate::exit::ExitCategory;
use crate::output::Diagnostic;
use crate::run::{SchemaReport, SchemaState, Services};

/// The services one `PostgreSQL` deployment provides.
pub type PostgresServices = Services<PostgresJobRepository, PostgresExplorer>;

/// A failure to open the repository.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendFailure {
    category: ExitCategory,
    diagnostic: Diagnostic,
}

impl BackendFailure {
    /// Returns the exit category this failure reports.
    #[must_use]
    pub const fn category(&self) -> ExitCategory {
        self.category
    }

    /// Borrows the redacted diagnostic.
    #[must_use]
    pub const fn diagnostic(&self) -> &Diagnostic {
        &self.diagnostic
    }
}

/// Reports the durable schema version of one `PostgreSQL` deployment.
struct PostgresSchema {
    config: PostgresConfig,
}

impl SchemaReport for PostgresSchema {
    fn schema_state(&self) -> BoxFuture<'_, Result<SchemaState, RepositoryError>> {
        Box::pin(async move {
            let installed = PostgresMigrator::installed_schema_version(&self.config).await?;
            Ok(SchemaState {
                installed,
                supported: PostgresMigrator::supported_schema_version(),
            })
        })
    }
}

/// Builds the connection configuration from resolved settings.
///
/// # Errors
///
/// Returns a redacted failure when no connection string was supplied or when a
/// bounded value is outside the adapter's accepted range.
pub fn connection_config(config: &Configuration) -> Result<PostgresConfig, BackendFailure> {
    let url = config.repository_url().ok_or_else(|| BackendFailure {
        category: ExitCategory::ConfigurationInvalid,
        diagnostic: Diagnostic::new(
            "REPOSITORY_URL_MISSING",
            "no repository connection was configured; set OXIDE_BATCH_REPOSITORY_URL \
             or repository.url",
        ),
    })?;
    let invalid = |detail: &'static str| BackendFailure {
        category: ExitCategory::ConfigurationInvalid,
        diagnostic: Diagnostic::new("REPOSITORY_CONFIG_INVALID", detail),
    };
    let mut built = PostgresConfig::new(url.value().expose())
        .map_err(|_| invalid("the repository connection string is not accepted"))?;
    built = built
        .with_pool_size(config.pool_size())
        .map_err(|_| invalid("the repository pool size is outside its accepted range"))?
        .with_connect_timeout(config.connect_timeout())
        .map_err(|_| invalid("the connect timeout is outside its accepted range"))?
        .with_statement_timeout(config.statement_timeout())
        .map_err(|_| invalid("the statement timeout is outside its accepted range"))?;
    let tls = match config.tls_mode() {
        TlsSetting::Plaintext => TlsMode::Plaintext,
        TlsSetting::VerifyFull => {
            let ca_certificate = match config.ca_certificate() {
                None => None,
                Some(pem) => Some(
                    CaCertificate::new(pem.value().expose().as_bytes().to_vec())
                        .map_err(|_| invalid("the certificate authority bundle is not accepted"))?,
                ),
            };
            TlsMode::VerifyFull { ca_certificate }
        }
    };
    Ok(built.with_tls_mode(tls))
}

/// Opens the repository and binds the portable services.
///
/// # Errors
///
/// Returns a redacted failure when the configuration is not accepted or the
/// repository is unavailable.
pub async fn connect(config: &Configuration) -> Result<PostgresServices, BackendFailure> {
    let connection = connection_config(config)?;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let repository = PostgresJobRepository::connect(connection.clone(), clock.clone())
        .await
        .map_err(|error| BackendFailure {
            category: crate::failure::repository(&error),
            diagnostic: crate::failure::repository_diagnostic(&error),
        })?;
    let explorer = JobExplorer::new(PostgresExplorer::new(repository.clone()));
    let operator = JobOperator::new(repository.clone(), clock.clone());
    let retention = RetentionService::new(repository, clock);
    Ok(Services::new(
        operator,
        retention,
        explorer,
        Box::new(PostgresSchema { config: connection }),
    ))
}
